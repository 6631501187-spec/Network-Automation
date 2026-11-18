"""Python worker: consume approved jobs from Kafka and deploy them with Paramiko.

Flow per Kafka message (topic network-config-jobs):
  1. Parse the message (job ID, plan ID, device IDs, config SHA-256 only).
  2. Claim the job from the backend: the backend re-checks approval and the hash
     and moves PENDING -> RUNNING exactly once (duplicate deliveries get 409).
  3. For each router: SSH, confirm the hostname matches the device ID, save the
     running-config as a backup, push the approved commands, collect output.
  4. Verification stage: wait for OSPF FULL, OSPF routes to every remote network,
     the expected OSPF process/router ID, and no static routes (DHCP S* allowed).
  5. Report SUCCESS/FAILED to the backend, then commit the Kafka offset.

Usage:
  python worker.py                       # run the consumer
  python worker.py --rollback <plan_id|latest>  # recovery: reset data-plane interfaces + OSPF
"""

import argparse
import fcntl
import ipaddress
import json
import os
import re
import socket
import sys
import time
from pathlib import Path
from uuid import UUID

import requests
from confluent_kafka import Consumer
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

from app.ios import config_digest, rollback_commands  # noqa: E402
from app.router_ssh import connect_router, send_command  # noqa: E402

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
TOPIC = os.getenv("KAFKA_TOPIC", "network-config-jobs")
BACKEND = os.getenv("BACKEND_URL", "http://127.0.0.1:8000").rstrip("/")
VERIFY_TIMEOUT = int(os.getenv("OSPF_VERIFY_TIMEOUT", "120"))
WORKER_ID = f"worker@{socket.gethostname()}:{os.getpid()}"


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


# --------------------------------------------------
# Backend API
# --------------------------------------------------

def backend_post(path, payload, retries=12):
    """POST to the backend, retrying while it is unreachable."""
    for attempt in range(retries):
        try:
            return requests.post(f"{BACKEND}{path}", json=payload, timeout=15)
        except requests.RequestException as exc:
            log(f"Backend unreachable ({exc.__class__.__name__}); retry {attempt + 1}/{retries}")
            time.sleep(5)
    raise RuntimeError(f"Backend {BACKEND} is unreachable.")


def report(job_id, status, stage, **fields):
    response = backend_post(
        f"/api/jobs/{job_id}/progress",
        {"status": status, "stage": stage, **fields},
    )
    if not response.ok:
        log(f"Progress report rejected ({response.status_code}): {response.text[:300]}")
    log(f"{status}: {stage}")


# --------------------------------------------------
# Router operations
# --------------------------------------------------

def deploy_device(device_id, commands):
    client, channel, host, hostname = connect_router(device_id)
    try:
        if hostname != device_id:
            raise RuntimeError(
                f"{host} reports hostname {hostname!r}, expected {device_id}; refusing to deploy."
            )
        backup = send_command(channel, "show running-config", timeout=30)
        outputs = [send_command(channel, command) for command in commands]
        return {
            "host": host,
            "hostname": hostname,
            "commands": commands,
            "backup_running_config": backup[-8000:],
            "configuration_output": "".join(outputs)[-6000:],
        }
    finally:
        channel.close()
        client.close()


def has_ospf_route(route_output, network):
    address = re.escape(str(network.network_address))
    for line in route_output.splitlines():
        text = line.strip()
        if text.startswith("O") and re.search(rf"(?<![\d.]){address}(?:/\d+)?(?![\d.])", text):
            return True
    return False


def static_routes(route_output):
    # "S*" is the DHCP default route used only for management; any other S is a violation.
    return [line.strip() for line in route_output.splitlines() if re.match(r"^S\s", line)]


def verify_device(device_id, router, remote_networks, process_id, remote_hosts):
    client, channel, host, _ = connect_router(device_id)
    expected_rid = router.get("router_id")
    result = {"host": host}
    try:
        deadline = time.monotonic() + VERIFY_TIMEOUT
        while True:
            neighbor = send_command(channel, "show ip ospf neighbor")
            routes = send_command(channel, "show ip route ospf")
            full = bool(re.search(r"\bFULL\b", neighbor))
            missing = [str(net) for net in remote_networks if not has_ospf_route(routes, net)]
            if (full and not missing) or time.monotonic() >= deadline:
                break
            time.sleep(3)

        protocols = send_command(channel, "show ip protocols")
        statics = send_command(channel, "show ip route static")
        bad_statics = static_routes(statics)
        process_ok = f'Routing Protocol is "ospf {process_id}"' in protocols
        rid_ok = not expected_rid or f"Router ID {expected_rid}" in protocols

        pings = {}
        lan = next((i for i in router.get("interfaces", []) if i.get("role") == "lan"), None)
        for name, address in remote_hosts.items():
            command = f"ping {address} repeat 3 timeout 1"
            if lan:
                command += f" source {lan['name']}"
            output = send_command(channel, command, timeout=20)
            match = re.search(r"Success rate is (\d+) percent", output)
            pings[name] = {"target": address, "success_percent": int(match.group(1)) if match else 0}

        result.update(
            neighbor_full=full,
            remote_networks_expected=[str(net) for net in remote_networks],
            remote_networks_missing=missing,
            ospf_process_ok=process_ok,
            router_id_ok=rid_ok,
            static_routes_found=bad_statics,
            ping_remote_pcs=pings,
            show_ip_ospf_neighbor=neighbor[-3000:],
            show_ip_route_ospf=routes[-3000:],
            show_ip_protocols=protocols[-3000:],
            show_ip_route_static=statics[-2000:],
        )

        problems = []
        if not full:
            problems.append("no OSPF neighbor in FULL state")
        if missing:
            problems.append(f"missing OSPF routes to {', '.join(missing)}")
        if not process_ok:
            problems.append(f"OSPF process {process_id} not running")
        if not rid_ok:
            problems.append(f"router ID is not {expected_rid}")
        if bad_statics:
            problems.append("static routes present")
        result["passed"] = not problems
        result["problems"] = problems
        return result
    finally:
        channel.close()
        client.close()


def addressed_networks(router):
    return {
        ipaddress.ip_interface(i["ipv4_cidr"]).network
        for i in router.get("interfaces", [])
        if i.get("role") in {"lan", "transit"} and i.get("ipv4_cidr")
    }


# --------------------------------------------------
# Job processing
# --------------------------------------------------

def process_job(claim):
    job_id = claim["job_id"]
    commands = claim["commands"]
    topology = claim.get("topology", {})
    devices = claim["devices"]
    routers = {r.get("name"): r for r in topology.get("routers", [])}
    process_id = topology.get("ospf_process_id", 1)

    router_results = {}
    verification = {}
    try:
        if config_digest(commands) != claim["config_sha256"]:
            raise RuntimeError("Approved configuration hash mismatch; nothing was deployed.")
        if set(devices) != set(commands) or not set(devices) <= set(routers):
            raise RuntimeError("Job devices do not match the approved configuration.")

        for device in devices:
            report(job_id, "RUNNING", f"deploying {device}", device=device)
            router_results[device] = deploy_device(device, commands[device])
            report(job_id, "RUNNING", f"configured {device}", device=device,
                   router_results=router_results)

        all_networks = set().union(*(addressed_networks(r) for r in routers.values()))
        pcs = {pc["name"]: pc for pc in topology.get("pcs", []) if pc.get("ipv4_cidr")}
        for device in devices:
            report(job_id, "RUNNING", f"verifying OSPF on {device}", device=device)
            own = addressed_networks(routers[device])
            remote_hosts = {
                name: str(ipaddress.ip_interface(pc["ipv4_cidr"]).ip)
                for name, pc in pcs.items()
                if ipaddress.ip_interface(pc["ipv4_cidr"]).network not in own
            }
            verification[device] = verify_device(
                device, routers[device], sorted(all_networks - own, key=str),
                process_id, remote_hosts,
            )
            report(job_id, "RUNNING", f"verified {device}", device=device,
                   verification=verification)

        failed = {d: v["problems"] for d, v in verification.items() if not v["passed"]}
        if failed:
            raise RuntimeError(f"OSPF verification failed: {json.dumps(failed)}")

        report(job_id, "SUCCESS",
               "deployed and verified: OSPF FULL, remote LANs learned via OSPF, no static routes",
               router_results=router_results, verification=verification)
    except Exception as exc:
        report(job_id, "FAILED", "deployment or verification failed",
               error=f"{type(exc).__name__}: {exc}",
               router_results=router_results, verification=verification)


def parse_message(raw):
    job = json.loads(raw.decode("utf-8"))
    job_id = str(UUID(job["job_id"]))
    plan_id = str(UUID(job["plan_id"]))
    sha = job["config_sha256"]
    if not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ValueError("config_sha256 is not a SHA-256 hex digest")
    if not isinstance(job.get("device_ids"), list) or not job["device_ids"]:
        raise ValueError("device_ids is missing")
    return job_id, plan_id, sha, job["device_ids"]


def run_consumer():
    os.chdir(BASE_DIR)
    Path("data").mkdir(exist_ok=True)
    lock = open("data/worker.lock", "a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("Another worker is already running.")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "network-config-worker",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 900000,
    })
    consumer.subscribe([TOPIC])
    log(f"{WORKER_ID} waiting for jobs on {TOPIC} ({BOOTSTRAP}); backend {BACKEND}")

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                log(f"Kafka error: {message.error()}")
                continue

            try:
                job_id, plan_id, sha, device_ids = parse_message(message.value())
            except (ValueError, KeyError, TypeError, UnicodeDecodeError) as exc:
                log(f"Skipping malformed message at offset {message.offset()}: {exc}")
                consumer.commit(message=message, asynchronous=False)
                continue

            log(f"Received job {job_id} (plan {plan_id}, devices {device_ids})")
            response = backend_post(
                f"/api/jobs/{job_id}/claim",
                {"config_sha256": sha, "worker_id": WORKER_ID},
            )
            if response.status_code in (404, 409):
                log(f"Not executing job {job_id}: {response.json().get('detail')}")
                consumer.commit(message=message, asynchronous=False)
                continue
            response.raise_for_status()

            process_job(response.json())
            consumer.commit(message=message, asynchronous=False)
            log("Kafka offset committed. Waiting for the next job...")
    except KeyboardInterrupt:
        log("Worker stopped.")
    finally:
        consumer.close()
        lock.close()


def run_rollback(plan_id):
    """Recovery: remove OSPF and reset the plan's data-plane interfaces (management untouched)."""
    plans_dir = BASE_DIR / "data" / "plans"
    if plan_id == "latest":
        deployed = [
            (p.get("deployment_finished_at", ""), p)
            for p in (json.loads(f.read_text()) for f in plans_dir.glob("*.json"))
            if p.get("deployment_status") in {"SUCCESS", "FAILED", "RUNNING"}
        ]
        if not deployed:
            raise SystemExit("No deployed plan found.")
        plan = max(deployed, key=lambda item: item[0])[1]
        log(f"Rolling back plan {plan['plan_id']}")
    else:
        plan = json.loads((plans_dir / f"{UUID(plan_id)}.json").read_text())
    topology = plan["topology"]
    for router in topology.get("routers", []):
        commands = rollback_commands(router, topology.get("ospf_process_id", 1))
        client, channel, host, hostname = connect_router(router["name"])
        try:
            if hostname != router["name"]:
                raise RuntimeError(f"{host} hostname {hostname!r} != {router['name']}")
            for command in commands:
                send_command(channel, command)
            log(f"{router['name']} ({host}): data-plane interfaces and OSPF reset")
        finally:
            channel.close()
            client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rollback", metavar="PLAN_ID",
                        help="Reset OSPF and the plan's LAN/transit interfaces, then exit "
                             "('latest' = most recently deployed plan).")
    args = parser.parse_args()
    if args.rollback:
        run_rollback(args.rollback)
    else:
        run_consumer()


if __name__ == "__main__":
    sys.exit(main())
