# AI-Driven Network Configuration and Automation Using OSPF — Short Report

**Student:** _name_ · **ID:** _student ID_ · **Date:** _date_

## 1. Objective

Build a web system that takes a network diagram image, uses an image-capable AI model to extract the topology and
addressing plan, generates Cisco IOS OSPFv2 configurations, and — after validation and explicit administrator
approval — deploys them to two Cisco routers through Apache Kafka, a Python worker and Paramiko SSH, so that PC1
(10.0.0.10) can ping PC2 (10.2.0.10) over OSPF area 0 without manual router configuration.

## 2. Lab environment

| Item | Setup |
|---|---|
| Emulation | GNS3 2.2 on Windows with VMware Workstation Pro |
| Routers | 2 × Cisco c7200 (Dynamips), IOS 15.2(4)S5 |
| Wiring | f0/0 management (DHCP, via switch and cloud to a VMware host-only network) · f1/0 R1–R2 10.1.0.0/24 · f2/0 LAN |
| Hosts | VPCS: PC1 10.0.0.10/24 gw 10.0.0.1 · PC2 10.2.0.10/24 gw 10.2.0.2 |
| Automation host | Ubuntu VM with a NIC on the routers' management network; Docker (Kafka), Python 3 virtual environment |
| AI | Groq API (OpenAI-compatible), image-capable model `qwen/qwen3.8-27b`, JSON output, temperature 0 |

Management SSH (f0/0) is independent of the data plane and is never modified by the generated configuration.

## 3. Architecture

```
Browser ─▶ FastAPI backend ─▶ Kafka (network-config-jobs) ─▶ Python worker ─▶ Paramiko SSH ─▶ R1 / R2
  ▲             │  plans + jobs (JSON)                           │ claim / progress
  └── job monitor ◀──────────────────────────────────────────────┘
```

1. **Frontend** (`app/topology.html`): upload and preview, Generate, a review-and-resolve editor, Review commands,
   Validate, Approve, Deploy, and live job status; job monitor at `/jobs`.
2. **AI analysis** (`POST /api/analyze`): the image goes to the vision model with a strict JSON schema (routers,
   interfaces with roles, links, PCs, OSPF settings, ambiguities). Addresses come only from the image; the model is
   told to flag missing, unreadable or contradictory information rather than guess. The administrator resolves
   flagged items in the editor (with a note), and can read the routers' real interface names
   (`show ip interface brief`, read-only).
3. **Configuration generation** (`app/ios.py`, `app/templates/router_ospf.j2`): validated JSON is rendered through a
   Jinja2 template: interface addresses, `router ospf 1`, router ID, `network … area 0` for each connected /24,
   `passive-interface` on LAN interfaces, transit interface active. No static routes.
4. **Validation and approval**: static checks (duplicate addresses, subnet consistency on every link, PC gateways,
   router IDs, at least one transit link) and live checks over SSH (hostname, privilege 15, interfaces exist,
   management interface not reused). Approval requires an explicit confirmation and stores the approved commands
   with their SHA-256 hash.
5. **Kafka producer**: Deploy creates a job record (PENDING) and publishes `{job_id, plan_id, device_ids,
   config_sha256, approved_config_ref}` to `network-config-jobs`. The message carries no configuration text or
   credentials.
6. **Worker** (`worker.py`): consumes with manual offset commits. For each message it calls the backend's claim
   endpoint, which re-checks approval and the hash and changes PENDING to RUNNING exactly once. Redelivered or
   duplicate messages are rejected, which prevents duplicate execution.
7. **Paramiko deployment**: per router, the worker logs in with credentials from the environment, checks that the
   hostname matches the device ID, saves the running-config, applies the approved commands, and records the output.
   Authentication, timeout and IOS command errors are reported as FAILED with a readable message.
8. **Automated verification**: polls `show ip ospf neighbor` and `show ip route ospf` (up to 120 s), then
   `show ip protocols` and `show ip route static`. A job succeeds only if the neighbor is FULL, every remote network
   appears as an `O` route, the OSPF process and router ID are correct, and there are no static routes. It also pings
   the far-side PC from each router's LAN interface.
9. **Job monitor**: PENDING / RUNNING / SUCCESS / FAILED with routers, created/started/finished times, deployment
   output and each verification check.

**Safety and recovery.** The AI never connects to routers; only approved, hash-matched jobs are executed; the
management interface and SSH settings are excluded from generated commands and protected by validation; secrets live
only in `.env`. After a failure the job shows the error and router output, the plan can be redeployed as a new job,
and `worker.py --rollback` removes OSPF and resets the data-plane interfaces while keeping management access.

## 4. Generated configuration (R1, exam diagram)

```
interface FastEthernet2/0
 ip address 10.0.0.1 255.255.255.0
 no shutdown
interface FastEthernet1/0
 ip address 10.1.0.1 255.255.255.0
 no shutdown
router ospf 1
 router-id 1.1.1.1
 passive-interface FastEthernet2/0
 no passive-interface FastEthernet1/0
 network 10.0.0.0 0.0.0.255 area 0
 network 10.1.0.0 0.0.0.255 area 0
```
R2 is symmetrical (10.2.0.2/24 on f2/0, 10.1.0.2/24 on f1/0, router ID 2.2.2.2).

## 5. Test results

| # | Test | Expected | Result | Evidence |
|---|---|---|---|---|
| 1 | Upload exam diagram | Preview, Generate available | Pass | 03 |
| 2 | AI analysis | Correct devices, addresses, links, OSPF plan; no false ambiguities | Pass — all addresses and router IDs correct, no ambiguities | 04 |
| 3 | Interface discovery | Real names; management interface identified | Pass — f0/0 marked SSH management | 05 |
| 4 | Generated IOS | Per-router interfaces, OSPF 1 area 0, passive LAN | Pass — matches §4 exactly | 06 |
| 5 | Validation | Static and live checks pass | Pass | 07 |
| 6 | Approval + Kafka | Job PENDING; message with IDs + hash only | Pass | 08–10 |
| 7 | Worker + Paramiko | Both routers configured, output collected | Pass — PENDING → RUNNING → SUCCESS in about 1 minute (_fill in from screenshot 12_) | 11–13 |
| 8 | OSPF verification | FULL; remote LAN via OSPF; no static routes | Pass — R1: 2.2.2.2 FULL, `O 10.2.0.0/24`; R2: 1.1.1.1 FULL, `O 10.0.0.0/24` | 14–17 |
| 9 | End-to-end | PC1 pings 10.2.0.10 | Pass — 4/4 replies, TTL 62 (two router hops) | 18 |
| 10 | Duplicate Kafka message | Not executed twice | Pass — rejected at claim ("Job is SUCCESS") | 19 |
| 11 | Alternative diagram (other addressing, conflicting interface labels, no PCs) | Conflict flagged, not guessed; resolvable | Pass — flagged, resolved in the editor, deployed and verified (FULL, remote LAN via OSPF, LAN-to-LAN ping 100%) | 20–21 |
| 12 | Management interface chosen for a LAN | Blocked | Pass — editor marks ✘ and validation rejects it | 22 |
| 13 | Rollback | Data plane reset, SSH kept | Pass — f1/0 and f2/0 reset, f0/0 still up | — |

## 6. Problems encountered and solutions

| Problem | Solution |
|---|---|
| Paramiko 5 could not connect: IOS 15.2 offers only SHA-1 key exchange, `ssh-rsa` and CBC ciphers | Pinned Paramiko 3.5.1 and enabled exactly those algorithms in a custom transport for router sessions only |
| Automation VM could not reach the routers' management network | Added a second VM network adapter on the same VMware network as the GNS3 cloud |
| Kafka container failed with a permission error on its data directory | Moved the log directory to the image's pre-owned `/var/lib/kafka/data`; topic created explicitly |
| AI sometimes reported style observations as ambiguities, blocking approval | Tightened the prompt (only missing/unreadable/contradictory data; PCs optional) and set temperature 0 |
| Flagged items could not be fixed in the UI | Added the review-and-resolve editor with interface discovery and recorded resolutions |
| Free-tier AI rate limit (≈1 analysis per minute) | Documented; the UI shows the error and the admin retries after a short wait |

## 7. Conclusion

The system meets all ten tasks: the diagram is analysed by AI into a validated plan, configurations are generated
from a template, reviewed and explicitly approved, delivered as Kafka jobs, executed once by the Python worker over
Paramiko, and verified automatically. OSPF reaches FULL adjacency, each router learns the remote LAN via OSPF, and
PC1 reaches PC2. The design also handles an alternative topology, flags conflicts instead of guessing, protects
management access, and provides failure reporting and rollback.
