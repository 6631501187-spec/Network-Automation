"""Pre-demo check: reach R1/R2 over SSH and trust their host keys.

Run from the project folder on the Ubuntu VM:
    .venv/bin/python lab/check_routers.py

Management addresses come from DHCP, so they can change after a lab restart.
Update R1_HOST/R2_HOST in .env first, then run this. It records the routers'
SSH host keys in ~/.ssh/known_hosts (trust on first use) and logs in read-only.
"""

import os
import socket
import sys
from pathlib import Path

import paramiko
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
load_dotenv(BASE_DIR / ".env")

from app.router_ssh import LegacyCiscoTransport, connect_router, send_command  # noqa: E402

KNOWN_HOSTS = Path.home() / ".ssh" / "known_hosts"


def main():
    KNOWN_HOSTS.parent.mkdir(mode=0o700, exist_ok=True)
    KNOWN_HOSTS.touch(mode=0o600, exist_ok=True)
    keys = paramiko.HostKeys(str(KNOWN_HOSTS))
    ok = True

    for router in ("R1", "R2"):
        host = os.getenv(f"{router}_HOST", "").strip()
        if not host:
            print(f"{router}: set {router}_HOST in .env")
            ok = False
            continue
        try:
            if keys.lookup(host) is None:
                with socket.create_connection((host, 22), timeout=8) as sock:
                    transport = LegacyCiscoTransport(sock)
                    transport.start_client(timeout=8)
                    key = transport.get_remote_server_key()
                    transport.close()
                keys.add(host, key.get_name(), key)
                keys.save(str(KNOWN_HOSTS))
                print(f"{router}: trusted new host key for {host}")

            client, channel, _, hostname = connect_router(router)
            brief = send_command(channel, "show ip interface brief")
            channel.close()
            client.close()
            status = "OK" if hostname == router else f"WRONG HOSTNAME {hostname!r}"
            print(f"{router} {host}: SSH {status}")
            for line in brief.splitlines():
                if line.startswith(("FastEthernet", "GigabitEthernet", "Ethernet")):
                    print("   ", line.rstrip())
            ok = ok and hostname == router
        except Exception as exc:
            print(f"{router} {host}: FAILED {type(exc).__name__}: {exc}")
            ok = False

    print("All routers ready." if ok else "Fix the problems above before the demo.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
