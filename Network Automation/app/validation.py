"""Read-only validation before configuration approval."""

import ipaddress
import re

from app.ios import commands_for_router
from app.router_ssh import connect_router, send_command


def validate_topology(topology):
    errors = list(topology.get("ambiguities", []))
    checks = []
    commands = {}
    endpoints = {}
    used_addresses = set()
    router_ids = set()
    routers = topology.get("routers", [])

    try:
        names = [router["name"] for router in routers]
        if len(names) != 2 or set(names) != {"R1", "R2"}:
            raise ValueError("This worker supports exactly R1 and R2.")

        for router in routers:
            name = router["name"]
            commands[name] = commands_for_router(
                router,
                topology.get("ospf_process_id"),
                topology.get("ospf_area"),
            )

            router_id = router.get("router_id")
            if not router_id:
                raise ValueError(f"{name}: specify a router ID.")
            if router_id in router_ids:
                raise ValueError("Router IDs must be unique.")
            router_ids.add(router_id)

            for interface in router["interfaces"]:
                if interface["role"] == "management":
                    continue

                address = ipaddress.IPv4Interface(interface["ipv4_cidr"])
                if address.ip in used_addresses:
                    raise ValueError(f"Duplicate address: {address.ip}")
                used_addresses.add(address.ip)
                endpoints[(name, interface["name"])] = {
                    "address": address,
                    "role": interface["role"],
                }

        # PCs are configured by hand and are optional: an unaddressed PC is not checked.
        pc_names = set()
        unaddressed_pcs = set()
        for pc in topology.get("pcs", []):
            if pc["name"] in pc_names or pc["name"] in names:
                raise ValueError("Device names must be unique.")
            pc_names.add(pc["name"])

            if not pc.get("ipv4_cidr"):
                unaddressed_pcs.add(pc["name"])
                continue

            address = ipaddress.IPv4Interface(pc["ipv4_cidr"])
            gateway = ipaddress.IPv4Address(pc["gateway"]) if pc.get("gateway") else None
            if address.ip in used_addresses:
                raise ValueError(f"Duplicate address: {address.ip}")
            used_addresses.add(address.ip)

            if gateway is not None and (gateway not in address.network or gateway == address.ip):
                raise ValueError(f"{pc['name']}: invalid gateway.")
            if address.network.prefixlen < 31 and address.ip in {
                address.network.network_address,
                address.network.broadcast_address,
            }:
                raise ValueError(f"{pc['name']}: unusable host address.")

            endpoints[(pc["name"], "e0")] = {
                "address": address,
                "role": "pc",
                "gateway": gateway,
            }

        linked = set()
        networks = set()
        for link in topology.get("links", []):
            # PCs have a single port; accept any/missing port label for them.
            a = (link["device_a"], "e0" if link["device_a"] in pc_names else link.get("interface_a"))
            b = (link["device_b"], "e0" if link["device_b"] in pc_names else link.get("interface_b"))
            left, right = endpoints.get(a), endpoints.get(b)

            if left is None and right is None:
                continue
            # A router LAN facing an unaddressed PC is fine; the PC is set up by hand.
            if left is None and a[0] in unaddressed_pcs and right["role"] == "lan":
                linked.add(b)
                continue
            if right is None and b[0] in unaddressed_pcs and left["role"] == "lan":
                linked.add(a)
                continue
            if left is None or right is None:
                raise ValueError(
                    f"Addressed link {a} -- {b} has an unresolved endpoint."
                )
            if a in linked or b in linked:
                raise ValueError("An addressed interface appears in multiple links.")
            linked.update((a, b))

            if left["address"].network != right["address"].network:
                raise ValueError(f"Subnet mismatch on {a} -- {b}.")

            roles = {left["role"], right["role"]}
            if roles == {"pc", "lan"}:
                pc = left if left["role"] == "pc" else right
                router = right if left["role"] == "pc" else left
                if pc["gateway"] is not None and pc["gateway"] != router["address"].ip:
                    raise ValueError(f"PC gateway does not match its router on {a} -- {b}.")
            elif left["role"] != "transit" or right["role"] != "transit":
                raise ValueError(f"Unexpected interface roles on {a} -- {b}.")

            network = left["address"].network
            if any(network.overlaps(other) for other in networks):
                raise ValueError("Separate addressed links have overlapping subnets.")
            networks.add(network)

        # A router LAN may have no drawn PC (stub network); everything else must be linked.
        missing = {
            key for key in set(endpoints) - linked if endpoints[key]["role"] != "lan"
        }
        if missing:
            raise ValueError(f"Addressed interfaces without a valid link: {sorted(missing)}")

        transit = [e for e in endpoints.values() if e["role"] == "transit"]
        if not transit:
            raise ValueError("No transit (router-to-router) interface; OSPF adjacency is impossible.")

        checks.append("Addresses, gateways, linked subnets, and router IDs checked.")
        if unaddressed_pcs:
            checks.append(
                "PC addressing not in the diagram (configured manually): "
                + ", ".join(sorted(unaddressed_pcs))
            )
    except (ValueError, TypeError, KeyError) as exc:
        errors.append(str(exc))

    # Only contact routers after the static checks succeed.
    if not errors:
        for router in routers:
            name = router["name"]
            client = channel = None
            try:
                client, channel, host, hostname = connect_router(name)
                if hostname != name:
                    raise ValueError(
                        f"{host} reports hostname {hostname!r}, expected {name}."
                    )
                output = send_command(channel, "show ip interface brief")

                actual = {}
                for line in output.splitlines():
                    fields = line.split()
                    if len(fields) >= 6 and re.fullmatch(
                        r"(?:FastEthernet|GigabitEthernet|Ethernet|Serial)"
                        r"\d+(?:/\d+){0,2}(?:\.\d+)?",
                        fields[0],
                    ):
                        actual[fields[0]] = fields[1]

                # SSH hosts in this lab are numeric management IPv4 addresses.
                management_ip = str(ipaddress.IPv4Address(host))
                management_interfaces = {
                    interface for interface, ip in actual.items()
                    if ip == management_ip
                }
                if not management_interfaces:
                    raise ValueError(
                        "Could not identify the SSH management interface."
                    )

                for interface in router["interfaces"]:
                    interface_name = interface["name"]
                    if interface_name not in actual:
                        raise ValueError(f"Interface does not exist: {interface_name}")
                    if (
                        interface_name in management_interfaces
                        and interface["role"] != "management"
                    ):
                        raise ValueError(
                            f"{interface_name} carries SSH and must be management."
                        )

                checks.append(
                    f"{name} ({host}): hostname, SSH privilege 15, interface names, "
                    "and management protection passed."
                )
            except Exception as exc:
                errors.append(f"{name}: {type(exc).__name__}: {exc}")
            finally:
                if channel is not None:
                    channel.close()
                if client is not None:
                    client.close()

    return {
        "passed": not errors,
        "checks": checks,
        "errors": errors,
        "proposed_commands": commands,
    }


INTERFACE_LINE = re.compile(
    r"^((?:FastEthernet|GigabitEthernet|Ethernet|Serial)\d+(?:/\d+){0,2}(?:\.\d+)?)\s+"
    r"(\S+)\s+\S+\s+\S+\s+(administratively down|up|down)\s+(\S+)"
)


def discover_interfaces(router_names):
    """Read-only: list each router's real interfaces with `show ip interface brief`."""
    result = {}
    for name in router_names:
        client = channel = None
        try:
            client, channel, host, hostname = connect_router(name)
            output = send_command(channel, "show ip interface brief")
            interfaces = []
            for line in output.splitlines():
                match = INTERFACE_LINE.match(line.strip())
                if match:
                    interface, ip, status, protocol = match.groups()
                    interfaces.append({
                        "name": interface,
                        "ip": None if ip == "unassigned" else ip,
                        "status": status,
                        "protocol": protocol,
                        "management": ip == host,
                    })
            result[name] = {"host": host, "hostname": hostname, "interfaces": interfaces}
        except Exception as exc:
            result[name] = {"error": f"{type(exc).__name__}: {exc}"}
        finally:
            if channel is not None:
                channel.close()
            if client is not None:
                client.close()
    return result
