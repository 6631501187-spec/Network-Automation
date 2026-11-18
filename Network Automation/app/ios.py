"""Generate IOS commands (shared by the review page, approval and the worker).

The topology JSON is validated here, then rendered through the Jinja2 template
app/templates/router_ospf.j2. Management interfaces are skipped entirely.
"""

import hashlib
import ipaddress
import json
import re
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined


INTERFACE_NAME = re.compile(
    r"(?:FastEthernet|GigabitEthernet|Ethernet|Serial)"
    r"\d+(?:/\d+){0,2}(?:\.\d+)?"
)

_TEMPLATES = Environment(
    loader=FileSystemLoader(Path(__file__).resolve().parent / "templates"),
    undefined=StrictUndefined,
    trim_blocks=True,
    lstrip_blocks=True,
    autoescape=False,
)


def config_digest(commands_by_router):
    """SHA-256 of the approved command lists; Kafka carries only this reference."""
    canonical = json.dumps(commands_by_router, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _check_area(area):
    area_text = str(area)
    if area_text.isdecimal():
        if not 0 <= int(area_text) <= 4294967295:
            raise ValueError("Invalid OSPF area.")
    else:
        try:
            ipaddress.IPv4Address(area_text)
        except ipaddress.AddressValueError:
            raise ValueError("Invalid OSPF area.") from None
    return area_text


def commands_for_router(router, process_id, area):
    name = router.get("name", "router")

    if type(process_id) is not int or not 1 <= process_id <= 65535:
        raise ValueError("OSPF process ID must be between 1 and 65535.")
    area_text = _check_area(area)

    interfaces = []
    seen_names = set()

    for interface in router.get("interfaces", []):
        interface_name = interface.get("name", "")
        role = interface.get("role")

        if not INTERFACE_NAME.fullmatch(interface_name):
            raise ValueError(f"{name}: invalid interface name {interface_name!r}.")
        if interface_name in seen_names:
            raise ValueError(f"{name}: duplicate interface {interface_name}.")
        seen_names.add(interface_name)

        if role == "management":
            continue
        if role not in {"lan", "transit"}:
            raise ValueError(f"{name} {interface_name}: resolve the interface role first.")

        cidr = interface.get("ipv4_cidr")
        if not cidr:
            raise ValueError(f"{name} {interface_name}: missing IPv4 address.")
        try:
            address = ipaddress.IPv4Interface(cidr)
        except ValueError:
            raise ValueError(f"{name} {interface_name}: invalid IPv4 address.") from None

        if address.ip.is_multicast or address.ip.is_unspecified:
            raise ValueError(f"{name} {interface_name}: unusable IPv4 address.")
        if address.network.prefixlen < 31 and address.ip in {
            address.network.network_address,
            address.network.broadcast_address,
        }:
            raise ValueError(f"{name} {interface_name}: address is a network or broadcast address.")

        interfaces.append({
            "name": interface_name,
            "role": role,
            "ip": str(address.ip),
            "mask": str(address.network.netmask),
            "network": address.network,
        })

    if not interfaces:
        raise ValueError(f"{name}: no addressed LAN or transit interfaces.")

    router_id = router.get("router_id")
    if router_id:
        try:
            parsed_id = ipaddress.IPv4Address(router_id)
        except ValueError:
            raise ValueError(f"{name}: invalid router ID.") from None
        if int(parsed_id) == 0:
            raise ValueError(f"{name}: router ID cannot be 0.0.0.0.")
        router_id = str(parsed_id)

    networks = sorted(
        {intf["network"] for intf in interfaces},
        key=lambda item: (int(item.network_address), item.prefixlen),
    )

    rendered = _TEMPLATES.get_template("router_ospf.j2").render(
        process_id=process_id,
        area=area_text,
        router_id=router_id,
        interfaces=interfaces,
        networks=[
            {"address": str(net.network_address), "wildcard": str(net.hostmask)}
            for net in networks
        ],
    )
    return [line.strip() for line in rendered.splitlines() if line.strip()]


def rollback_commands(router, process_id):
    """Recovery block: remove OSPF and reset the data-plane interfaces (never management)."""
    commands = ["configure terminal", f"no router ospf {process_id}"]
    for interface in router.get("interfaces", []):
        if interface.get("role") in {"lan", "transit"} and INTERFACE_NAME.fullmatch(
            interface.get("name", "")
        ):
            commands.extend([
                f"default interface {interface['name']}",
                f"interface {interface['name']}",
                "shutdown",
                "exit",
            ])
    commands.append("end")
    return commands
