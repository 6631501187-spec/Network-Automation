"""Paramiko SSH helpers shared by the validation step and the worker.

Credentials and management addresses come from the environment (.env):
SSH_USERNAME, SSH_PASSWORD, <ROUTER>_HOST. Nothing is hardcoded.
"""

import os
import re
import socket
import time

import paramiko


PROMPT_RE = re.compile(r"(?m)^([A-Za-z0-9_.:/-]+)(?:\([^)]+\))?[>#]\s*$")
IOS_ERROR_RE = re.compile(
    r"(?im)^%\s*(?:Invalid input|Incomplete command|Ambiguous command|"
    r"Authorization failed|Access denied)"
)


class RouterError(RuntimeError):
    """A readable SSH/IOS failure that can be shown in the job monitor."""


class LegacyCiscoTransport(paramiko.Transport):
    """Allow the legacy SSH algorithms offered by IOS 15.2 on the c7200 lab routers.

    The routers only offer diffie-hellman-group14-sha1, ssh-rsa host keys and CBC
    ciphers. Paramiko 3.5.x still supports these; Paramiko 5 removed them.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        options = self.get_security_options()
        options.kex = ("diffie-hellman-group14-sha1",)
        options.key_types = ("ssh-rsa",)
        options.ciphers = ("aes128-cbc", "aes256-cbc")


def wait_for_prompt(channel, timeout=12):
    received = bytearray()
    deadline = time.time() + timeout

    while time.time() < deadline:
        if channel.recv_ready():
            received.extend(channel.recv(65535))
            text = received.decode("utf-8", errors="replace").replace("\r", "")
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            if lines and PROMPT_RE.fullmatch(lines[-1]):
                return text
        elif channel.closed:
            break
        else:
            time.sleep(0.1)

    text = received.decode("utf-8", errors="replace")
    raise RouterError(f"Timed out waiting for the router prompt. Last output: {text[-800:]}")


def send_command(channel, command, timeout=15):
    channel.send(command + "\n")
    output = wait_for_prompt(channel, timeout=timeout)
    if IOS_ERROR_RE.search(output):
        raise RouterError(f"IOS rejected command {command!r}: {output[-800:]}")
    return output


def prompt_hostname(output):
    lines = [line.strip() for line in output.replace("\r", "").splitlines() if line.strip()]
    match = PROMPT_RE.fullmatch(lines[-1]) if lines else None
    return match.group(1) if match else ""


def connect_router(router_name):
    """Open an interactive privileged session. Returns (client, channel, host, hostname)."""
    host = os.getenv(f"{router_name}_HOST", "").strip()
    username = os.getenv("SSH_USERNAME", "").strip()
    password = os.getenv("SSH_PASSWORD", "")

    if not host or not username or not password:
        raise RouterError(
            f"Missing SSH settings for {router_name}: set {router_name}_HOST, "
            "SSH_USERNAME and SSH_PASSWORD in .env"
        )

    client = paramiko.SSHClient()
    client.load_system_host_keys()  # unknown host keys are rejected

    try:
        client.connect(
            hostname=host,
            username=username,
            password=password,
            timeout=8,
            banner_timeout=8,
            auth_timeout=8,
            allow_agent=False,
            look_for_keys=False,
            transport_factory=LegacyCiscoTransport,
        )
    except paramiko.AuthenticationException:
        client.close()
        raise RouterError(
            f"{router_name} ({host}): SSH authentication failed; check SSH_USERNAME/SSH_PASSWORD."
        ) from None
    except (socket.timeout, TimeoutError):
        client.close()
        raise RouterError(f"{router_name} ({host}): SSH connection timed out.") from None
    except paramiko.SSHException as exc:
        client.close()
        raise RouterError(f"{router_name} ({host}): SSH error: {exc}") from None
    except OSError as exc:
        client.close()
        raise RouterError(f"{router_name} ({host}): cannot reach SSH port: {exc}") from None

    try:
        channel = client.invoke_shell(width=180, height=48)
        banner = wait_for_prompt(channel)
        hostname = prompt_hostname(banner)
        send_command(channel, "terminal length 0")
        privilege = send_command(channel, "show privilege")
        if "Current privilege level is 15" not in privilege:
            raise RouterError(f"{router_name}: SSH user is not privilege level 15.")
    except Exception:
        client.close()
        raise

    return client, channel, host, hostname
