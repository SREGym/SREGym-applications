#!/usr/bin/env python3
"""Authorized company guest/provider access; no lab filesystem or private client access."""

import contextlib
import fcntl
import json
import os
import re
import selectors
import shlex
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import company_instances
import lab
import world_policy

PORT = 28311
TRANSFERS = {}
TRANSFER_LOCK = threading.Lock()
NAMES = set(world_policy.config()["memory_mib"])
LOCKS = {n: threading.Lock() for n in NAMES}


def node(n):
    if n in company_instances.load():
        return n
    if n in NAMES:
        rescue = lab.rescue_name(n)
        if lab.pid_of(lab.VM / (rescue + ".pid")):
            return lab.register_rescue(n)
        return n
    for source in NAMES:
        if n == lab.rescue_name(source):
            return lab.register_rescue(source)
    raise ValueError("No company access to this server")


CONSOLE_OUTPUT_LIMIT = 16 * 2**20


class ConsoleFailure(Exception):
    def __init__(self, result):
        self.result = result
        super().__init__(result["error"])


class ConsoleCapture:
    """Bounded serial text capture; framing is parsed even after output is full."""

    def __init__(self, token):
        self.begin = ("\n__START_" + token + "__\n").encode()
        self.end = ("\n__DONE_" + token + "__:").encode()
        self.boot_frame = ("\n__BOOT_" + token + "__:").encode()
        self.boot_id = None
        self.pending = bytearray()
        self.output = bytearray()
        self.started = False
        self.exit_code = None
        self.truncated = False

    def retain(self, data):
        room = CONSOLE_OUTPUT_LIMIT - len(self.output)
        self.output.extend(data[:room])
        self.truncated |= len(data) > room

    def feed(self, data):
        # Serial output drops CR bytes.
        self.pending.extend(data.replace(b"\r", b""))
        if not self.started:
            match = re.search(re.escape(self.boot_frame) + rb"([0-9a-fA-F-]{36})\n", self.pending)
            if match:
                self.boot_id = str(uuid.UUID(match[1].decode()))
            pos = self.pending.find(self.begin)
            if pos < 0:
                del self.pending[: -max(len(self.begin), len(self.boot_frame) + 37)]
                return
            del self.pending[: pos + len(self.begin)]
            self.started = True
        pos = self.pending.find(self.end)
        if pos < 0:
            count = max(0, len(self.pending) - len(self.end) + 1)
            self.retain(self.pending[:count])
            del self.pending[:count]
            return
        self.retain(self.pending[:pos])
        del self.pending[:pos]
        tail = self.pending[len(self.end) :]
        if b"\n" not in tail:
            if len(tail) > 3:
                raise ValueError("Invalid console completion frame")
            return
        status = tail.split(b"\n", 1)[0]
        if not status.isdigit() or len(status) > 3 or int(status) > 255:
            raise ValueError("Invalid console completion frame")
        self.exit_code = int(status)
        self.pending.clear()

    def text(self):
        if self.started and self.exit_code is None:
            self.retain(self.pending)
            self.pending.clear()
        return self.output.decode(errors="replace")


def qemu_identity(n):
    pid = lab.pid_of(lab.VM / (n + ".pid"))
    if not pid or not lab.owned(pid, "qemu-system-x86_64"):
        return None
    try:
        fields = Path("/proc", str(pid), "stat").read_text().rsplit(")", 1)[1].split()
        return dict(pid=pid, start_ticks=fields[19])
    except FileNotFoundError:
        return None


def previous_vm_ended(previous):
    try:
        fields = Path("/proc", str(previous["pid"]), "stat").read_text().rsplit(")", 1)[1].split()
        return fields[0] == "Z" or fields[19] != previous["start_ticks"]
    except FileNotFoundError:
        return True
    except (KeyError, TypeError, ValueError, OSError):
        return False


def reconcile_console(n, guard, current, budget):
    """A timeout never cancels a command; a proven prior boot ending can."""
    try:
        previous = json.loads(guard.read_text())
    except (ValueError, OSError):
        return False  # An unreadable reservation is never released.
    if not isinstance(previous, dict) or not previous.get("vm") or not current:
        return False
    reason = None
    if previous["vm"] != current and previous_vm_ended(previous["vm"]):
        reason = "Prior QEMU incarnation ended; a new VM process is present"
    elif previous["vm"] == current and previous.get("boot_id"):
        try:
            response = subprocess.run(
                lab.ssh_arguments(n) + ["cat /proc/sys/kernel/random/boot_id"],
                capture_output=True,
                text=True,
                timeout=min(2, budget),
            )
            observed = str(uuid.UUID(response.stdout.strip())) if response.returncode == 0 else None
        except (subprocess.TimeoutExpired, ValueError, OSError):
            observed = None
        if observed and observed != previous["boot_id"]:
            reason = "Guest kernel restarted; prior guest commands ended"
    if reason is None:
        return False
    audit = lab.ARTIFACTS / world_policy.config()["artifact_subdir"]
    audit.mkdir(parents=True, exist_ok=True)
    with (audit / "console-lifecycle-releases.jsonl").open("a") as record:
        record.write(
            json.dumps(dict(host_epoch=time.time(), server=n, previous=previous, current_vm=current, reason=reason))
            + "\n"
        )
        record.flush()
        os.fsync(record.fileno())
    guard.unlink()
    return True


def console(n, command, timeout):
    if n not in NAMES:
        raise ValueError("Console unavailable for this server")
    if not 0 < timeout <= 1800:
        raise ValueError("Console timeout must be between 0 and 1800 seconds")
    deadline = time.monotonic() + timeout
    token = uuid.uuid4().hex
    capture = ConsoleCapture(token)
    connection = None
    claimed = False
    dispatched = False
    terminal = False
    reservation_lock = None
    reservation = None
    guard = lab.VM / (n + ".console-pending")

    def remaining():
        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("Console request timed out")
        return value

    def failure(state, error):
        return ConsoleFailure(
            dict(
                request_id=token,
                state=state,
                command_state=(
                    "completed" if capture.exit_code is not None else "unknown" if dispatched else "not_started"
                ),
                last_observed_state=(
                    "completed" if capture.exit_code is not None else "running" if capture.started else "unknown"
                ),
                exit_code=capture.exit_code,
                stdout=capture.text(),
                truncated=capture.truncated,
                completion_confirmed=capture.exit_code is not None,
                reuse_blocked=(True if dispatched or state == "blocked" else False if claimed else None),
                error=error,
            )
        )

    if not LOCKS[n].acquire(timeout=max(0, deadline - time.monotonic())):
        raise failure("timeout", "Console queue timed out; command not sent")
    try:
        remaining()
        # Thread locks do not serialize the health watcher and API process.
        reservation_lock = (lab.VM / (n + ".console-lock")).open("a+")
        while True:
            try:
                fcntl.flock(reservation_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(min(0.02, remaining()))
        instance = qemu_identity(n)
        if instance is None:
            raise failure("error", "VM process unavailable; command not sent")
        if guard.exists():
            reconcile_console(n, guard, instance, remaining())
        try:
            # Exclusive persistent reservation survives service restart/crash.
            # It is removed only after completion or if no serial send was tried.
            with guard.open("x") as record:
                claimed = True
                reservation = dict(request_id=token, vm=instance, boot_id=None)
                record.write(json.dumps(reservation) + "\n")
        except FileExistsError:
            raise failure("blocked", "Console blocked: completion of the prior request is unknown") from None
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(remaining())
        connection.connect(str(lab.VM / (n + ".console")))
        begin = "__START_" + token + "__"
        end = "__DONE_" + token + "__"
        # Only the guest shell interprets this command. Closing the socket does
        # not cancel it. Mark uncertain BEFORE any send (including a partial one).
        wrapper = (
            "stty -echo; printf '\\n__BOOT_"
            + token
            + '__:%s\\n\' "$(cat /proc/sys/kernel/random/boot_id)"; '
            + "printf '\\n"
            + begin
            + "\\n'; sudo -n sh -c "
            + shlex.quote(command)
            + "; rc=$?; printf '\\n"
            + end
            + ':%s\\n\' "$rc"\r'
        ).encode()
        connection.settimeout(remaining())
        dispatched = True
        connection.sendall(b"\r")
        time.sleep(min(0.2, remaining()))
        connection.settimeout(remaining())
        connection.sendall(wrapper)
        while True:
            connection.settimeout(min(0.3, remaining()))
            try:
                chunk = connection.recv(65536)
            except TimeoutError:
                continue
            if not chunk:
                raise EOFError("Console disconnected before completion")
            capture.feed(chunk)
            if capture.boot_id and reservation["boot_id"] is None:
                reservation["boot_id"] = capture.boot_id
                temporary = guard.with_name(guard.name + "." + token)
                temporary.write_text(json.dumps(reservation) + "\n")
                temporary.replace(guard)
            if capture.exit_code is not None:
                terminal = True
                return dict(
                    request_id=token,
                    state="terminal",
                    command_state="completed",
                    exit_code=capture.exit_code,
                    stdout=capture.text(),
                    truncated=capture.truncated,
                    completion_confirmed=True,
                    reuse_blocked=False,
                    stderr="Console output limit 16 MiB exceeded.\n" if capture.truncated else "",
                )
    except ConsoleFailure:
        raise
    except TimeoutError as error:
        raise failure("timeout", str(error)) from error
    except Exception as error:
        raise failure("error", str(error)) from error
    finally:
        try:
            if connection is not None:
                # Socket close is never cancellation evidence.
                with contextlib.suppress(OSError):
                    connection.close()
            if claimed and (terminal or not dispatched):
                try:
                    guard.unlink()
                except OSError as error:
                    raise failure("blocked", "Console reservation cleanup failed: " + str(error)) from error
        finally:
            if reservation_lock is not None:
                reservation_lock.close()
            LOCKS[n].release()


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, data):
        payload = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def companyctl(self):
        """Serve the on-call CLI, pointed at the address this request reached."""
        host = self.headers.get("Host", "")
        if not re.fullmatch(r"[A-Za-z0-9.-]+:[0-9]+", host):
            return self.reply(400, {"error": "Host with port required"})
        script = Path(__file__).with_name("companyctl").read_text()
        payload = script.replace(f"http://127.0.0.1:{PORT}", f"http://{host}", 1).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/x-python")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def forward(self):
        path = self.path[len("/provider") :]
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))) if self.command == "POST" else None
        request = urllib.request.Request(
            "http://127.0.0.1:28301" + path,
            data=body,
            method=self.command,
            headers={"Content-Type": "application/json", "X-Client": self.headers.get("X-Client", "oncall")},
        )
        try:
            response = urllib.request.urlopen(request, timeout=700 if path == "/v2.1/instances" else 120)
        except urllib.error.HTTPError as e:
            response = e
        with response:
            payload = response.read()
            self.send_response(response.status)
            for key in ["Content-Type", "Retry-After"]:
                if response.headers.get(key):
                    self.send_header(key, response.headers[key])
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    def do_GET(self):
        if self.path.startswith("/provider/"):
            return self.forward()
        if self.path.startswith("/stream-status/"):
            with TRANSFER_LOCK:
                result = TRANSFERS.get(self.path.rsplit("/", 1)[1])
            return self.reply(200 if result else 404, result or {"error": "Unknown stream"})
        if self.path == "/companyctl":
            return self.companyctl()
        if self.path == "/inventory":
            return self.reply(
                200,
                {
                    "servers": sorted(NAMES),
                    "access": ["exec", "console", "stream", "upload", "provider stop/start/rescue/unrescue"],
                },
            )
        return self.reply(404, {"error": "Unknown resource"})

    def do_POST(self):
        try:
            import world_run_guard

            world_run_guard.require_open()
            if self.path.startswith("/provider/"):
                return self.forward()
            u = urllib.parse.urlparse(self.path)
            if u.path == "/upload":
                a = urllib.parse.parse_qs(u.query)
                n = node(a["server"][0])
                path = a["path"][0]
                if not path.startswith("/"):
                    raise ValueError("Guest absolute path required")
                size = int(self.headers["Content-Length"])
                p = subprocess.Popen(
                    lab.ssh_arguments(n) + ["sudo -n install -D -m0644 /dev/stdin " + shlex.quote(path)],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                while size:
                    chunk = self.rfile.read(min(size, 65536))
                    if not chunk:
                        raise EOFError("Incomplete upload")
                    p.stdin.write(chunk)
                    size -= len(chunk)
                p.stdin.close()
                output = p.stdout.read(65536)
                status = p.wait(timeout=120)
                return self.reply(200, dict(exit_code=status, stdout=output.decode(errors="replace")))
            size = int(self.headers.get("Content-Length", "0"))
            if size > 1048576:
                return self.reply(413, {"error": "Request too large"})
            body = json.loads(self.rfile.read(size))
            n = node(body["server"])
            command = body["command"]
            timeout = min(1800, max(1, int(body.get("timeout", 120))))
            if u.path == "/console":
                return self.reply(200, console(n, command, timeout))
            if u.path == "/exec":
                p = subprocess.Popen(lab.ssh_arguments(n) + [command], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                sel = selectors.DefaultSelector()
                sel.register(p.stdout, selectors.EVENT_READ)
                deadline = time.monotonic() + timeout
                output = bytearray()
                truncated = False
                try:
                    while True:
                        if time.monotonic() > deadline:
                            raise TimeoutError("Guest command timed out")
                        if not sel.select(1):
                            continue
                        chunk = os.read(p.stdout.fileno(), 65536)
                        if not chunk:
                            break
                        output.extend(chunk)
                        if len(output) > 16 * 2**20:
                            truncated = True
                            break
                finally:
                    sel.close()
                    if p.poll() is None and (truncated or time.monotonic() > deadline) and lab.owned(p.pid, "ssh"):
                        p.terminate()
                status = p.wait(timeout=15)
                return self.reply(
                    200,
                    dict(
                        exit_code=75 if truncated else status,
                        stdout=output[: 16 * 2**20].decode(errors="replace"),
                        stderr="Output limit 16 MiB exceeded; use stream for full binary/text output."
                        if truncated
                        else "",
                        truncated=truncated,
                    ),
                )
            if u.path == "/stream":
                transfer = body.get("transfer_id", uuid.uuid4().hex)
                if not isinstance(transfer, str) or len(transfer) > 80:
                    raise ValueError("Invalid transfer ID")
                with TRANSFER_LOCK:
                    if transfer in TRANSFERS:
                        raise ValueError("Transfer ID already used")
                    if len(TRANSFERS) > 1000:
                        TRANSFERS.pop(next(iter(TRANSFERS)))
                    TRANSFERS[transfer] = {"state": "running"}
                p = subprocess.Popen(lab.ssh_arguments(n) + [command], stdout=subprocess.PIPE, stderr=None)
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Connection", "close")
                self.end_headers()
                sel = selectors.DefaultSelector()
                sel.register(p.stdout, selectors.EVENT_READ)
                deadline = time.monotonic() + timeout
                error = None
                size = 0
                try:
                    while True:
                        if time.monotonic() > deadline:
                            raise TimeoutError("Stream timed out")
                        if not sel.select(1):
                            continue
                        data = os.read(p.stdout.fileno(), 65536)
                        if not data:
                            break
                        self.wfile.write(data)
                        self.wfile.flush()
                        size += len(data)
                    status = p.wait(timeout=10)
                except Exception as e:
                    error = str(e)
                    status = 124
                finally:
                    sel.close()
                    if p.poll() is None and lab.owned(p.pid, "ssh"):
                        p.terminate()
                        p.wait(timeout=10)
                    with TRANSFER_LOCK:
                        TRANSFERS[transfer] = {"state": "complete", "exit_code": status, "bytes": size, "error": error}
                self.close_connection = True
                return
            return self.reply(404, {"error": "Unknown operation"})
        except ConsoleFailure as e:
            self.reply(503, e.result)
        except (ValueError, KeyError) as e:
            self.reply(400, {"error": str(e)})
        except Exception as e:
            self.reply(503, {"error": str(e)})


def serve(config):
    """Listen on host loopback, and on the configured container-facing address when there is one."""
    addresses = ["127.0.0.1"] + ([config["agent_address"]] if config.get("agent_address") else [])
    servers = [ThreadingHTTPServer((address, PORT), Handler) for address in addresses]
    for server in servers[1:]:
        threading.Thread(target=server.serve_forever, daemon=True).start()
    servers[0].serve_forever()


if __name__ == "__main__":
    serve(world_policy.config())
