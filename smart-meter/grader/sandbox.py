#!/usr/bin/env python3
"""Private QEMU forks of a completed D capture. Never imports world/lab.py.

Device layouts and migration handling match world/lab.py and world/revive.py.
All sockets, overlays, logs and processes belong to this invocation under grader/.
"""

import base64
import contextlib
import inspect
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import tarfile
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
IPS = {
    "us-parent-01": 10,
    "us-work-01": 11,
    "us-work-02": 12,
    "us-work-03": 13,
    "eu-parent-01": 20,
    "eu-work-01": 21,
    "eu-work-02": 22,
    "eu-work-03": 23,
    "ops-helper-01": 30,
}


def mac(host, nic):
    # The lab's MAC suffix is the literal IP octet (0x10, 0x20, 0x30),
    # not the hexadecimal rendering of decimal 10, 20, 30.
    return f"52:54:00:72:{nic:02x}:{IPS[host]:02d}"


def python_command(code):
    encoded = base64.b64encode(code.encode()).decode()
    return "python3 -c " + shlex.quote("import base64;exec(base64.b64decode('" + encoded + "'))")


class Switch:
    def __init__(self, directory):
        self.directory = directory
        self.path = directory / "s"
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.sock.bind(str(self.path))
        self.sock.settimeout(0.1)
        self.peers, self.table, self.done = set(), {}, False
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()

    def loop(self):
        while not self.done:
            try:
                data, source = self.sock.recvfrom(65536)
            except TimeoutError:
                continue
            except OSError:
                return
            if len(data) < 14 or Path(source).parent != self.directory:
                continue
            self.peers.add(source)
            self.table[data[6:12]] = source
            targets = self.peers if data[0] & 1 or data[:6] not in self.table else {self.table[data[:6]]}
            for target in list(targets):
                if target != source:
                    with contextlib.suppress(OSError):
                        self.sock.sendto(data, target)

    def close(self):
        self.done = True
        self.sock.close()
        self.thread.join(1)


class Fork:
    def __init__(self, manifest, dest, project):
        if not dest.resolve().is_relative_to(HERE):
            raise ValueError("Private fork output must stay under grader/")
        self.manifest, self.dest, self.project = manifest, dest, project
        dest.mkdir(parents=True, exist_ok=True)
        (HERE / "q").mkdir(exist_ok=True)
        self.sockets = Path(tempfile.mkdtemp(prefix="d", dir=HERE / "q"))
        if len(str(self.sockets)) > 86:
            raise RuntimeError("Grader path is too long for private Unix sockets")
        self.switch = Switch(self.sockets)
        self.processes, self.logs, self.ids, self.ports = {}, [], {}, {}
        # One serial console per host: commands on the same host never interleave.
        self.console_locks = {}
        self.console_locks_guard = threading.Lock()
        self.storage_processes = []
        self.working_disks = []
        self.cfg = manifest.get("world_config", {})
        self.key = project / "world/vm/id_ed25519"

    def qmp(self, host, execute, arguments=None, timeout=120):
        path = self.sockets / (str(self.ids[host]) + ".q")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect(str(path))
            stream = sock.makefile("rwb", buffering=0)
            json.loads(stream.readline())
            stream.write(b'{"execute":"qmp_capabilities","id":"cap"}\n')
            while json.loads(stream.readline()).get("id") != "cap":
                pass
            request = dict(execute=execute, id="req")
            if arguments is not None:
                request["arguments"] = arguments
            stream.write(json.dumps(request).encode() + b"\n")
            while True:
                value = json.loads(stream.readline())
                if value.get("id") == "req":
                    if "error" in value:
                        raise RuntimeError(str(value["error"]))
                    return value["return"]

    def start(self, host, resume=False):
        if host not in IPS:
            raise ValueError("No captured device layout for " + host)
        index = len(self.ids)
        self.ids[host] = index
        memory = self.cfg.get("memory_mib", {}).get(host, self.cfg.get("private_client_mib", 2048))
        available = int(re.search(r"MemAvailable:\s+(\d+)", Path("/proc/meminfo").read_text())[1]) // 1024
        if memory + 2048 > available:
            raise RuntimeError("Private fork would leave less than 2 GiB available RAM")
        source = Path(self.manifest["disks"][host])
        disk = self.dest / (host + ".qcow2")
        external = self.manifest.get("ram", {}).get(host)
        tag = self.manifest.get("snapshot_tag") or self.manifest.get("tag")
        if external or not tag:
            subprocess.run(
                ["qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", str(source), str(disk)], check=True
            )
            self.working_disks.append(disk)
            drive = str(disk)
        else:
            # COW the qcow2 FILE BYTES, then open that view as qcow2. This
            # preserves internal RAM snapshots without copying their gigabytes.
            # The raw backing is read-only; every header/data write is private.
            subprocess.run(
                [
                    "qemu-img",
                    "create",
                    "-q",
                    "-f",
                    "qcow2",
                    "-F",
                    "raw",
                    "-b",
                    str(source),
                    str(disk),
                    str(source.stat().st_size + 4 * 2**30),
                ],
                check=True,
            )
            self.working_disks.append(disk)
            block_socket = self.sockets / (str(index) + ".b")
            blocklog = (self.dest / (host + "-block.log")).open("wb")
            self.logs.append(blocklog)
            server = subprocess.Popen(
                [
                    "nice",
                    "-n",
                    "10",
                    "qemu-nbd",
                    "--persistent",
                    "--format=qcow2",
                    "--socket=" + str(block_socket),
                    str(disk),
                ],
                stdout=blocklog,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            self.storage_processes.append(server)
            ready = time.monotonic() + 10
            while not block_socket.exists():
                if server.poll() is not None:
                    raise RuntimeError("Private file-byte COW export failed")
                if time.monotonic() > ready:
                    raise TimeoutError("Private file-byte COW export")
                time.sleep(0.02)
            drive = "nbd+unix:///?socket=" + str(block_socket)
        command = [
            "qemu-system-x86_64",
            "-name",
            "grader-" + self.sockets.name + "-" + host,
            "-enable-kvm",
            "-cpu",
            "Westmere-v2,enforce",
            "-machine",
            "pc-q35-8.2,mem-merge=off",
            "-m",
            str(memory),
            "-smp",
            "2",
            "-display",
            "none",
            "-device",
            "virtio-balloon-pci,free-page-reporting=on",
            "-drive",
            f"file={drive},if=virtio,format=qcow2,cache=none,discard=unmap",
        ]
        single = self.cfg.get("single_fabric", True) and host != "ops-helper-01"
        if not single:
            # No seed/proxy/guest forwards to any host service, including the builder.
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            self.ports[host] = port
            command += [
                "-netdev",
                f"user,id=net0,restrict=on,hostfwd=tcp:127.0.0.1:{port}-:22",
                "-device",
                f"virtio-net-pci,netdev=net0,mac={mac(host, 0)}",
            ]
        nicpath = self.sockets / (str(index) + ".n")
        command += [
            "-netdev",
            f"dgram,id=net1,local.type=unix,local.path={nicpath},remote.type=unix,remote.path={self.switch.path}",
            "-device",
            f"virtio-net-pci,netdev=net1,mac={mac(host, 1)}" + (",addr=0x4" if single else ""),
            "-chardev",
            f"socket,id=console,path={self.sockets / (str(index) + '.c')},server=on,wait=off",
            "-serial",
            "chardev:console",
            "-qmp",
            f"unix:{self.sockets / (str(index) + '.q')},server=on,wait=off",
        ]
        seed = self.cfg.get("seeds", {}).get(host)
        if host == "ops-helper-01":
            seed = self.cfg.get("private_client_seed_iso")
        if seed:
            if not Path(seed).is_file():
                raise FileNotFoundError("Captured read-only seed is missing: " + seed)
            command += ["-drive", f"file={seed},media=cdrom,readonly=on,cache=none"]
        if self.manifest.get("clock"):
            clock = datetime.fromtimestamp(self.manifest["clock"], UTC).strftime("%Y-%m-%dT%H:%M:%S")
            command += ["-rtc", "base=" + clock + ",clock=vm"]
        # Every host starts paused; saved RAM, if any, is loaded before it runs.
        command += ["-S", "-incoming", "defer"] if external else ["-S"]
        log = (self.dest / (host + "-qemu.log")).open("wb")
        self.logs.append(log)
        (self.dest / (host + "-command.json")).write_text(json.dumps(command, indent=2) + "\n")
        process = subprocess.Popen(
            ["nice", "-n", "10", *command], stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
        self.processes[host] = process
        end = time.monotonic() + 15
        while not (self.sockets / (str(index) + ".q")).exists():
            if process.poll() is not None:
                raise RuntimeError(host + " QEMU exited; see its grader log")
            if time.monotonic() > end:
                raise TimeoutError(host + " private QMP did not start")
            time.sleep(0.03)
        self.switch.peers.add(str(nicpath))
        if external:
            self.qmp(host, "migrate-incoming", dict(uri="exec:gzip -dc " + shlex.quote(external)))
            end = time.monotonic() + 100
            while True:
                migration = self.qmp(host, "query-migrate")
                if migration.get("status") == "completed":
                    break
                if migration.get("status") in ("failed", "cancelled"):
                    raise RuntimeError(host + " migration: " + str(migration))
                if time.monotonic() > end:
                    raise TimeoutError(host + " migration failed or timed out")
                time.sleep(0.1)
        elif host in self.manifest.get("live", []):
            if not tag:
                raise ValueError("Live D host has no saved RAM: " + host)
            response = self.qmp(host, "human-monitor-command", {"command-line": "loadvm " + tag})
            if response.strip():
                raise RuntimeError("Could not load saved D RAM: " + response)
        if self.qmp(host, "query-status").get("running"):
            raise RuntimeError("Private D fork unexpectedly resumed during loading")
        if resume:
            self.qmp(host, "cont")

    def console(self, host, command, timeout=40):
        """Framed serial protocol from world_tools.ConsoleCapture, own socket only."""
        with self.console_locks_guard:
            lock = self.console_locks.setdefault(host, threading.Lock())
        with lock:
            return self._console(host, command, timeout)

    def _console(self, host, command, timeout=40):
        token = uuid.uuid4().hex
        begin, end = ("\n__START_" + token + "__\n").encode(), ("\n__DONE_" + token + "__:").encode()
        wrapper = (
            "stty -echo; printf '\\n__START_"
            + token
            + "__\\n'; sudo -n sh -c "
            + shlex.quote(command)
            + "; rc=$?; printf '\\n__DONE_"
            + token
            + '__:%s\\n\' "$rc"\r'
        ).encode()
        deadline = time.monotonic() + timeout
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(2)
            sock.connect(str(self.sockets / (str(self.ids[host]) + ".c")))
            sock.sendall(b"\r")
            time.sleep(0.15)
            sock.sendall(wrapper)
            data = bytearray()
            while time.monotonic() < deadline:
                sock.settimeout(min(1, max(0.01, deadline - time.monotonic())))
                try:
                    chunk = sock.recv(65536)
                except TimeoutError:
                    continue
                if not chunk:
                    break
                data.extend(chunk.replace(b"\r", b""))
                start = data.find(begin)
                finish = data.find(end, start + len(begin)) if start >= 0 else -1
                if finish >= 0 and b"\n" in data[finish + len(end) :]:
                    status = data[finish + len(end) :].split(b"\n", 1)[0]
                    return dict(
                        exit_code=int(status), stdout=data[start + len(begin) : finish].decode(errors="replace")
                    )
                if len(data) > 128 << 20:
                    raise RuntimeError("Private console output exceeded 128 MiB")
            return dict(
                exit_code=None, stdout=data.decode(errors="replace"), error="Private serial command completion unknown"
            )

    def close(self):
        # Exact Popen children, each in its own process group. No global PID files.
        for process in self.processes.values():
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in self.processes.values():
            try:
                process.wait(4)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(4)
        for process in self.storage_processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in self.storage_processes:
            try:
                process.wait(4)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(4)
        self.switch.close()
        for log in self.logs:
            log.close()
        for disk in self.working_disks:
            if disk.is_file():
                disk.unlink()
        shutil.rmtree(self.sockets)

    def ssh_args(self, host):
        if host not in self.ports or host not in self.processes or self.processes[host].poll() is not None:
            raise ValueError("SSH is permitted only to this fork's own running user-mode NIC")
        return [
            "ssh",
            "-i",
            str(self.key),
            "-p",
            str(self.ports[host]),
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=2",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "UserKnownHostsFile=" + str(self.dest / "known_hosts"),
            "-o",
            "LogLevel=ERROR",
            "ubuntu@127.0.0.1",
        ]


def client_readout(manifest, dest, project):
    if "ops-helper-01" not in manifest.get("live", []):
        return dict(status="ungraded", reason="No live saved client RAM")
    fork = Fork(manifest, dest / "client-fork", project)
    start = time.monotonic()
    try:
        fork.start("ops-helper-01")
        resume = time.monotonic()
        fork.qmp("ops-helper-01", "cont")
        code = """import pathlib,json,time,os,signal,subprocess,base64
before={'native_epoch':time.time(),'native_monotonic':time.monotonic(),'stats':{}}
pids=[]
for region in ['us','eu']:
 p=pathlib.Path('/var/lib/private/'+region+'-stats.json')
 if not p.exists():continue
 row=json.loads(p.read_text());before['stats'][region]=row;pid=int(row['pid'])
 command=pathlib.Path('/proc/'+str(pid)+'/cmdline').read_bytes()
 if b'/opt/private/sim.py' not in command:raise RuntimeError('Unexpected saved client PID')
 os.kill(pid,signal.SIGSTOP);pids.append(pid)
print('BOUNDARY '+json.dumps(before),flush=True)
p=subprocess.Popen(['systemctl','stop','private-devices-us','private-devices-eu'])
time.sleep(.1)
# systemd resumes and ends the client itself; a client that already exited has stopped.
for pid in pids:
 try:os.kill(pid,signal.SIGCONT)
 except ProcessLookupError:pass
if p.wait(timeout=20):raise RuntimeError('Client file-buffer flush failed')
"""
        ready = time.monotonic() + 35
        while True:
            response = subprocess.run(fork.ssh_args("ops-helper-01") + ["true"], capture_output=True, timeout=4)
            if response.returncode == 0:
                break
            if time.monotonic() > ready:
                raise TimeoutError("Private saved-client SSH did not become ready")
            time.sleep(0.2)
        response = subprocess.run(
            fork.ssh_args("ops-helper-01") + ["sudo -n " + python_command(code)],
            capture_output=True,
            text=True,
            timeout=35,
        )
        result = dict(exit_code=response.returncode, stdout=response.stdout, stderr=response.stderr)
        (dest / "client-console.json").write_text(json.dumps(result) + "\n")
        client = dest / "client"
        client.mkdir(exist_ok=True)
        boundary = None
        for line in result["stdout"].splitlines():
            if line.startswith("BOUNDARY "):
                boundary = json.loads(line[9:])
        if result["exit_code"] == 0:
            tarpath = dest / "client-files.tar.gz"
            with tarpath.open("wb") as stream:
                subprocess.run(
                    fork.ssh_args("ops-helper-01") + ["sudo -n tar -C /var/lib/private -czf - ."],
                    stdout=stream,
                    check=True,
                    timeout=40,
                )
            with tarfile.open(tarpath) as archive:
                for member in archive:
                    name = Path(member.name).name
                    if member.isfile() and name == member.name.removeprefix("./"):
                        with archive.extractfile(member) as stream:
                            (client / name).write_bytes(stream.read())
        return dict(
            status="read" if result["exit_code"] == 0 else "ungraded",
            resume_host_monotonic=resume,
            finished_host_monotonic=time.monotonic(),
            native_before=boundary,
            seconds=time.monotonic() - start,
            network="private isolated switch with only the client; no company peers, provider, or forwards",
            boundary_uncertainty="At most elapsed resume-to-inspection; use run native clock bounds first",
        )
    finally:
        fork.close()


def queue_readout(manifest, dest, project, hosts):
    """Positive custody witnesses from live sessions on isolated single-host forks.

    Absence is deliberately not a loss proof: a native timer or local consumer
    could run between resume and inspection. Freed RAM bytes are never credited.
    """
    summaries = {}
    observations_path = dest / "queue-observations.jsonl"

    with observations_path.open("w") as observations:
        for host in hosts:
            if host not in manifest.get("live", []):
                continue
            fork = Fork(manifest, dest / ("queue-fork-" + host), project)
            summary = dict(status="ungraded", absence_proves_loss=False)
            summaries[host] = summary
            try:
                fork.start(host)
                started = time.monotonic()
                fork.qmp(host, "cont")
                # Erlang dedupes payloads across sessions and writes them to a file in the broker
                # container; the guest canonicalizes them and returns one gzip block. Large
                # backlogs then fit the time limit and the console.
                expression = """Cs=ets:tab2list(emqx_channel_info),
All=lists:foldl(fun({{C,P},_,_},Acc)->
 case binary:match(C,<<"gateway">>) of nomatch->Acc;_->
  try S=emqx_connection:get_state(P),Ch=maps:get(channel,S),Se=element(4,Ch),
   Q=emqx_mqueue:to_list(emqx_session:info(mqueue,Se)),
   I=[element(3,D)||{_,D}<-emqx_inflight:to_list(emqx_session:info(inflight,Se))],
   {messages,B}=process_info(P,messages),
   M=[Msg||{deliver,_,Msg}<-B],
   io:format("CHANNEL ~s ~p ~p ~p~n",[base64:encode(C),length(Q),length(I),length(M)]),
   [emqx_message:payload(X)||X<-Q++I++M]++Acc
  catch Class:Reason->io:format("QUEUE_ERROR ~p ~p~n",[Class,Reason]),Acc end
 end end,[],Cs),
U=lists:usort(All),
ok=file:write_file("/tmp/grader-queue-payloads",lists:join(<<10>>,[base64:encode(X)||X<-U])),
io:format("QUEUE_UNIQUE ~p~n",[length(U)]),
io:format("QUEUE_COMPLETE~n",[])."""
                code = """import subprocess,json,os,signal,time,base64,gzip
print('QUEUE_CLOCK '+json.dumps({'native_epoch':time.time()}),flush=True)
containers=json.loads(subprocess.check_output(['crictl','ps','-o','json']))['containers']
for c in containers:
 name=c.get('metadata',{}).get('name','')
 if name in ['gateway','writer','backfill']:
  info=json.loads(subprocess.check_output(['crictl','inspect',c['id']]))
  pid=int(info['info']['pid'])
  try:
   group=next(l.split(':',2)[2].strip() for l in open('/proc/'+str(pid)+'/cgroup') if l.startswith('0::'))
   open('/sys/fs/cgroup'+group+'/cgroup.freeze','w').write('1')
  except (OSError,StopIteration):os.kill(pid,signal.SIGSTOP)
ids=[c['id'] for c in containers if c.get('metadata',{}).get('name')=='emqx']
if len(ids)!=1:raise RuntimeError('Expected one live broker container')
# crictl exec streams through 127.0.0.1, which a host without its local routing rule cannot reach:
# enter the broker container's namespaces directly, as its own user and with its environment.
bpid=int(json.loads(subprocess.check_output(['crictl','inspect',ids[0]]))['info']['pid'])
benv=dict(x.split('=',1) for x in open('/proc/%d/environ'%bpid).read().split(chr(0)) if '=' in x)
bst=dict(l.split(':',1) for l in open('/proc/%d/status'%bpid) if ':' in l)
inside=['nsenter','-t',str(bpid),'-a','-S',bst['Uid'].split()[0],'-G',bst['Gid'].split()[0],'-w']
result=subprocess.run(inside+['/opt/emqx/bin/emqx','eval',EXPRESSION],capture_output=True,text=True,timeout=120,env=benv)
print(result.stdout,flush=True)
print(result.stderr,flush=True)
if result.returncode:raise SystemExit(result.returncode)
data=subprocess.run(inside+['cat','/tmp/grader-queue-payloads'],capture_output=True,timeout=120,check=True,env=benv).stdout
rows={};bad=0
for line in data.split(bytes([10])):
 if not line:continue
 try:
  _,digest,p=canonical(json.loads(base64.b64decode(line)))
  rows[digest]=p
 except Exception:bad+=1
blob=base64.b64encode(gzip.compress(chr(10).join(json.dumps(p,sort_keys=True) for p in rows.values()).encode())).decode()
print('QUEUE_CANONICAL '+str(len(rows))+' '+str(bad),flush=True)
for i in range(0,len(blob),4000):print('QGZ '+blob[i:i+4000],flush=True)
print('QUEUE_BLOB_END',flush=True)
"""
                import gzip

                import semantic

                helpers = (
                    "from decimal import Decimal, InvalidOperation\nimport hashlib\n"
                    "from datetime import datetime, timezone\n"
                    + inspect.getsource(semantic.millis)
                    + "\n"
                    + inspect.getsource(semantic.canonical)
                    + "\n"
                )
                result = fork.console(
                    host, python_command(helpers + "EXPRESSION=" + repr(expression) + "\n" + code), timeout=300
                )
                (dest / (host + "-queue-console.json")).write_text(json.dumps(result) + "\n")
                lines = result["stdout"].splitlines()
                blob = "".join(line[4:] for line in lines if line.startswith("QGZ "))
                complete = "QUEUE_COMPLETE" in result["stdout"] and "QUEUE_BLOB_END" in result["stdout"]
                records = 0
                if blob:
                    for text in gzip.decompress(base64.b64decode(blob)).decode().splitlines():
                        observations.write(
                            json.dumps(dict(kind="copy", source=host + "/native-session-RAM", payload=json.loads(text)))
                            + "\n"
                        )
                        records += 1
                summary.update(
                    status="positive_witnesses" if result["exit_code"] == 0 and complete else "ungraded",
                    payload_occurrences=records,
                    readout_seconds=time.monotonic() - started,
                    live_table_read_completed=complete,
                    canonical_summary=next((line for line in lines if line.startswith("QUEUE_CANONICAL")), None),
                    errors=[s for s in lines if s.startswith("QUEUE_ERROR")],
                )
            except Exception as e:
                summary["error"] = repr(e)
            finally:
                fork.close()
    return summaries


PROBE = """import json,socket,subprocess,time,pathlib
rules=subprocess.run(['ip','-j','rule'],capture_output=True,text=True)
def reach(address,port):
 try:
  s=socket.create_connection((address,port),timeout=.4);s.close();return True
 except ConnectionRefusedError:return 'refused'
 except OSError:return False
connections={address+':'+str(port):reach(address,port) for address,port in TARGETS}
# A target briefly bouncing its own interface is not lost connectivity; lost
# routing rules do not heal, so a target still unreachable after ~5 s is lost.
for pause in (2,3):
 failed=[k for k,v in connections.items() if not v]
 if not failed:break
 time.sleep(pause)
 for k in failed:
  address,port=k.rsplit(':',1);connections[k]=reach(address,int(port))
pod_targets=[]
try:
 pods=json.loads(subprocess.check_output(['crictl','pods','-o','json'],timeout=5)).get('items',[]) if DISCOVER else []
 for pod in pods:
  name=pod.get('metadata',{}).get('name','')
  port=1883 if name.startswith('emqx') else 5432 if name.startswith('postgres') else 9092 if name.startswith('kafka') else None
  if port:
   info=json.loads(subprocess.check_output(['crictl','inspectp',pod['id']],timeout=3))
   ip=info.get('status',{}).get('network',{}).get('ip')
   if ip:pod_targets.append([ip,port])
except Exception:pass
print(json.dumps(dict(native_epoch=time.time(),rules=json.loads(rules.stdout),connections=connections,pod_targets=pod_targets,
 networkd=subprocess.run(['systemctl','show','systemd-networkd','-p','MainPID','-p','ExecMainStartTimestamp','-p','ExecMainStartTimestampMonotonic','-p','ActiveState'],capture_output=True,text=True).stdout)))
"""


def console_json(text, required):
    for line in reversed(text.splitlines()):
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and set(required) <= value.keys():
            return value
    raise ValueError("No complete structured probe result")


def probe(fork, host, pod_targets=(), discover=True):
    region = host[:2]
    peers = [(f"10.72.0.{n}", 22) for h, n in IPS.items() if h != "ops-helper-01"]
    peers += [(f"10.72.0.{10 if region == 'us' else 20}", 6443)]
    peers += list(pod_targets)
    result = fork.console(
        host, python_command("TARGETS=" + repr(peers) + "\nDISCOVER=" + repr(discover) + "\n" + PROBE), timeout=25
    )
    if result["exit_code"] == 0:
        try:
            return console_json(result["stdout"], ["rules", "connections"])
        except ValueError:
            pass
    return dict(error="Could not read private fork probe", detail=result)


def fresh_unit(before, current):
    return bool(
        current
        and (
            (current.get("InvocationID") and current.get("InvocationID") != before.get("InvocationID"))
            or int(current.get("ExecMainStartTimestampMonotonic", "0") or 0)
            > int(before.get("ExecMainStartTimestampMonotonic", "0") or 0)
        )
    )


def terminal_unit(before, current):
    if not fresh_unit(before, current):
        return None
    active = current.get("ActiveState")
    ended = active in ("inactive", "failed") or (active == "active" and current.get("SubState") == "exited")
    if not ended or int(current.get("MainPID", "0") or 0):
        return None
    start = int(current.get("ExecMainStartTimestampMonotonic", "0") or 0)
    finish = int(current.get("ExecMainExitTimestampMonotonic", "0") or 0)
    if not start or finish < start:
        return None
    return 0 if current.get("Result") == "success" and current.get("ExecMainStatus") == "0" else 1


def trigger(fork, host, unit, wait_seconds=45):
    code = """import subprocess,time,json,pathlib,os
unit=UNIT
restored=subprocess.run(['systemctl','unmask',unit+'.service',unit+'.timer'],capture_output=True,text=True)
subprocess.run(['systemctl','daemon-reload'],check=True)
enabled=subprocess.run(['systemctl','enable','--now',unit+'.timer'],capture_output=True,text=True)
subprocess.run(['systemctl','reset-failed',unit+'.service'],capture_output=True)
events=[];commands=set()
fields=['InvocationID','ControlGroup','MainPID','ActiveState','SubState','Result','ExecMainStatus','ExecMainStartTimestamp','ExecMainExitTimestamp','ExecMainStartTimestampMonotonic','ExecMainExitTimestampMonotonic']
def state():
 try:
  p=subprocess.run(['systemctl','show',unit+'.service',*['--property='+k for k in fields]],capture_output=True,text=True,timeout=2)
  if p.returncode:return None
  return dict(l.split('=',1) for l in p.stdout.splitlines() if '=' in l)
 except subprocess.SubprocessError:return None
before=state()
if before is None:raise RuntimeError('No pre-trigger native unit state')
def observe_processes(current):
 if not fresh_unit(before,current):return
 pids=set()
 group=current.get('ControlGroup','')
 if group:
  root=pathlib.Path('/sys/fs/cgroup')/group.lstrip('/')
  for path in [root/'cgroup.procs',*root.glob('**/cgroup.procs')]:
   try:pids.update(path.read_text().split())
   except OSError:pass
 if current.get('MainPID','0')!='0':pids.add(current['MainPID'])
 for pid in list(pids):
  try:pids.update(pathlib.Path('/proc/'+pid+'/task/'+pid+'/children').read_text().split())
  except OSError:pass
 for pid in pids:
  try:
   args=pathlib.Path('/proc/'+pid+'/cmdline').read_bytes().split(bytes([0]))
   argv=[a.decode(errors='replace') for a in args if a]
   commands.add(tuple(a[:200] for a in argv[:7]))
  except OSError:pass
def sample():
 p=subprocess.run(['ip','-j','rule'],capture_output=True,text=True,timeout=2)
 try:rules=json.loads(p.stdout)
 except ValueError:return
 if p.returncode:return
 key=[r for r in rules if str(r.get('table')) in ['2004','2005']]
 if not events or events[-1]['rules']!=key:events.append(dict(native_epoch=time.time(),rules=key))
sample()
started_epoch=int(time.time())-1
dispatch=subprocess.run(['systemctl','restart','--no-block',unit+'.service'],capture_output=True,text=True,timeout=5)
deadline=time.monotonic()+WAIT_SECONDS
last={};result=None
while time.monotonic()<deadline:
 sample();current=state()
 if current:
  last=current;observe_processes(current);result=terminal_unit(before,current)
  if result is not None:break
 time.sleep(.2)
sample()
journal=subprocess.run(['journalctl','-u','systemd-networkd','-o','cat','--since','@'+str(started_epoch)],capture_output=True,text=True,timeout=10).stdout
print(json.dumps(dict(networkd_reconfigured=sum('Re-configuring with' in l for l in journal.splitlines()),exit_code=result if result is not None else 124,completion_confirmed=result is not None,
 dispatch_exit_code=dispatch.returncode,stdout=dispatch.stdout,stderr=dispatch.stderr,unit_state=last,
 traced_commands=sorted(commands),automation_restored=restored.returncode==0 and enabled.returncode==0,rule_transitions=events)))
"""
    helpers = inspect.getsource(fresh_unit) + "\n" + inspect.getsource(terminal_unit) + "\n"
    result = fork.console(
        host,
        python_command("UNIT=" + repr(unit) + "\nWAIT_SECONDS=" + repr(wait_seconds) + "\n" + helpers + "\n" + code),
        timeout=wait_seconds + 15,
    )
    if result["exit_code"] == 0:
        try:
            return console_json(result["stdout"], ["exit_code", "rule_transitions"])
        except ValueError:
            pass
    return result


ROUTINE = """import subprocess,json
ids=subprocess.run(['crictl','ps','--name','postgres','-q'],capture_output=True,text=True,timeout=20).stdout.split()
if not ids:
 print('ROUTINE '+json.dumps(dict(error='no postgres container on this host')));raise SystemExit(0)
def q(sql):
 r=subprocess.run(['crictl','exec',ids[0],'psql','-U','postgres','-d','ledger','-At','-c',sql],capture_output=True,text=True,timeout=90)
 if r.returncode:raise SystemExit('query failed: '+r.stderr[-300:])
 return [json.loads(l) for l in r.stdout.splitlines() if l.startswith('{')]
out=dict(partner=q("SELECT row_to_json(t) FROM (SELECT origin,tenant,hour,readings,quantity FROM partner_totals) t"),
 closed=q("SELECT row_to_json(t) FROM (SELECT tenant,hour,readings,quantity FROM closed_reports) t"),
 ledger=q("SELECT row_to_json(t) FROM (SELECT tenant,extract(epoch FROM date_trunc('hour',event_time AT TIME ZONE 'UTC'))::bigint AS hour,count(*) AS readings,sum(quantity) AS quantity FROM readings GROUP BY 1,2) t"))
print('ROUTINE '+json.dumps(out))
"""


def routine_reporting(fork, candidates):
    """Read one region's partner statements, settled reports and ledger aggregates on the fork."""
    details = {}
    for host in candidates:
        result = fork.console(host, python_command(ROUTINE), timeout=150)
        for line in reversed(result.get("stdout", "").splitlines()):
            if line.startswith("ROUTINE "):
                value = json.loads(line[8:])
                if "error" not in value:
                    return dict(value, host=host)
                break
        details[host] = dict(exit_code=result.get("exit_code"), tail=result.get("stdout", "")[-400:])
    return dict(error="No ledger database answered in this region", details=details)


def compare_probes(previous, after, transitions):
    def keys(rules):
        return {
            (str(r.get("table")), str(r.get("fwmark")), str(r.get("fwmask")))
            for r in rules
            if str(r.get("table")) in ("2004", "2005")
        }

    sampled = [keys(e["rules"]) for e in transitions if "rules" in e]
    old = keys(previous["rules"]) if "rules" in previous else sampled[0] if sampled else set()
    removed = old - keys(after["rules"]) if "rules" in after else set()
    for observed in sampled:
        removed.update(old - observed)
    lost = sorted(
        k for k, v in previous.get("connections", {}).items() if v and after.get("connections", {}).get(k) is False
    )
    changed = bool(previous.get("networkd") and after.get("networkd") and previous["networkd"] != after["networkd"])
    return sorted(removed), lost, bool(changed or removed)


def finish_recurrence(result):
    hosts = result["hosts"]
    result["tested_hosts"] = len(hosts)
    result["hosts_losing_rules"] = [
        h for h, v in hosts.items() if any(t["removed_policy_rules"] for t in v["triggers"])
    ]
    result["hosts_losing_connectivity"] = [
        h for h, v in hosts.items() if any(t["lost_connectivity"] for t in v["triggers"])
    ]
    result["trigger_failures"] = [
        dict(host=h, unit=t["unit"])
        for h, v in hosts.items()
        for t in v["triggers"]
        if t["command"].get("exit_code") != 0
    ]
    result["automation_restore_failures"] = [
        dict(host=h, unit=t["unit"])
        for h, v in hosts.items()
        for t in v["triggers"]
        if not t["command"].get("automation_restored", False)
    ]
    result["no_actual_lifecycle_witness"] = [
        dict(host=h, unit=t["unit"])
        for h, v in hosts.items()
        for t in v["triggers"]
        if t.get("phase") in result.get("witness_phases", ["role"]) and not t["actual_network_lifecycle_witness"]
    ]
    incomplete = (
        bool(result.get("error"))
        or len(hosts) != 8
        or any(
            len(v["triggers"]) != len(result["declared_matrix"])
            or v["before"].get("error")
            or v.get("after", {}).get("error")
            for v in hosts.values()
        )
    )
    # A re-fire shows that rules survive only on a host that had them, so every tested host must start
    # with Cilium's proxy rules.
    result["hosts_without_rules_before"] = [
        h
        for h, v in hosts.items()
        if "rules" in v["before"] and not {"2004", "2005"} <= {str(r.get("table")) for r in v["before"]["rules"]}
    ]

    # The role must run from the source of truth: its checkout was moved aside, so a real role run clones the
    # platform repository again. A unit that ran but never cloned did not run the platform role.
    def recloned(v, t):
        paths = {f["path"].rstrip("/") for f in v.get("fresh_checkouts", []) if f.get("phase") == t.get("phase")}
        return not paths or any(
            isinstance(c, list) and c[:2] == ["git", "clone"] and str(c[-1]).rstrip("/") in paths
            for c in t["command"].get("traced_commands", [])
        )

    result["role_without_source_clone"] = [
        dict(host=h, unit=t["unit"])
        for h, v in hosts.items()
        for t in v["triggers"]
        if t["command"].get("exit_code") == 0 and not recloned(v, t)
    ]
    result["status"] = (
        "fail"
        if result["hosts_losing_rules"]
        or result["hosts_losing_connectivity"]
        or result["hosts_without_rules_before"]
        or result["role_without_source_clone"]
        else "ungraded"
        if incomplete
        or result["trigger_failures"]
        or result["automation_restore_failures"]
        or result["no_actual_lifecycle_witness"]
        else "pass"
    )
    return result


def recurrence_unit(phase, manifest):
    value = manifest.get("recurrence_units", {"package": "company-package", "role": "company-role"}).get(phase)
    if value:
        value = value.removesuffix(".service")
        if not re.fullmatch(r"[A-Za-z0-9_.@-]+", value):
            raise ValueError("Invalid declared recurrence unit")
    return value


def recurrence(manifest, dest, project, limit=360):
    original_hosts = [h for h in manifest["disks"] if h in IPS and h != "ops-helper-01"]
    hosts = [h for h in original_hosts if h in manifest.get("live", [])]
    cfg = manifest.get("world_config", {})
    memory = sum(cfg.get("memory_mib", {}).get(h, 2048) for h in hosts)
    available = int(re.search(r"MemAvailable:\s+(\d+)", Path("/proc/meminfo").read_text())[1]) // 1024
    if memory + 3072 > available:
        return dict(
            status="ungraded",
            reason="Insufficient available RAM for an independent D fork",
            required_mib=memory + 3072,
            available_mib=available,
        )
    fork = Fork(manifest, dest / "recurrence-fork", project)
    end = time.monotonic() + limit
    result = dict(
        status="ungraded",
        post_deadline_scope_test=True,
        hosts={},
        declared_matrix=list(manifest.get("recurrence_matrix", ["package", "role"])),
        witness_phases=list(manifest.get("recurrence_witness_phases", ["role"])),
        execution_model="Eligible per-host maintenance timers in parallel; phases in the declared order",
        unit_selection={
            h: {p: recurrence_unit(p, manifest) for p in manifest.get("recurrence_matrix", ["package", "role"])}
            for h in hosts
        },
        stopped_hosts_ungraded=[h for h in original_hosts if h not in hosts],
        required_original_hosts=8,
        ungraded=[
            "Provider template replacement and fresh-image/reboot matrix",
            "Data and report invariants during destructive template replacement",
            "Restoration of host-side provider/controller processes is unavailable in this isolated fork",
        ],
    )
    try:
        for host in hosts:
            if time.monotonic() > end:
                raise TimeoutError("Recurrence setup budget exhausted")
            fork.start(host)
        for host in hosts:
            fork.qmp(host, "cont")
        # Native starts restore existing running processes; cold hosts need their own boot.
        with ThreadPoolExecutor(4) as pool:
            before = dict(zip(hosts, pool.map(lambda h: probe(fork, h), hosts), strict=True))
        pod_targets = {
            r: sorted({tuple(t) for h, v in before.items() if h.startswith(r) for t in v.get("pod_targets", [])})
            for r in ("us", "eu")
        }
        pod_owner = {f"{ip}:{port}": h for h, v in before.items() for ip, port in v.get("pod_targets", [])}
        with ThreadPoolExecutor(4) as pool:
            before = dict(
                zip(hosts, pool.map(lambda h: probe(fork, h, pod_targets[h[:2]], discover=False), hosts), strict=True)
            )
        for host in hosts:
            result["hosts"][host] = dict(
                was_running_at_D=host in manifest.get("live", []), before=before[host], triggers=[]
            )

        def host_phase(host, phase):
            region = host[:2]
            if time.monotonic() > end:
                raise TimeoutError("Recurrence trigger budget exhausted")
            # Re-fire the units saved on the host; the grader supplies no downloads, settings, clock
            # changes or packages.
            unit = result["unit_selection"][host][phase]
            wait_seconds = min(120 if phase == "package" else 45, max(1, int(end - time.monotonic() - 30)))
            # A state may declare a per-phase working checkout that the unit re-clones from the
            # source of truth when absent (a replaced host does exactly this). It is moved aside,
            # so a fix held only in a host's local copy does not count.
            fresh = manifest.get("recurrence_fresh_checkouts", {}).get(phase)
            if fresh:
                moved = fork.console(
                    host,
                    "if [ -e "
                    + shlex.quote(fresh)
                    + " ]; then mv "
                    + shlex.quote(fresh)
                    + " "
                    + shlex.quote(fresh + ".before-recurrence")
                    + "; fi; true",
                    timeout=20,
                )
                result["hosts"][host].setdefault("fresh_checkouts", []).append(
                    dict(phase=phase, path=fresh, exit_code=moved.get("exit_code"))
                )
            triggered = (
                trigger(fork, host, unit, wait_seconds=wait_seconds)
                if unit
                else dict(exit_code=None, error="No saved durable unit for this phase")
            )
            after = probe(fork, host, pod_targets[region], discover=False)
            previous = result["hosts"][host].get("after", before[host])
            removed, lost, witness = compare_probes(previous, after, triggered.get("rule_transitions", []))
            # A refused port is a stopped process on a working network (compare_probes counts only
            # no-answer). A pod target whose pod is gone from its host after the phase was replaced.
            replaced = []
            for key in [k for k in lost if k in pod_owner]:
                owner = pod_owner[key]
                current = probe(fork, owner, (), discover=True)
                if "pod_targets" in current and key not in {f"{ip}:{port}" for ip, port in current["pod_targets"]}:
                    replaced.append(key)
            lost = [k for k in lost if k not in replaced]
            result["hosts"][host].setdefault("replaced_pod_targets", []).extend(replaced)
            result["hosts"][host]["triggers"].append(
                dict(
                    unit=unit,
                    phase=phase,
                    terminal=triggered.get("completion_confirmed", False),
                    actual_network_lifecycle_witness=witness or bool(triggered.get("networkd_reconfigured")),
                    command=triggered,
                    removed_policy_rules=removed,
                    lost_connectivity=lost,
                )
            )
            result["hosts"][host]["after"] = after

        for phase in result["declared_matrix"]:
            with ThreadPoolExecutor(max(1, len(hosts))) as pool:
                futures = [pool.submit(host_phase, h, phase) for h in hosts]
                for future in futures:
                    future.result()
        # Routine operations continue on the fork (writer rollups, partner readers, mirroring,
        # report maintenance); reporting is read again after they have run.
        wait = float(manifest.get("reporting_routine_seconds", 0) or 0)
        if wait:
            if time.monotonic() + wait + 120 > end:
                raise TimeoutError("Recurrence budget leaves no time for the routine reporting cycle")
            time.sleep(wait)
            ledger_first = {"us": "us-work-01", "eu": "eu-work-02"}
            result["routine_reporting"] = {
                r: routine_reporting(
                    fork, sorted([h for h in hosts if h.startswith(r)], key=lambda h: h != ledger_first[r])
                )
                for r in ("us", "eu")
            }
    except Exception as e:
        result["error"] = repr(e)
    finally:
        fork.close()
    return finish_recurrence(result)
