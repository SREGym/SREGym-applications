#!/usr/bin/env python3
"""Export a saved Smart Meter world as one relocatable bundle, or install a bundle under world/vm/states.

  python3 world/bundle.py export usage-reporting smart-meter-usage-reporting.tar.zst [--from STATES_DIR] [--root OLD=@WORLD@ ...]
  python3 world/bundle.py install smart-meter-usage-reporting.tar.zst [--name usage-reporting]
  (a .zst name is compressed with zstd on the way; '-' as the bundle writes a plain tar to stdout or reads one from stdin)

A saved state names absolute paths: its disks and their backing chain, and the census truth the grader
reads (accepted prefixes, identity database, disk extractions). Export ships the state, every backing file
(a pinned input among them goes to its input path under world/) and that truth, and writes those paths as @STATE@/... and @WORLD@/...
It also ships the build-tree files a revived world reads: the guests' SSH key and the seeds the hosts boot from.
Saved guest memory travels recompressed from the build's gzip -1 to zstd, which revive reads as it is.
Install unpacks into world/vm/states/<name> (those build-tree files into their places under world/), checks every
byte against the bundle's SHA256SUMS, renders the paths for this folder and points each disk at its new backing file.
"""

import argparse
import contextlib
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

WORLD = Path(__file__).resolve().parent
STATES = WORLD / "vm" / "states"
INPUTS = WORLD.parent / "inputs" / "inputs.json"
TRUTH_KEYS = (
    "accepted_prefix_dir",
    "private_identity_hash_database",
    "stopped_disk_extraction",
    "held_disk_extraction",
)
TOP = "smart-meter-state"
# Window 2^27 is the largest zstd decompresses without extra flags.
ZSTD = ["zstd", "-19", "--long=27", "-T0", "-q"]


def sha256(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def chain(disk):
    info = json.loads(
        subprocess.check_output(["qemu-img", "info", "-U", "--backing-chain", "--output=json", str(disk)], text=True)
    )
    return [Path(row["filename"]).resolve() for row in info[1:]]


def pinned_inputs():
    if not INPUTS.exists():
        return {}
    return {row["sha256"]: row["dest"] for row in json.loads(INPUTS.read_text())["inputs"] if row.get("sha256")}


def rewrite(text, roots):
    for old, new in sorted(roots, key=lambda pair: -len(pair[0])):
        text = text.replace(old, new)
    return text


@contextlib.contextmanager
def opened(path, write):
    """The bundle as a byte stream: '-' is stdout or stdin, a .zst name goes through zstd, anything else is a plain tar."""
    if path == "-":
        yield sys.stdout.buffer if write else sys.stdin.buffer
    elif path.endswith(".zst"):
        command = [*ZSTD, "-f", "-o", path] if write else ["zstd", "-dcq", path]
        pipe = dict(stdin=subprocess.PIPE) if write else dict(stdout=subprocess.PIPE)
        with subprocess.Popen(command, **pipe) as zstd:
            yield zstd.stdin if write else zstd.stdout
            if not write:
                zstd.stdout.read()  # the tar reader can stop before the end-of-archive padding
        if zstd.returncode:
            raise SystemExit(f"zstd failed on {path}")
    else:
        with open(path, "wb" if write else "rb") as stream:
            yield stream


def recompress(gz, dest):
    with open(dest, "wb") as out, subprocess.Popen(["gzip", "-dc", str(gz)], stdout=subprocess.PIPE) as gunzip:
        subprocess.run([*ZSTD, "-c"], stdin=gunzip.stdout, stdout=out, check=True)
    if gunzip.returncode:
        raise SystemExit(f"gzip failed on {gz}")


def world_files(state, roots):
    """Build-tree files the revived hosts and the seed service read, by their path under world/."""
    cfg = json.loads((state / "config.json").read_text())
    seed_root = Path(cfg["seed_service_root"])  # world/vm/build/seed of the tree that built the state
    paths = [seed_root.parents[1] / "id_ed25519", seed_root.parents[1] / "id_ed25519.pub"]
    paths += [Path(p) for p in [*cfg["seeds"].values(), cfg["private_client_seed_iso"]]]
    paths += sorted(p for p in seed_root.rglob("*") if p.is_file())
    files = {}
    for path in paths:
        rel = rewrite(str(path), roots)
        if not rel.startswith("@WORLD@/"):
            raise SystemExit(f"{path} is outside the world folder; pass --root for its tree")
        files["world/" + rel.removeprefix("@WORLD@/")] = path
    return files


def export(name, out, extra_roots, states=STATES):
    state = (Path(states) / name).resolve()
    if not (state / "ready.json").exists():
        raise SystemExit(f"{state} is not a saved state (no ready.json)")
    manifest = json.loads((state / "manifest.json").read_text())
    truth = json.loads((state / "starting-truth.json").read_text())
    files = {}  # path in bundle -> local file
    for path in sorted(state.rglob("*")):
        if path.is_file():
            files["state/" + str(path.relative_to(state))] = path
    # Census truth outside the state folder travels with it.
    for key in TRUTH_KEYS:
        if truth.get(key):
            source = Path(truth[key])
            if source.is_dir():
                for path in sorted(source.rglob("*")):
                    if path.is_file():
                        files[f"truth/{key}/{path.relative_to(source)}"] = path
            else:
                files[f"truth/{key}/{source.name}"] = source
            truth[key] = f"@STATE@/truth/{key}" + ("" if source.is_dir() else "/" + source.name)
    # Backing chains of the disks (hosts may sit on different chains): ship every backing file once.
    # Each row names the file below it.
    pins = pinned_inputs()
    rows, first = {}, {}
    for host in manifest["allnames"]:
        links = chain(state / "disks" / (host + ".qcow2"))
        if not links:
            raise SystemExit(f"{host} has no backing file")
        first[host] = links[0].name
        for path, below in zip(links, [*links[1:], None], strict=True):
            if path.name in rows:
                if rows[path.name]["path"] != path:
                    raise SystemExit(f"two backing files are named {path.name}")
                continue
            digest = sha256(path)
            row = dict(id=path.name, sha256=digest, below=below.name if below else None, path=path)
            if digest in pins:
                # A pinned input (the Ubuntu base image) travels as a world file: install puts it at its input
                # path, where the chain and agent-created instances find it, so a run needs no inputs fetch.
                row["input"] = pins[digest]
                files["world/" + pins[digest]] = path
            else:
                files["backing/" + path.name] = path
                row["file"] = "backing/" + path.name
            rows[path.name] = row
    backing = [{k: v for k, v in row.items() if k != "path"} for row in rows.values()]
    manifest["disks"] = {h: f"@STATE@/disks/{h}.qcow2" for h in manifest["allnames"]}
    roots = [(str(state), "@STATE@"), (str(WORLD), "@WORLD@")] + [tuple(r.split("=", 1)) for r in extra_roots]
    files.update(world_files(state, roots))
    edited = {
        "state/manifest.json": json.dumps(manifest, indent=2) + "\n",
        "state/starting-truth.json": json.dumps(truth, indent=2) + "\n",
    }
    for rel, path in files.items():
        if rel.startswith("state/") and path.suffix in (".json", ".jsonl") and path.parent == state:
            edited[rel] = rewrite(edited.get(rel, path.read_text()), roots)
    meta = dict(state=name, backing=backing, disk_names=manifest["allnames"], disk_first=first)
    scratch = Path(states) if out == "-" else Path(out).resolve().parent
    with (
        tempfile.TemporaryDirectory(dir=scratch, prefix=".bundle-ram-") as tmp,
        opened(out, write=True) as stream,
        tarfile.open(fileobj=stream, mode="w|") as tar,
    ):
        # The guests' memory: recompressed copies replace the build's gzip files (ram/<host>.gz -> ram/<host>.zst).
        for rel in [r for r in files if r.startswith("state/ram/") and r.endswith(".gz")]:
            dest = Path(tmp) / Path(rel).with_suffix(".zst").name
            recompress(files.pop(rel), dest)
            files[rel.removesuffix(".gz") + ".zst"] = dest
        sums = {
            rel: hashlib.sha256(edited[rel].encode()).hexdigest() if rel in edited else sha256(path)
            for rel, path in files.items()
        }

        def add_bytes(rel, data):
            info = tarfile.TarInfo(f"{TOP}/{rel}")
            info.size, info.mode = len(data), 0o644
            tar.addfile(info, io.BytesIO(data))

        add_bytes("bundle.json", (json.dumps(meta, indent=2) + "\n").encode())
        add_bytes("SHA256SUMS", "".join(f"{d}  {r}\n" for r, d in sorted(sums.items())).encode())
        for rel, path in files.items():
            if rel in edited:
                add_bytes(rel, edited[rel].encode())
            else:
                tar.add(path, arcname=f"{TOP}/{rel}", recursive=False)
    if out != "-":
        print(f"exported {name}: {len(files)} files to {out}")


def install(bundle, name):
    STATES.mkdir(parents=True, exist_ok=True)
    partial = STATES / f".install-{os.getpid()}"
    try:
        with opened(bundle, write=False) as stream, tarfile.open(fileobj=stream, mode="r|") as tar:
            tar.extractall(partial, filter="data")
        root = partial / TOP
        meta = json.loads((root / "bundle.json").read_text())
        name = name or meta["state"]
        final = STATES / name
        if final.exists():
            raise SystemExit(f"{final} already exists")
        for line in (root / "SHA256SUMS").read_text().splitlines():
            digest, rel = line.split("  ", 1)
            if sha256(root / rel) != digest:
                raise SystemExit(f"sha256 mismatch for {rel}; the bundle is damaged")
        # Build-tree files go to their places under world/; an existing different one (a key made by an
        # earlier revive, another world's seeds) is refused before anything moves.
        placed = sorted(p for p in (root / "world").rglob("*") if p.is_file())
        for path in placed:
            dest = WORLD / path.relative_to(root / "world")
            if dest.exists() and sha256(dest) != sha256(path):
                raise SystemExit(f"{dest} exists and differs from the bundle's copy; remove it and install again")
        for path in placed:
            dest = WORLD / path.relative_to(root / "world")
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(dest))
        # Lay out the final folder: the state itself, plus backing/ and truth/ inside it.
        shutil.move(str(root / "state"), str(final))
        for sub in ("backing", "truth"):
            if (root / sub).exists():
                shutil.move(str(root / sub), str(final / sub))
    finally:
        shutil.rmtree(partial, ignore_errors=True)
    roots = [("@STATE@", str(final)), ("@WORLD@", str(WORLD))]
    for path in final.glob("*.json*"):
        path.write_text(rewrite(path.read_text(), roots))
    # Point every chain at its new places: each shipped backing onto the file below it, then each disk onto its first.
    paths = {}
    for row in meta["backing"]:
        path = final / row["file"] if "file" in row else WORLD / row["input"]
        if "input" in row and not (path.exists() and sha256(path) == row["sha256"]):
            raise SystemExit(f"{path} is missing or differs from the pinned input")
        paths[row["id"]] = path
    for row in meta["backing"]:
        if row["below"] and paths[row["id"]].is_relative_to(final):
            rebase(paths[row["id"]], paths[row["below"]])
    for host in meta["disk_names"]:
        rebase(final / "disks" / (host + ".qcow2"), paths[meta["disk_first"][host]])
    # Saved worlds are immutable: runs write to their own overlays.
    for path in [*final.rglob("*.qcow2"), *paths.values()]:
        path.chmod(0o444 if path.is_relative_to(WORLD / "base") else 0o400)
    print(f"installed {name} at {final}")


def rebase(disk, backing):
    disk.chmod(0o600)
    subprocess.run(
        ["qemu-img", "rebase", "-u", "-f", "qcow2", "-F", fmt(backing), "-b", str(backing), str(disk)], check=True
    )


def fmt(path):
    return json.loads(subprocess.check_output(["qemu-img", "info", "-U", "--output=json", str(path)], text=True))[
        "format"
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    e = sub.add_parser("export")
    e.add_argument("state")
    e.add_argument("out")
    e.add_argument(
        "--from", dest="states", default=STATES, help="folder that holds the saved state (default world/vm/states)"
    )
    e.add_argument(
        "--root",
        action="append",
        default=[],
        help="also write OLD paths as NEW (for example a build tree path as @WORLD@)",
    )
    i = sub.add_parser("install")
    i.add_argument("bundle")
    i.add_argument("--name")
    args = parser.parse_args()
    if args.command == "export":
        export(args.state, args.out, args.root, args.states)
    else:
        install(args.bundle, args.name)


if __name__ == "__main__":
    main()
