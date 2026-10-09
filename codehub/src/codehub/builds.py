"""Reproducible Python-source validation and bundles from actual Git trees."""

import ast
import hashlib
import io
import tarfile
import zipfile
from pathlib import PurePosixPath

from codehub.domain import DomainError, canonical

MAX_ARCHIVE = 32 * 1024 * 1024
MAX_FILE = 2 * 1024 * 1024
MAX_FILES = 10000


def build_bundle(archive: bytes, *, project_id: str, commit_sha: str) -> tuple[bytes, dict]:
    if len(archive) > MAX_ARCHIVE:
        raise DomainError("Build input exceeds 32 MiB")
    files = {}
    total = 0
    try:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as source:
            for member in source:
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts or member.issym() or member.islnk():
                    raise DomainError("Build trees cannot contain links or unsafe paths")
                if member.isdir():
                    continue
                if not member.isfile() or member.size > MAX_FILE or len(files) >= MAX_FILES:
                    raise DomainError("Build tree exceeds file limits")
                total += member.size
                if total > MAX_ARCHIVE:
                    raise DomainError("Build tree exceeds content capacity")
                reader = source.extractfile(member)
                content = reader.read(MAX_FILE + 1) if reader else b""
                if len(content) != member.size or len(content) > MAX_FILE:
                    raise DomainError("Build tree contains incomplete or oversized content")
                if path.suffix == ".py":
                    try:
                        ast.parse(content.decode("utf-8"), filename=member.name)
                    except (UnicodeError, SyntaxError, RecursionError) as exc:
                        raise DomainError(f"Python source validation failed: {member.name}") from exc
                if member.name in files or member.name == "CODEHUB-BUILD.json":
                    raise DomainError("Build tree contains duplicate or reserved paths")
                files[member.name] = content
    except tarfile.TarError as exc:
        raise DomainError("Invalid Git archive") from exc
    if not files:
        raise DomainError("Build requires a nonempty source tree")
    manifest = {
        "project_id": project_id,
        "commit_sha": commit_sha,
        "builder": "python-validate-bundle-v1",
        "files": {name: hashlib.sha256(content).hexdigest() for name, content in sorted(files.items())},
    }
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, content in [*sorted(files.items()), ("CODEHUB-BUILD.json", canonical(manifest).encode())]:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            bundle.writestr(info, content)
    return output.getvalue(), manifest
