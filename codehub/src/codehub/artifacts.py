"""Content-addressed artifact persistence with atomic publication."""

import hashlib
import os
import re
from pathlib import Path
from tempfile import NamedTemporaryFile

from codehub.domain import Conflict, DomainError, NotFound


class ArtifactStore:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def path(self, checksum):
        if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise DomainError("Artifact checksum must be SHA256")
        return self.root / checksum

    def put(self, checksum, content):
        if len(content) > 32 * 1024 * 1024 or hashlib.sha256(content).hexdigest() != checksum:
            raise DomainError("Artifact exceeds capacity or checksum does not match")
        path = self.path(checksum)
        if path.exists():
            if hashlib.sha256(path.read_bytes()).hexdigest() != checksum:
                raise Conflict("Existing artifact content is corrupt")
            return
        temporary = None
        try:
            with NamedTemporaryFile(dir=self.root, delete=False) as output:
                temporary = Path(output.name)
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
        finally:
            if temporary and temporary.exists():
                temporary.unlink()

    def get(self, checksum):
        path = self.path(checksum)
        if not path.exists():
            raise NotFound("Artifact does not exist")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != checksum:
            raise Conflict("Artifact content is corrupt")
        return content
