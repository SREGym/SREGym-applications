import hashlib
import io
import json
import subprocess
import tarfile
import zipfile
from uuid import uuid4

import pytest

from codehub.artifacts import ArtifactStore
from codehub.builds import build_bundle
from codehub.domain import DomainError
from codehub.repository import Repository


def git(path, *args):
    return (
        subprocess.run(
            ["git", "-c", "commit.gpgsign=false", "-C", str(path), *args], check=True, capture_output=True, timeout=15
        )
        .stdout.decode()
        .strip()
    )


def test_real_git_push_archive_build_and_persistent_artifact(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-b", "main")
    git(source, "config", "user.name", "Morgan Lee")
    git(source, "config", "user.email", "morgan@example.test")
    (source / "routing.py").write_text("def choose_region(regions):\n    return min(regions)\n", encoding="utf-8")
    git(source, "add", "routing.py")
    git(source, "commit", "-m", "Choose a stable region")
    commit = git(source, "rev-parse", "HEAD")
    project = str(uuid4())
    repositories = Repository(tmp_path / "repositories")
    path = repositories.ensure(project)
    git(source, "push", str(path), "HEAD:refs/heads/main")
    assert repositories.commit(project, commit)["commit_sha"] == commit
    assert repositories.fsck(project)["refs"]["refs/heads/main"] == commit
    content, manifest = build_bundle(repositories.archive(project, commit), project_id=project, commit_sha=commit)
    repeated, _ = build_bundle(repositories.archive(project, commit), project_id=project, commit_sha=commit)
    assert content == repeated
    assert manifest["files"]["routing.py"] == hashlib.sha256((source / "routing.py").read_bytes()).hexdigest()
    with zipfile.ZipFile(io.BytesIO(content)) as bundle:
        assert json.loads(bundle.read("CODEHUB-BUILD.json"))["commit_sha"] == commit
        assert bundle.read("routing.py") == (source / "routing.py").read_bytes()
    checksum = hashlib.sha256(content).hexdigest()
    ArtifactStore(tmp_path / "artifacts").put(checksum, content)
    assert ArtifactStore(tmp_path / "artifacts").get(checksum) == content
    clone = tmp_path / "clone"
    subprocess.run(
        ["git", "clone", "--branch", "main", str(path), str(clone)], check=True, capture_output=True, timeout=15
    )
    assert (clone / "routing.py").read_bytes() == (source / "routing.py").read_bytes()


def archive_file(name, content=b"print('hello')\n", *, link=False):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        info = tarfile.TarInfo(name)
        if link:
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            archive.addfile(info)
        else:
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


@pytest.mark.parametrize("name", ["../outside.py", "/absolute.py"])
def test_build_rejects_unsafe_tree_paths(name):
    with pytest.raises(DomainError):
        build_bundle(archive_file(name), project_id=str(uuid4()), commit_sha="a" * 40)


def test_build_rejects_links_and_invalid_python():
    with pytest.raises(DomainError):
        build_bundle(archive_file("pointer", link=True), project_id=str(uuid4()), commit_sha="a" * 40)
    with pytest.raises(DomainError):
        build_bundle(archive_file("broken.py", b"def invalid(:\n"), project_id=str(uuid4()), commit_sha="a" * 40)
