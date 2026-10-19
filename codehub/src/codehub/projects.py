import re

from codehub.domain import DomainError, slug, text


def ref_name(payload, field):
    value = text(payload, field, 200)
    if not value.startswith("refs/heads/") or any(p in value for p in ("..", "@{", "\\", "//")):
        raise DomainError(f"{field} must be a branch ref")
    if re.search(r"[\s~^:?*\[\x00-\x1f]", value) or value.endswith(("/", ".", ".lock")):
        raise DomainError(f"{field} contains an invalid ref name")
    if any(part.startswith(".") or part.endswith(".lock") for part in value.split("/")) or "\x7f" in value:
        raise DomainError(f"{field} contains an invalid ref component")
    return value


def project_document(op, actor_id, existing):
    if op.project_id != op.entity_id:
        raise DomainError("Project identity must equal project_id")
    return {
        "slug": slug(op.payload),
        "name": text(op.payload, "name", 200),
        "default_ref": ref_name(op.payload, "default_ref"),
        "author_id": existing.get("author_id", actor_id) if existing else actor_id,
    }
