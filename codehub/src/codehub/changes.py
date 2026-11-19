from codehub.domain import DomainError, Forbidden, identifier, sha, text
from codehub.projects import ref_name


def change_document(op, actor_id, existing):
    state = op.payload.get("state")
    if state not in {"open", "closed", "merged"}:
        raise DomainError("Change state must be open, closed or merged")
    if existing and existing["state"] == "merged" and state != "merged":
        raise DomainError("A merged change cannot be reopened")
    return {
        "title": text(op.payload, "title", 200),
        "head_sha": sha(op.payload, "head_sha"),
        "base_sha": sha(op.payload, "base_sha"),
        "head_ref": ref_name(op.payload, "head_ref"),
        "base_ref": ref_name(op.payload, "base_ref"),
        "state": state,
        "author_id": existing.get("author_id", actor_id) if existing else actor_id,
    }


def review_document(op, actor_id, lookup):
    change_id = identifier(op.payload.get("change_id"), "change_id")
    change = lookup("changes", change_id)
    if not change or change["project_id"] != op.project_id or change["state"] != "open":
        raise DomainError("Review must reference an open change in this project")
    head = sha(op.payload, "head_sha")
    if head != change["head_sha"]:
        raise DomainError("Review must identify the current change head")
    verdict = op.payload.get("verdict")
    if verdict not in {"approve", "request_changes", "comment"}:
        raise DomainError("Unsupported review verdict")
    if verdict == "approve" and change["author_id"] == actor_id:
        raise Forbidden("Authors cannot approve their own change")
    body = op.payload.get("body", "")
    if not isinstance(body, str) or len(body) > 16000:
        raise DomainError("Review body must be at most 16000 characters")
    return {"change_id": change_id, "head_sha": head, "verdict": verdict, "body": body, "author_id": actor_id}
