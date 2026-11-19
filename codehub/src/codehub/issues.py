from codehub.domain import DomainError, identifier, text


def issue_document(op, actor_id, existing):
    state = op.payload.get("state")
    if state not in {"open", "closed"}:
        raise DomainError("Issue state must be open or closed")
    body = op.payload.get("body", "")
    if not isinstance(body, str) or len(body) > 16000 or "\x00" in body:
        raise DomainError("Issue body must be at most 16000 characters")
    return {
        "title": text(op.payload, "title", 200),
        "body": body,
        "state": state,
        "author_id": existing.get("author_id", actor_id) if existing else actor_id,
    }


def comment_document(op, actor_id, lookup):
    issue_id = identifier(op.payload.get("issue_id"), "issue_id")
    issue = lookup("issues", issue_id)
    if not issue or issue["project_id"] != op.project_id:
        raise DomainError("Comment must reference an issue in this project")
    return {"issue_id": issue_id, "body": text(op.payload, "body", 16000), "author_id": actor_id}
