from urllib.parse import urlsplit

from codehub.domain import KINDS, DomainError, text


def webhook_document(op, actor_id, existing):
    url = text(op.payload, "url", 2048)
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise DomainError("Webhook requires an HTTP(S) URL without embedded credentials")
    try:
        parsed.port
    except ValueError as exc:
        raise DomainError("Webhook URL has an invalid port") from exc
    if parsed.fragment:
        raise DomainError("Webhook URL cannot contain a fragment")
    events = op.payload.get("events")
    if not isinstance(events, list) or not events or any(event not in KINDS for event in events):
        raise DomainError("Webhook events must be a nonempty list of supported operation kinds")
    enabled = op.payload.get("enabled", True)
    if type(enabled) is not bool:
        raise DomainError("Webhook enabled must be a boolean")
    return {
        "url": url,
        "events": sorted(set(events)),
        "enabled": enabled,
        "author_id": existing.get("author_id", actor_id) if existing else actor_id,
    }
