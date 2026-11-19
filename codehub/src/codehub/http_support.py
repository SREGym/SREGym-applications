"""Authentication helpers for public and service protocols."""

import base64
import hmac
import logging
import os

from fastapi import Request

from codehub.config import secret
from codehub.domain import Forbidden


def bearer(request: Request) -> str:
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise Forbidden("Bearer credentials are required")
    return token


def git_credential(request: Request) -> str:
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() == "bearer":
        return token
    if scheme.lower() == "basic":
        try:
            decoded = base64.b64decode(token, validate=True).decode("utf-8")
            _, separator, password = decoded.partition(":")
            if separator and password:
                return password
        except (ValueError, UnicodeError):
            pass
    raise Forbidden("Git HTTP credentials are required")


def require_service(request: Request):
    if not hmac.compare_digest(bearer(request), secret("CODEHUB_SERVICE_TOKEN")):
        raise Forbidden("Service credentials are required")


def service_headers() -> dict:
    return {"Authorization": f"Bearer {secret('CODEHUB_SERVICE_TOKEN')}"}


def serve(app):
    import uvicorn

    logging.basicConfig(level=os.environ.get("CODEHUB_LOG_LEVEL", "INFO"), format="%(message)s")
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), access_log=False)
