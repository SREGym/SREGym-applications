"""Ordinary deployment configuration and credential loading."""

import os
from dataclasses import dataclass
from pathlib import Path


def secret(name: str, *, optional: bool = False) -> str:
    filename = os.environ.get(f"{name}_FILE")
    value = Path(filename).read_text(encoding="utf-8").strip() if filename else os.environ.get(name, "")
    if not value and not optional:
        raise RuntimeError(f"Required credential {name}_FILE is missing")
    return value


@dataclass(frozen=True)
class Settings:
    mysql_host: str
    mysql_port: int
    mysql_database: str
    mysql_user: str
    mysql_password: str
    region: str
    repository_url: str
    search_urls: tuple[str, ...]
    delivery_url: str
    data_dir: Path
    mysql_read_host: str | None = None
    mysql_read_port: int | None = None
    routes_file: Path | None = None
    delivery_urls: tuple[str, ...] = ()

    @classmethod
    def load(cls) -> "Settings":
        return cls(
            mysql_host=os.environ.get("CODEHUB_MYSQL_HOST", "mysql"),
            mysql_port=int(os.environ.get("CODEHUB_MYSQL_PORT", "3306")),
            mysql_database=os.environ.get("CODEHUB_MYSQL_DATABASE", "codehub"),
            mysql_user=os.environ.get("CODEHUB_MYSQL_USER", "codehub"),
            mysql_password=secret("CODEHUB_MYSQL_PASSWORD"),
            region=os.environ.get("CODEHUB_REGION", "region-a"),
            repository_url=os.environ.get("CODEHUB_REPOSITORY_URL", "http://repository:8080").rstrip("/"),
            search_urls=tuple(
                part.strip().rstrip("/")
                for part in os.environ.get("CODEHUB_SEARCH_URLS", "http://search:8080").split(",")
                if part.strip()
            ),
            delivery_url=os.environ.get("CODEHUB_DELIVERY_URL", "http://delivery:8080").rstrip("/"),
            delivery_urls=tuple(
                part.strip().rstrip("/")
                for part in os.environ.get(
                    "CODEHUB_DELIVERY_URLS", os.environ.get("CODEHUB_DELIVERY_URL", "http://delivery:8080")
                ).split(",")
                if part.strip()
            ),
            data_dir=Path(os.environ.get("CODEHUB_DATA_DIR", "/var/lib/codehub")),
            mysql_read_host=os.environ.get("CODEHUB_MYSQL_READ_HOST"),
            mysql_read_port=int(
                os.environ.get("CODEHUB_MYSQL_READ_PORT", os.environ.get("CODEHUB_MYSQL_PORT", "3306"))
            ),
            routes_file=Path(os.environ["CODEHUB_DATABASE_ROUTES_FILE"])
            if os.environ.get("CODEHUB_DATABASE_ROUTES_FILE")
            else None,
        )
