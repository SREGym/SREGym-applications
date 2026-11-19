"""Explicit schema migration; API startup never creates or resets domain data."""

import sys
from pathlib import Path

from codehub.config import Settings
from codehub.storage.mysql import MySQLStorage


if __name__ == "__main__":
    MySQLStorage(Settings.load()).migrate(Path(sys.argv[1]))
