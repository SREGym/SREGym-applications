"""Smart Meter's JSON inputs name files under the world folder as @WORLD@/...; render them for this install."""

import json
from pathlib import Path

WORLD = Path(__file__).resolve().parent


def render(text):
    return text.replace("@WORLD@", str(WORLD))


def load(path):
    return json.loads(render(Path(path).read_text()))
