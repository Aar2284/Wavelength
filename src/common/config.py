import json
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "settings.json"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return json.load(f)
