import json
import os
import tempfile
from pathlib import Path


class CursorStateError(Exception):
    pass


class CursorStore:
    def __init__(self, path):
        self.path = Path(path)
        self._cursors = self._load()

    def _load(self):
        if not self.path.exists():
            return {}
        try:
            with open(self.path, encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise CursorStateError(f"corrupt cursor file: {self.path}") from exc
        if not isinstance(data, dict):
            raise CursorStateError(f"cursor file must be a JSON object: {self.path}")
        try:
            return {str(username): int(uts) for username, uts in data.items()}
        except (TypeError, ValueError) as exc:
            raise CursorStateError(f"non-integer cursor value in {self.path}") from exc

    def get(self, username):
        return self._cursors.get(username)

    def advance(self, username, uts):
        uts = int(uts)
        current = self._cursors.get(username)
        if current is not None and uts <= current:
            return False
        self._cursors[username] = uts
        self._save()
        return True

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temp_name = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=".poll_state-", suffix=".tmp"
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(self._cursors, handle, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(temp_name, self.path)
        except BaseException:
            try:
                os.unlink(temp_name)
            except OSError:
                pass
            raise
