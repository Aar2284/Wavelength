import json
import os
import tempfile
import time
from pathlib import Path


class PollStateError(Exception):
    pass


class CursorStateError(PollStateError):
    pass


def _read_json_object(path, error_cls):
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise error_cls(f"corrupt poll state file: {path}") from exc
    if not isinstance(data, dict):
        raise error_cls(f"poll state must be a JSON object: {path}")
    return data


def _write_json_object(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=".poll_state-", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


class CursorStore:
    def __init__(self, path):
        self.path = Path(path)
        self._cursors = self._load()

    def _load(self):
        data = _read_json_object(self.path, CursorStateError)
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
        _write_json_object(self.path, self._cursors)
        return True


class SeenStore:
    def __init__(self, path):
        self.path = Path(path)
        self._seen = self._load()

    def _load(self):
        data = _read_json_object(self.path, PollStateError)
        try:
            return {str(release_id): int(uts) for release_id, uts in data.items()}
        except (TypeError, ValueError) as exc:
            raise PollStateError(f"non-integer seen value in {self.path}") from exc

    def __contains__(self, release_id):
        return str(release_id) in self._seen

    def __len__(self):
        return len(self._seen)

    def record(self, release_ids, uts=None):
        uts = int(time.time()) if uts is None else int(uts)
        added = False
        for release_id in release_ids:
            key = str(release_id)
            if key not in self._seen:
                self._seen[key] = uts
                added = True
        if added:
            _write_json_object(self.path, self._seen)
        return added
