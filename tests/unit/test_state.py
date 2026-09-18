import json

import pytest

from src.pollers.state import CursorStateError, CursorStore


def test_fresh_store_returns_none(tmp_path):
    store = CursorStore(tmp_path / "state" / "poll_state.json")
    assert store.get("alice") is None


def test_advance_then_get(tmp_path):
    store = CursorStore(tmp_path / "poll_state.json")
    assert store.advance("alice", 1700000000) is True
    assert store.get("alice") == 1700000000
    assert store.get("bob") is None


def test_persists_across_instances(tmp_path):
    path = tmp_path / "poll_state.json"
    CursorStore(path).advance("alice", 1700000000)
    reloaded = CursorStore(path)
    assert reloaded.get("alice") == 1700000000


def test_advance_ignores_older_and_equal(tmp_path):
    store = CursorStore(tmp_path / "poll_state.json")
    store.advance("alice", 1700000000)
    assert store.advance("alice", 1699999999) is False
    assert store.advance("alice", 1700000000) is False
    assert store.get("alice") == 1700000000


def test_missing_parent_directories_are_created(tmp_path):
    path = tmp_path / "deep" / "nested" / "poll_state.json"
    store = CursorStore(path)
    store.advance("alice", 42)
    assert path.exists()


def test_no_temp_files_left_behind(tmp_path):
    path = tmp_path / "poll_state.json"
    store = CursorStore(path)
    store.advance("alice", 1)
    store.advance("bob", 2)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["poll_state.json"]


def test_file_is_valid_json_object(tmp_path):
    path = tmp_path / "poll_state.json"
    CursorStore(path).advance("alice", 1700000000)
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data == {"alice": 1700000000}


def test_corrupt_file_raises(tmp_path):
    path = tmp_path / "poll_state.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(CursorStateError):
        CursorStore(path)


def test_non_object_root_raises(tmp_path):
    path = tmp_path / "poll_state.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(CursorStateError):
        CursorStore(path)


def test_non_integer_cursor_raises(tmp_path):
    path = tmp_path / "poll_state.json"
    path.write_text('{"alice": "yesterday"}', encoding="utf-8")
    with pytest.raises(CursorStateError):
        CursorStore(path)
