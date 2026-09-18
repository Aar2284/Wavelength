import pytest
import requests

from src.pollers.events_poller import (
    LastfmError,
    build_event,
    fetch_recent_tracks,
    poll_user,
    read_seed_users,
    select_new_tracks,
)
from src.pollers.state import CursorStore

SETTINGS = {
    "base_url": "https://ws.audioscrobbler.com/2.0/",
    "method_recent_tracks": "user.getRecentTracks",
    "tracks_per_request": 50,
}

NEWEST_UTS = 1700000000
OLDER_UTS = 1699999000

SAMPLE_RESPONSE = {
    "recenttracks": {
        "track": [
            {
                "name": "Now Playing Song",
                "artist": {"#text": "Live Artist"},
                "mbid": "",
                "@attr": {"nowplaying": "true"},
            },
            {
                "name": "New Song",
                "artist": {"#text": "New Artist"},
                "mbid": "mbid-new",
                "date": {"uts": str(NEWEST_UTS)},
            },
            {
                "name": "Old Song",
                "artist": {"#text": "Old Artist"},
                "mbid": "mbid-old",
                "date": {"uts": str(OLDER_UTS)},
            },
        ],
        "@attr": {"user": "alice", "total": "3"},
    }
}


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeHttp:
    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        if self.error is not None:
            raise self.error
        return FakeResponse(self.payload)


class FakeProducer:
    def __init__(self, fail_flush=False):
        self.sent = []
        self.fail_flush = fail_flush
        self.flushed = False

    def send(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})

    def flush(self):
        if self.fail_flush:
            raise RuntimeError("broker unavailable")
        self.flushed = True


def test_read_seed_users_skips_blanks_and_comments(tmp_path):
    path = tmp_path / "seed_users.txt"
    path.write_text("# comment\nalice\n\n   \nbob  \n", encoding="utf-8")
    assert read_seed_users(path) == ["alice", "bob"]


def test_build_event_maps_lastfm_track():
    track = SAMPLE_RESPONSE["recenttracks"]["track"][1]
    event = build_event("alice", track)
    assert event == {
        "userId": "alice",
        "trackName": "New Song",
        "artistName": "New Artist",
        "timestamp": "2023-11-14T22:13:20+00:00",
        "nowPlaying": False,
        "source": "live",
    }


def test_build_event_skips_track_without_date():
    now_playing = SAMPLE_RESPONSE["recenttracks"]["track"][0]
    assert build_event("alice", now_playing) is None


def test_select_new_tracks_without_cursor_keeps_all_dated():
    tracks = SAMPLE_RESPONSE["recenttracks"]["track"]
    fresh = select_new_tracks(tracks)
    assert [uts for uts, _track in fresh] == [NEWEST_UTS, OLDER_UTS]


def test_select_new_tracks_filters_cursor_and_now_playing():
    tracks = SAMPLE_RESPONSE["recenttracks"]["track"]
    fresh = select_new_tracks(tracks, cursor=OLDER_UTS)
    assert [uts for uts, _track in fresh] == [NEWEST_UTS]


def test_fetch_first_run_omits_from_param():
    http = FakeHttp(payload=SAMPLE_RESPONSE)
    fetch_recent_tracks(SETTINGS, "key", "alice", cursor=None, http=http)
    assert "from" not in http.calls[0]["params"]
    assert http.calls[0]["params"]["user"] == "alice"
    assert http.calls[0]["params"]["api_key"] == "key"


def test_fetch_passes_cursor_as_from():
    http = FakeHttp(payload=SAMPLE_RESPONSE)
    fetch_recent_tracks(SETTINGS, "key", "alice", cursor=NEWEST_UTS, http=http)
    assert http.calls[0]["params"]["from"] == NEWEST_UTS


def test_fetch_raises_on_api_error():
    http = FakeHttp(payload={"error": 6, "message": "Invalid API key"})
    with pytest.raises(LastfmError, match="Invalid API key"):
        fetch_recent_tracks(SETTINGS, "key", "alice", http=http)


def test_poll_user_publishes_new_tracks_and_advances_cursor(tmp_path):
    store = CursorStore(tmp_path / "poll_state.json")
    producer = FakeProducer()
    http = FakeHttp(payload=SAMPLE_RESPONSE)

    sent = poll_user(SETTINGS, "key", "alice", store, producer, "listening-events", http=http)

    assert sent == 2
    assert producer.flushed is True
    assert [msg["topic"] for msg in producer.sent] == ["listening-events"] * 2
    assert [msg["key"] for msg in producer.sent] == ["alice", "alice"]
    assert [msg["value"]["trackName"] for msg in producer.sent] == ["New Song", "Old Song"]
    assert all(msg["value"]["source"] == "live" for msg in producer.sent)
    assert store.get("alice") == NEWEST_UTS


def test_poll_user_resumes_from_stored_cursor(tmp_path):
    store = CursorStore(tmp_path / "poll_state.json")
    store.advance("alice", NEWEST_UTS)
    producer = FakeProducer()
    http = FakeHttp(payload=SAMPLE_RESPONSE)

    sent = poll_user(SETTINGS, "key", "alice", store, producer, "listening-events", http=http)

    assert http.calls[0]["params"]["from"] == NEWEST_UTS
    assert sent == 0
    assert producer.sent == []


def test_poll_user_does_not_advance_when_fetch_fails(tmp_path):
    store = CursorStore(tmp_path / "poll_state.json")
    producer = FakeProducer()
    http = FakeHttp(error=requests.ConnectionError("broker/API down"))

    with pytest.raises(requests.ConnectionError):
        poll_user(SETTINGS, "key", "alice", store, producer, "listening-events", http=http)

    assert store.get("alice") is None
    assert producer.sent == []


def test_poll_user_does_not_advance_when_flush_fails(tmp_path):
    store = CursorStore(tmp_path / "poll_state.json")
    producer = FakeProducer(fail_flush=True)
    http = FakeHttp(payload=SAMPLE_RESPONSE)

    with pytest.raises(RuntimeError):
        poll_user(SETTINGS, "key", "alice", store, producer, "listening-events", http=http)

    assert store.get("alice") is None
