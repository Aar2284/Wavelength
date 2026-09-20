from datetime import date

import pytest
import requests

from src.pollers.catalog_poller import (
    MusicBrainzError,
    RateLimiter,
    artist_credit_name,
    build_query,
    build_release_event,
    extract_releases,
    fetch_releases,
    poll_catalog,
)
from src.pollers.state import SeenStore

SETTINGS = {
    "base_url": "https://musicbrainz.org/ws/2/",
    "release_search_path": "release",
    "user_agent_env": "MB_USER_AGENT",
    "rate_limit_sec": 1.0,
}

CFG = {
    "musicbrainz": SETTINGS,
    "kafka": {"topics": {"catalog_releases": "catalog-releases"}},
}

SAMPLE_PAYLOAD = {
    "count": 4,
    "releases": [
        {
            "id": "rel-1",
            "title": "Alpha",
            "artist-credit": [{"name": "Solo Act"}],
            "date": "2026-11-21",
        },
        {
            "id": "rel-2",
            "title": "Beta",
            "artist-credit": [
                {"name": "Part A", "joinphrase": " & "},
                {"name": "Part B"},
            ],
            "date": "2026",
        },
        {"title": "Missing Id", "artist-credit": [{"name": "Nobody"}]},
        {"id": "rel-3", "title": "Gamma", "artist-credit": []},
    ],
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

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "params": dict(params or {}),
                "headers": dict(headers or {}),
                "timeout": timeout,
            }
        )
        if self.error is not None:
            raise self.error
        return FakeResponse(self.payload)


class FakeProducer:
    def __init__(self, fail_flush=False):
        self.sent = []
        self.fail_flush = fail_flush

    def send(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})

    def flush(self):
        if self.fail_flush:
            raise RuntimeError("broker unavailable")


def test_build_query_covers_yesterday_to_today():
    assert build_query(date(2026, 11, 21)) == "date:[2026-11-20 TO 2026-11-21]"


def test_artist_credit_joins_with_joinphrases():
    credit = [{"name": "Part A", "joinphrase": " & "}, {"name": "Part B"}]
    assert artist_credit_name(credit) == "Part A & Part B"
    assert artist_credit_name([]) == ""
    assert artist_credit_name(None) == ""


def test_extract_releases_normalizes_and_skips_missing_id():
    releases = extract_releases(SAMPLE_PAYLOAD)
    assert [r["releaseId"] for r in releases] == ["rel-1", "rel-2", "rel-3"]
    assert releases[0] == {
        "releaseId": "rel-1",
        "title": "Alpha",
        "artist": "Solo Act",
        "releaseDate": "2026-11-21",
    }
    assert releases[1]["artist"] == "Part A & Part B"
    assert releases[1]["releaseDate"] == "2026"
    assert releases[2]["artist"] == ""
    assert releases[2]["releaseDate"] is None


def test_build_release_event_matches_smoke_schema():
    release = {
        "releaseId": "rel-1",
        "title": "Alpha",
        "artist": "Solo Act",
        "releaseDate": "2026-11-21",
    }
    event = build_release_event(release)
    assert set(event) == {"releaseId", "title", "artist", "releaseDate", "sourceTag"}
    assert event["sourceTag"] == "live"
    assert "sourceTag" not in release


def test_fetch_sends_user_agent_and_search_params():
    http = FakeHttp(payload=SAMPLE_PAYLOAD)
    fetch_releases(SETTINGS, "Wavelength/1.0 (me@example.com)", "date:[x TO y]", http=http)
    call = http.calls[0]
    assert call["url"] == "https://musicbrainz.org/ws/2/release/"
    assert call["headers"]["User-Agent"] == "Wavelength/1.0 (me@example.com)"
    assert call["params"] == {"query": "date:[x TO y]", "fmt": "json", "limit": 100}


def test_fetch_raises_on_api_error():
    http = FakeHttp(payload={"error": 500, "message": "Maintenance"})
    with pytest.raises(MusicBrainzError, match="Maintenance"):
        fetch_releases(SETTINGS, "ua", "date:[x TO y]", http=http)


def test_fetch_propagates_transport_errors():
    http = FakeHttp(error=requests.ConnectionError("offline"))
    with pytest.raises(requests.ConnectionError):
        fetch_releases(SETTINGS, "ua", "date:[x TO y]", http=http)


def test_poll_publishes_unseen_and_records_them(tmp_path):
    seen = SeenStore(tmp_path / "catalog_seen.json")
    producer = FakeProducer()
    http = FakeHttp(payload=SAMPLE_PAYLOAD)

    published, matched, total = poll_catalog(
        CFG, "ua", seen, producer, http=http, today=date(2026, 11, 21)
    )

    assert (published, matched, total) == (3, 3, 3)
    assert [m["topic"] for m in producer.sent] == ["catalog-releases"] * 3
    assert [m["key"] for m in producer.sent] == ["rel-1", "rel-2", "rel-3"]
    assert all(m["value"]["sourceTag"] == "live" for m in producer.sent)
    assert (tmp_path / "catalog_seen.json").exists()


def test_poll_republishes_nothing_on_second_cycle(tmp_path):
    seen = SeenStore(tmp_path / "catalog_seen.json")
    producer = FakeProducer()
    http = FakeHttp(payload=SAMPLE_PAYLOAD)

    poll_catalog(CFG, "ua", seen, producer, http=http, today=date(2026, 11, 21))
    published, matched, total = poll_catalog(
        CFG, "ua", seen, producer, http=http, today=date(2026, 11, 22)
    )

    assert (published, matched, total) == (0, 3, 3)
    assert len(producer.sent) == 3


def test_poll_does_not_record_when_flush_fails(tmp_path):
    seen = SeenStore(tmp_path / "catalog_seen.json")
    producer = FakeProducer(fail_flush=True)
    http = FakeHttp(payload=SAMPLE_PAYLOAD)

    with pytest.raises(RuntimeError):
        poll_catalog(CFG, "ua", seen, producer, http=http, today=date(2026, 11, 21))

    assert len(seen) == 0


def test_rate_limiter_enforces_minimum_interval():
    limiter = RateLimiter(0.05)
    limiter.wait()
    limiter.wait()
    assert limiter._last_request is not None
