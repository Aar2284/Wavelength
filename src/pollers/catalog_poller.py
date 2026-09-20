import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from kafka import KafkaProducer

from src.common.config import load_config
from src.common.serde import JsonSerializer, Utf8StringSerializer
from src.pollers.state import SeenStore

ROOT = Path(__file__).resolve().parents[2]


class MusicBrainzError(Exception):
    pass


class RateLimiter:
    def __init__(self, min_interval_sec):
        self.min_interval = float(min_interval_sec)
        self._last_request = None

    def wait(self):
        if self._last_request is not None:
            elapsed = time.monotonic() - self._last_request
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
        self._last_request = time.monotonic()


def build_query(window_end):
    window_start = window_end - timedelta(days=1)
    return f"date:[{window_start.isoformat()} TO {window_end.isoformat()}]"


def artist_credit_name(artist_credit):
    parts = []
    for entry in artist_credit or []:
        parts.append(entry.get("name") or "")
        parts.append(entry.get("joinphrase") or "")
    return "".join(parts).strip()


def extract_releases(payload):
    releases = []
    for release in payload.get("releases", []):
        release_id = release.get("id")
        if not release_id:
            continue
        releases.append(
            {
                "releaseId": release_id,
                "title": release.get("title"),
                "artist": artist_credit_name(release.get("artist-credit")),
                "releaseDate": release.get("date"),
            }
        )
    return releases


def build_release_event(release, source_tag="live"):
    event = dict(release)
    event["sourceTag"] = source_tag
    return event


def fetch_releases(settings, user_agent, query, http=None, limiter=None):
    http = http if http is not None else requests
    if limiter is not None:
        limiter.wait()
    url = settings["base_url"] + settings["release_search_path"].strip("/") + "/"
    params = {"query": query, "fmt": "json", "limit": 100}
    response = http.get(
        url, params=params, headers={"User-Agent": user_agent}, timeout=20
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise MusicBrainzError(
            f"release search error {payload['error']}: {payload.get('message', 'unknown')}"
        )
    return payload


def poll_catalog(cfg, user_agent, seen, producer, http=None, limiter=None, today=None):
    settings = cfg["musicbrainz"]
    topic = cfg["kafka"]["topics"]["catalog_releases"]
    if today is None:
        today = datetime.now(timezone.utc).date()
    payload = fetch_releases(
        settings, user_agent, build_query(today), http=http, limiter=limiter
    )
    releases = extract_releases(payload)
    fresh = [r for r in releases if r["releaseId"] not in seen]
    for release in fresh:
        producer.send(
            topic, key=release["releaseId"], value=build_release_event(release)
        )
    producer.flush()
    if fresh:
        seen.record([r["releaseId"] for r in fresh])
    return len(fresh), len(releases), len(seen)


def make_producer(cfg):
    return KafkaProducer(
        bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
        key_serializer=Utf8StringSerializer(),
        value_serializer=JsonSerializer(),
    )


def run_cycle(cfg, user_agent, seen, producer, limiter, http=None):
    published, matched, total = poll_catalog(
        cfg, user_agent, seen, producer, http=http, limiter=limiter
    )
    print(
        f"[catalog-poller] cycle done: {published} new of {matched} matched, "
        f"{total} seen in total"
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description="MusicBrainz catalog-releases poller")
    parser.add_argument(
        "--once",
        action="store_true",
        help="run a single poll cycle and exit instead of scheduling",
    )
    args = parser.parse_args(argv)

    load_dotenv()
    cfg = load_config()
    settings = cfg["musicbrainz"]
    user_agent = os.environ.get(settings["user_agent_env"])
    if not user_agent:
        raise SystemExit(f"missing {settings['user_agent_env']} in .env")

    seen = SeenStore(ROOT / cfg["polling"]["state_dir"] / "catalog_seen.json")
    producer = make_producer(cfg)
    limiter = RateLimiter(settings["rate_limit_sec"])
    interval = int(cfg["polling"]["catalog_interval_sec"])

    try:
        if args.once:
            run_cycle(cfg, user_agent, seen, producer, limiter)
            return
        scheduler = BackgroundScheduler(timezone="UTC")
        scheduler.add_job(
            run_cycle,
            "interval",
            seconds=interval,
            args=[cfg, user_agent, seen, producer, limiter],
            id="catalog-poll",
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        print(f"[catalog-poller] scheduled every {interval}s, Ctrl+C to stop")
        try:
            while True:
                time.sleep(3600)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown(wait=False)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
