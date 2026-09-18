import argparse
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from kafka import KafkaProducer

from src.common.config import load_config
from src.common.serde import JsonSerializer, Utf8StringSerializer
from src.pollers.state import CursorStore

ROOT = Path(__file__).resolve().parents[2]


class LastfmError(Exception):
    pass


def read_seed_users(path):
    users = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            name = line.strip()
            if name and not name.startswith("#"):
                users.append(name)
    return users


def fetch_recent_tracks(settings, api_key, username, cursor=None, http=None):
    http = http if http is not None else requests
    params = {
        "method": settings["method_recent_tracks"],
        "user": username,
        "api_key": api_key,
        "limit": settings["tracks_per_request"],
        "format": "json",
    }
    if cursor is not None:
        params["from"] = cursor
    response = http.get(settings["base_url"], params=params, timeout=10)
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise LastfmError(
            f"user.getRecentTracks error {payload['error']} for {username}: "
            f"{payload.get('message', 'unknown')}"
        )
    tracks = payload.get("recenttracks", {}).get("track", [])
    if isinstance(tracks, dict):
        tracks = [tracks]
    return tracks


def select_new_tracks(tracks, cursor=None):
    fresh = []
    for track in tracks:
        date = track.get("date") or {}
        uts = date.get("uts")
        if not uts:
            continue
        uts = int(uts)
        if cursor is not None and uts <= cursor:
            continue
        fresh.append((uts, track))
    return fresh


def build_event(username, track):
    date = track.get("date") or {}
    uts = date.get("uts")
    if not uts:
        return None
    return {
        "userId": username,
        "trackName": track.get("name"),
        "artistName": (track.get("artist") or {}).get("#text"),
        "timestamp": datetime.fromtimestamp(int(uts), tz=timezone.utc).isoformat(),
        "nowPlaying": False,
        "source": "live",
    }


def poll_user(settings, api_key, username, store, producer, topic, http=None):
    cursor = store.get(username)
    tracks = fetch_recent_tracks(settings, api_key, username, cursor=cursor, http=http)
    fresh = select_new_tracks(tracks, cursor)
    sent = 0
    newest = None
    for uts, track in fresh:
        event = build_event(username, track)
        if event is None:
            continue
        producer.send(topic, key=username, value=event)
        sent += 1
        if newest is None or uts > newest:
            newest = uts
    producer.flush()
    if newest is not None:
        store.advance(username, newest)
    return sent


def poll_all(cfg, api_key, store, producer, http=None, usernames=None):
    lastfm = cfg["lastfm"]
    topic = cfg["kafka"]["topics"]["listening_events"]
    if usernames is None:
        usernames = read_seed_users(ROOT / lastfm["user_list"])
    sent = 0
    failed = 0
    for username in usernames:
        try:
            sent += poll_user(lastfm, api_key, username, store, producer, topic, http=http)
        except (LastfmError, requests.RequestException, ValueError) as exc:
            failed += 1
            print(f"[events-poller] skipped {username}: {exc}")
    return sent, len(usernames), failed


def make_producer(cfg):
    return KafkaProducer(
        bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
        key_serializer=Utf8StringSerializer(),
        value_serializer=JsonSerializer(),
    )


def run_cycle(cfg, api_key, store, producer, http=None):
    sent, users, failed = poll_all(cfg, api_key, store, producer, http=http)
    print(f"[events-poller] cycle done: {sent} events from {users} users, {failed} failed")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Last.fm listening-events poller")
    parser.add_argument(
        "--once",
        action="store_true",
        help="run a single poll cycle and exit instead of scheduling",
    )
    args = parser.parse_args(argv)

    load_dotenv()
    cfg = load_config()
    lastfm = cfg["lastfm"]
    api_key = os.environ.get(lastfm["api_key_env"])
    if not api_key:
        raise SystemExit(f"missing {lastfm['api_key_env']} in .env")

    store = CursorStore(ROOT / cfg["polling"]["state_file"])
    producer = make_producer(cfg)
    interval = int(cfg["polling"]["events_interval_sec"])

    try:
        if args.once:
            run_cycle(cfg, api_key, store, producer)
            return
        scheduler = BackgroundScheduler(timezone="UTC")
        scheduler.add_job(
            run_cycle,
            "interval",
            seconds=interval,
            args=[cfg, api_key, store, producer],
            id="events-poll",
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        print(f"[events-poller] scheduled every {interval}s, Ctrl+C to stop")
        try:
            while True:
                time.sleep(3600)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown(wait=False)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
