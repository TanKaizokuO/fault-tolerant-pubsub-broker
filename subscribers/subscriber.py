#!/usr/bin/env python3
"""Python subscriber for the fault-tolerant pub-sub broker (docs/spec.md §5).

Starts an HTTP callback server on localhost, registers with the broker
(retrying every 1 s for up to 10 s), then logs and deduplicates events.
Exiting (Ctrl+C / SIGTERM) does NOT deregister: exit is the crash simulation
for the failure demo. Standard library only.
"""

from __future__ import annotations

import argparse
import http.client
import json
import math
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

REGISTER_RETRY_INTERVAL_S = 1.0
REGISTER_WINDOW_S = 10.0
REGISTER_REQUEST_TIMEOUT_S = 2.0
HANDLER_TIMEOUT_S = 10.0
POLL_INTERVAL_S = 0.1
DEDUP_CAPACITY = 1000
VALID_MODES = frozenset({"unicast", "multicast", "broadcast"})
REQUIRED_EVENT_FIELDS = ("event_id", "mode", "topic", "payload", "timestamp")

_log_lock = threading.Lock()


def log(tag: str, message: str) -> None:
    stamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    with _log_lock:
        print(f"[{stamp}] [{tag}] {message}", flush=True)


# --------------------------------------------------------------------- CLI

def _parse_id(value: str) -> str:
    if not value:
        raise argparse.ArgumentTypeError("id must be non-empty")
    return value


def _parse_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"port must be an integer, got {value!r}") from None
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be in 1..65535")
    return port


def _parse_topics(value: str) -> list[str]:
    if not value.strip():
        return []
    topics: list[str] = []
    for raw in value.split(","):
        topic = raw.strip()
        if not topic:
            raise argparse.ArgumentTypeError(f"empty topic name in {value!r}")
        if topic not in topics:
            topics.append(topic)
    return topics


_BROKER_SCHEME = "http://"
_BROKER_FORBIDDEN = frozenset("/ \t?#")


def _parse_broker(value: str) -> str:
    """Accept only http://host[:port], trailing slashes allowed; mirrors the C++ subscriber."""
    if not value.startswith(_BROKER_SCHEME):
        raise argparse.ArgumentTypeError(f"--broker must start with http:// (got {value!r})")
    authority = value[len(_BROKER_SCHEME):].rstrip("/")
    if not authority or _BROKER_FORBIDDEN.intersection(authority):
        raise argparse.ArgumentTypeError(
            f"--broker must be http://host[:port] without a path, query, or fragment (got {value!r})"
        )
    return _BROKER_SCHEME + authority


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pub-sub subscriber: registers with the broker and logs delivered events."
    )
    parser.add_argument("--id", required=True, type=_parse_id, help="subscriber id, e.g. S1")
    parser.add_argument("--port", required=True, type=_parse_port, help="local callback port")
    parser.add_argument(
        "--topics", default="", type=_parse_topics, help="comma-separated topics, e.g. sports,news"
    )
    parser.add_argument(
        "--broker", default="http://localhost:8080", type=_parse_broker, help="broker base URL"
    )
    return parser.parse_args(argv)


# ------------------------------------------------------------ deduplication

class SeenEventIds:
    """Bounded set of event IDs with FIFO eviction. Thread-safe."""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._order: deque[str] = deque()
        self._members: set[str] = set()
        self._lock = threading.Lock()

    def add(self, event_id: str) -> bool:
        """Record event_id. Returns True if it was new, False if already seen."""
        with self._lock:
            if event_id in self._members:
                return False
            if len(self._order) >= self._capacity:
                self._members.discard(self._order.popleft())
            self._order.append(event_id)
            self._members.add(event_id)
            return True


# ------------------------------------------------------------ callback server

def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")


def _parse_finite_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise ValueError(f"number {text} is out of range")
    return value


def parse_event(raw: bytes) -> dict[str, Any]:
    """Validate a POST /event body (spec §5.1).

    Raises ValueError with a client-facing message when the body is malformed.
    NaN/Infinity literals and non-finite numbers are rejected here, and so is
    over-deep nesting: json.loads accepts the first two by default and raises
    RecursionError on the last.
    """
    try:
        body = json.loads(raw, parse_constant=_reject_constant, parse_float=_parse_finite_float)
    except RecursionError:
        raise ValueError("malformed JSON: nested too deeply") from None
    except ValueError as exc:
        raise ValueError(f"malformed JSON: {exc}") from None
    if not isinstance(body, dict):
        raise ValueError("body must be a JSON object")
    missing = [name for name in REQUIRED_EVENT_FIELDS if name not in body]
    if missing:
        raise ValueError(f"missing field(s): {', '.join(missing)}")
    event_id = body["event_id"]
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("event_id must be a non-empty string")
    mode = body["mode"]
    if not isinstance(mode, str) or mode not in VALID_MODES:
        raise ValueError("mode must be unicast, multicast or broadcast")
    topic = body["topic"]
    if topic is not None and not isinstance(topic, str):
        raise ValueError("topic must be a string or null")
    timestamp = body["timestamp"]
    if not isinstance(timestamp, str) or not timestamp:
        raise ValueError("timestamp must be a non-empty string")
    try:
        # The log line re-encodes the payload; reject anything that cannot be encoded.
        json.dumps(body["payload"])
    except RecursionError:
        raise ValueError("payload is nested too deeply") from None
    return body


def format_event(event: dict[str, Any]) -> str:
    line = f"EVENT {event['event_id']} {event['mode']}"
    if event["topic"] is not None:
        line += f" topic={event['topic']}"
    payload = json.dumps(event["payload"], separators=(",", ":"), ensure_ascii=False)
    return f"{line} payload={payload}"


class CallbackHandler(BaseHTTPRequestHandler):
    timeout = HANDLER_TIMEOUT_S  # bounds reads from slow or stalled clients

    def do_POST(self) -> None:
        if urlsplit(self.path).path != "/event":
            self._send_json(404, {"error": f"no route for POST {self.path}"})
            return
        length = self._content_length()
        if length is None:
            self._send_json(400, {"error": "missing or invalid Content-Length"})
            return
        try:
            event = parse_event(self.rfile.read(length))
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
            return

        subscriber_id = self.server.subscriber_id  # type: ignore[attr-defined]
        event_id = event["event_id"]
        if self.server.seen.add(event_id):  # type: ignore[attr-defined]
            log(subscriber_id, format_event(event))
            self._send_json(200, {"status": "processed"})
        else:
            log(subscriber_id, f"DUPLICATE {event_id} ignored")
            self._send_json(200, {"status": "duplicate"})

    def _content_length(self) -> int | None:
        value = self.headers.get("Content-Length")
        if value is None:
            return None
        try:
            length = int(value)
        except ValueError:
            return None
        return length if length >= 0 else None

    def _send_json(self, status: int, body: dict[str, Any]) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress the default per-request stderr access log; events are logged explicitly.
        return


class SubscriberServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, port: int, subscriber_id: str) -> None:
        super().__init__(("127.0.0.1", port), CallbackHandler)
        self.subscriber_id = subscriber_id
        self.seen = SeenEventIds(DEDUP_CAPACITY)


# ---------------------------------------------------------------- registration

# Bypass environment proxies (http_proxy and friends): the broker is addressed directly.
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _post_json(url: str, payload: dict[str, Any], timeout: float) -> int:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with _OPENER.open(request, timeout=timeout) as response:
        return response.status


def register_with_broker(
    tag: str, broker: str, payload: dict[str, Any], stop: StopRequest
) -> bool:
    """POST /subscribe, retrying every second for up to 10 s.

    Every failure (connection error, timeout, any non-2xx response) is retried
    until the 10 s window closes. Returns True once the broker accepts the registration.
    """
    url = f"{broker}/subscribe"
    deadline = time.monotonic() + REGISTER_WINDOW_S
    attempt = 0
    while True:
        attempt += 1
        try:
            status = _post_json(url, payload, REGISTER_REQUEST_TIMEOUT_S)
            if 200 <= status < 300:
                log(tag, f"REGISTERED with {broker} (HTTP {status}) after {attempt} attempt(s)")
                return True
            reason = f"unexpected HTTP {status}"
        except urllib.error.HTTPError as exc:
            reason = f"HTTP {exc.code}"
        except (OSError, http.client.HTTPException) as exc:
            reason = str(exc) or type(exc).__name__

        log(tag, f"REGISTER attempt {attempt} to {url} failed: {reason}")
        if stop.is_set():
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if stop.wait(min(REGISTER_RETRY_INTERVAL_S, remaining)):
            return False


# ------------------------------------------------------------------- lifecycle

class StopRequest:
    """Stop flag set from signal handlers.

    Signal handlers must not acquire locks: threading.Event.set() can deadlock when the
    interrupted thread already holds the Event's lock. A plain attribute write is safe, so
    waiters poll the flag in short sleeps instead.
    """

    def __init__(self) -> None:
        self._requested = False

    def request(self) -> None:
        self._requested = True

    def is_set(self) -> bool:
        return self._requested

    def wait(self, timeout: float | None = None) -> bool:
        """Block until a stop is requested or the timeout elapses; returns whether requested."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self._requested:
            pause = POLL_INTERVAL_S
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                pause = min(pause, remaining)
            time.sleep(pause)
        return self._requested


def run(args: argparse.Namespace) -> int:
    tag = args.id
    stop = StopRequest()

    def request_stop(signum: int, _frame: object) -> None:
        # Signal handlers must not take locks (print() or Event.set()); a plain flag is safe.
        stop.request()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    callback_url = f"http://localhost:{args.port}/event"
    try:
        server = SubscriberServer(args.port, tag)
    except OSError as exc:
        log(tag, f"ERROR cannot listen on 127.0.0.1:{args.port}: {exc}")
        return 1

    serve_thread = threading.Thread(target=server.serve_forever, name="callback-server", daemon=True)
    serve_thread.start()
    log(tag, f"LISTENING on {callback_url}")

    try:
        registration = {"id": tag, "callback_url": callback_url, "topics": args.topics}
        if not register_with_broker(tag, args.broker, registration, stop):
            if stop.is_set():
                log(tag, "INTERRUPTED before registration completed; exiting")
            else:
                log(tag, f"ERROR registration with {args.broker} failed; exiting")
            return 1
        stop.wait()
        log(
            tag,
            "STOPPING (not deregistered; broker marks this subscriber DEAD on the next failed delivery)",
        )
        return 0
    finally:
        server.shutdown()
        server.server_close()
        serve_thread.join()


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
