"""Integration tests: the real broker and real subscriber processes over HTTP.

Run from the repository root after building the C++ targets:

    cmake -S . -B build && cmake --build build -j
    python3 -m unittest discover tests

Binaries default to build/broker and build/subscriber; override with the
BROKER_BIN and CPP_SUBSCRIBER_BIN environment variables. Every test starts its
own broker and subscribers on free localhost ports. Before a test ends, every
spawned process must have exited and every port must refuse connections.

Assertions use the HTTP delivery report and GET /subscribers wherever the spec
defines them. Subscriber stdout is searched only for event IDs, plus the
documented "duplicate" marker from spec section 5.1.
"""

import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
BROKER_BIN = os.environ.get("BROKER_BIN", os.path.join(REPO_ROOT, "build", "broker"))
CPP_SUBSCRIBER_BIN = os.environ.get(
    "CPP_SUBSCRIBER_BIN", os.path.join(REPO_ROOT, "build", "subscriber"))
PY_SUBSCRIBER_SCRIPT = os.path.join(REPO_ROOT, "subscribers", "subscriber.py")

STARTUP_TIMEOUT = 15.0           # broker /health and subscriber registration
LOG_TIMEOUT = 10.0               # subscriber log line appearing after delivery
EXIT_TIMEOUT = 10.0              # SIGTERM -> exit
HTTP_TIMEOUT = 30.0              # one broker request; /publish waits for retries
STARTUP_FAILURE_TIMEOUT = 25.0   # spec: subscriber gives up after a 10 s retry window
ISO_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def setUpModule():
    missing = [p for p in (BROKER_BIN, CPP_SUBSCRIBER_BIN, PY_SUBSCRIBER_SCRIPT)
               if not os.path.exists(p)]
    if missing:
        raise RuntimeError(
            "missing " + ", ".join(missing)
            + "; build first: cmake -S . -B build && cmake --build build -j")


# ---------------------------------------------------------------- helpers

def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def port_accepts(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def wait_until(predicate, timeout, what):
    """Poll predicate until it returns a truthy value; return that value."""
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(0.05)


def decode_body(raw: bytes):
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except ValueError:
        return text


def http(method, url, body=None):
    """Return (status, decoded body). Non-2xx responses are returned, not raised.

    bytes bodies are sent verbatim (used for malformed JSON).
    """
    data = None
    headers = {}
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
            return response.status, decode_body(response.read())
    except urllib.error.HTTPError as error:
        return error.code, decode_body(error.read())


def mentions(text: str, event_id: str) -> list:
    """Log lines naming event_id as a whole token ("e-1" must not match "e-10")."""
    pattern = re.compile(rf"(?<![\w.-]){re.escape(event_id)}(?![\w.-])")
    return [line for line in text.splitlines() if pattern.search(line)]


def without(mapping: dict, key: str) -> dict:
    return {k: v for k, v in mapping.items() if k != key}


def wait_for_exit(proc, timeout):
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


# -------------------------------------------------------- process wrappers

class ManagedProcess:
    """A spawned child whose stdout/stderr go to a unique file in the test's log dir.

    A file avoids pipe-buffer deadlocks and lets tests poll for event IDs.
    """

    def __init__(self, name, argv, port, log_dir):
        self.name = name
        self.port = port
        fd, self.log_path = tempfile.mkstemp(prefix=f"{name}-", suffix=".log", dir=log_dir)
        self._log_file = os.fdopen(fd, "wb")
        self.proc = subprocess.Popen(
            argv, cwd=REPO_ROOT, stdin=subprocess.DEVNULL,
            stdout=self._log_file, stderr=subprocess.STDOUT)
        self._closed = False
        self._graceful = False

    def log_text(self) -> str:
        with open(self.log_path, "rb") as handle:
            return handle.read().decode("utf-8", errors="replace")

    def assert_running(self):
        code = self.proc.poll()
        if code is not None:
            raise AssertionError(f"{self.name} exited with code {code}:\n{self.log_text()}")

    def kill(self):
        """Simulate a crash: SIGKILL, no cleanup, no deregistration."""
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=EXIT_TIMEOUT)

    def stop(self) -> bool:
        """SIGTERM and wait; SIGKILL after EXIT_TIMEOUT. Idempotent.

        Returns True only if the process exited on SIGTERM alone.
        """
        if not self._closed:
            if self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=EXIT_TIMEOUT)
                    self._graceful = True
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait(timeout=EXIT_TIMEOUT)
            self._log_file.close()
            self._closed = True
        return self._graceful


class Broker(ManagedProcess):
    def __init__(self, log_dir):
        port = free_port()
        self.base = f"http://localhost:{port}"
        super().__init__("broker", [BROKER_BIN, "--port", str(port)], port, log_dir)

    def wait_ready(self):
        def healthy():
            self.assert_running()
            try:
                status, _ = http("GET", self.base + "/health")
            except OSError:
                return False
            return status == 200
        wait_until(healthy, STARTUP_TIMEOUT, "broker /health")


class Subscriber(ManagedProcess):
    def __init__(self, language, sid, topics, broker_base, log_dir, port=None):
        self.sid = sid
        port = port or free_port()
        self.callback_url = f"http://localhost:{port}/event"
        flags = ["--id", sid, "--port", str(port),
                 "--topics", ",".join(topics), "--broker", broker_base]
        if language == "python":
            # -u: unbuffered stdout so event lines appear promptly.
            argv = [sys.executable, "-u", PY_SUBSCRIBER_SCRIPT, *flags]
        elif language == "cpp":
            # No stdbuf wrapper: the C++ subscriber flushes every log line itself
            # (std::endl), and stdbuf works via LD_PRELOAD, which AddressSanitizer
            # rejects ("ASan runtime does not come first").
            argv = [CPP_SUBSCRIBER_BIN, *flags]
        else:
            raise ValueError(f"unknown language {language!r}")
        super().__init__(f"{sid}-{language}", argv, port, log_dir)


class FakeCallback:
    """In-process callback endpoint for cases needing an exact status, a hang, or the
    raw request body. Real subscribers cannot be made to misbehave on demand."""

    def __init__(self):
        self.received = []   # decoded callback bodies, in arrival order
        self.hang = False    # when True, read the request and never answer
        self._statuses = deque([200])
        self._lock = threading.Lock()
        self._release = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
                with owner._lock:
                    owner.received.append(decode_body(raw))
                    status = owner._statuses[0]
                    if len(owner._statuses) > 1:
                        owner._statuses.popleft()
                if owner.hang:
                    owner._release.wait(HTTP_TIMEOUT)
                if 200 <= status < 300:
                    reply = {"status": "processed"}
                else:
                    reply = {"error": "scripted failure"}
                data = json.dumps(reply).encode("utf-8")
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except OSError:
                    pass  # broker already gave up on this attempt

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("localhost", 0), Handler)
        self.url = f"http://localhost:{self._server.server_address[1]}/event"
        self._serving = threading.Event()

        def serve():
            self._serving.set()
            self._server.serve_forever()

        threading.Thread(target=serve, daemon=True).start()
        self._serving.wait(5)

    def script(self, *statuses):
        """Statuses for successive requests; the last one repeats."""
        with self._lock:
            self._statuses = deque(statuses)

    def close(self):
        self._release.set()
        self._server.shutdown()
        self._server.server_close()


# ------------------------------------------------------------ test base

class IntegrationCase(unittest.TestCase):
    """Owns every process and callback a test starts; nothing outlives the test."""

    def setUp(self):
        self.log_dir = tempfile.mkdtemp(prefix="pubsub-it-")
        self.addCleanup(shutil.rmtree, self.log_dir, True)
        self.broker = None
        self._processes = []
        # Cleanups run last-in first-out, so this runs after every stop registered later.
        self.addCleanup(self._assert_all_reaped)

    def _assert_all_reaped(self):
        survivors = [p for p in self._processes if p.proc.poll() is None]
        for process in survivors:
            process.proc.kill()
        self.assertEqual([], [p.name for p in survivors],
                         "processes still running after the test")
        listening = [f"{p.name}:{p.port}" for p in self._processes if port_accepts(p.port)]
        self.assertEqual([], listening, "ports still accepting connections after exit")

    def _track(self, process):
        self._processes.append(process)
        self.addCleanup(process.stop)
        return process

    def start_broker(self):
        self.broker = self._track(Broker(self.log_dir))
        self.broker.wait_ready()
        return self.broker

    def start_subscriber(self, language, sid, topics, port=None):
        sub = self._track(Subscriber(language, sid, topics, self.broker.base,
                                     self.log_dir, port))
        self.wait_registered(sub)
        return sub

    def wait_registered(self, sub):
        def registered():
            sub.assert_running()
            entry = self.subscriber_entry(sub.sid)
            return bool(entry and entry["callback_url"] == sub.callback_url
                        and entry["state"] == "ALIVE")
        wait_until(registered, STARTUP_TIMEOUT, f"{sub.sid} to register ALIVE")

    def fake_callback(self):
        callback = FakeCallback()
        self.addCleanup(callback.close)
        return callback

    # HTTP API
    def api(self, method, path, body=None):
        return http(method, self.broker.base + path, body)

    def register(self, sid, callback_url, topics, expect=201):
        status, body = self.api("POST", "/subscribe",
                                {"id": sid, "callback_url": callback_url, "topics": topics})
        self.assertEqual(status, expect, body)
        return body

    def subscribers(self):
        status, body = self.api("GET", "/subscribers")
        self.assertEqual(status, 200, body)
        return body

    def subscriber_entry(self, sid):
        return next((s for s in self.subscribers() if s["id"] == sid), None)

    def publish(self, body):
        status, report = self.api("POST", "/publish", body)
        self.assertEqual(status, 200, report)
        self.assertIn("event_id", report)
        self.assertIsInstance(report["deliveries"], list)
        return report

    def deliveries(self, report):
        entries = report["deliveries"]
        ids = [e["id"] for e in entries]
        self.assertEqual(len(ids), len(set(ids)), "a subscriber is listed twice")
        return {e["id"]: e for e in entries}

    # Subscriber output
    def wait_for_event(self, sub, event_id):
        wait_until(lambda: mentions(sub.log_text(), event_id), LOG_TIMEOUT,
                   f"{sub.sid} to log {event_id}")

    def assert_not_received(self, sub, event_id):
        self.assertEqual([], mentions(sub.log_text(), event_id),
                         f"{sub.sid} unexpectedly logged {event_id}")


class ClusterCase(IntegrationCase):
    """Spec section 10 cast: S1 (Python, sports), S2 (Python, news), S3 (C++, sports)."""

    def setUp(self):
        super().setUp()
        self.start_broker()
        self.s1 = self.start_subscriber("python", "S1", ["sports"])
        self.s2 = self.start_subscriber("python", "S2", ["news"])
        self.s3 = self.start_subscriber("cpp", "S3", ["sports"])


# ------------------------------------------------------------------ tests

class ApiContractTests(IntegrationCase):
    """Routes, status codes, and error bodies from spec sections 4.1-4.5, no subscribers."""

    def setUp(self):
        super().setUp()
        self.start_broker()

    def test_health_reports_ok(self):
        self.assertEqual((200, {"status": "ok"}), self.api("GET", "/health"))

    def test_registry_starts_empty(self):
        self.assertEqual([], self.subscribers())

    def test_new_registration_returns_201_alive_with_zero_counters(self):
        body = self.register("S1", "http://localhost:9999/event", ["sports"])
        self.assertEqual({"id": "S1", "state": "ALIVE", "topics": ["sports"]}, body)
        entry = self.subscriber_entry("S1")
        self.assertEqual("http://localhost:9999/event", entry["callback_url"])
        self.assertEqual(["sports"], entry["topics"])
        self.assertEqual(("ALIVE", 0, 0), (entry["state"], entry["failures"], entry["delivered"]))

    def test_reregistration_returns_200_and_replaces_callback_and_topics(self):
        self.register("S1", "http://localhost:9001/event", ["sports"])
        status, body = self.api("POST", "/subscribe", {
            "id": "S1", "callback_url": "http://localhost:9101/event", "topics": ["news", "weather"]})
        self.assertEqual(200, status)
        self.assertEqual({"id": "S1", "state": "ALIVE", "topics": ["news", "weather"]}, body)
        entry = self.subscriber_entry("S1")
        self.assertEqual("http://localhost:9101/event", entry["callback_url"])
        self.assertEqual(["news", "weather"], entry["topics"])
        self.assertEqual(1, len(self.subscribers()))

    def test_empty_topics_is_valid(self):
        body = self.register("S9", "http://localhost:9009/event", [])
        self.assertEqual({"id": "S9", "state": "ALIVE", "topics": []}, body)

    def test_invalid_subscribe_bodies_return_400_with_error(self):
        valid = {"id": "S1", "callback_url": "http://localhost:9001/event", "topics": ["sports"]}
        cases = {
            "missing id": without(valid, "id"),
            "non-string id": {**valid, "id": 7},
            "missing callback_url": without(valid, "callback_url"),
            "non-string callback_url": {**valid, "callback_url": 9001},
            "missing topics": without(valid, "topics"),
            "topics is a string": {**valid, "topics": "sports"},
            "topics has a non-string": {**valid, "topics": ["sports", 3]},
            "malformed JSON": b"{not json",
        }
        for name, body in cases.items():
            with self.subTest(name):
                status, reply = self.api("POST", "/subscribe", body)
                self.assertEqual(400, status)
                self.assertIn("error", reply)
        self.assertEqual([], self.subscribers())

    def test_malformed_callback_urls_are_rejected_with_400(self):
        # "?" or "#" inside the host used to be accepted; the rest are plain malformed URLs.
        malformed = [
            "http://local?host:9001/event",
            "http://localhost#frag:9001/event",
            "http://localhost?x=1:9001/event",
            "http://localhost:9001?x",
            "http://localhost:9001#x",
            "http://:9001/event",
            "http://localhost:/event",
            "http://localhost:0/event",
            "http://localhost:65536/event",
            "http://user@localhost:9001/event",
            "https://localhost:9001/event",
            "localhost:9001/event",
            "",
        ]
        for url in malformed:
            with self.subTest(url=url):
                status, reply = self.api("POST", "/subscribe",
                                         {"id": "S1", "callback_url": url, "topics": []})
                self.assertEqual(400, status, reply)
                self.assertIn("error", reply)
        self.assertEqual([], self.subscribers())

        # Valid URLs register, including a query on the path and a fragment (never sent).
        valid = [
            "http://localhost:9001/event",
            "http://localhost:9001",
            "http://127.0.0.1:9001/event?replay=1",
            "http://localhost:9001/event#frag",
        ]
        for index, url in enumerate(valid):
            with self.subTest(url=url):
                self.register(f"V{index}", url, [])
        self.assertEqual(len(valid), len(self.subscribers()))

    def test_delete_removes_subscriber_and_unknown_id_is_404(self):
        self.register("S1", "http://localhost:9001/event", ["sports"])
        self.assertEqual((200, {"id": "S1", "removed": True}),
                         self.api("DELETE", "/subscribe/S1"))
        self.assertEqual([], self.subscribers())
        for path in ("/subscribe/S1", "/subscribe/never"):
            with self.subTest(path):
                status, body = self.api("DELETE", path)
                self.assertEqual(404, status)
                self.assertIn("error", body)

    def test_invalid_publish_bodies_return_400_with_error(self):
        cases = {
            "missing mode": {"target": "sports", "payload": {}},
            "unknown mode": {"mode": "anycast", "target": "sports", "payload": {}},
            "unicast without target": {"mode": "unicast", "payload": {}},
            "multicast without target": {"mode": "multicast", "payload": {}},
            "malformed JSON": b"{oops",
        }
        for name, body in cases.items():
            with self.subTest(name):
                status, reply = self.api("POST", "/publish", body)
                self.assertEqual(400, status)
                self.assertIn("error", reply)

    def test_unicast_to_unknown_id_returns_404(self):
        status, body = self.api("POST", "/publish", {
            "mode": "unicast", "target": "ghost", "payload": {}})
        self.assertEqual(404, status)
        self.assertIn("error", body)

    def test_fanout_with_no_alive_recipients_returns_empty_deliveries(self):
        for body in ({"mode": "broadcast", "payload": {"n": 1}},
                     {"mode": "multicast", "target": "sports", "payload": {"n": 2}}):
            with self.subTest(mode=body["mode"]):
                report = self.publish(body)
                self.assertEqual(body["mode"], report["mode"])
                self.assertEqual([], report["deliveries"])

    def test_event_id_is_assigned_monotonically_when_omitted(self):
        ids = [self.publish({"mode": "broadcast", "payload": i})["event_id"] for i in range(3)]
        for event_id in ids:
            self.assertRegex(event_id, r"^e-\d+$")
        numbers = [int(event_id.split("-")[1]) for event_id in ids]
        self.assertEqual(sorted(set(numbers)), numbers)

    def test_supplied_event_id_is_used_as_is(self):
        report = self.publish({"mode": "broadcast", "event_id": "order-123.v2", "payload": None})
        self.assertEqual("order-123.v2", report["event_id"])


class CallbackContractTests(IntegrationCase):
    """What the broker sends to a callback, and how it reacts to callback responses.

    Uses in-process callbacks so the exact body, status codes, and hangs are controlled.
    """

    def setUp(self):
        super().setUp()
        self.start_broker()
        self.fake = self.fake_callback()
        self.register("F1", self.fake.url, ["sports", "news"])

    def test_callback_body_matches_spec_for_each_mode(self):
        payload = {"score": "2-1", "tags": ["final", None], "ok": True, "n": 1.5, "u": "π"}
        self.publish({"mode": "unicast", "target": "F1", "payload": payload, "event_id": "c-uni"})
        self.publish({"mode": "multicast", "target": "sports", "payload": payload,
                      "event_id": "c-multi"})
        self.publish({"mode": "broadcast", "payload": payload, "event_id": "c-bcast"})
        bodies = {b["event_id"]: b for b in self.fake.received}
        expected = {"c-uni": ("unicast", None),
                    "c-multi": ("multicast", "sports"),
                    "c-bcast": ("broadcast", None)}
        self.assertEqual(set(expected), set(bodies))
        for event_id, (mode, topic) in expected.items():
            with self.subTest(event_id):
                body = bodies[event_id]
                self.assertEqual({"event_id", "mode", "topic", "payload", "timestamp"}, set(body))
                self.assertEqual(mode, body["mode"])
                self.assertEqual(topic, body["topic"])
                self.assertEqual(payload, body["payload"])
                self.assertRegex(body["timestamp"], ISO_UTC)

    def test_multicast_matches_any_listed_topic(self):
        other = self.fake_callback()
        self.register("F2", other.url, ["news"])
        report = self.publish({"mode": "multicast", "target": "news", "payload": {}})
        self.assertEqual({"F1", "F2"}, set(self.deliveries(report)))
        report = self.publish({"mode": "multicast", "target": "sports", "payload": {}})
        self.assertEqual({"F1"}, set(self.deliveries(report)))

    def test_reregistration_redirects_later_deliveries_to_new_callback(self):
        new = self.fake_callback()
        self.register("F1", new.url, ["sports"], expect=200)
        report = self.publish({"mode": "broadcast", "payload": {}, "event_id": "redir-1"})
        self.assertEqual("DELIVERED", self.deliveries(report)["F1"]["status"])
        self.assertEqual(["redir-1"], [b["event_id"] for b in new.received])
        self.assertEqual([], [b for b in self.fake.received if b.get("event_id") == "redir-1"])

    def test_5xx_is_retried_until_the_callback_succeeds(self):
        self.fake.script(500, 200)
        d = self.deliveries(self.publish({"mode": "unicast", "target": "F1", "payload": {}}))
        self.assertEqual("DELIVERED", d["F1"]["status"])
        self.assertEqual(2, d["F1"]["attempts"])
        self.assertEqual("processed", d["F1"]["ack"])
        self.assertEqual(2, len(self.fake.received))

    def test_persistent_5xx_fails_after_three_attempts_and_marks_dead(self):
        self.fake.script(500)
        report = self.publish({"mode": "unicast", "target": "F1", "payload": {}})
        d = self.deliveries(report)
        self.assertEqual("FAILED", d["F1"]["status"])
        self.assertEqual(3, d["F1"]["attempts"])
        self.assertTrue(d["F1"].get("error"))
        self.assertEqual(3, len(self.fake.received))
        entry = self.subscriber_entry("F1")
        self.assertEqual(("DEAD", 1), (entry["state"], entry["failures"]))
        # A DEAD unicast target is reported, not attempted.
        report = self.publish({"mode": "unicast", "target": "F1", "payload": {}})
        self.assertEqual("SKIPPED_DEAD", self.deliveries(report)["F1"]["status"])
        self.assertEqual(3, len(self.fake.received))

    def test_4xx_fails_after_a_single_attempt(self):
        self.fake.script(400)
        d = self.deliveries(self.publish({"mode": "unicast", "target": "F1", "payload": {}}))
        self.assertEqual("FAILED", d["F1"]["status"])
        self.assertEqual(1, d["F1"]["attempts"])
        self.assertEqual(1, len(self.fake.received))

    def test_dead_subscriber_recovers_when_it_reregisters(self):
        self.fake.script(500)
        self.publish({"mode": "unicast", "target": "F1", "payload": {}})
        self.assertEqual("DEAD", self.subscriber_entry("F1")["state"])
        self.fake.script(200)
        self.register("F1", self.fake.url, ["sports", "news"], expect=200)
        entry = self.subscriber_entry("F1")
        self.assertEqual(("ALIVE", 0), (entry["state"], entry["failures"]))
        report = self.publish({"mode": "unicast", "target": "F1", "payload": {}, "event_id": "back"})
        self.assertEqual("DELIVERED", self.deliveries(report)["F1"]["status"])

    def test_hanging_callback_times_out_without_delaying_healthy_subscriber(self):
        s1 = self.start_subscriber("python", "S1", ["sports"])
        self.fake.hang = True
        started = time.monotonic()
        report = self.publish({"mode": "multicast", "target": "sports", "payload": {},
                               "event_id": "hang-1"})
        elapsed = time.monotonic() - started
        d = self.deliveries(report)
        self.assertEqual("FAILED", d["F1"]["status"])
        self.assertEqual(3, d["F1"]["attempts"])
        self.assertEqual("DELIVERED", d["S1"]["status"])
        # Three 2 s read timeouts plus backoff; the bound only checks that timeouts were honoured.
        self.assertGreaterEqual(elapsed, 4.0)
        self.assertLess(elapsed, 20.0)
        self.wait_for_event(s1, "hang-1")


class DeliveryModeTests(ClusterCase):
    def test_unicast_reaches_only_the_target(self):
        before = self.subscriber_entry("S2")["delivered"]
        report = self.publish({"mode": "unicast", "target": "S2", "payload": {"n": 1},
                               "event_id": "uni-1"})
        d = self.deliveries(report)
        self.assertEqual({"S2"}, set(d))
        self.assertEqual(("DELIVERED", 1, "processed"),
                         (d["S2"]["status"], d["S2"]["attempts"], d["S2"]["ack"]))
        self.wait_for_event(self.s2, "uni-1")
        self.assert_not_received(self.s1, "uni-1")
        self.assert_not_received(self.s3, "uni-1")
        self.assertEqual(before + 1, self.subscriber_entry("S2")["delivered"])

    def test_multicast_reaches_only_subscribers_of_the_topic(self):
        d = self.deliveries(self.publish({"mode": "multicast", "target": "sports",
                                          "payload": {}, "event_id": "mc-1"}))
        self.assertEqual({"S1", "S3"}, set(d))
        self.assertTrue(all(e["status"] == "DELIVERED" for e in d.values()))
        self.wait_for_event(self.s1, "mc-1")
        self.wait_for_event(self.s3, "mc-1")
        self.assert_not_received(self.s2, "mc-1")

    def test_broadcast_reaches_every_alive_subscriber(self):
        d = self.deliveries(self.publish({"mode": "broadcast", "payload": {}, "event_id": "bc-1"}))
        self.assertEqual({"S1", "S2", "S3"}, set(d))
        self.assertTrue(all(e["status"] == "DELIVERED" for e in d.values()))
        for sub in (self.s1, self.s2, self.s3):
            self.wait_for_event(sub, "bc-1")

    def test_multicast_to_a_topic_nobody_holds_is_empty(self):
        report = self.publish({"mode": "multicast", "target": "weather", "payload": {}})
        self.assertEqual([], report["deliveries"])

    def test_duplicate_event_id_is_acked_duplicate_and_not_reprocessed(self):
        first = self.deliveries(self.publish({"mode": "broadcast", "payload": {"n": 1},
                                              "event_id": "dup-1"}))
        second = self.deliveries(self.publish({"mode": "broadcast", "payload": {"n": 1},
                                               "event_id": "dup-1"}))
        for sid in ("S1", "S2", "S3"):
            with self.subTest(sid):
                self.assertEqual("processed", first[sid]["ack"])
                self.assertEqual("duplicate", second[sid]["ack"])
        for sub in (self.s1, self.s2, self.s3):
            with self.subTest(sub.sid):
                wait_until(lambda: len(mentions(sub.log_text(), "dup-1")) >= 2,
                           LOG_TIMEOUT, f"{sub.sid} to log both deliveries of dup-1")
                lines = mentions(sub.log_text(), "dup-1")
                # Spec section 7: one line per action, so one "processed" and one "duplicate".
                self.assertEqual(2, len(lines))
                self.assertEqual(1, sum("duplicate" in line.lower() for line in lines))


class FailureRecoveryTests(ClusterCase):
    def _kill_and_confirm_dead(self, sub, event_id):
        sub.kill()
        d = self.deliveries(self.publish({"mode": "broadcast", "payload": {},
                                          "event_id": event_id}))
        self.assertEqual("FAILED", d[sub.sid]["status"])
        self.assertEqual(3, d[sub.sid]["attempts"])
        self.assertTrue(d[sub.sid].get("error"))
        entry = self.subscriber_entry(sub.sid)
        self.assertEqual(("DEAD", 1), (entry["state"], entry["failures"]))
        return d

    def _restart_and_confirm_recovery(self, old, language, topics, event_id):
        fresh = self.start_subscriber(language, old.sid, topics, port=old.port)
        entry = self.subscriber_entry(old.sid)
        self.assertEqual(("ALIVE", 0), (entry["state"], entry["failures"]))
        d = self.deliveries(self.publish({"mode": "broadcast", "payload": {},
                                          "event_id": event_id}))
        self.assertEqual({"S1", "S2", "S3"}, set(d))
        self.assertTrue(all(e["status"] == "DELIVERED" for e in d.values()))
        self.wait_for_event(fresh, event_id)
        return fresh

    def test_crash_is_not_noticed_until_a_delivery_fails(self):
        self.s2.kill()
        entry = self.subscriber_entry("S2")
        self.assertEqual(("ALIVE", 0), (entry["state"], entry["failures"]))
        self.assertFalse(port_accepts(self.s2.port))

    def test_killed_subscriber_is_marked_dead_and_does_not_stall_fanout(self):
        self.s2.kill()
        started = time.monotonic()
        d = self.deliveries(self.publish({"mode": "broadcast", "payload": {},
                                          "event_id": "kill-1"}))
        elapsed = time.monotonic() - started
        self.assertEqual({"S1", "S2", "S3"}, set(d))
        self.assertEqual("FAILED", d["S2"]["status"])
        self.assertEqual(3, d["S2"]["attempts"])
        self.assertTrue(d["S2"].get("error"))
        for sid in ("S1", "S3"):
            self.assertEqual(("DELIVERED", 1, "processed"),
                             (d[sid]["status"], d[sid]["attempts"], d[sid]["ack"]))
        self.assertLess(elapsed, 10.0)  # refused connections fail fast; backoff is 0.6 s
        entry = self.subscriber_entry("S2")
        self.assertEqual(("DEAD", 1), (entry["state"], entry["failures"]))
        self.wait_for_event(self.s1, "kill-1")
        self.wait_for_event(self.s3, "kill-1")

    def test_dead_subscriber_is_skipped_by_fanout_and_reported_for_unicast(self):
        self._kill_and_confirm_dead(self.s2, "skip-0")
        d = self.deliveries(self.publish({"mode": "broadcast", "payload": {},
                                          "event_id": "skip-1"}))
        self.assertEqual({"S1", "S3"}, set(d))
        report = self.publish({"mode": "multicast", "target": "news", "payload": {},
                               "event_id": "skip-2"})
        self.assertEqual([], report["deliveries"])
        d = self.deliveries(self.publish({"mode": "unicast", "target": "S2", "payload": {},
                                          "event_id": "skip-3"}))
        self.assertEqual("SKIPPED_DEAD", d["S2"]["status"])
        self.assertEqual(0, d["S2"].get("attempts", 0))
        self.assertEqual(1, self.subscriber_entry("S2")["failures"])

    def test_killed_python_subscriber_recovers_by_restarting_and_reregistering(self):
        self._kill_and_confirm_dead(self.s2, "rec-0")
        self.s2 = self._restart_and_confirm_recovery(self.s2, "python", ["news"], "rec-1")

    def test_killed_cpp_subscriber_recovers_by_restarting_and_reregistering(self):
        self._kill_and_confirm_dead(self.s3, "rec-0")
        self.s3 = self._restart_and_confirm_recovery(self.s3, "cpp", ["sports"], "rec-1")

    def test_deleted_subscriber_no_longer_receives_fanout(self):
        self.assertEqual((200, {"id": "S3", "removed": True}), self.api("DELETE", "/subscribe/S3"))
        self.assertIsNone(self.subscriber_entry("S3"))
        d = self.deliveries(self.publish({"mode": "broadcast", "payload": {}, "event_id": "del-1"}))
        self.assertEqual({"S1", "S2"}, set(d))
        self.wait_for_event(self.s1, "del-1")
        self.wait_for_event(self.s2, "del-1")
        self.s3.assert_running()  # deletion is the broker's decision; the process is untouched
        self.assert_not_received(self.s3, "del-1")
        status, body = self.api("DELETE", "/subscribe/S3")
        self.assertEqual(404, status)
        self.assertIn("error", body)

    def test_dead_subscriber_can_be_deleted(self):
        self._kill_and_confirm_dead(self.s2, "del-0")
        self.assertEqual((200, {"id": "S2", "removed": True}), self.api("DELETE", "/subscribe/S2"))
        self.assertIsNone(self.subscriber_entry("S2"))
        d = self.deliveries(self.publish({"mode": "broadcast", "payload": {}, "event_id": "del-1"}))
        self.assertEqual({"S1", "S3"}, set(d))


class ProcessLifecycleTests(IntegrationCase):
    def test_subscriber_exits_on_sigterm_and_keeps_its_registration(self):
        self.start_broker()
        for language in ("python", "cpp"):
            with self.subTest(language=language):
                sub = self.start_subscriber(language, f"L-{language}", ["news"])
                sub.assert_running()
                self.assertTrue(sub.stop(), "subscriber did not exit on SIGTERM")
                self.assertFalse(port_accepts(sub.port))
                # Exiting is the crash simulation; the broker learns only by failed delivery.
                self.assertEqual("ALIVE", self.subscriber_entry(sub.sid)["state"])

    def test_broker_stop_releases_its_port(self):
        self.start_broker()
        port = self.broker.port
        self.broker.stop()
        self.assertIsNotNone(self.broker.proc.poll())
        self.assertFalse(port_accepts(port))

    def test_subscribers_exit_nonzero_when_registration_never_succeeds(self):
        dead_broker = f"http://localhost:{free_port()}"
        subs = [self._track(Subscriber(lang, f"U-{lang}", ["x"], dead_broker, self.log_dir))
                for lang in ("python", "cpp")]
        for sub in subs:
            with self.subTest(sub.sid):
                code = wait_for_exit(sub.proc, STARTUP_FAILURE_TIMEOUT)
                self.assertIsNotNone(code, "subscriber kept running without a broker")
                self.assertNotEqual(0, code, sub.log_text())


class BrokerUrlTests(IntegrationCase):
    """--broker is the same http://host[:port] base for both subscribers (spec 5.2)."""

    REJECTED = (
        "https://localhost:8080",
        "http://localhost:8080/api",
        "http://localhost:8080?x=1",
        "http://localhost:8080#frag",
        "localhost:8080",
        "http://",
    )

    def test_both_subscribers_reject_non_base_urls_with_exit_2(self):
        commands = {
            "python": [sys.executable, PY_SUBSCRIBER_SCRIPT],
            "cpp": [CPP_SUBSCRIBER_BIN],
        }
        for language, command in commands.items():
            for broker in self.REJECTED:
                with self.subTest(language=language, broker=broker):
                    proc = subprocess.run(
                        [*command, "--id", "X", "--port", str(free_port()), "--broker", broker],
                        cwd=REPO_ROOT, stdin=subprocess.DEVNULL, capture_output=True,
                        text=True, timeout=STARTUP_TIMEOUT)
                    self.assertEqual(2, proc.returncode, proc.stderr)
                    self.assertIn("--broker", proc.stderr)

    def test_base_with_trailing_slash_registers_for_both_languages(self):
        self.start_broker()
        for language, sid in (("python", "P1"), ("cpp", "C1")):
            with self.subTest(language=language):
                sub = self._track(Subscriber(language, sid, ["sports"],
                                             self.broker.base + "/", self.log_dir))
                self.wait_registered(sub)



if __name__ == "__main__":
    unittest.main(verbosity=2)
