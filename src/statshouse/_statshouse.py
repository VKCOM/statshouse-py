import argparse
import logging
import math
import os
import random
import socket
import struct
import sys
import threading
import time
import urllib.parse
from numbers import Real, Integral
from queue import Queue, Empty
from typing import Callable, Dict, List, Optional, Sequence, Tuple, TypeVar, Union

import msgpack

DEFAULT_STATSHOUSE_ADDR = "127.0.0.1:13337"
DEFAULT_STATSHOUSE_NETWORK = "udp"
DEFAULT_SEND_PERIOD = 1.0  # seconds
ERROR_REPORTING_PERIOD = 60.0  # seconds
MAX_EMPTY_SEND_COUNT = 2
DEFAULT_MAX_BUCKET_SIZE = 1024
TCP_CONN_BUCKET_COUNT = 512

TAG_STRING_TOP = "_s"
TAG_HOST = "_h"

T = TypeVar("T")
OneOrMany = Union[T, Sequence[T]]
Tags = Union[Tuple[Optional[str], ...], List[Optional[str]], Dict[str, str]]
LoggerFunc = Callable[[str, ...], None]


class Bucket:
    """Bucket for accumulating metrics, similar to Go client."""

    def __init__(self, metric_name: str, tags_dict: Dict[str, str], max_size: int = DEFAULT_MAX_BUCKET_SIZE):
        self.metric_name = metric_name
        self.tags_dict = tags_dict
        self.max_size = max_size
        self.mu = threading.Lock()
        self.ts_unix_sec = int(time.time())
        self.attached = False

        # Current accumulation
        self.count = 0.0
        self.value_count = 0
        self.unique_count = 0
        self.value: List[float] = []
        self.unique: List[int] = []
        self.stop: List[str] = []
        self.r = None  # random generator for sampling

        # To send (swapped)
        self.count_to_send = 0.0
        self.value_count_to_send = 0
        self.unique_count_to_send = 0
        self.value_to_send: List[float] = []
        self.unique_to_send: List[int] = []
        self.stop_to_send: List[str] = []
        self.empty_send_count = 0

    def append_value(self, *values: float):
        """Append values with reservoir sampling if over max_size."""
        for v in values:
            self.value_count += 1
            if len(self.value) < self.max_size:
                self.value.append(v)
            else:
                if self.r is None:
                    self.r = random.Random()
                n = self.r.randint(0, self.value_count - 1)
                if n < len(self.value):
                    self.value[n] = v

    def append_unique(self, *values: int):
        """Append unique values with reservoir sampling if over max_size."""
        for v in values:
            self.unique_count += 1
            if len(self.unique) < self.max_size:
                self.unique.append(v)
            else:
                if self.r is None:
                    self.r = random.Random()
                n = self.r.randint(0, self.unique_count - 1)
                if n < len(self.unique):
                    self.unique[n] = v

    def swap_to_send(self, now_unix_sec: int):
        """Swap current data to send buffers (reuse memory like Go)."""
        with self.mu:
            self.ts_unix_sec = now_unix_sec
            self.count, self.count_to_send = 0.0, self.count
            self.value_count, self.value_count_to_send = 0, self.value_count
            self.unique_count, self.unique_count_to_send = 0, self.unique_count
            self.value_to_send.clear()
            self.value, self.value_to_send = self.value_to_send, self.value
            self.unique_to_send.clear()
            self.unique, self.unique_to_send = self.unique_to_send, self.unique
            self.stop_to_send.clear()
            self.stop, self.stop_to_send = self.stop_to_send, self.stop

    def empty_send(self) -> bool:
        """Check if bucket has nothing to send."""
        return (
                self.count_to_send == 0
                and len(self.value_to_send) == 0
                and len(self.unique_to_send) == 0
                and len(self.stop_to_send) == 0
        )


class TCPConn:
    """TCP connection with async send and auto-reconnect, similar to Go tcpConn."""

    def __init__(self, client: "Client", addr: Tuple[str, int], app: str, env: str):
        self.client = client
        self.addr = addr
        self.app = app
        self.env = env
        self.conn: Optional[socket.socket] = None
        self._closed = False
        self._closed_lock = threading.Lock()
        self.would_block_size = 0
        self.w = Queue(maxsize=TCP_CONN_BUCKET_COUNT)
        self.close_event = threading.Event()
        self.send_thread = threading.Thread(target=self._send_loop, daemon=True)
        self.send_thread.start()

    def connect(self):
        """Connect and send handshake."""
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn.connect(self.addr)
        self.conn.sendall(b"statshousev1")

    def write(self, data: bytes) -> bytes:
        """Write data to send queue (non-blocking)."""
        if len(data) == 0:
            return data
        with self._closed_lock:
            if self._closed:
                raise RuntimeError("write after close")
        try:
            self.w.put_nowait(data)
            return data
        except Exception:
            # Queue full - would block
            self.would_block_size += len(data)
            raise RuntimeError("would block")

    def close(self):
        """Close connection."""
        with self._closed_lock:
            if self._closed:
                return  # Already closed
            self._closed = True
        # Try to put None in queue, but don't block if queue is full
        # _send_loop() will check self._closed and exit
        try:
            self.w.put_nowait(None)  # Non-blocking, signal to stop
        except Exception:
            # Queue is full - _send_loop() will check self._closed and exit
            pass
        self.close_event.wait()

    def _send_loop(self):
        """Main send loop with auto-reconnect."""
        buf = None
        err = None
        dial_time = 0.0

        while True:
            if err is not None:
                # Reconnect (no more than once per second)
                if self.conn:
                    self.conn.close()
                sleep_time = max(0.0, 1.0 - (time.time() - dial_time))
                time.sleep(sleep_time)

                dial_time = time.time()
                try:
                    self.connect()
                    err = None
                except Exception as e:
                    self.client.rare_log("[statshouse] failed to dial statshouse: %v", e)
                    try:
                        check_buf = self.w.get(timeout=0.0)  # Non-blocking check
                        if check_buf is None:  # Close signal
                            break
                        # Check if closed (in case queue is full and None couldn't be put)
                        with self._closed_lock:
                            if self._closed:
                                break
                        self.w.put_nowait(check_buf)
                    except Empty:
                        pass
                    continue

            # Read from queue only if we don't have a buffer to retry (same as Go)
            # In Go, read() is always called here after reconnect (successful or not)
            if buf is None:
                try:
                    buf = self.w.get(timeout=0.1)
                except Empty:
                    continue

                if buf is None:  # Close signal
                    break

            # Send data (retry if buf was set from previous failed attempt)
            if self.conn is None:
                err = RuntimeError("connection not established")
                continue
            try:
                self.conn.sendall(buf)
                # Success - clear buf to read next from queue
                buf = None
            except Exception as e:
                self.client.rare_log("[statshouse] failed to send data to statshouse: %v", e)
                err = e
                # Keep buf for retry after reconnect (same as Go)
                continue

            # Report would block data loss (same as Go)
            if self.would_block_size > 0:
                lost = self.would_block_size
                self.would_block_size = 0
                self.client.rare_log("[statshouse] lost %v bytes", lost)
                # Send metric about data loss (same as Go)
                error_metric = {
                    "name": "__src_client_write_err",
                    "tags": {
                        "0": self.env,
                        "1": "2",  # lang: python (2 = python, 1 = golang)
                        "2": "1",  # kind: would block
                        "3": self.app,  # application name
                    },
                    "value": (float(lost),),
                }
                try:
                    self.client._flush([error_metric], 0)
                except Exception:
                    pass  # Ignore errors when reporting errors

        if self.conn:
            self.conn.close()
        self.close_event.set()


class Client:
    """StatsHouse client with batching and async send, similar to Go client."""

    def __init__(
            self,
            addr: str,
            env: str,
            network: str = DEFAULT_STATSHOUSE_NETWORK,
            app_name: str = "",
            logger: Optional[LoggerFunc] = None,
            max_bucket_size: int = DEFAULT_MAX_BUCKET_SIZE,
    ):
        # Logging
        self.log_mu = threading.Lock()
        if logger is None:
            default_logger = logging.getLogger("statshouse")
            default_logger.setLevel(logging.WARNING)
            if not default_logger.handlers:
                handler = logging.StreamHandler()
                handler.setLevel(logging.WARNING)
                formatter = logging.Formatter('%(message)s')
                handler.setFormatter(formatter)
                default_logger.addHandler(handler)
            self.log_f = default_logger.warning
        else:
            self.log_f = logger
        self.log_t = 0.0

        # Transport
        self.transport_mu = threading.RLock()
        self.app = app_name
        self.env = env
        self.network = network or DEFAULT_STATSHOUSE_NETWORK
        self.addr_str = addr
        self.addr: Optional[Tuple[str, int]] = None
        self.conn: Optional[Union[socket.socket, TCPConn]] = None

        # Data
        self.mu = threading.RLock()
        self.w: Dict[Tuple[str, Tuple[str, ...]], Bucket] = {}  # metric key -> bucket
        self.r: List[Bucket] = []  # buckets to send
        self.ts_unix_sec = int(time.time())
        self.max_bucket_size = max_bucket_size

        # Reusable buffer for metrics batch (same as Go packet.buf reuse)
        self._metrics_batch: List[Dict] = []

        # Shutdown
        self.close_once = threading.Lock()
        self.close_called = False
        self.close_ch = Queue()
        self.send_thread = threading.Thread(target=self._run, daemon=True)
        self.send_thread.start()

        # Initialize transport
        if addr:
            p = urllib.parse.urlsplit("//" + addr, "", False)
            self.addr = (p.hostname, p.port)
            if self.network == "udp":
                self.conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            elif self.network == "tcp":
                # TCP connection will be created on first send
                pass
            else:
                raise ValueError(
                    f"unsupported statshouse network: {self.network!r}, expected 'udp' or 'tcp'"
                )

    def rare_log(self, format_str: str, *args):
        """Log errors, but not more than once per ERROR_REPORTING_PERIOD.

        Public method (same as Go rareLog) for use by TCPConn.
        """
        with self.log_mu:
            now = time.time()
            if now - self.log_t < ERROR_REPORTING_PERIOD:
                return
            self.log_t = now
        if args:
            self.log_f(format_str.replace("%v", "%s"), *args)
        else:
            self.log_f(format_str)

    def _rare_log(self, format_str: str, *args):
        """Internal alias for rare_log (for backward compatibility)."""
        self.rare_log(format_str, *args)

    def _run(self):
        """Main send loop, runs every DEFAULT_SEND_PERIOD seconds."""
        now = time.time()
        while True:
            try:
                # Calculate next send time (aligned to second boundary)
                next_send = math.ceil(now / DEFAULT_SEND_PERIOD) * DEFAULT_SEND_PERIOD
                timeout = max(0.1, next_send - time.time())

                # Wait for next send time or close signal
                try:
                    signal = self.close_ch.get(timeout=timeout)
                    if signal is not None:  # Close signal
                        self._send(0)  # Last send
                        signal.put(None)  # Signal done
                        break
                except Empty:
                    pass

                # Send accumulated metrics
                now = time.time()
                self._send(int(now))
            except Exception as e:
                self._rare_log("[statshouse] error in send loop: %v", e)
                now = time.time()

    def _send(self, now_unix_sec: int):
        """Send accumulated metrics."""
        # Load and switch second
        with self.mu:
            buckets_to_send = self.r[:]
            send_unix_sec = self.ts_unix_sec
            self.ts_unix_sec = now_unix_sec

        # Swap buckets
        for bucket in buckets_to_send:
            bucket.swap_to_send(now_unix_sec)

        # Send (reuse buffer like Go packet.buf)
        with self.transport_mu:
            # Clear and reuse buffer (same as Go packet.buf reuse)
            self._metrics_batch.clear()
            for bucket in buckets_to_send:
                if bucket.empty_send():
                    continue

                # Get metric name and tags from bucket
                metric_name = bucket.metric_name
                tags_dict = bucket.tags_dict.copy()

                # Convert bucket to msgpack format
                if bucket.count_to_send > 0:
                    self._metrics_batch.append({
                        "name": metric_name,
                        "tags": tags_dict,
                        "counter": bucket.count_to_send,
                    })

                if bucket.value_to_send:
                    metric = {
                        "name": metric_name,
                        "tags": tags_dict,
                        "value": tuple(bucket.value_to_send),
                    }
                    if bucket.value_count_to_send != len(bucket.value_to_send):
                        metric["counter"] = float(bucket.value_count_to_send)
                    self._metrics_batch.append(metric)

                if bucket.unique_to_send:
                    metric = {
                        "name": metric_name,
                        "tags": tags_dict,
                        "unique": tuple(bucket.unique_to_send),
                    }
                    if bucket.unique_count_to_send != len(bucket.unique_to_send):
                        metric["counter"] = float(bucket.unique_count_to_send)
                    self._metrics_batch.append(metric)

                for skey in bucket.stop_to_send:
                    stop_tags = tags_dict.copy()
                    stop_tags[TAG_STRING_TOP] = skey
                    self._metrics_batch.append({
                        "name": metric_name,
                        "tags": stop_tags,
                        "counter": 1.0,
                    })

            if self._metrics_batch:
                self._flush(self._metrics_batch, send_unix_sec)

        # Cleanup empty buckets (same logic as Go)
        with self.mu:
            i, n = 0, len(self.r)
            while i < n:
                bucket = self.r[i]
                if not bucket.empty_send():
                    i += 1
                    with bucket.mu:
                        bucket.empty_send_count = 0
                    continue

                # Bucket is empty - check BEFORE incrementing (same as Go)
                with bucket.mu:
                    if bucket.empty_send_count < MAX_EMPTY_SEND_COUNT:
                        i += 1
                        bucket.empty_send_count += 1
                        continue
                    else:
                        bucket.empty_send_count = 0  # Reset before removal (same as Go)
                    # Remove bucket (same order as Go: bucket.mu then client.mu)
                    # Find and remove from self.w
                    for key, b in list(self.w.items()):
                        if b is bucket:
                            del self.w[key]
                            break
                    bucket.attached = False

                # Remove from self.r (in-place, same as Go)
                n -= 1
                self.r[i] = self.r[n]
                self.r[n] = None  # Release reference
            # Trim self.r
            if n < len(self.r):
                self.r = self.r[:n]

    def _flush(self, metrics_batch: List[Dict], ts_unix_sec: int):
        """Flush metrics batch to transport."""
        if not metrics_batch:
            return

        if ts_unix_sec != 0:
            for metric in metrics_batch:
                metric["ts"] = ts_unix_sec

        packet = {"metrics": tuple(metrics_batch)}
        data = msgpack.packb(packet)

        if self.addr is None:
            return

        if self.network == "udp":
            if self.conn is None:
                self.conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                self.conn.sendto(data, self.addr)
            except Exception as e:
                self._rare_log("[statshouse] failed to send data to statshouse: %v", e)
        else:  # TCP
            if self.conn is None:
                try:
                    self.conn = TCPConn(self, self.addr, self.app, self.env)
                    self.conn.connect()
                except Exception as e:
                    self._rare_log("[statshouse] failed to dial statshouse: %v", e)
                    return

            if isinstance(self.conn, TCPConn):
                try:
                    # TCP: prefix 4 bytes (little-endian)
                    header = struct.pack("<I", len(data))
                    self.conn.write(header + data)
                    # Data is now in queue, will be sent by TCPConn._send_loop()
                    # Even if network send fails, data stays in queue and will be retried (same as Go)
                except RuntimeError as e:
                    if "would block" in str(e):
                        # Queue full - data is lost (same as Go)
                        # Connection stays, will retry on next send
                        pass
                    # Other RuntimeError (e.g. "write after close") - ignore, connection managed by _send_loop()
                except Exception as e:
                    self._rare_log("[statshouse] failed to send data to statshouse: %v", e)
                    # Connection stays, data in queue will be retried by _send_loop() (same as Go)

    def _normalize_tags(self, tags: Tags) -> Dict[str, str]:
        """Normalize tags to dictionary format."""
        if isinstance(tags, (tuple, list)):
            tags = {f"{i + 1}": v for i, v in enumerate(tags) if v is not None}
        has_env = "env" in tags or "0" in tags
        if not has_env:
            tags["0"] = self.env
        return tags

    def _get_bucket(self, metric: str, tags: Tags) -> Bucket:
        """Get or create bucket for metric+tags."""
        # Create key
        tags_dict = self._normalize_tags(tags)
        key = (metric, tuple(sorted(tags_dict.items())))

        with self.mu:
            if key in self.w:
                return self.w[key]

            bucket = Bucket(metric, tags_dict, max_size=self.max_bucket_size)
            bucket.attached = True
            self.w[key] = bucket
            self.r.append(bucket)
            return bucket

    def count(self, metric: str, tags: Tags, n: Real, *, ts: Real = 0):
        """Record counter metric."""
        if ts != 0:
            # Historic metric - send immediately
            self._send_historic(metric, tags, {"counter": float(n)}, int(ts))
        else:
            bucket = self._get_bucket(metric, tags)
            with bucket.mu:
                bucket.count += float(n)

    def value(
            self, metric: str, tags: Tags, v: OneOrMany[Real], *, ts: Real = 0, n: Real = 0
    ):
        """Record value metric."""
        v = (v,) if isinstance(v, Real) else v
        if ts != 0:
            # Historic metric - send immediately
            metric_dict = {"value": tuple(float(f) for f in v)}
            if n != 0:
                metric_dict["counter"] = float(n)
            self._send_historic(metric, tags, metric_dict, int(ts))
        else:
            bucket = self._get_bucket(metric, tags)
            with bucket.mu:
                if n != 0:
                    bucket.count += float(n)
                bucket.append_value(*(float(f) for f in v))

    def unique(
            self,
            metric: str,
            tags: Tags,
            v: OneOrMany[Integral],
            *,
            ts: Real = 0,
            n: Real = 0,
    ):
        """Record unique metric."""
        v = (v,) if isinstance(v, Integral) else v
        if ts != 0:
            # Historic metric - send immediately
            metric_dict = {"unique": tuple(int(i) for i in v)}
            if n != 0:
                metric_dict["counter"] = float(n)
            self._send_historic(metric, tags, metric_dict, int(ts))
        else:
            bucket = self._get_bucket(metric, tags)
            with bucket.mu:
                if n != 0:
                    bucket.count += float(n)
                bucket.append_unique(*(int(i) for i in v))

    def _send_historic(self, metric: str, tags: Tags, metric_data: Dict, ts_unix_sec: int):
        """Send historic metric immediately."""
        tags_dict = self._normalize_tags(tags)
        metric_dict = {
            "name": metric,
            "tags": tags_dict,
            "ts": ts_unix_sec,
            **metric_data,
        }
        with self.transport_mu:
            self._flush([metric_dict], ts_unix_sec)

    def close(self):
        """Close client and flush remaining metrics."""
        with self.close_once:
            if self.close_called:
                return
            self.close_called = True

        # Signal close
        ch = Queue()
        self.close_ch.put(ch)
        ch.get()  # Wait for send thread to finish

        # Close transport
        with self.transport_mu:
            if self.conn:
                if isinstance(self.conn, TCPConn):
                    self.conn.close()
                else:
                    self.conn.close()
                self.conn = None


def _init_global() -> Client:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--statshouse-addr",
        type=str,
        default=os.getenv("STATSHOUSE_ADDR", DEFAULT_STATSHOUSE_ADDR),
    )
    p.add_argument(
        "--statshouse-env", type=str, default=os.getenv("STATSHOUSE_ENV", "")
    )
    p.add_argument(
        "--statshouse-network",
        type=str,
        default=os.getenv("STATSHOUSE_NETWORK", DEFAULT_STATSHOUSE_NETWORK),
        help="transport protocol for StatsHouse agent connection, 'udp' (default) or 'tcp'",
    )

    args, left = p.parse_known_args()
    sys.argv = sys.argv[:1] + left

    return Client(
        addr=args.statshouse_addr,
        env=args.statshouse_env,
        network=args.statshouse_network,
    )


__sh = _init_global()


def count(metric: str, tags: Tags, n: Real, *, ts: Real = 0):
    return __sh.count(metric, tags, n, ts=ts)


def value(metric: str, tags: Tags, v: OneOrMany[Real], *, ts: Real = 0, n: Real = 0):
    return __sh.value(metric, tags, v, ts=ts, n=n)


def unique(
        metric: str, tags: Tags, v: OneOrMany[Integral], *, ts: Real = 0, n: Real = 0
):
    return __sh.unique(metric, tags, v, ts=ts, n=n)
