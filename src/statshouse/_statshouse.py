import argparse
import os
import sys
import math
import socket
import struct
import urllib.parse
from numbers import Real, Integral
from typing import Dict, List, Optional, Sequence, Tuple, TypeVar, Union

import msgpack


DEFAULT_STATSHOUSE_ADDR = "127.0.0.1:13337"
DEFAULT_STATSHOUSE_NETWORK = "udp"

TAG_STRING_TOP = "_s"
TAG_HOST = "_h"

T = TypeVar("T")
OneOrMany = Union[T, Sequence[T]]
Tags = Union[Tuple[Optional[str], ...], List[Optional[str]], Dict[str, str]]


class Client:
    def __init__(self, addr: str, env: str, network: str = DEFAULT_STATSHOUSE_NETWORK):
        self._env = env
        self._network = network or DEFAULT_STATSHOUSE_NETWORK
        self._sock = None
        self._addr = None

        if addr:
            p = urllib.parse.urlsplit("//" + addr, "", False)
            self._addr = (p.hostname, p.port)
            if self._network == "udp":
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            elif self._network == "tcp":
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(self._addr)
                # send TCP handshake header, same Go-client
                sock.sendall(b"statshousev1")
                self._sock = sock
            else:
                raise ValueError(
                    f"unsupported statshouse network: {self._network!r}, expected 'udp' or 'tcp'"
                )

    def _send(self, packet, ts: Real):
        if self._sock is None:
            return
        if ts != 0:
            packet["metrics"][0]["ts"] = math.floor(ts)
        data = msgpack.packb(packet)
        if self._network == "udp":
            self._sock.sendto(data, self._addr)
        else:
            # TCP: prefix 4 bytes (little-endian)m sane Go-client
            header = struct.pack("<I", len(data))
            self._sock.sendall(header + data)

    def _normalize_tags(self, tags: Tags) -> Dict[str, str]:
        if isinstance(tags, (tuple, list)):
            tags = {f"{i + 1}": v for i, v in enumerate(tags) if v is not None}
        has_env = "env" in tags or "0" in tags
        if not has_env:
            tags["0"] = self._env
        return tags

    def count(self, metric: str, tags: Tags, n: Real, *, ts: Real = 0):
        packet = {
            "metrics": (
                {
                    "name": metric,
                    "tags": self._normalize_tags(tags),
                    "counter": float(n),
                },
            ),
        }
        self._send(packet, ts)

    def value(
        self, metric: str, tags: Tags, v: OneOrMany[Real], *, ts: Real = 0, n: Real = 0
    ):
        v = (v,) if isinstance(v, Real) else v
        packet = {
            "metrics": (
                {
                    "name": metric,
                    "tags": self._normalize_tags(tags),
                    "value": tuple(
                        float(f) for f in v
                    ),  # convert everything to native Python types for msgpack
                },
            ),
        }
        if n != 0:
            packet["metrics"][0]["counter"] = float(n)
        self._send(packet, ts)

    def unique(
        self,
        metric: str,
        tags: Tags,
        v: OneOrMany[Integral],
        *,
        ts: Real = 0,
        n: Real = 0,
    ):
        v = (v,) if isinstance(v, Integral) else v
        packet = {
            "metrics": (
                {
                    "name": metric,
                    "tags": self._normalize_tags(tags),
                    "unique": tuple(
                        int(i) for i in v
                    ),  # convert everything to native Python types for msgpack
                },
            ),
        }
        if n != 0:
            packet["metrics"][0]["counter"] = float(n)
        self._send(packet, ts)


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
