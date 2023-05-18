import argparse
import os
import sys
import socket
import urllib.parse
from numbers import Real, Integral
from typing import Dict, List, Optional, Sequence, Tuple, TypeVar, Union

import msgpack


DEFAULT_STATSHOUSE_ADDR = "127.0.0.1:13337"

T = TypeVar("T")
OneOrMany = Union[T, Sequence[T]]
Tags = Union[Tuple[Optional[str], ...], List[Optional[str]], Dict[str, str]]


class StatsHouse:
    def __init__(self, addr: str, env: str):
        self._env = env
        if addr:
            p = urllib.parse.urlsplit("//" + addr, "", False)
            self._addr = (p.hostname, p.port)
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            self._sock = None

    def _send(self, data: bytes):
        if self._sock is not None:
            self._sock.sendto(data, self._addr)

    def _normalize_tags(self, tags: Tags) -> Dict[str, str]:
        if isinstance(tags, (tuple, list)):
            tags = {f"{i + 1}": v for i, v in enumerate(tags) if v is not None}
        has_env = "env" in tags or "0" in tags
        if not has_env:
            tags["0"] = self._env
        return tags

    def count(self, metric: str, tags: Tags, n: Real):
        packet = {
            "metrics": (
                {
                    "name": metric,
                    "tags": self._normalize_tags(tags),
                    "counter": float(n),
                },
            ),
        }
        data = msgpack.packb(packet)
        self._send(data)

    def value(self, metric: str, tags: Tags, v: OneOrMany[Real]):
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
        data = msgpack.packb(packet)
        self._send(data)

    def unique(self, metric: str, tags: Tags, v: OneOrMany[Integral]):
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
        data = msgpack.packb(packet)
        self._send(data)


def _init_global() -> StatsHouse:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--statshouse-addr",
        type=str,
        default=os.getenv("STATSHOUSE_ADDR", DEFAULT_STATSHOUSE_ADDR),
    )
    p.add_argument(
        "--statshouse-env", type=str, default=os.getenv("STATSHOUSE_ENV", "")
    )

    args, left = p.parse_known_args(sys.argv)
    sys.argv = sys.argv[:1] + left

    return StatsHouse(addr=args.statshouse_addr, env=args.statshouse_env)


__sh = _init_global()


def count(metric: str, tags: Tags, n: Real):
    return __sh.count(metric, tags, n)


def value(metric: str, tags: Tags, v: OneOrMany[Real]):
    return __sh.value(metric, tags, v)


def unique(metric: str, tags: Tags, v: OneOrMany[Integral]):
    return __sh.unique(metric, tags, v)
