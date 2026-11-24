"""LMDB-backed pydbsp storage for Kafi Streams.

Install the optional dependency with ``pip install 'kafi[lmdb]'``.
"""

from contextlib import contextmanager
from pathlib import Path
from threading import local
from typing import Any

import cloudpickle
import msgpack

from pydbsp.core import Antichain
from pydbsp.progress import NodeId, Time
from pydbsp.storage import _MISSING


class LMDBStorage:
    """Store pydbsp's ``(node, time)`` cells in an LMDB environment."""

    def __init__(self, path: str | Path, map_size: int = 1 << 40):
        try:
            import lmdb
        except ImportError as e:
            raise ImportError("Install LMDB support with `pip install 'kafi[lmdb]'`.") from e

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._env = lmdb.open(str(path), map_size=map_size, subdir=True, create=True)
        self._local = local()

    @contextmanager
    def transaction(self):
        """Batch evaluator reads, writes and evictions in one LMDB transaction."""
        current = getattr(self._local, "txn", None)
        if current is not None:
            yield
            return

        txn = self._env.begin(write=True)
        self._local.txn = txn
        try:
            yield
        except BaseException:
            txn.abort()
            raise
        else:
            txn.commit()
        finally:
            self._local.txn = None

    @staticmethod
    def _prefix(node: NodeId) -> bytes:
        return node.to_bytes(8, "big", signed=False)

    @classmethod
    def _key(cls, node: NodeId, t: Time) -> bytes:
        return cls._prefix(node) + msgpack.packb(t, use_bin_type=True)

    def _read_txn(self):
        return getattr(self._local, "txn", None)

    def write(self, node: NodeId, t: Time, value: Any) -> None:
        key = self._key(node, t)
        data = cloudpickle.dumps(value)
        txn = self._read_txn()
        if txn is not None:
            txn.put(key, data)
        else:
            with self.transaction():
                self._read_txn().put(key, data)

    def read(self, node: NodeId, t: Time, default: Any = _MISSING) -> Any:
        txn = self._read_txn()
        if txn is None:
            with self._env.begin() as txn:
                data = txn.get(self._key(node, t))
        else:
            data = txn.get(self._key(node, t))
        if data is None:
            if default is _MISSING:
                raise KeyError((node, t))
            return default
        return cloudpickle.loads(data)

    def contains(self, node: NodeId, t: Time) -> bool:
        txn = self._read_txn()
        if txn is None:
            with self._env.begin() as txn:
                return txn.get(self._key(node, t)) is not None
        return txn.get(self._key(node, t)) is not None

    def evict(self, node: NodeId, t: Time) -> None:
        txn = self._read_txn()
        if txn is not None:
            txn.delete(self._key(node, t))
        else:
            with self.transaction():
                self._read_txn().delete(self._key(node, t))

    def evict_dominated(self, node: NodeId, dead: Antichain[Time]) -> int:
        prefix = self._prefix(node)
        txn = self._read_txn()
        if txn is None:
            with self.transaction():
                return self._evict_dominated(self._read_txn(), prefix, dead)
        return self._evict_dominated(txn, prefix, dead)

    @staticmethod
    def _evict_dominated(txn, prefix: bytes, dead: Antichain[Time]) -> int:
        keys = []
        with txn.cursor() as cursor:
            if cursor.set_range(prefix):
                while cursor.key().startswith(prefix):
                    t = tuple(msgpack.unpackb(cursor.key()[len(prefix):], raw=False))
                    if dead.covers(t):
                        keys.append(cursor.key())
                    if not cursor.next():
                        break
        for key in keys:
            txn.delete(key)
        return len(keys)

    def times(self, node: NodeId) -> list[Time]:
        prefix = self._prefix(node)
        times = []
        with self._env.begin() as txn:
            with txn.cursor() as cursor:
                if cursor.set_range(prefix):
                    while cursor.key().startswith(prefix):
                        times.append(tuple(msgpack.unpackb(cursor.key()[len(prefix):], raw=False)))
                        if not cursor.next():
                            break
        return times

    def size(self, node: NodeId | None = None) -> int:
        if node is not None:
            return len(self.times(node))
        with self._env.begin() as txn:
            return txn.stat()["entries"]

    def state_size(self) -> int:
        """Return the current LMDB data file size in bytes."""
        data_file = self._path / "data.mdb"
        return data_file.stat().st_size if data_file.exists() else 0

    def close(self) -> None:
        self._env.close()
