from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

import duckdb


@dataclass
class TxnStream:
    parquet_path: str
    start_step: int | None = None
    batch_size: int = 2048

    pos: int = 0
    last_step: int | None = None

    _con: duckdb.DuckDBPyConnection | None = field(default=None, init=False, repr=False)
    _cur: Any = field(default=None, init=False, repr=False)
    _cols: list[str] | None = field(default=None, init=False, repr=False)
    _buf: deque[tuple[Any, ...]] = field(default_factory=deque, init=False, repr=False)
    _eof: bool = field(default=False, init=False, repr=False)

    def _open(self) -> None:
        if self._cur is not None:
            return

        self._con = duckdb.connect(database=":memory:")

        where = ""
        params: list[Any] = [self.parquet_path]
        if self.start_step is not None:
            where = "WHERE step >= ?"
            params.append(self.start_step)

        q = f"""
        SELECT
          step, type, amount,
          nameOrig, nameDest,
          oldbalanceOrg, newbalanceOrig,
          oldbalanceDest, newbalanceDest
        FROM read_parquet(?)
        {where}
        ORDER BY step
        """

        self._cur = self._con.execute(q, params)
        self._cols = [c[0] for c in self._cur.description]

    def cursor(self) -> dict[str, Any]:
        return {"pos": self.pos, "last_step": self.last_step, "start_step": self.start_step}

    def next_one(self) -> dict[str, Any] | None:
        if self._eof:
            return None

        self._open()

        if not self._buf:
            rows = self._cur.fetchmany(self.batch_size)
            if not rows:
                self._eof = True
                return None
            self._buf.extend(rows)

        row = self._buf.popleft()
        tx = dict(zip(self._cols or [], row))

        self.pos += 1
        step_val = tx.get("step")
        if step_val is not None:
            self.last_step = int(step_val)

        return tx

    def reset(self) -> None:
        self.pos = 0
        self.last_step = None
        self._buf.clear()
        self._eof = False
        self._cols = None
        self._cur = None
        if self._con is not None:
            try:
                self._con.close()
            finally:
                self._con = None
