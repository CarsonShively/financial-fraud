from __future__ import annotations

import duckdb
from dataclasses import dataclass, field
from typing import Any

@dataclass
class TxnStream:
    parquet_path: str
    start_step: int | None = None
    batch_size: int = 2048

    pos: int = 0
    last_step: int | None = None

    _con: duckdb.DuckDBPyConnection | None = field(default=None, init=False, repr=False)
    _cur: duckdb.DuckDBPyConnection | None = field(default=None, init=False, repr=False)
    _cols: list[str] | None = field(default=None, init=False, repr=False)
    _buffer: list[tuple[Any, ...]] = field(default_factory=list, init=False, repr=False)
    _eof: bool = field(default=False, init=False, repr=False)

    def _ensure_cursor(self) -> None:
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
        """
        Return the next transaction dict and advance the in-memory cursor.
        NOTE: With ORDER BY step only, ties within the same step are arbitrary.
        """
        if self._eof:
            return None

        self._ensure_cursor()

        if not self._buffer:
            rows = self._cur.fetchmany(self.batch_size)
            if not rows:
                self._eof = True
                return None
            self._buffer.extend(rows)

        r = self._buffer.pop(0)
        tx = dict(zip(self._cols or [], r))

        self.pos += 1
        step_val = tx.get("step")
        if isinstance(step_val, int):
            self.last_step = step_val
        else:
            self.last_step = int(step_val) if step_val is not None else self.last_step

        return tx

    def reset(self) -> None:
        """Reset the in-memory cursor (restarts stream from start_step)."""
        self.pos = 0
        self.last_step = None
        self._buffer.clear()
        self._eof = False
        self._cols = None
        self._cur = None
        if self._con is not None:
            try:
                self._con.close()
            finally:
                self._con = None
