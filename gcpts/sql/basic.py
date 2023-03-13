from typing import Callable, Optional, List, Union
import pandas as pd
from gcpts.protocol import GCPTSProtocol
from gcpts.dt import (
    to_quarter_start_dt,
    to_month_start_dt,
    to_quarter_end_dt,
    to_month_end_dt,
)


def to_where(
    start_dt: Optional[str],
    end_dt: Optional[str],
    partition_key: str = "partition_dt",
    partition_interval: str = "quarterly",
    type: str = "TIMESTAMP",
    tz: Optional[str] = None,
):
    start_dt_offset_fn: Callable
    end_dt_offset_fn: Callable

    match partition_interval:
        case "quarterly":
            start_dt_offset_fn = to_quarter_start_dt
            end_dt_offset_fn = to_quarter_end_dt
        case "monthly":
            start_dt_offset_fn = to_month_start_dt
            end_dt_offset_fn = to_month_end_dt
        case _:
            raise ValueError(f"Invalid partition interval: {partition_interval}")

    where = []

    if start_dt is not None:
        _start_dt = pd.Timestamp(start_dt, tz=tz)
        if tz is not None:
            _start_dt = _start_dt.tz_convert("UTC").tz_localize(None)
        _start_dt = start_dt_offset_fn(_start_dt)

        where += [
            f"{partition_key} >= CAST('{_start_dt:%Y-%m-%d}' AS DATE)",
            f"dt >= CAST('{start_dt}' AS {type})",
        ]

    if end_dt is not None:
        _end_dt = pd.Timestamp(end_dt, tz=tz)
        if tz is not None:
            _end_dt = _end_dt.tz_convert("UTC").tz_localize(None)
        _end_dt = end_dt_offset_fn(_end_dt)

        where += [
            f"{partition_key} <= CAST('{_end_dt:%Y-%m-%d}' AS DATE)",
            f"dt <= CAST('{end_dt}' AS {type})",
        ]
    return where


class Query:
    def query(
        self: GCPTSProtocol,
        table_name: str,
        field: Union[str, List[str]],
        symbols: Optional[List[str]] = None,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
        partition_key: str = "partition_dt",
        partition_interval: str = "quarterly",
        type: str = "TIMESTAMP",
    ) -> Union[pd.DataFrame, pd.Series]:
        where = to_where(
            start_dt=start_dt,
            end_dt=end_dt,
            partition_key=partition_key,
            partition_interval=partition_interval,
            type=type,
        )

        if symbols is not None and len(symbols) > 0:
            predicated = "'" + "','".join(symbols) + "'"
            where += [f"symbol in ({predicated})"]
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        if isinstance(field, str):
            stmt = f"SELECT {field}, symbol, dt FROM {table_id}"
        else:
            stmt = f"SELECT {','.join(field)}, symbol, dt FROM {table_id}"

        if len(where) > 0:
            condition = " AND ".join(where)
            stmt += f" WHERE {condition}"
        df = pd.read_gbq(stmt, project_id=self.project_id, use_bqstorage_api=True)

        df["dt"] = pd.to_datetime(df["dt"])

        if isinstance(field, str):
            return df.set_index(["dt", "symbol"])[field].unstack().sort_index()
        else:
            return df.set_index(["dt", "symbol"])[field].sort_index()
