from dataclasses import dataclass

from typing import List, Optional, Any
from logging import getLogger
from google.cloud import bigquery
import pandas as pd
from gcpts.protocol import GCPTSProtocol

from .basic import to_where


logger = getLogger(__name__)


@dataclass(frozen=True)
class Expr:
    def to_repr(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class And(Expr):
    exprs: List[Expr]

    def to_repr(self) -> str:
        return " AND ".join([f"({item.to_repr()})" for item in self.exprs])


@dataclass(frozen=True)
class Or(Expr):
    exprs: List[Expr]

    def to_repr(self) -> str:
        return " OR ".join([f"({item.to_repr()})" for item in self.exprs])


@dataclass(frozen=True)
class GT(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} > {self.value}"


@dataclass(frozen=True)
class GTE(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} >= {self.value}"


@dataclass(frozen=True)
class LT(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} < {self.value}"


@dataclass(frozen=True)
class LTE(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} <= {self.value}"


@dataclass(frozen=True)
class Like(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, str)
        return f"{self.field} like {self.value}"


@dataclass(frozen=True)
class Eq(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        if isinstance(self.value, str):
            assert isinstance(self.value, str)
            return f"{self.field} = '{self.value}'"
        if isinstance(self.value, bool):
            str_value = "true" if self.value is True else "false"
            return f"{self.field} = {str_value}"
        if isinstance(self.value, int):
            return f"{self.field} = {self.value}"
        raise NotImplementedError


class ResampleQuery:
    def resample_query(
        self: GCPTSProtocol,
        table_name: str,
        field: str,
        symbols: Optional[List[str]] = None,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
        interval: str = "day",
        tz: Optional[str] = None,
        op: str = "last",
        where: Optional[Expr] = None,
        cast: Optional[str] = None,
        verbose: int = 0,
        offset_repr: Optional[str] = None,
    ):
        bq_client = bigquery.Client()
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        inner_stmt = _build_inner_view(
            table_id=table_id,
            field=field,
            symbols=symbols,
            start_dt=start_dt,
            end_dt=end_dt,
            interval=interval,
            tz=tz,
            where=where,
            offset_repr=offset_repr,
        )

        field_name = "value"
        if cast is not None:
            field_name = f"cast(value as {cast})"

        operator = {
            "last": "ARRAY_AGG({field_name} ORDER BY timestamp desc)[OFFSET(0)]",
            "first": "ARRAY_AGG({field_name} ORDER BY timestamp asc)[OFFSET(0)]",
            "max": "max({field_name})",
            "min": "min({field_name})",
            "sum": "sum({field_name})",
        }[op].format(field_name=field_name)

        stmt = f"select dt, {operator} AS value, symbol \
            from ({inner_stmt}) as t group by dt, symbol order by dt"
        if verbose > 0:
            print(stmt)

        execute_fn = bq_client.query

        df = execute_fn(query=stmt).to_dataframe()

        df = df.set_index(["dt", "symbol"])["value"].unstack()

        df.index = pd.to_datetime(
            df.index,
            format="%Y-%m-%d %H:%M:%S.%f %Z",
            cache=True,
            infer_datetime_format=True,
        )

        if tz is not None:
            if df.index.tz is None:
                df = df.tz_localize(tz)
            else:
                df = df.tz_convert(tz)

        df.index.name = "dt"

        return df.sort_index()


def _build_inner_view(
    table_id: str,
    field: str,
    start_dt: Optional[str] = None,
    end_dt: Optional[str] = None,
    symbols: Optional[List[str]] = None,
    interval: str = "day",
    tz: Optional[str] = None,
    where: Optional[Expr] = None,
    extras: Optional[List[str]] = None,
    offset_repr: Optional[str] = None,
):
    where_conditions = to_where(
        start_dt,
        end_dt,
        partition_key="partition_dt",
        partition_interval="monthly",
        type="DATETIME",
        tz=tz,
    )

    where_term = ""
    if len(where_conditions) > 0:
        where_term = " AND ".join(where_conditions)

    in_term = ""
    if symbols is not None:
        in_term = "symbol in ('" + "','".join(symbols) + "')"

    all_term = where_term

    if len(in_term) > 0:
        if len(all_term) > 0:
            all_term = f"{all_term} AND {in_term}"
        else:
            all_term = in_term

    if where is not None:
        if len(all_term) > 0:
            all_term = f"{all_term} AND ({where.to_repr()})"
        else:
            all_term = where.to_repr()

    if len(all_term) > 0:
        all_term = f" where {all_term}"

    dt_term = to_resampled_dt(interval=interval, tz=tz, offset_repr=offset_repr)

    dt_repr = "dt" if tz is None else f"dt AT TIME ZONE '{tz}'"

    extra_repr = ""
    if extras is not None:
        extra_repr = "," + ",".join(extras)

    return f"""
select
    ({dt_term}) as dt,
    ({dt_repr}) as timestamp,
    {field} as value,
    symbol{extra_repr}
from
    {table_id}
{all_term}
order by timestamp asc
    """


def to_resampled_dt(
    interval: str, tz: Optional[str], offset_repr: Optional[str] = None
):
    """
    Args:
        tz: timezone to be used to resample for date intervals.
        offset_repr: offset term used to shift timestamp before resampling.
    """
    dt_repr = "dt" if tz is None else f"dt AT TIME ZONE '{tz}'"

    if offset_repr is not None:
        dt_repr = f"({dt_repr} {offset_repr})"

    return f"date_trunc({dt_repr}, {interval.upper()})"
