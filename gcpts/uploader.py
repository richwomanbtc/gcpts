from typing import Dict
from typing import Optional
import pandas as pd
from gcpts.protocol import GCPTSProtocol
from google.cloud import bigquery


def upsert_table(
    self: GCPTSProtocol,
    df: pd.DataFrame,
    table: str,
    dtypes: Optional[Dict[str, str]] = None,
) -> None:
    _dtypes = {
        "partition_dt": "datetime64[ns]",
        "dt": "datetime64[ns]",
        "symbol": "string",
    }

    for (key, value) in _dtypes.items():
        if key not in df.columns:
            raise ValueError(f"Column {key} must be given with dtype {value}")

    if dtypes is not None:
        for k, v in dtypes.items():
            _dtypes[k] = v

    df = df.astype(_dtypes)
    df["partition_dt"] = df["partition_dt"].dt.date

    table_id = f"{self.project_id}.{self.dataset_id}.{table}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="partition_dt"
        ),
    )
    dates = df["partition_dt"].unique()
    client = bigquery.Client()
    for date in dates:
        partition_table_id = f"{table_id}${date.strftime('%Y%m%d')}"
        part_df = df.loc[df["partition_dt"] == date]
        job = client.load_table_from_dataframe(
            part_df, partition_table_id, job_config=job_config
        )
        print(job.result())
    client.close()


class Uploader:
    def upload(
        self: GCPTSProtocol,
        table: str,
        df: pd.DataFrame,
        dtype: Optional[Dict[str, str]] = None,
    ):
        upsert_table(self, df, table, dtype)
