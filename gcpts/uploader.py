import io
from typing import Dict
from typing import Optional
import pandas as pd
from gcpts.protocol import GCPTSProtocol
from google.cloud import bigquery


def upsert_table(
    self: GCPTSProtocol,
    df: pd.DataFrame,
    table_name: str,
    dtypes: Optional[Dict[str, str]] = None,
) -> None:
    _dtypes = {
        "partition_dt": "datetime64[ns, UTC]",
        "dt": "datetime64[ns, UTC]",
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

    table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="partition_dt",
        ),
        source_format=bigquery.SourceFormat.CSV,
    )
    dates = df["partition_dt"].unique()

    jobs = []
    for date in dates:
        partition_table_id = f"{table_id}${date.strftime('%Y%m%d')}"
        part_df = df.loc[df["partition_dt"] == date]
        b_buf = io.BytesIO()
        part_df.to_csv(b_buf, index=False)
        b_buf.seek(0)
        jobs.append(
            self.bq_client.load_table_from_file(
                b_buf,
                partition_table_id,
                job_config=job_config,
            )
        )
    for job in jobs:
        print(job.result())


class Uploader:
    def upload(
        self: GCPTSProtocol,
        table_name: str,
        df: pd.DataFrame,
        dtype: Optional[Dict[str, str]] = None,
    ):
        upsert_table(self, df, table_name, dtype)
