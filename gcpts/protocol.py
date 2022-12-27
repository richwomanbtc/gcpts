from typing import Protocol
from google.cloud import bigquery


class GCPTSProtocol(Protocol):
    project_id: str
    dataset_id: str
    bq_client: bigquery.Client
