from typing import Protocol


class GCPTSProtocol(Protocol):
    bucket_name: str
    project_id: str
    dataset_id: str
