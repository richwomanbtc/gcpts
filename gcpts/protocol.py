from typing import Protocol


class GCPTSProtocol(Protocol):
    project_id: str
    dataset_id: str
