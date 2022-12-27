from dataclasses import dataclass
from gcpts.uploader import Uploader
from gcpts.sql.basic import Query
from gcpts.sql.resample import ResampleQuery


@dataclass
class GCPTS(Query, ResampleQuery, Uploader):
    project_id: str
    dataset_id: str
