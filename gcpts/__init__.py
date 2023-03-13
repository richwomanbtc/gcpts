from gcpts.uploader import Uploader
from gcpts.sql.basic import Query
from gcpts.sql.resample import ResampleQuery
from google.cloud import bigquery

__version__ = "0.1.0"


class GCPTS(Query, ResampleQuery, Uploader):
    def __init__(self, project_id, dataset_id, *args, **kwargs):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=self.project_id, *args, **kwargs)
