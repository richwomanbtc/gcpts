# GCP time series

[![codecov](https://codecov.io/gh/richwomanbtc/gcpts/branch/main/graph/badge.svg?token=J728V34ZR5)](https://codecov.io/gh/richwomanbtc/gcpts)

## Requirements
- Python 3.10+

## Installation
```
pip install gcpts
```

## Test
```
poetry run pytest -s -vv
```

## Usage

```python
import gcpts
import pandas as pd
import numpy as np



gcpts_client = gcpts.GCPTS(
    project_id="example_project", 
    dataset_id="example_dataset"
)

# Prepare example data, your data need to have 3 columns named symbol, dt, partition_dt
df = pd.DataFrame(np.random.randn(5000, 4))

df.columns = ['open', 'high', 'low', 'close']

# symbol represent a group of data for given data columns
df['symbol'] = 'BTCUSDT'

# timestamp should be UTC timezone but without tz info
df['dt'] = pd.date_range('2022-01-01', '2022-05-01', freq='15Min')[:5000]

# partition_dt must be date, data will be updated partition by partition with use of this column.
# Every time, you have to upload all the data for a given partition_dt, otherwise older will be gone.
df['partition_dt'] = df['dt'].dt.date.map(lambda x: x.replace(day=1))

gcpts_client.upload(table_name='example_table', df=df)
```

```python
# Query for raw data.
raw_clsoe = gcpts_client.query(
    table_name='example_table',
    field='close',
    start_dt='2022-02-01 00:00:00', # yyyy-mm-dd HH:MM:SS, inclusive
    end_dt='2022-02-05 23:59:59', # yyyy-mm-dd HH:MM:SS, inclusive
    symbols=['BTCUSDT'],
)

# Query for raw data with resampling
resampeld_daily_close = gcpts_client.resample_query(
    table_name='example_table',
    field='close',
    start_dt='2022-01-01 00:00:00', # yyyy-mm-dd HH:MM:SS, inclusive
    end_dt='2022-01-31 23:59:59', # yyyy-mm-dd HH:MM:SS, inclusive
    symbols=['BTCUSDT'],
    interval='day', # month | week | day | hour | {1,2,3,4,6,8,12}hour | minute | {5,15,30}minute
    op='last', # last | first | min | max | sum
)
```

## Disclaimer
This allows you to have SQL injection. Please use it for your own purpose only and do not allow putting arbitrary requests to this library.