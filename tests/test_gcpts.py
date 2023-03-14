import uuid
import pandas as pd
import numpy as np
import pytest
import os
from gcpts import GCPTS
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


@pytest.fixture(scope="session")
def gcpts():
    project_id = os.environ["PROJECT_ID"]
    dataset_id = os.environ["DATASET_ID"]
    return GCPTS(project_id=project_id, dataset_id=dataset_id)


@pytest.fixture(scope="session")
def bq_client(gcpts):
    bq_client = bigquery.Client(
        gcpts.project_id,
    )
    yield bq_client
    bq_client.close()


@pytest.fixture(scope="session")
def table_name():
    yield "test_symbol" + uuid.uuid4().hex


@pytest.fixture
def df():
    df = pd.DataFrame(np.random.randn(500, 4), columns=["open", "high", "low", "close"])
    df["symbol"] = "BTCUSDT"
    df["dt"] = pd.date_range("2022-01-01", "2022-05-01", freq="15Min", tz="utc")[:500]
    df["partition_dt"] = df["dt"].dt.date.astype(str)
    return df


@pytest.fixture
def df_vol():
    df_vol = pd.DataFrame(
        np.random.randn(500, 5), columns=["open", "high", "low", "close", "volume"]
    )
    df_vol["symbol"] = "BTCUSDT"
    df_vol["dt"] = pd.date_range("2022-05-01", "2022-10-01", freq="15Min", tz="utc")[
        :500
    ]
    df_vol["partition_dt"] = df_vol["dt"].dt.date.astype(str)
    return df_vol


@pytest.fixture(autouse=True)
def delete_after_run(gcpts, table_name, bq_client):
    yield
    print("\ntear down")
    try:
        table = bq_client.get_table(
            f"{gcpts.project_id}.{gcpts.dataset_id}.{table_name}"
        )
        bq_client.delete_table(table)
        print(f"Table {table.full_table_id} is deleted.")
    except NotFound:
        pass


def test_upload_and_create_table(gcpts, df, bq_client, table_name):
    # upload and create table
    gcpts.upload(table_name, df)

    table = bq_client.get_table(f"{gcpts.project_id}.{gcpts.dataset_id}.{table_name}")

    result_df = (
        bq_client.query(
            f"SELECT * FROM `{gcpts.project_id}.{gcpts.dataset_id}.{table_name}`"
        )
        .result()
        .to_dataframe()
    )
    assert len(result_df) == 500
    assert set(result_df.columns) == {
        "partition_dt",
        "dt",
        "symbol",
        "open",
        "high",
        "low",
        "close",
    }

    assert set(table.schema) == {
        bigquery.SchemaField("partition_dt", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("dt", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("open", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("high", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("low", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("close", "FLOAT", mode="NULLABLE"),
    }


def test_upload(gcpts, df, df_vol, bq_client, table_name):
    # upload and create table
    gcpts.upload(table_name, df)
    # upload another df
    gcpts.upload(table_name, df_vol)

    table = bq_client.get_table(f"{gcpts.project_id}.{gcpts.dataset_id}.{table_name}")
    assert set(table.schema) == {
        bigquery.SchemaField("partition_dt", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("dt", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("open", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("high", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("low", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("close", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("volume", "FLOAT", mode="NULLABLE"),
    }
    result_df = (
        bq_client.query(
            f"SELECT * FROM `{gcpts.project_id}.{gcpts.dataset_id}.{table_name}`"
        )
        .result()
        .to_dataframe()
    )
    assert len(result_df) == 500 * 2
    assert set(result_df.columns) == {
        "partition_dt",
        "dt",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }


def test_upload_two_times(gcpts, df, df_vol, bq_client, table_name):
    # upload and create table
    gcpts.upload(table_name, df)
    # upload another df
    gcpts.upload(table_name, df_vol)
    # test if the data is correct when uploading again
    gcpts.upload(table_name, df_vol)

    result_df = (
        bq_client.query(
            f"SELECT * FROM `{gcpts.project_id}.{gcpts.dataset_id}.{table_name}`"
        )
        .result()
        .to_dataframe()
    )
    assert len(result_df) == 500 * 2
    assert set(result_df.columns) == {
        "partition_dt",
        "dt",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }


def test_basic_query(gcpts, df, df_vol, table_name):
    gcpts.upload(table_name, df)
    gcpts.upload(table_name, df_vol)

    result_df = gcpts.query(
        table_name,
        "open",
        symbols=["BTCUSDT"],
        start_dt="2022-01-01 00:00:00",  # yyyy-mm-dd HH:MM:SS, inclusive
        end_dt="2022-01-15 23:59:59",  # yyyy-mm-dd HH:MM:SS, inclusive
    )

    expected_df = (
        df.set_index([pd.to_datetime(df["dt"], utc=True), "symbol"])
        .sort_index()["open"]
        .unstack()
        .loc[pd.IndexSlice["2022-01-01 00:00:00":"2022-01-15 23:59:59", :]]
    )

    pd.testing.assert_frame_equal(result_df, expected_df)

    # volumes before 05/01 have to be nan
    result_df = gcpts.query(
        table_name,
        "volume",
        symbols=["BTCUSDT"],
        start_dt="2022-01-01 00:00:00",  # yyyy-mm-dd HH:MM:SS, inclusive
        end_dt="2022-01-15 23:59:59",  # yyyy-mm-dd HH:MM:SS, inclusive
    )

    assert result_df.isna().sum().sum() == len(result_df)


def test_list_field_query(gcpts, df, df_vol, table_name):
    gcpts.upload(table_name, df)
    gcpts.upload(table_name, df_vol)

    result_df = gcpts.query(
        table_name,
        ["open", "close"],
        symbols=["BTCUSDT"],
        start_dt="2022-01-01 00:00:00",  # yyyy-mm-dd HH:MM:SS, inclusive
        end_dt="2022-01-15 23:59:59",  # yyyy-mm-dd HH:MM:SS, inclusive
    )

    expected_df = (
        df.set_index([pd.to_datetime(df["dt"], utc=True), "symbol"])
        .sort_index()[["open", "close"]]
        .loc[pd.IndexSlice["2022-01-01 00:00:00":"2022-01-15 23:59:59", :]]
    )
    print(result_df)
    print(expected_df)
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_resample_query(gcpts, df, df_vol, table_name):
    gcpts.upload(table_name, df)
    gcpts.upload(table_name, df_vol)

    result_df = gcpts.resample_query(
        table_name,
        "close",
        symbols=["BTCUSDT"],
        start_dt="2022-01-01 00:00:00",  # yyyy-mm-dd HH:MM:SS, inclusive
        end_dt="2022-01-15 23:59:59",  # yyyy-mm-dd HH:MM:SS, inclusive
        interval="day",
        op="last",
    )

    expected_df = (
        df.set_index([pd.to_datetime(df["dt"], utc=True), "symbol"])
        .sort_index()["close"]
        .unstack()
        .loc[pd.IndexSlice["2022-01-01 00:00:00":"2022-01-15 23:59:59", :]]
        .resample("1D")
        .last()
    )
    pd.testing.assert_frame_equal(result_df, expected_df, check_freq=False)
