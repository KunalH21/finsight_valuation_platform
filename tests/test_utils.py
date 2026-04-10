import json
import os
import sys
import types
import pytest

# Ensure the repository root is on PYTHONPATH for pytest execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Stub boto3 for import-time loading of ingestion.utils when boto3 is not installed in the environment.
if 'boto3' not in sys.modules:
    boto3_stub = types.ModuleType('boto3')
    boto3_stub.client = lambda *args, **kwargs: None
    sys.modules['boto3'] = boto3_stub

from ingestion import utils


class MockDataFrame:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return self._data


def make_income_statement_df():
    return MockDataFrame({
        '2024-12-31': {
            'Total Revenue': 1000,
            'Net Income': 200,
            'EBITDA': 300,
        }
    })


def make_balance_sheet_df():
    return MockDataFrame({
        '2024-12-31': {
            'Total Assets': 5000,
            'Total Liab': 2500,
        }
    })


def make_cashflow_df():
    return MockDataFrame({
        '2024-12-31': {
            'Operating Cash Flow': 400,
            'Free Cash Flow': 150,
        }
    })


def test_upload_to_s3_constructs_expected_partition_path(monkeypatch):
    captured = {}

    def fake_client(service_name, region_name=None):
        class FakeS3Client:
            def put_object(self, Bucket, Key, Body):
                captured['Bucket'] = Bucket
                captured['Key'] = Key
                captured['Body'] = Body

        return FakeS3Client()

    monkeypatch.setattr(utils.boto3, 'client', fake_client)

    data = {
        'ticker': 'FAKE',
        'ingestion_timestamp': '2024-11-05T12:00:00',
        'data_source': 'yfinance',
        'income_statement': make_income_statement_df().to_dict(),
        'balance_sheet': make_balance_sheet_df().to_dict(),
        'cash_flow': make_cashflow_df().to_dict(),
        'info': {},
    }

    utils.upload_to_s3(data, 'FAKE')

    assert captured['Key'] == 'financials/year=2024/ticker=FAKE/data.json'
    assert captured['Bucket'] == utils.config.S3_BRONZE_BUCKET
    assert isinstance(captured['Body'], str)


def test_upload_to_s3_serializes_expected_payload(monkeypatch):
    captured = {}

    def fake_client(service_name, region_name=None):
        class FakeS3Client:
            def put_object(self, Bucket, Key, Body):
                captured['Body'] = Body

        return FakeS3Client()

    monkeypatch.setattr(utils.boto3, 'client', fake_client)

    data = {
        'ticker': 'FAKE',
        'ingestion_timestamp': '2024-11-05T12:00:00',
        'data_source': 'yfinance',
        'income_statement': make_income_statement_df().to_dict(),
        'balance_sheet': make_balance_sheet_df().to_dict(),
        'cash_flow': make_cashflow_df().to_dict(),
        'info': {},
    }

    utils.upload_to_s3(data, 'FAKE')

    uploaded = json.loads(captured['Body'])
    assert uploaded['ticker'] == 'FAKE'
    assert uploaded['ingestion_timestamp'] == '2024-11-05T12:00:00'


def test_stringify_keys_converts_nested_keys_to_strings():
    data = {
        2024: {
            1: 'value',
        },
        'items': [{2: 'nested'}],
    }

    result = utils.stringify_keys(data)

    assert result == {
        '2024': {
            '1': 'value',
        },
        'items': [{'2': 'nested'}],
    }


def test_upload_to_s3_reraises_with_original_exception(monkeypatch):
    class FakeS3Client:
        def put_object(self, Bucket, Key, Body):
            raise RuntimeError('s3 unavailable')

    monkeypatch.setattr(utils, 'get_s3_client', lambda: FakeS3Client())

    data = {
        'ticker': 'FAKE',
        'ingestion_timestamp': '2024-11-05T12:00:00',
        'data_source': 'yfinance',
        'income_statement': {},
        'balance_sheet': {},
        'cash_flow': {},
        'info': {},
    }

    with pytest.raises(RuntimeError, match='s3 unavailable'):
        utils.upload_to_s3(data, 'FAKE')
