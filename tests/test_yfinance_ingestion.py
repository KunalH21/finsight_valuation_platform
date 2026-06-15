import datetime
import json
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

if 'boto3' not in sys.modules:
    boto3_stub = types.ModuleType('boto3')
    boto3_stub.client = lambda *args, **kwargs: None
    sys.modules['boto3'] = boto3_stub

if 'yfinance' not in sys.modules:
    yfinance_stub = types.ModuleType('yfinance')
    yfinance_stub.Ticker = lambda ticker: None
    sys.modules['yfinance'] = yfinance_stub

from ingestion import utils, yfinance_ingestion


class MockDataFrame:
    def __init__(self, data):
        self._data = data

    @property
    def empty(self):
        return not bool(self._data)

    def to_dict(self):
        return self._data


class MockTicker:
    def __init__(self, financials, balance_sheet, cashflow, info):
        self.financials = financials
        self.balance_sheet = balance_sheet
        self.cashflow = cashflow
        self.info = info


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


def get_line_items(statement):
    return set().union(*(period_data.keys() for period_data in statement.values()))


def test_fetch_company_financials_returns_dict_for_valid_ticker(monkeypatch):
    mock_ticker = MockTicker(
        financials=make_income_statement_df(),
        balance_sheet=make_balance_sheet_df(),
        cashflow=make_cashflow_df(),
        info={'sector': 'Technology'},
    )
    monkeypatch.setattr(yfinance_ingestion.yf, 'Ticker', lambda ticker: mock_ticker)

    result = yfinance_ingestion.fetch_company_financials('FAKE')

    assert isinstance(result, dict)
    assert result['ticker'] == 'FAKE'
    assert result['data_source'] == 'yfinance'
    assert result['info'] == {'sector': 'Technology'}


def test_fetch_company_financials_adds_ingestion_timestamp(monkeypatch):
    mock_ticker = MockTicker(
        financials=make_income_statement_df(),
        balance_sheet=make_balance_sheet_df(),
        cashflow=make_cashflow_df(),
        info={},
    )
    monkeypatch.setattr(yfinance_ingestion.yf, 'Ticker', lambda ticker: mock_ticker)

    result = yfinance_ingestion.fetch_company_financials('FAKE')

    assert 'ingestion_timestamp' in result
    assert isinstance(result['ingestion_timestamp'], str)
    datetime.datetime.fromisoformat(result['ingestion_timestamp'])


def test_fetch_company_financials_preserves_required_income_fields(monkeypatch):
    mock_ticker = MockTicker(
        financials=make_income_statement_df(),
        balance_sheet=make_balance_sheet_df(),
        cashflow=make_cashflow_df(),
        info={},
    )
    monkeypatch.setattr(yfinance_ingestion.yf, 'Ticker', lambda ticker: mock_ticker)

    result = yfinance_ingestion.fetch_company_financials('FAKE')
    line_items = get_line_items(result['income_statement'])

    assert {'Total Revenue', 'Net Income', 'EBITDA'} <= line_items


def test_fetch_company_financials_returns_none_when_financials_missing(monkeypatch):
    mock_ticker = MockTicker(
        financials=MockDataFrame({}),
        balance_sheet=make_balance_sheet_df(),
        cashflow=make_cashflow_df(),
        info={},
    )
    monkeypatch.setattr(yfinance_ingestion.yf, 'Ticker', lambda ticker: mock_ticker)

    assert yfinance_ingestion.fetch_company_financials('BAD') is None


def test_fetch_company_financials_exposes_balance_sheet_and_cash_flow(monkeypatch):
    mock_ticker = MockTicker(
        financials=make_income_statement_df(),
        balance_sheet=make_balance_sheet_df(),
        cashflow=make_cashflow_df(),
        info={},
    )
    monkeypatch.setattr(yfinance_ingestion.yf, 'Ticker', lambda ticker: mock_ticker)

    result = yfinance_ingestion.fetch_company_financials('FAKE')

    assert 'balance_sheet' in result
    assert 'cash_flow' in result
    assert list(result['balance_sheet'].values())[0]['Total Assets'] == 5000
    assert list(result['cash_flow'].values())[0]['Operating Cash Flow'] == 400


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
    uploaded = json.loads(captured['Body'])
    assert uploaded['ticker'] == 'FAKE'
    assert uploaded['ingestion_timestamp'] == '2024-11-05T12:00:00'


def test_fetch_company_financials_returns_non_null_ticker(monkeypatch):
    mock_ticker = MockTicker(
        financials=make_income_statement_df(),
        balance_sheet=make_balance_sheet_df(),
        cashflow=make_cashflow_df(),
        info={},
    )
    monkeypatch.setattr(yfinance_ingestion.yf, 'Ticker', lambda ticker: mock_ticker)

    result = yfinance_ingestion.fetch_company_financials('FAKE')

    assert result['ticker'] is not None
    assert result['ticker'] != ''
