import datetime
import os
import sys
import types
from unittest.mock import Mock

import pytest

# Ensure the repository root is on PYTHONPATH for pytest execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Stub boto3 for import-time loading of ingestion.utils when boto3 is not installed in the environment.
if 'boto3' not in sys.modules:
    boto3_stub = types.ModuleType('boto3')
    boto3_stub.client = lambda *args, **kwargs: None
    sys.modules['boto3'] = boto3_stub

# Stub yfinance for import-time loading of ingestion.yfinance_ingestion when yfinance is not installed.
if 'yfinance' not in sys.modules:
    yfinance_stub = types.ModuleType('yfinance')
    yfinance_stub.Ticker = lambda ticker: None
    sys.modules['yfinance'] = yfinance_stub

from ingestion import yfinance_ingestion


class MockDataFrame:
    def __init__(self, data):
        self._data = data

    @property
    def empty(self):
        return not bool(self._data)

    def to_dict(self):
        return self._data


class MockTicker:
    def __init__(self, financials, balance_sheet, cashflow, info, history_rows=None):
        self.financials = financials
        self.balance_sheet = balance_sheet
        self.cashflow = cashflow
        self.info = info
        self._history_rows = history_rows or []

    def history(self, period):
        assert period == '5y'
        return MockHistoryFrame(self._history_rows)


class MockHistoryFrame:
    def __init__(self, rows):
        self._rows = rows

    def reset_index(self):
        return self

    def to_dict(self, orient):
        assert orient == 'records'
        return self._rows


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
        history_rows=[{'Date': '2024-12-31', 'Close': 123.45, 'Volume': 1000}],
    )
    monkeypatch.setattr(yfinance_ingestion.yf, 'Ticker', lambda ticker: mock_ticker)

    result = yfinance_ingestion.fetch_company_financials('FAKE')

    assert isinstance(result, dict)
    assert result['ticker'] == 'FAKE'
    assert result['data_source'] == 'yfinance'
    assert result['info'] == {'sector': 'Technology'}
    assert result['price_history'] == [{'Date': '2024-12-31', 'Close': 123.45, 'Volume': 1000}]


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


def test_fetch_company_financials_includes_price_history(monkeypatch):
    mock_ticker = MockTicker(
        financials=make_income_statement_df(),
        balance_sheet=make_balance_sheet_df(),
        cashflow=make_cashflow_df(),
        info={},
        history_rows=[
            {'Date': '2024-12-30', 'Close': 100.0, 'Volume': 1000},
            {'Date': '2024-12-31', 'Close': 101.5, 'Volume': 1200},
        ],
    )
    monkeypatch.setattr(yfinance_ingestion.yf, 'Ticker', lambda ticker: mock_ticker)

    result = yfinance_ingestion.fetch_company_financials('FAKE')

    assert result['price_history'] == [
        {'Date': '2024-12-30', 'Close': 100.0, 'Volume': 1000},
        {'Date': '2024-12-31', 'Close': 101.5, 'Volume': 1200},
    ]


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


def test_fetch_company_financials_handles_yfinance_exceptions(monkeypatch):
    def raise_error(ticker):
        raise RuntimeError('yfinance failure')

    monkeypatch.setattr(yfinance_ingestion.yf, 'Ticker', raise_error)

    assert yfinance_ingestion.fetch_company_financials('FAIL') is None


def test_run_ingestion_reuses_s3_client_and_skips_empty_payloads(monkeypatch):
    upload_mock = Mock()
    sleep_mock = Mock()
    shared_client = object()

    monkeypatch.setattr(yfinance_ingestion.utils, 'get_s3_client', lambda: shared_client)

    results = [
        {'ticker': 'AAPL', 'ingestion_timestamp': '2024-11-05T12:00:00'},
        None,
        {'ticker': 'MSFT', 'ingestion_timestamp': '2024-11-05T12:00:00'},
    ]

    def fake_fetch(_ticker):
        return results.pop(0)

    monkeypatch.setattr(yfinance_ingestion, 'fetch_company_financials', fake_fetch)

    yfinance_ingestion.run_ingestion(
        ['AAPL', 'EMPTY', 'MSFT'],
        uploader=upload_mock,
        sleeper=sleep_mock,
    )

    assert upload_mock.call_count == 2
    assert upload_mock.call_args_list[0].kwargs['s3_client'] is shared_client
    assert upload_mock.call_args_list[1].kwargs['s3_client'] is shared_client
    assert sleep_mock.call_count == 2
