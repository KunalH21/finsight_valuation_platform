import yfinance as yf
import logging
import time
import datetime
from ingestion import config, utils

logger = logging.getLogger(__name__)

def fetch_company_financials(ticker: str) -> dict:
    """
    Fetches raw financial statements and bundles them into a self-describing 
    JSON object for the Bronze layer.
    """
    try:
        stock = yf.Ticker(ticker)
        
        # [1] Pitfall check: Some tickers have incomplete data. 
        # Never let one bad ticker crash the full ingestion run.
        if stock.financials is None or stock.financials.empty:
            logger.warning(f"No financial data found for {ticker}. Skipping.")
            return None

        # [2, 3] MAANG Rule: Preserve raw response exactly. 
        # Do not rename 'Total Revenue' to 'revenue' yet.
        balance_sheet = stock.balance_sheet.to_dict() if getattr(stock, 'balance_sheet', None) is not None else {}
        cash_flow = stock.cashflow.to_dict() if getattr(stock, 'cashflow', None) is not None else {}
        info = stock.info if getattr(stock, 'info', None) is not None else {}
        price_history = []

        if hasattr(stock, 'history'):
            history_df = stock.history(period='5y')
            if history_df is not None:
                price_history = history_df.reset_index().to_dict(orient='records')

        return {
            'ticker': ticker,
            'ingestion_timestamp': datetime.datetime.utcnow().isoformat(),
            'data_source': 'yfinance',
            'income_statement': stock.financials.to_dict(),
            'balance_sheet':    balance_sheet,
            'cash_flow':        cash_flow,
            'info':             info, # Added for dim_company metadata [4]
            'price_history':    price_history
        }
    except Exception as e:
        logger.error(f"Unexpected error fetching {ticker}: {e}")
        return None


def run_ingestion(tickers, uploader=utils.upload_to_s3, sleeper=time.sleep):
    s3_client = utils.get_s3_client()

    logger.info(f"Starting ingestion for {len(tickers)} companies...")

    for ticker in tickers:
        data = fetch_company_financials(ticker)

        if not data:
            continue

        uploader(data, ticker, s3_client=s3_client)
        sleeper(0.5)

    logger.info("Week 1 Ingestion Milestone Complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    run_ingestion(config.TEST_TICKERS)
