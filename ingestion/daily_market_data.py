import yfinance as yf
from ingestion import utils, config
import logging
import json

def run_market_ingestion(tickers):
    """Fetches daily price/market cap and pushes to S3 Bronze."""
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            # period='1d' is the specific requirement for the daily DAG [3, 4]
            hist = stock.history(period='1d')
            
            if hist.empty:
                continue

            payload = {
                "ticker": ticker,
                "price": float(hist['Close'].iloc[-1]),
                "price_date": str(hist.index[-1].date()),
                "market_cap": stock.info.get('marketCap'),
                "ingestion_timestamp": utils.datetime.datetime.now().isoformat()
            }
            
            # Use data_type="market_data" to trigger the new S3 folder logic
            utils.upload_to_s3(payload, ticker, data_type="market_data")
            
        except Exception as e:
            logging.error(f"Error fetching {ticker}: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Use TEST_TICKERS (the small list) for your local terminal test
    run_market_ingestion(config.TEST_TICKERS)