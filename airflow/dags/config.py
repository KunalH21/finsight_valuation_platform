from datetime import timedelta

# Why this matters: This is the "Data Contract" for your factory.
DEFAULT_ARGS = {
    'owner': 'Kunal',
    'depends_on_past': False, # Allows us to run today even if yesterday failed
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5), # If the API is busy, wait 5 mins
}

# These are the "Heartbeats" we discussed
QUARTERLY_CRON = '0 6 1 2,5,8,11 *' # 6 AM on Earnings Months
DAILY_MARKET_CRON = '0 22 * * 1-5'   # 10 PM on Weekdays