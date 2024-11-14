import datetime

today_date = datetime.datetime.today().strftime('%Y-%m-%d')

CONFIG = {
    'TICKERS': ['PHAT'],
    'START_DATE': '2024-01-01',  # You can keep this static or update it to today's date
    'END_DATE': today_date,  # This will set the END_DATE to today's date
    'BASE_DIR': 'sec_data',
    'USER_AGENT': 'Your Name your@email.com',
    'DATABASE_URL': 'NA'
}
