import os
import pandas as pd
import asyncio
from nlp_functions import pull_sec_data_single_ticker, fetch_cik_from_ticker
import datamule as dm
import aiohttp
import subprocess
from config import CONFIG  # Import the config dictionary

# Function to download filings for a given ticker
async def download_filings(ticker, cik, base_dir):
    url = f'https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik}&action=getcompany'

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                html = await response.text()
                filing_links = []
                start_index = html.find('<a href="')
                while start_index != -1:
                    end_index = html.find('"', start_index + 9)
                    link = html[start_index + 9:end_index]
                    if link.endswith('.htm') or link.endswith('.xml'):
                        filing_links.append('https://www.sec.gov' + link)
                    start_index = html.find('<a href="', end_index)
                
                filing_dir = os.path.join(base_dir, ticker, 'filings')
                if not os.path.exists(filing_dir):
                    os.makedirs(filing_dir)

                for filing_url in filing_links:
                    filing_name = filing_url.split('/')[-1]
                    filing_path = os.path.join(filing_dir, filing_name)
                    
                    try:
                        async with session.get(filing_url) as filing_response:
                            if filing_response.status == 200:
                                with open(filing_path, 'wb') as f:
                                    f.write(await filing_response.read())
                                print(f"Downloaded filing: {filing_name}")
                            else:
                                print(f"Failed to download filing: {filing_name}")
                    except Exception as e:
                        print(f"Error downloading filing {filing_url}: {str(e)}")
            else:
                print(f"Failed to fetch filings page for ticker {ticker}")

# Function to process SEC data for multiple tickers and save as CSV
async def process_sec_data(tickers, start, end, base_dir):
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    downloader = dm.Downloader()
    all_data = []

    for ticker in tickers:
        try:
            df, unit_dfs = await pull_sec_data_single_ticker(ticker, downloader)
            df_filtered = df[(df['Filed Date'] >= start) & (df['Filed Date'] <= end)]

            ticker_dir = os.path.join(base_dir, ticker)
            if not os.path.exists(ticker_dir):
                os.makedirs(ticker_dir)

            df_filtered.to_csv(f'{ticker_dir}/sec_data_{ticker}.csv', index=False)
            print(f"Data for ticker {ticker} saved successfully.")

            for unit_name, unit_df in unit_dfs.items():
                unit_df_filtered = unit_df[(unit_df['Filed Date'] >= start) & (unit_df['Filed Date'] <= end)]
                unit_df_filtered.to_csv(f'{ticker_dir}/{unit_name}_{ticker}.csv', index=False)
                print(f"Unit data for {unit_name} of ticker {ticker} saved successfully.")

            all_data.append(df_filtered)

            cik = await fetch_cik_from_ticker(ticker, downloader.headers)
            await download_filings(ticker, cik, base_dir)

        except Exception as e:
            print(f"Failed to process data for ticker {ticker}: {str(e)}")

    all_data_df = pd.concat(all_data, ignore_index=True)
    all_data_df.to_csv(f'{base_dir}/sec_data_all_tickers.csv', index=False)
    print(f"Combined data for all tickers saved to sec_data_all_tickers.csv.")

# Function to run the Jupyter notebook
def run_notebook(notebook_path):
    try:
        print(f"Running Jupyter notebook: {notebook_path}")
        subprocess.run(['jupyter', 'nbconvert', '--to', 'notebook', '--execute', notebook_path], check=True)
        print(f"Successfully ran notebook: {notebook_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error running notebook: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while running the notebook: {e}")

# Main function to run the processing
def main():
    # Initialize parameters from config.py
    tickers = CONFIG['TICKERS']
    start = CONFIG['START_DATE']
    end = CONFIG['END_DATE']
    base_dir = CONFIG['BASE_DIR']
    notebook_path = 'sec_filings_download.ipynb'  # Path to your Jupyter notebook

    # Run the process
    asyncio.run(process_sec_data(tickers, start, end, base_dir))

    # Run the Jupyter notebook after processing SEC data
    run_notebook(notebook_path)

# Run the script
if __name__ == '__main__':
    main()
