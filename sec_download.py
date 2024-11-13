import os
import pandas as pd
import asyncio
from nlp_functions import pull_sec_data_single_ticker, fetch_cik_from_ticker
import datamule as dm
import aiohttp

# Function to download filings for a given ticker
async def download_filings(ticker, cik, base_dir):
    # SEC EDGAR API URL for filings
    url = f'https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik}&action=getcompany'

    # Create a session to make the request
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                html = await response.text()
                # Extract the document links (XML/HTML files, etc.) from the page
                # We are looking for <a href='...'> to get filing URLs (e.g., 10-K, 10-Q)
                filing_links = []
                start_index = html.find('<a href="')  # Start of first link
                while start_index != -1:
                    end_index = html.find('"', start_index + 9)  # End of the link
                    link = html[start_index + 9:end_index]
                    if link.endswith('.htm') or link.endswith('.xml'):  # Filter for filings
                        filing_links.append('https://www.sec.gov' + link)
                    start_index = html.find('<a href="', end_index)
                
                # Create a folder for filings if it doesn't exist
                filing_dir = os.path.join(base_dir, ticker, 'filings')
                if not os.path.exists(filing_dir):
                    os.makedirs(filing_dir)

                # Download each filing document
                for filing_url in filing_links:
                    filing_name = filing_url.split('/')[-1]
                    filing_path = os.path.join(filing_dir, filing_name)
                    
                    # Download the filing document
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
    # Ensure the base directory exists
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    # Initialize the downloader
    downloader = dm.Downloader()

    # Initialize an empty list to collect all dataframes
    all_data = []

    # Process each ticker
    for ticker in tickers:
        try:
            # Pull SEC data for a single ticker
            df, unit_dfs = await pull_sec_data_single_ticker(ticker, downloader)

            # Filter the DataFrame based on the provided date range
            df_filtered = df[(df['Filed Date'] >= start) & (df['Filed Date'] <= end)]

            # Save the filtered data to CSV
            ticker_dir = os.path.join(base_dir, ticker)
            if not os.path.exists(ticker_dir):
                os.makedirs(ticker_dir)

            df_filtered.to_csv(f'{ticker_dir}/sec_data_{ticker}.csv', index=False)
            print(f"Data for ticker {ticker} saved successfully.")

            # Optionally, process and save split DataFrames by unit types
            for unit_name, unit_df in unit_dfs.items():
                unit_df_filtered = unit_df[(unit_df['Filed Date'] >= start) & (unit_df['Filed Date'] <= end)]
                unit_df_filtered.to_csv(f'{ticker_dir}/{unit_name}_{ticker}.csv', index=False)
                print(f"Unit data for {unit_name} of ticker {ticker} saved successfully.")

            # Append the cleaned and filtered data to the all_data list
            all_data.append(df_filtered)

            # Download filings (e.g., 10-K, 10-Q) for each ticker
            # Fetch CIK from the ticker to get filings
            cik = await fetch_cik_from_ticker(ticker, downloader.headers)
            await download_filings(ticker, cik, base_dir)

        except Exception as e:
            print(f"Failed to process data for ticker {ticker}: {str(e)}")

    # Combine all the dataframes for all tickers into one large dataframe
    all_data_df = pd.concat(all_data, ignore_index=True)

    # Save the combined dataframe as a CSV file
    all_data_df.to_csv(f'{base_dir}/sec_data_all_tickers.csv', index=False)
    print(f"Combined data for all tickers saved to sec_data_all_tickers.csv.")

# Main function to run the processing
def main():
    # Initialize parameters
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    start = '2024-01-01'
    end = '2024-06-01'
    base_dir = 'sec_data'

    # Run the process
    asyncio.run(process_sec_data(tickers, start, end, base_dir))

# Run the script
if __name__ == '__main__':
    main()
