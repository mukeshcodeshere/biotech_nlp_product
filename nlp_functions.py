import datamule as dm
import pandas as pd
import aiohttp
import asyncio
from collections import defaultdict

# Function to fetch CIK using the ticker symbol from SEC's EDGAR system
async def fetch_cik_from_ticker(ticker, headers):
    url = f'https://www.sec.gov/cgi-bin/browse-edgar?CIK={ticker}&action=getcompany'
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                html = await response.text()
                # Extract the CIK from the page's HTML
                start_index = html.find('CIK=') + len('CIK=') 
                end_index = html.find('&amp;', start_index)
                cik = html[start_index:end_index]
                return cik
            else:
                raise ValueError(f"Failed to fetch CIK for ticker: {ticker}")

# Define the function to pull SEC data for a given ticker
async def pull_sec_data_single_ticker(ticker, downloader):
    # Function to fetch company data using CIK
    async def fetch_company_data(cik, headers):
        url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json'
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                return await response.json()

    # Function to flatten the JSON data from the SEC API
    def flatten_json_data(company_data):
        all_data = []
        
        # Iterate through all taxonomies (dei, us-gaap, etc.)
        for taxonomy in company_data.get('facts', {}):
            # Iterate through all concepts within each taxonomy
            for concept in company_data['facts'][taxonomy]:
                # Get the units data
                units_data = company_data['facts'][taxonomy][concept].get('units', {})
                
                # Iterate through all unit types
                for unit_type, values in units_data.items():
                    # Process each individual record
                    for record in values:
                        data_point = {
                            'Taxonomy': taxonomy,
                            'Concept': concept,
                            'Unit': unit_type,
                            'Value': record.get('val'),
                            'Start': record.get('start'),
                            'End': record.get('end'),
                            'Accession': record.get('accn'),
                            'Fiscal Year': record.get('fy'),
                            'Fiscal Period': record.get('fp'),
                            'Form': record.get('form'),
                            'Filed Date': record.get('filed'),
                            'Frame': record.get('frame')
                        }
                        all_data.append(data_point)
        
        return all_data

    # Function to split the DataFrame by unit types
    def split_by_units(df):
        dfs = {}
        for unit in df['Unit'].unique():
            unit_name = unit.replace('/', '_per_')
            dfs[f'df_{unit_name}'] = df[df['Unit'] == unit].copy()
        return dfs

    # Fetch the CIK for the ticker
    cik = await fetch_cik_from_ticker(ticker, downloader.headers)

    # Fetch the company's financial data using the CIK
    company_data = await fetch_company_data(cik, downloader.headers)

    # Flatten the JSON data into a list of records
    flattened_data = flatten_json_data(company_data)

    # Convert the flattened data into a pandas DataFrame
    df = pd.DataFrame(flattened_data)

    # Clean the DataFrame by converting date columns to datetime
    df['Start'] = pd.to_datetime(df['Start'], errors='coerce')
    df['End'] = pd.to_datetime(df['End'], errors='coerce')
    df['Filed Date'] = pd.to_datetime(df['Filed Date'], errors='coerce')

    # Sort the DataFrame by taxonomy, concept, and end date
    df = df.sort_values(['Taxonomy', 'Concept', 'End'])

    # Optionally, split the DataFrame by units (optional)
    unit_dfs = split_by_units(df)

    # Return the main DataFrame and the split DataFrames by unit type
    return df, unit_dfs

# # Example of how to call the function
# async def main(ticker):
#     downloader = dm.Downloader()
#     df, unit_dfs = await pull_sec_ticker(ticker, downloader)
#    
#     # Optionally, save to CSV
#     df.to_csv(f'sec_data_ticker_{ticker}.csv', index=False)
#    
#     # Return the DataFrame and split DataFrames
#     return df, unit_dfs

# # Run the function with a sample ticker symbol (e.g., "AAPL")
# ticker = "AAPL"
# df, unit_dfs = asyncio.run(main(ticker))
