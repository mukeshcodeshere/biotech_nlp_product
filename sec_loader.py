import os
import json
import pandas as pd
from typing import List, Dict, Optional
from glob import glob
from selectolax.parser import HTMLParser
import logging

# Set up basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_all_json_files(directory: str) -> List[Dict]:
    """
    Load all JSON files from the given directory and its subdirectories.
    
    Parameters:
        directory (str): The directory where the JSON files are stored.
    
    Returns:
        List[Dict]: A list of JSON data loaded from each file.
    """
    json_files = []  # List to store JSON data
    for file_path in glob(os.path.join(directory, '**', 'CIK*.json'), recursive=True):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                json_files.append(data)
                logger.info(f"Loaded JSON file: {file_path}")
                # Print a snippet of the loaded JSON file (e.g., first 3 keys or first entry)
                print_json_snippet(data)
        except Exception as e:
            logger.error(f"Failed to load JSON file {file_path}: {e}")
    
    return json_files

def print_json_snippet(data: Dict) -> None:
    """ Print a snippet of the JSON data (first few keys or entries) """
    if isinstance(data, dict):
        keys = list(data.keys())[:5]  # Show the first 5 keys
        print(f"JSON snippet (first 5 keys): {keys}")
    elif isinstance(data, list):
        print(f"JSON snippet (first 3 items): {data[:3]}")  # Show first 3 items of a list
    else:
        print(f"JSON snippet: {str(data)[:200]}...")  # Show first 200 characters of the data

def load_all_html_files(directory: str) -> List[HTMLParser]:
    """
    Load all HTML files from the given directory and its subdirectories.
    
    Parameters:
        directory (str): The directory where the HTML files are stored.
    
    Returns:
        List[HTMLParser]: A list of HTMLParser objects, each parsed from an HTML file.
    """
    html_files = []  # List to store parsed HTML files
    for file_path in glob(os.path.join(directory, '**', '*.htm'), recursive=True):
        try:
            with open(file_path, 'r') as f:
                html_content = f.read()
                html_parser = HTMLParser(html_content)
                html_files.append(html_parser)
                logger.info(f"Loaded HTML file: {file_path}")
                # Print a snippet of the loaded HTML content (first 300 chars)
                print_html_snippet(html_content)
        except Exception as e:
            logger.error(f"Failed to load HTML file {file_path}: {e}")
    
    return html_files

def print_html_snippet(html_content: str) -> None:
    """ Print a snippet of the raw HTML content (first 300 characters) """
    print(f"HTML snippet: {html_content[:300]}...")  # Show first 300 characters of HTML content

def load_all_csv_files(directory: str) -> List[pd.DataFrame]:
    """
    Load all CSV files from the given directory and its subdirectories.
    
    Parameters:
        directory (str): The directory where the CSV files are stored.
    
    Returns:
        List[pd.DataFrame]: A list of DataFrames, each containing data from a CSV file.
    """
    csv_files = []  # List to store DataFrames
    for file_path in glob(os.path.join(directory, '**', '*.csv'), recursive=True):
        try:
            df = pd.read_csv(file_path)
            csv_files.append(df)
            logger.info(f"Loaded CSV file: {file_path}")
            # Print the head of the DataFrame (first 5 rows)
            print_csv_snippet(df)
        except Exception as e:
            logger.error(f"Failed to load CSV file {file_path}: {e}")
    
    return csv_files

def print_csv_snippet(df: pd.DataFrame) -> None:
    """ Print a snippet of the CSV data (first 5 rows) """
    print(f"CSV snippet (first 5 rows):\n{df.head()}")

def load_all_files_in_ticker(ticker_directory: str) -> Dict:
    """
    Load all files within a given ticker directory.
    
    Parameters:
        ticker_directory (str): The directory for a specific Ticker, e.g., 'sec_data/AAPL'.
    
    Returns:
        Dict: A dictionary containing loaded data, with keys like 'json', 'html', 'csv'.
    """
    data = {}

    # Load JSON files from the company_concepts folder
    company_concepts_dir = os.path.join(ticker_directory, 'company_concepts')
    data['json'] = load_all_json_files(company_concepts_dir)

    # Load HTML files from the filings folder
    filings_dir = os.path.join(ticker_directory, 'filings')
    data['html'] = load_all_html_files(filings_dir)

    # Load CSV files from the some_csvs_here folder
    csvs_dir = os.path.join(ticker_directory, 'some_csvs_here')
    data['csv'] = load_all_csv_files(csvs_dir)

    return data

def load_all_files_in_directory(base_directory: str) -> Dict[str, Dict]:
    """
    Load all files from all tickers in the given base directory.
    
    Parameters:
        base_directory (str): The base directory where all ticker directories are stored (e.g., 'sec_data').
    
    Returns:
        Dict[str, Dict]: A dictionary where each key is a ticker symbol, and each value is a dictionary of loaded files.
    """
    all_data = {}
    
    # Iterate through all directories (tickers) in the base directory
    for ticker_directory in glob(os.path.join(base_directory, '*')):
        if os.path.isdir(ticker_directory):
            ticker_name = os.path.basename(ticker_directory)
            logger.info(f"Loading files for ticker: {ticker_name}")
            all_data[ticker_name] = load_all_files_in_ticker(ticker_directory)

    return all_data

def print_dataframe_heads(all_ticker_data: Dict[str, Dict]):
    """
    Print the heads of the DataFrames for each ticker in the loaded data.
    
    Parameters:
        all_ticker_data (Dict[str, Dict]): A dictionary containing data for each ticker.
    """
    for ticker, data in all_ticker_data.items():
        logger.info(f"Printing DataFrame heads for ticker: {ticker}")
        
        # Print the head of each CSV DataFrame for this ticker
        for i, df in enumerate(data.get('csv', [])):
            logger.info(f"Head of CSV DataFrame {i + 1} for ticker {ticker}:")
            print(df.head())  # Print the first few rows of each DataFrame

# Example usage:
base_directory = 'sec_data'
all_ticker_data = load_all_files_in_directory(base_directory)

# Print the heads of all loaded CSV DataFrames
print_dataframe_heads(all_ticker_data)
