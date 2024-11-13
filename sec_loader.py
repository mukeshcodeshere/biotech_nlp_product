import os
import logging
import warnings
import json
import pandas as pd
from datetime import datetime
from glob import glob
from typing import List, Optional
from config import CONFIG  # Import the config dictionary
from selectolax.parser import HTMLParser

# Suppress warnings
warnings.filterwarnings("ignore")

# Get today's date
today = datetime.today()
today_date = today.strftime('%Y-%m-%d')

tickers = CONFIG['TICKERS']
start = CONFIG['START_DATE']
end = CONFIG['END_DATE']
base_dir = CONFIG['BASE_DIR']

class SECFilingLoader:
    def __init__(self, base_dir: str = 'sec_data', concepts_dir: str = 'company_concepts'):
        self.base_dir = base_dir
        self.concepts_dir = concepts_dir  # Directory where Company Concepts are stored
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def extract_text_from_html(self, file_path: str) -> str:
        """Extract clean text from HTML filing using selectolax (faster than BeautifulSoup)."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Parse HTML using selectolax
            tree = HTMLParser(html_content)
            
            # Remove script and style elements
            for tag in tree.css('script'):
                tag.decompose()
            for tag in tree.css('style'):
                tag.decompose()
            
            # Extract text with newlines between elements
            if tree.body:
                text = tree.body.text(separator='\n')
                # Clean up extra whitespace
                text = ' '.join(text.split())
                return text
            return ""
            
        except Exception as e:
            self.logger.error(f"Error extracting text from {file_path}: {str(e)}")
            return ""

    def classify_filing_type(self, content: str, filename: str) -> str:
        """Classify the filing type as 10-Q or 10-K based on the content or filename."""
        if '10-K' in content or '10-K' in filename:
            return '10-K'
        elif '10-Q' in content or '10-Q' in filename:
            return '10-Q'
        else:
            return 'Other'  # You could also use 'Unknown' if you prefer

    def load_filings(self, ticker: str) -> pd.DataFrame:
        """Load SEC filings data with full text content for a specific ticker."""
        try:
            filings_path = os.path.join(self.base_dir, ticker, 'filings')
            filing_files = glob(os.path.join(filings_path, '*.htm'))
            
            if not filing_files:
                raise FileNotFoundError(f"No filings found for {ticker}")
            
            filings_data = []
            for file_path in filing_files:
                try:
                    filename = os.path.basename(file_path)
                    accession_num, filing_date = filename.replace('.htm', '').split('_')
                    
                    # Read and clean the file content
                    clean_text = self.extract_text_from_html(file_path)
                    
                    filing_info = {
                        'ticker': ticker,  # Ensure 'ticker' is added
                        'accession_number': accession_num,
                        'filing_date': filing_date,
                        'file_path': file_path,
                        'file_size': os.path.getsize(file_path),
                        'content': clean_text,
                        'content_length': len(clean_text),
                    }
                    filings_data.append(filing_info)
                    
                except Exception as e:
                    self.logger.error(f"Error processing file {file_path}: {str(e)}")
                    continue
            
            # Convert to DataFrame
            df = pd.DataFrame(filings_data)
            
            # Convert dates and format columns
            df['filing_date'] = pd.to_datetime(df['filing_date'])
            df['file_size'] = df['file_size'] / 1024  # Convert to KB
            
            # Sort by filing date
            df = df.sort_values('filing_date', ascending=False)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading filings for {ticker}: {str(e)}")
            return pd.DataFrame()

    def load_company_concepts(self, ticker: str) -> pd.DataFrame:
        """Load Company Concepts data for a specific ticker."""
        try:
            concepts_path = os.path.join(self.base_dir, ticker, self.concepts_dir)
            concepts_files = glob(os.path.join(concepts_path, '*.json'))
            
            if not concepts_files:
                raise FileNotFoundError(f"No concepts found for {ticker}")
            
            concepts_data = []
            for file_path in concepts_files:
                with open(file_path, 'r', encoding='utf-8') as f:
                    concepts_data.append(json.load(f))
            
            # Convert to DataFrame
            concepts_df = pd.DataFrame(concepts_data)
            concepts_df['ticker'] = ticker  # Ensure 'ticker' is added
            
            return concepts_df
        
        except Exception as e:
            self.logger.error(f"Error loading company concepts for {ticker}: {str(e)}")
            return pd.DataFrame()

    def load_all_filings(self, tickers: Optional[List[str]] = None) -> pd.DataFrame:
        """Load filings and company concepts data with text for multiple tickers."""
        all_data = []  # List to hold the DataFrames for all tickers
        if tickers is None:
            # Get all tickers from the base directory
            tickers = [d for d in os.listdir(self.base_dir) if os.path.isdir(os.path.join(self.base_dir, d))]
        
        for ticker in tickers:
            self.logger.info(f"Loading filings and company concepts for {ticker}...")
            
            # Load SEC filings
            filings_df = self.load_filings(ticker)
            
            # Load Company Concepts
            concepts_df = self.load_company_concepts(ticker)
            
            # Merge both dataframes if they are not empty
            if not filings_df.empty and not concepts_df.empty:
                merged_df = pd.merge(filings_df, concepts_df, on='ticker', how='left')
                all_data.append(merged_df)
                self.logger.info(f"Loaded {len(filings_df)} filings and concepts for {ticker}")
            elif not filings_df.empty:
                all_data.append(filings_df)
                self.logger.info(f"Loaded {len(filings_df)} filings for {ticker}")
            elif not concepts_df.empty:
                all_data.append(concepts_df)
                self.logger.info(f"Loaded company concepts for {ticker}")
        
        # Concatenate all dataframes into a single DataFrame
        all_data_df = pd.concat(all_data, ignore_index=True)
        
        # Return the merged data for all tickers as a single DataFrame
        return all_data_df

def load_sec_data():
    # Create an instance of the SECFilingLoader
    loader = SECFilingLoader(base_dir=CONFIG['BASE_DIR'])

    # Load all filings and company concepts for specified tickers
    all_data_df = loader.load_all_filings(tickers=CONFIG['TICKERS'])

    # Extract required columns
    required_cols = ["ticker", "filing_date", "content"]  # Adjust columns as needed
    all_data_df_min = all_data_df[required_cols]

    # Load SEC facts (assuming the CSV path is correct)
    sec_facts_file_path = os.path.join(CONFIG['BASE_DIR'], 'sec_data_all_tickers.csv')
    df_sec_facts = pd.read_csv(sec_facts_file_path)

    # Convert 'Filed Date' and 'filing_date' columns to datetime, if not already
    df_sec_facts['Filed Date'] = pd.to_datetime(df_sec_facts['Filed Date'], errors='coerce')
    all_data_df_min['filing_date'] = pd.to_datetime(all_data_df_min['filing_date'], errors='coerce')

    # Sort both DataFrames by their respective date columns, latest at the top
    df_sec_facts = df_sec_facts.sort_values(by="Filed Date", ascending=False)
    all_data_df_min = all_data_df_min.sort_values(by="filing_date", ascending=False)

    # Return both DataFrames
    return df_sec_facts, all_data_df_min