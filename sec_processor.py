import os
import pandas as pd
from difflib import ndiff
from sec_loader import load_sec_data  # Assuming this module is available and working

def get_latest_10q_info():
    # Load the SEC data
    df_sec_facts, all_data_df_min = load_sec_data()

    # Step 2: Sort the dataframe by 'ticker' and 'filing_date' in descending order (most recent first)
    df_10q_sorted = all_data_df_min.sort_values(by=['ticker', 'filing_date'], ascending=[True, False])

    # Step 3: For each ticker, select the latest two filings
    latest_two_10q_per_ticker = df_10q_sorted.groupby('ticker').head(2)

    # Function to return the latest and previous filing information
    def get_latest_10q_per_ticker(group):
        if len(group) == 1:
            return pd.Series({
                'ticker_10Q_latest': group.iloc[0]['ticker'],
                'ticker_10Q_previous': None,
                'content_10Q_latest': group.iloc[0]['content'],
                'content_10Q_previous': None
            })
        elif len(group) == 2:
            return pd.Series({
                'ticker_10Q_latest': group.iloc[0]['ticker'],
                'ticker_10Q_previous': group.iloc[1]['ticker'],
                'content_10Q_latest': group.iloc[0]['content'],
                'content_10Q_previous': group.iloc[1]['content']
            })

    # Apply this function to each group to create the two columns
    latest_10q_info = latest_two_10q_per_ticker.groupby('ticker').apply(get_latest_10q_per_ticker)

    # Reset the index so that 'ticker' becomes a column again
    latest_10q_info = latest_10q_info.reset_index(drop=True)

    # Function to compare the latest and previous content of the 10Q filings
    def compare_10q_content(row):
        if row['content_10Q_previous'] is None:
            return "No previous filing to compare"
        
        # Using ndiff to compare the content line by line
        diff = list(ndiff(row['content_10Q_previous'].splitlines(), row['content_10Q_latest'].splitlines()))
        
        # Return the differences or a message if no differences
        changes = [line for line in diff if line.startswith('+ ') or line.startswith('- ')]
        
        if not changes:
            return "No differences"
        else:
            return "\n".join(changes)

    # Apply the comparison function to each row
    latest_10q_info['comparison'] = latest_10q_info.apply(compare_10q_content, axis=1)

    return latest_10q_info