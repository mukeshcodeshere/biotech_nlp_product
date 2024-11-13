import os
import subprocess
import time
import pandas as pd
import streamlit as st
from config import CONFIG
from sec_loader import load_sec_data
from file_deletion import delete_existing_files  # Import the deletion functions
from sec_processor import get_latest_10q_info

# Get the current working directory
cwd = os.getcwd()
st.write(cwd)

# Streamlit UI layout
st.title('SEC Data Downloader')
st.write("""
This application allows you to download SEC filings for the selected tickers and process them into structured data.
The process will update your config file and run the necessary download and data processing.
""")

# Initialize session state to track progress if not already initialized
if 'step' not in st.session_state:
    st.session_state.step = 1  # Start at step 1

# Configuration parameters
tickers_input = st.text_input("Enter tickers (comma separated)", ','.join(CONFIG['TICKERS']))
start_date_input = st.date_input("Select start date", pd.to_datetime(CONFIG['START_DATE']))
end_date_input = st.date_input("Select end date", pd.to_datetime(CONFIG['END_DATE']))
base_dir_input = st.text_input("Base directory for saving data (leave as default)", CONFIG['BASE_DIR'])

# Step 1: Update Configuration (Only available in step 1)
if st.session_state.step == 1:
    if st.button('Update Configuration'):
        # Validate inputs and update the CONFIG dictionary in config.py
        tickers = [ticker.strip() for ticker in tickers_input.split(',')]
        start_date = start_date_input.strftime('%Y-%m-%d')
        end_date = end_date_input.strftime('%Y-%m-%d')
        base_dir = base_dir_input if base_dir_input else CONFIG['BASE_DIR']  # Default to original value if not provided

        # Update the configuration dictionary
        CONFIG['TICKERS'] = tickers
        CONFIG['START_DATE'] = start_date
        CONFIG['END_DATE'] = end_date
        CONFIG['BASE_DIR'] = base_dir

        # Write the updated config back to the config.py file
        config_file_path = 'config.py'
        with open(config_file_path, 'w') as f:
            f.write(f"# config.py\nCONFIG = {{\n")
            f.write(f"    'TICKERS': {tickers},\n")
            f.write(f"    'START_DATE': '{start_date}',\n")
            f.write(f"    'END_DATE': '{end_date}',\n")
            f.write(f"    'BASE_DIR': '{base_dir}',\n")
            f.write(f"    'USER_AGENT': 'Your Name your@email.com'\n")
            f.write(f"}}\n")

        st.success("Configuration updated successfully!")
        # Move to the next step
        st.session_state.step = 2

# Step 2: Download and Process SEC Data (Only available in step 2)
if st.session_state.step == 2:
    if st.button('Download and Process SEC Data'):
        # Delete existing files in the base directory before downloading
        delete_existing_files(CONFIG['BASE_DIR'])  # Call to the new module

        # Show a loading spinner while the download process is running
        with st.spinner("Downloading SEC filings and processing data..."):
            try:
                # Run the `sec_download.py` script as an external process
                subprocess.run(['python', 'sec_download.py'], check=True)
                st.success("SEC data processing completed successfully!")

                # Now check if the file exists after the download process
                file_path = os.path.join(CONFIG['BASE_DIR'], 'sec_data_all_tickers.csv')
                if not os.path.exists(file_path):
                    st.error(f"Error: The expected file '{file_path}' was not created. Please check the process.")
                else:
                    # Optionally, check if the file has been recently modified (timestamp check)
                    last_modified = time.ctime(os.path.getmtime(file_path))
                    st.info(f"File successfully created. Last modified at: {last_modified}")

                # Move to the next step
                st.session_state.step = 3

            except subprocess.CalledProcessError as e:
                st.error(f"Error occurred while running sec_download.py: {e}")

# Step 3: Load and Display Data (Only available in step 3)
if st.session_state.step == 3:
    df_sec_facts, all_data_df_min = load_sec_data()
    st.write("Stock Data")
    st.write(df_sec_facts.head())
    st.write("Filings Data")
    st.write(all_data_df_min.head())
    st.session_state.step = 4

if st.session_state.step == 4:
    df_clean = get_latest_10q_info()
    st.write("Comparison for one ticker - example")
    st.write(df_clean.comparison[0])

