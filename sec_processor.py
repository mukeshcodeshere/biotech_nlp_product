import os
import subprocess
import streamlit as st
import pandas as pd
from config import CONFIG
import subprocess
import time
from sec_loader import load_sec_data

df_sec_facts, all_data_df_min = load_sec_data()
print("Stock Data")
print(df_sec_facts.head())
print("Filings Data")
print(all_data_df_min.head())
