import yfinance as yf
import pandas as pd

# Define NIFTY 50 symbol on Yahoo Finance
NIFTY_TICKER = "^NSEI"

def fetch_nifty_data(period="1y", interval="1d"):

    try:
        # Fetch data from Yahoo Finance
        nifty_data = yf.download(NIFTY_TICKER, period=period, interval=interval)

        # Reset index to make 'Date' a column
        nifty_data.reset_index(inplace=True)

        return nifty_data

    except Exception as e:
        print(f" Error fetching data: {e}")
        return None

# Run the function
if __name__ == "__main__":
    nifty_df = fetch_nifty_data(period="1y", interval="1d") 
    if nifty_df is not None:
        print(nifty_df.head())  # Show first few rows
