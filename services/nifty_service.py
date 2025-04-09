import pandas as pd
import yfinance as yf
from datetime import datetime
from db import nifty_collection
from .utils import validate_dates

def scrape_nifty_history(from_date, to_date):
    ticker = "^NSEI"
    try:
        from_dt = datetime.strptime(from_date, "%d-%b-%Y")
        to_dt = datetime.strptime(to_date, "%d-%b-%Y")
        
        nifty_df = yf.download(ticker, start=from_dt, end=to_dt, interval="1d", progress=False)
        if nifty_df.empty:
            return None
        
        nifty_df = nifty_df["Close"].reset_index()
        nifty_df.columns = ["Date", "Close"]
        nifty_df["Date"] = nifty_df["Date"].dt.strftime("%d-%b-%Y")
        
        return [{"date": row["Date"], "close": float(row["Close"])} for _, row in nifty_df.iterrows()]
    except Exception as e:
        print(f"Error scraping Nifty data: {str(e)}")
        return None

def add_nifty_data(nifty_data):
    code = "NIFTY50"
    existing_doc = nifty_collection.find_one({"code": code})
    existing_history = existing_doc["history"] if existing_doc and "history" in existing_doc else []
    existing_dates = set(entry["date"] for entry in existing_history)
    
    new_entries = [entry for entry in nifty_data if entry["date"] not in existing_dates]
    if not new_entries:
        return
    
    updated_history = existing_history + new_entries
    updated_history.sort(key=lambda x: datetime.strptime(x["date"], "%d-%b-%Y"))
    
    nifty_collection.update_one(
        {"code": code},
        {"$set": {"history": updated_history}},
        upsert=True
    )

def list_nifty_data(from_date, to_date):
    code = "NIFTY50"
    doc = nifty_collection.find_one({"code": code})
    if not doc or "history" not in doc:
        return pd.DataFrame(columns=["Date", "Close"])
    
    history = [
        entry for entry in doc["history"]
        if from_date <= entry["date"] <= to_date
    ]
    
    df = pd.DataFrame(history, columns=["date", "close"])
    df = df.rename(columns={"date": "Date", "close": "Close"})
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y")
    df = df.sort_values("Date")
    df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")
    
    return df

def get_nifty_data(from_date, to_date):
    error = validate_dates(from_date, to_date)
    if error:
        return None, error
    
    df = list_nifty_data(from_date, to_date)
    if not df.empty:
        stored_start = df["Date"].min()
        stored_end = df["Date"].max()
        if stored_start <= from_date and stored_end >= to_date:
            return df, None
        else:
            if stored_start > from_date:
                start_nifty_data = scrape_nifty_history(from_date, stored_start)
                if start_nifty_data:
                    add_nifty_data(start_nifty_data)
            if stored_end < to_date:
                end_nifty_data = scrape_nifty_history(stored_end, to_date)
                if end_nifty_data:
                    add_nifty_data(end_nifty_data)
            updated_df = list_nifty_data(from_date, to_date)
            return updated_df, None
    else:
        nifty_data = scrape_nifty_history(from_date, to_date)
        if nifty_data:
            add_nifty_data(nifty_data)
            return list_nifty_data(from_date, to_date), None
        return None, "Failed to fetch Nifty data"