import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime, timedelta
import yfinance as yf
import matplotlib.pyplot as plt
from db import nav_collection, mf_collection, nifty_collection

# Load providers configuration
with open('providers.json', 'r') as f:
    PROVIDERS = json.load(f)["providers"]

def scrape_nav_history(mf_id, sc_id, f_date, t_date):
    provider = next(p for p in PROVIDERS if p["providerName"] == "amfi")
    url = provider["url"]
    payload = {'mfID': mf_id, 'scID': sc_id, 'fDate': f_date, 'tDate': t_date}
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.select('tr')
        nav_data = []
        for row in rows[1:]:
            cols = row.find_all('td')
            if len(cols) >= 4:
                nav_value = cols[0].get_text(strip=True)
                nav_date = cols[3].get_text(strip=True)
                nav_data.append({"date": nav_date, "nav": nav_value})
        return nav_data
    return None

def add_nav(mf_id, sc_id, nav_data):
    code = f"{mf_id}@{sc_id}"
    existing_doc = nav_collection.find_one({"code": code})
    existing_nav = existing_doc["nav_history"] if existing_doc and "nav_history" in existing_doc else []
    existing_dates = set(entry["date"] for entry in existing_nav)
    
    new_entries = [entry for entry in nav_data if entry["date"] not in existing_dates]
    if not new_entries:
        return
    
    updated_nav = existing_nav + new_entries
    updated_nav.sort(key=lambda x: datetime.strptime(x["date"], "%d-%b-%Y"))
    
    nav_collection.update_one(
        {"code": code},
        {"$set": {"nav_history": updated_nav}},
        upsert=True
    )

def list_nav(mf_id, sc_id, from_date, to_date):
    code = f"{mf_id}@{sc_id}"
    doc = nav_collection.find_one({"code": code})
    if not doc or "nav_history" not in doc:
        return pd.DataFrame(columns=["date", "nav"])
    
    nav_history = [
        entry for entry in doc["nav_history"]
        if from_date <= entry["date"] <= to_date
    ]
    
    df = pd.DataFrame(nav_history, columns=["date", "nav"])
    df["date"] = pd.to_datetime(df["date"], format="%d-%b-%Y")
    df = df.sort_values("date")
    df["date"] = df["date"].dt.strftime("%d-%b-%Y")
    
    return df

def describe_nav(df):
    if df.empty:
        return {"startDate": None, "endDate": None, "nullDates": [], "average": None, "stdDev": None}
    
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    start_date = df["date"].min()
    end_date = df["date"].max()
    null_dates = df[df["nav"].isna()]["date"].tolist()
    avg_nav = df["nav"].mean()
    std_nav = df["nav"].std()
    
    return {
        "startDate": start_date,
        "endDate": end_date,
        "nullDates": null_dates,
        "average": avg_nav,
        "stdDev": std_nav
    }

def validate_dates(from_date, to_date):
    current_date = datetime.now().strftime("%d-%b-%Y")
    five_years_ago = (datetime.now() - timedelta(days=5*365)).strftime("%d-%b-%Y")
    
    try:
        from_dt = datetime.strptime(from_date, "%d-%b-%Y")
        to_dt = datetime.strptime(to_date, "%d-%b-%Y")
        if from_dt < datetime.strptime(five_years_ago, "%d-%b-%Y"):
            return "FromDate cannot be older than 5 years"
        if to_dt > datetime.strptime(current_date, "%d-%b-%Y"):
            return "ToDate cannot be after current date"
        return None
    except ValueError:
        return "Invalid date format. Use DD-MMM-YYYY"

def get_mf_ids(company, fund):
    mf = mf_collection.find_one({"company": company, "fund": fund})
    if mf:
        return mf["mfID"], mf["scID"]
    return None, None

def get_nav_data(mf_name, from_date, to_date):
    mf = mf_collection.find_one({"fund": mf_name})
    if not mf:
        return None, "Mutual Fund not found"
    
    mf_id, sc_id = mf["mfID"], mf["scID"]
    error = validate_dates(from_date, to_date)
    if error:
        return None, error

    df = list_nav(mf_id, sc_id, from_date, to_date)
    if not df.empty:
        stored_start = df["date"].min()
        stored_end = df["date"].max()
        if stored_start <= from_date and stored_end >= to_date:
            return df, None
        else:
            if stored_start > from_date:
                start_nav_data = scrape_nav_history(mf_id, sc_id, from_date, stored_start)
                if start_nav_data:
                    add_nav(mf_id, sc_id, start_nav_data)
            if stored_end < to_date:
                end_nav_data = scrape_nav_history(mf_id, sc_id, stored_end, to_date)
                if end_nav_data:
                    add_nav(mf_id, sc_id, end_nav_data)
            updated_df = list_nav(mf_id, sc_id, from_date, to_date)
            return updated_df, None
    else:
        nav_data = scrape_nav_history(mf_id, sc_id, from_date, to_date)
        if nav_data:
            add_nav(mf_id, sc_id, nav_data)
            return list_nav(mf_id, sc_id, from_date, to_date), None
        return None, "Failed to fetch data from AMFI"

def fetch_nifty_data(from_date, to_date):
    ticker = "^NSEI"
    try:
        from_dt = datetime.strptime(from_date, "%d-%b-%Y")
        to_dt = datetime.strptime(to_date, "%d-%b-%Y")
        
        nifty_df = yf.download(ticker, start=from_dt, end=to_dt, interval="1d", progress=False)
        if nifty_df.empty:
            return None, "Failed to fetch Nifty data from yfinance"
        
        nifty_df = nifty_df["Close"].reset_index()
        nifty_df.columns = ["Date", "Close"]
        nifty_df["Date"] = nifty_df["Date"].dt.strftime("%d-%b-%Y")
        
        nifty_data = [
            {"date": row["Date"], "close": float(row["Close"])}
            for _, row in nifty_df.iterrows()
        ]
        
        nifty_collection.update_one(
            {"index": "NIFTY50"},
            {"$set": {"history": nifty_data}},
            upsert=True
        )
        
        return nifty_df[["Date", "Close"]], None
    
    except Exception as e:
        print(f"Error in fetch_nifty_data: {str(e)}")
        return None, f"Error fetching Nifty data: {str(e)}"

def compare_mf_nifty(mf_name, from_date, to_date):
    try:
        # Fetch MF data
        mf_df, mf_error = get_nav_data(mf_name, from_date, to_date)
        if mf_error:
            return None, None, None, mf_error
        
        # Fetch Nifty data from MongoDB or yfinance
        nifty_doc = nifty_collection.find_one({"index": "NIFTY50"})
        if nifty_doc and "history" in nifty_doc:
            nifty_df = pd.DataFrame(nifty_doc["history"], columns=["date", "close"])
            nifty_df = nifty_df.rename(columns={"date": "Date", "close": "Close"})
            nifty_df["Date"] = pd.to_datetime(nifty_df["Date"], format="%d-%b-%Y")
            nifty_df = nifty_df[(nifty_df["Date"] >= pd.to_datetime(from_date)) & 
                              (nifty_df["Date"] <= pd.to_datetime(to_date))]
        else:
            nifty_df, nifty_error = fetch_nifty_data(from_date, to_date)
            if nifty_error:
                return None, None, None, nifty_error
        
        # Convert dates to datetime for alignment
        mf_df["date"] = pd.to_datetime(mf_df["date"], format="%d-%b-%Y")
        nifty_df["Date"] = pd.to_datetime(nifty_df["Date"], format="%d-%b-%Y")
        
        # Align datasets by date
        aligned_df = pd.merge(mf_df[["date", "nav"]], nifty_df[["Date", "Close"]], 
                            left_on="date", right_on="Date", how="inner")
        if aligned_df.empty:
            return None, None, None, "No overlapping data between MF and Nifty"
        
        aligned_df = aligned_df.drop(columns=["Date"])  # Remove duplicate Date column
        aligned_df = aligned_df.rename(columns={"date": "Date", "nav": "NAV"})
        
        # Convert NAV to numeric
        aligned_df["NAV"] = pd.to_numeric(aligned_df["NAV"], errors="coerce")
        
        # Normalize data (start at 100)
        aligned_df["nav_norm"] = (aligned_df["NAV"] / aligned_df["NAV"].iloc[0]) * 100
        aligned_df["nifty_norm"] = (aligned_df["Close"] / aligned_df["Close"].iloc[0]) * 100
        
        # Calculate correlation
        correlation = aligned_df["NAV"].corr(aligned_df["Close"])
        
        # Visualize
        plt.figure(figsize=(10, 6))
        plt.plot(aligned_df["Date"], aligned_df["nav_norm"], label=f"{mf_name} (Normalized NAV)")
        plt.plot(aligned_df["Date"], aligned_df["nifty_norm"], label="Nifty 50 (Normalized)")
        plt.title(f"Comparison: {mf_name} vs Nifty 50")
        plt.xlabel("Date")
        plt.ylabel("Normalized Value (Base = 100)")
        plt.legend()
        plt.grid(True)
        plot_path = f"comparison_{mf_name.replace(' ', '_')}.png"
        plt.savefig(plot_path)
        plt.close()
        
        # Format Date back to string for response
        aligned_df["Date"] = aligned_df["Date"].dt.strftime("%d-%b-%Y")
        return aligned_df, correlation, plot_path, None
    
    except Exception as e:
        return None, None, None, f"Error in comparison: {str(e)}"