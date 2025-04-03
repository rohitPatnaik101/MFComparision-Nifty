import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime, timedelta
from db import nav_collection, mf_collection

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
        return nav_data  # Already in chronological order from AMFI
    return None

def add_nav(mf_id, sc_id, nav_data):
    code = f"{mf_id}@{sc_id}"
    # Use $addToSet to avoid duplicates based on date
    for entry in nav_data:
        nav_collection.update_one(
            {"code": code},
            {"$addToSet": {"nav_history": entry}},
            upsert=True
        )

def list_nav(mf_id, sc_id, from_date, to_date):
    code = f"{mf_id}@{sc_id}"
    doc = nav_collection.find_one({"code": code})
    if not doc or "nav_history" not in doc:
        return pd.DataFrame(columns=["date", "nav"])
    
    # Filter nav_history for the requested date range
    nav_history = [
        entry for entry in doc["nav_history"]
        if from_date <= entry["date"] <= to_date
    ]
    # Convert to DataFrame (sorted by date)
    df = pd.DataFrame(nav_history, columns=["date", "nav"]).sort_values("date")
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
        # Full data available
        if stored_start <= from_date and stored_end >= to_date:
            return df, None
        # Partial data, calculate missing ranges
        else:
            # Calculate missing start range
            if stored_start > from_date:
                start_nav_data = scrape_nav_history(mf_id, sc_id, from_date, stored_start)
                if start_nav_data:
                    add_nav(mf_id, sc_id, start_nav_data)
            
            # Calculate missing end range
            if stored_end < to_date:
                end_nav_data = scrape_nav_history(mf_id, sc_id, stored_end, to_date)
                if end_nav_data:
                    add_nav(mf_id, sc_id, end_nav_data)
            
            # Fetch updated data after adding missing parts
            updated_df = list_nav(mf_id, sc_id, from_date, to_date)
            return updated_df, None
    else:
        # No data at all, scrape and store
        nav_data = scrape_nav_history(mf_id, sc_id, from_date, to_date)
        if nav_data:
            add_nav(mf_id, sc_id, nav_data)
            return list_nav(mf_id, sc_id, from_date, to_date), None
        return None, "Failed to fetch data from AMFI"