import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime, timedelta
from db import nav_collection, mf_collection
from .utils import validate_dates
import os

# Load providers configuration from the same directory as nav_service.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROVIDERS_PATH = os.path.join(BASE_DIR, 'providers.json')
with open(PROVIDERS_PATH, 'r') as f:
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
    
    from_dt = datetime.strptime(from_date, "%d-%b-%Y")
    to_dt = datetime.strptime(to_date, "%d-%b-%Y")
    
    nav_history = [
        entry for entry in doc["nav_history"]
        if from_dt <= datetime.strptime(entry["date"], "%d-%b-%Y") <= to_dt
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
        stored_start_dt = datetime.strptime(stored_start, "%d-%b-%Y")
        stored_end_dt = datetime.strptime(stored_end, "%d-%b-%Y")
        from_dt = datetime.strptime(from_date, "%d-%b-%Y")
        to_dt = datetime.strptime(to_date, "%d-%b-%Y")
        if stored_start_dt <= from_dt and stored_end_dt >= to_dt:
            return df, None
        else:
            if stored_start_dt > from_dt:
                start_nav_data = scrape_nav_history(mf_id, sc_id, from_date, stored_start)
                if start_nav_data:
                    add_nav(mf_id, sc_id, start_nav_data)
            if stored_end_dt < to_dt:
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