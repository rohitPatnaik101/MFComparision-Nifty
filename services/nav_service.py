import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime, timedelta
from db import nav_collection, mf_collection
from .utils import validate_dates
import os
from io import StringIO
from statsmodels.tsa.arima.model import ARIMA
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error
import numpy as np

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
        print("No document or nav_history found")
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

def get_aum_data(mf_name, year_quarter):
    mf = mf_collection.find_one({"fund": mf_name})
    if not mf:
        return None, "Mutual Fund not found"
    
    mf_id = mf["mfID"]
    
    year_quarter_map = {
        "January - March 2025": "1",
        "October - December 2024": "2",
        "July - September 2024": "3",
        "April - June 2024": "4",
    }
    year_id = year_quarter_map.get(year_quarter)
    if not year_id:
        return None, f"Invalid Year_Quarter: {year_quarter}"
    
    url = "https://www.amfiindia.com/modules/AverageAUMDetails"
    payload = {
        "AUmType": "S",
        "AumCatType": "Typewise",
        "MF_Id": str(mf_id),
        "Year_Id": year_id,
        "Year_Quarter": year_quarter
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.amfiindia.com",
        "Referer": "https://www.amfiindia.com/research-information/aum-data/average-aum"
    }
    
    try:
        response = requests.post(url, data=payload, headers=headers)
        if response.status_code != 200:
            return None, "Failed to fetch AUM data from AMFI"
        
        tables = pd.read_html(StringIO(response.text))
        if not tables:
            return None, "No AUM data tables found"
        
        df = tables[0]
        df.columns = [' '.join(col).strip() if isinstance(col, tuple) else col for col in df.columns]
        
        scheme_col = next((col for col in df.columns if "Scheme NAV Name" in col), None)
        aum_col = next((col for col in df.columns if "Average AUM for The Month" in col and "Fund Of Funds" not in col), None)
        
        if not scheme_col or not aum_col:
            return None, "Required AUM columns not found"
        
        match = df[df[scheme_col].str.contains(mf_name, case=False, na=False)]
        if match.empty:
            return None, f"Scheme not found: {mf_name}"
        
        aum_value = float(match.iloc[0][aum_col])
        return {"fund": mf_name, "year_quarter": year_quarter, "aum_lakhs": aum_value}, None
    
    except Exception as e:
        return None, f"Error fetching AUM data: {str(e)}"

def predict_nav(mf_name, from_date, to_date):
    try:
        # Step 1: Fetch and preprocess data
        df, error = get_nav_data(mf_name, from_date, to_date)
        if error:
            return None, None, error
        
        if df.empty or len(df) < 50:  # Need sufficient data for modeling
            return None, None, "Insufficient data for prediction"
        
        print(f"Initial data size: {len(df)}")
        
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df["date"] = pd.to_datetime(df["date"], format="%d-%b-%Y")
        df = df.sort_values("date")
        df["nav"] = df["nav"].interpolate()  # Fill gaps
        df = df.dropna()  # Drop any remaining NaNs
        
        if df.empty:
            return None, None, "No valid data after preprocessing"
        
        # Set date as index with daily frequency
        df.set_index("date", inplace=True)
        df = df.asfreq('D', method='ffill')  # Ensure daily frequency, forward-fill gaps
        
        print(f"Preprocessed data size: {len(df)}")
        
        # Step 2: Train-test split (80% train, 20% test)
        train_size = int(0.8 * len(df))
        train_df = df[:train_size]
        test_df = df[train_size:]
        
        print(f"Train size: {len(train_df)}, Test size: {len(test_df)}")
        
        # Step 3: Train ARIMA for long-term trend
        arima_model = ARIMA(train_df["nav"], order=(5, 1, 0))
        arima_fit = arima_model.fit()
        
        # Predict ARIMA for train and test periods
        arima_train_pred = arima_fit.predict(start=train_df.index[0], end=train_df.index[-1])
        arima_test_pred = arima_fit.forecast(steps=len(test_df))
        arima_test_pred.index = test_df.index  # Align test predictions with test_df index
        
        print(f"ARIMA train pred size: {len(arima_train_pred)}, ARIMA test pred size: {len(arima_test_pred)}")
        
        # Step 4: Calculate residuals
        train_residuals = train_df["nav"] - arima_train_pred
        
        # Step 5: Feature engineering for XGBoost
        def create_features(data, nav_col="nav"):
            df_features = data.copy()
            df_features["lag1"] = df_features[nav_col].shift(1)
            df_features["lag2"] = df_features[nav_col].shift(2)
            df_features["lag3"] = df_features[nav_col].shift(3)
            df_features["rolling_mean_3"] = df_features[nav_col].rolling(window=3).mean()
            df_features["rolling_std_3"] = df_features[nav_col].rolling(window=3).std()
            return df_features
        
        # Apply feature engineering
        train_features = create_features(train_df)
        test_features = create_features(test_df)
        
        # Drop NaNs and align residuals
        train_features = train_features.dropna()
        test_features = test_features.dropna()
        
        print(f"Train features size: {len(train_features)}, Test features size: {len(test_features)}")
        
        # Align residuals with feature indices
        train_residuals = train_residuals.loc[train_features.index]
        test_residuals = test_df["nav"].loc[test_features.index] - arima_test_pred.loc[test_features.index]
        
        # Features and target for XGBoost
        feature_cols = ["lag1", "lag2", "lag3", "rolling_mean_3", "rolling_std_3"]
        X_train = train_features[feature_cols]
        y_train = train_residuals
        X_test = test_features[feature_cols]
        y_test = test_residuals
        
        # Debug shapes
        print(f"X_train shape: {X_train.shape}, y_train shape: {y_train.shape}")
        print(f"X_test shape: {X_test.shape}, y_test shape: {y_test.shape}")
        
        # Step 6: Train XGBoost on residuals
        xgb_model = xgb.XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=3)
        xgb_model.fit(X_train, y_train)
        
        # Predict residuals
        xgb_train_pred = xgb_model.predict(X_train)
        xgb_test_pred = xgb_model.predict(X_test)
        
        # Step 7: Combine predictions
        final_train_pred = arima_train_pred.loc[train_features.index] + xgb_train_pred
        final_test_pred = arima_test_pred.loc[test_features.index] + xgb_test_pred
        
        # Step 8: Evaluate performance
        test_nav_mean = test_df["nav"].loc[test_features.index].mean()
        rmse = np.sqrt(mean_squared_error(test_df["nav"].loc[test_features.index], final_test_pred))
        mae = mean_absolute_error(test_df["nav"].loc[test_features.index], final_test_pred)
        rmse_percent = (rmse / test_nav_mean) * 100
        mae_percent = (mae / test_nav_mean) * 100
        
        metrics = {
            "rmse_percent": rmse_percent,
            "mae_percent": mae_percent
        }
        
        print(f"Test NAV mean: {test_nav_mean}, RMSE: {rmse}, MAE: {mae}")
        print(f"RMSE %: {rmse_percent}, MAE %: {mae_percent}")
        
        # Step 9: Forecast next 14 days
        last_date = df.index.max()
        future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=14, freq='D')
        
        # ARIMA forecast for next 14 days
        arima_future_pred = arima_fit.forecast(steps=14)
        arima_future_pred.index = future_dates  # Align with future dates
        
        # Prepare features for XGBoost future prediction
        last_data = df.tail(10).copy()  # Use recent data for feature continuity
        future_features = []
        last_nav = df["nav"].iloc[-1]
        
        for i in range(14):
            # Simulate NAV for feature engineering
            if i == 0:
                prev_nav = last_nav
            else:
                prev_nav = arima_future_pred.iloc[i-1] + xgb_model.predict(future_features[-1].reshape(1, -1))[0]
            
            new_row = pd.DataFrame({
                "nav": [prev_nav]
            }, index=[future_dates[i]])
            last_data = pd.concat([last_data, new_row])
            features = create_features(last_data)
            features = features.dropna()
            if features.empty:
                return None, None, "Failed to generate future features"
            future_features.append(features[feature_cols].iloc[-1].values)
        
        future_features = np.array(future_features)
        xgb_future_pred = xgb_model.predict(future_features)
        
        # Final future predictions
        final_future_pred = arima_future_pred + xgb_future_pred
        
        # Format predictions
        predictions = [
            {"date": future_dates[i].strftime("%d-%b-%Y"), "nav_predicted": float(final_future_pred.iloc[i])}
            for i in range(14)
        ]
        
        return predictions, metrics, None
    
    except Exception as e:
        return None, None, f"Error in NAV prediction: {str(e)}"