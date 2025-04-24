import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from PIL import Image
import io

# Flask API base URL
API_BASE_URL = "http://localhost:5000"

# Function to fetch mutual funds list
def get_mutual_funds():
    try:
        response = requests.get(f"{API_BASE_URL}/api/list_mfs")
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        st.error(f"Error fetching mutual funds: {e}")
        return []

# Function to format date
def format_date(date):
    return date.strftime("%d-%b-%Y")

# Streamlit app
st.title("Mutual Fund Comparison Dashboard")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Select a feature", ["NAV Prediction", "AUM Query", "NAV vs Nifty Comparison", "Historical NAV"])

# Fetch mutual funds
mf_list = get_mutual_funds()
mf_options = [mf["Fund"] for mf in mf_list] if mf_list else []
mf_options.insert(0, "Select a mutual fund")

# Common inputs
st.sidebar.subheader("Common Inputs")
selected_mf = st.sidebar.selectbox("Mutual Fund", mf_options)
from_date = st.sidebar.date_input("From Date", value=datetime(2024, 1, 1), min_value=datetime(2020, 1, 1), max_value=datetime.now())
to_date = st.sidebar.date_input("To Date", value=datetime(2025, 4, 1), min_value=from_date, max_value=datetime.now())

# Page: NAV Prediction
if page == "NAV Prediction":
    st.header("14-Day NAV Prediction")
    if selected_mf != "Select a mutual fund" and from_date and to_date:
        if st.button("Predict NAV"):
            payload = {
                "MFName": selected_mf,
                "FromDate": format_date(from_date),
                "ToDate": format_date(to_date)
            }
            try:
                response = requests.post(f"{API_BASE_URL}/api/nav_pred", json=payload)
                response.raise_for_status()
                data = response.json()
                
                if "error" in data:
                    st.error(data["error"])
                else:
                    predictions = pd.DataFrame(data["predictions"])
                    metrics = data["metrics"]
                    
                    st.subheader("Predictions")
                    st.dataframe(predictions.style.format({"nav_predicted": "{:.2f}"}))
                    
                    st.subheader("Performance Metrics")
                    st.write(f"RMSE (%): {metrics['rmse_percent']:.2f}%")
                    st.write(f"MAE (%): {metrics['mae_percent']:.2f}%")
                    

                    
            except requests.RequestException as e:
                st.error(f"Error predicting NAV: {e}")

# Page: AUM Query
elif page == "AUM Query":
    st.header("Average AUM Query")
    year_quarter_options = [
        "January - March 2025",
        "October - December 2024",
        "July - September 2024",
        "April - June 2024"
    ]
    selected_year_quarter = st.selectbox("Year-Quarter", year_quarter_options)
    
    if selected_mf != "Select a mutual fund" and selected_year_quarter:
        if st.button("Fetch AUM"):
            payload = {
                "MFName": selected_mf,
                "Year_Quarter": selected_year_quarter
            }
            try:
                response = requests.post(f"{API_BASE_URL}/api/get_aum", json=payload)
                response.raise_for_status()
                data = response.json()
                
                if "error" in data:
                    st.error(data["error"])
                else:
                    aum_data = data["aum_data"]
                    st.write(f"Fund: {aum_data['fund']}")
                    st.write(f"Year-Quarter: {aum_data['year_quarter']}")
                    st.write(f"AUM (Lakhs): {aum_data['aum_lakhs']:,.2f}")
                    
            except requests.RequestException as e:
                st.error(f"Error fetching AUM: {e}")

# Page: NAV vs Nifty Comparison
elif page == "NAV vs Nifty Comparison":
    st.header("NAV vs Nifty 50 Comparison")
    if selected_mf != "Select a mutual fund" and from_date and to_date:
        if st.button("Compare"):
            payload = {
                "MFName": selected_mf,
                "FromDate": format_date(from_date),
                "ToDate": format_date(to_date)
            }
            try:
                response = requests.post(f"{API_BASE_URL}/api/compare_mf_nifty", json=payload)
                response.raise_for_status()
                data = response.json()
                
                if "error" in data:
                    st.error(data["error"])
                else:
                    comparison_data = pd.DataFrame(data["comparison_data"])
                    correlation = data["correlation"]
                    plot_url = f"{API_BASE_URL}{data['plot']}"
                    
                    st.subheader("Comparison Data")
                    st.dataframe(comparison_data.style.format({"NAV": "{:.2f}", "Close": "{:.2f}", "nav_norm": "{:.2f}", "nifty_norm": "{:.2f}"}))
                    
                    st.subheader("Correlation")
                    st.write(f"Correlation between NAV and Nifty 50: {correlation:.4f}")
                    
                    st.subheader("Comparison Plot")
                    try:
                        plot_response = requests.get(plot_url)
                        plot_response.raise_for_status()
                        img = Image.open(io.BytesIO(plot_response.content))
                        st.image(img, caption="NAV vs Nifty 50 (Normalized)")
                    except requests.RequestException as e:
                        st.error(f"Error fetching plot: {e}")
                    
            except requests.RequestException as e:
                st.error(f"Error comparing NAV and Nifty: {e}")

# Page: Historical NAV
elif page == "Historical NAV":
    st.header("Historical NAV Data")
    if selected_mf != "Select a mutual fund" and from_date and to_date:
        if st.button("Fetch NAV Data"):
            payload = {
                "MFName": selected_mf,
                "FromDate": format_date(from_date),
                "ToDate": format_date(to_date)
            }
            try:
                response = requests.post(f"{API_BASE_URL}/api/get_nav", json=payload)
                response.raise_for_status()
                data = response.json()
                
                if "error" in data:
                    st.error(data["error"])
                else:
                    nav_data = pd.DataFrame(data["nav_data"])
                    stats = data["stats"]
                    
                    st.subheader("NAV Data")
                    st.dataframe(nav_data.style.format({"nav": "{:}"}))
                    
                    st.subheader("Statistics")
                    st.write(f"Start Date: {stats['startDate']}")
                    st.write(f"End Date: {stats['endDate']}")
                    st.write(f"Average NAV: {stats['average']:.2f}")
                    st.write(f"Standard Deviation: {stats['stdDev']:.2f}")
                    st.write(f"Null Dates: {', '.join(stats['nullDates']) if stats['nullDates'] else 'None'}")

                    
            except requests.RequestException as e:
                st.error(f"Error fetching NAV data: {e}")