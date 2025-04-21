import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO

# Target URL
url = "https://www.amfiindia.com/modules/AverageAUMDetails"

# Payload
payload = {
    "AUmType": "S",
    "AumCatType": "Typewise",
    "MF_Id": "53",  # Axis Mutual Fund
    "Year_Id": "1",  # 2025
    "Year_Quarter": "January - March 2025"
}

# Headers to mimic a browser
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.amfiindia.com",
    "Referer": "https://www.amfiindia.com/research-information/aum-data/average-aum"
}

response = requests.post(url, data=payload, headers=headers)

# Parse HTML tables
tables = pd.read_html(StringIO(response.text))
if not tables:
    raise ValueError("No tables found!")

df = tables[0]
df.columns = [' '.join(col).strip() if isinstance(col, tuple) else col for col in df.columns]

# 🔍 Find the Scheme and AUM columns
scheme_col = next((col for col in df.columns if "Scheme NAV Name" in col), None)
aum_col = next((col for col in df.columns if "Average AUM for The Month" in col and "Fund Of Funds" not in col), None)

if not scheme_col or not aum_col:
    raise ValueError("Required columns not found!")

# 🔎 Search for your specific scheme
search_term = "Axis Aggressive Hybrid Fund"
match = df[df[scheme_col].str.contains(search_term, case=False, na=False)]

if not match.empty:
    aum_value = match.iloc[0][aum_col]
    print(f"\n{search_term} - Average AUM: ₹{aum_value} Lakhs")
else:
    print(f"\n❌ Scheme not found: {search_term}")
