import requests
from bs4 import BeautifulSoup

# Store current NAV (you can also use a database if needed)
current_nav = None

def scrape_nav_history(mf_id, sc_id, f_date, t_date):
    global current_nav  # Access the global current_nav variable

    url = "https://www.amfiindia.com/modules/NavHistoryPeriod"
    payload = {
        'mfID': mf_id,
        'scID': sc_id,
        'fDate': f_date,
        'tDate': t_date
    }

    response = requests.post(url, data=payload)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.select('tr')  # Select all table rows

        nav_data = []
        for row in rows[1:]:  # Skip header row
            cols = row.find_all('td')
            if len(cols) >= 4:
                nav_value = cols[0].get_text(strip=True)
                nav_date = cols[3].get_text(strip=True)
                nav_data.append((nav_date, nav_value))

        # Store the latest NAV (most recent entry, i.e., last one in the list)
        if nav_data:
            current_nav = nav_data[-1]  # The most recent NAV is the last element
            print(f"Current NAV is set to: {current_nav[1]} on {current_nav[0]}")
        
        return nav_data
    else:
        print(f"Error: Received status code {response.status_code}")
        return None

# Example usage
nav_history = scrape_nav_history(70, 149029, "13-Apr-2024", "01-Apr-2025")
for date, value in nav_history:
    print(f"NAV Date: {date}, NAV: {value}")

# Access the current NAV
print(f"\nStored Current NAV: {current_nav[1]} on {current_nav[0]}" if current_nav else "No current NAV stored.")
