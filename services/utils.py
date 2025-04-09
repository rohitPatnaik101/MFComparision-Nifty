from datetime import datetime, timedelta

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