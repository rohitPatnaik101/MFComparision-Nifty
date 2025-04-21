from flask import Flask, jsonify, request, send_file
import pandas as pd
import matplotlib.pyplot as plt
from db import mf_collection
from services.nav_service import get_nav_data, describe_nav, get_mf_ids, get_aum_data, predict_nav
from services.nifty_service import get_nifty_data

app = Flask(__name__)

@app.route('/api/list_mfs', methods=['GET'])
def list_mfs():
    mfs = [{"company": mf["company"], "Fund": mf["fund"]} for mf in mf_collection.find()]
    return jsonify(mfs)

@app.route('/api/get_nav', methods=['POST'])
def get_nav():
    data = request.get_json()
    mf_name = data.get("MFName")
    from_date = data.get("FromDate")
    to_date = data.get("ToDate")

    df, error = get_nav_data(mf_name, from_date, to_date)
    if error:
        return jsonify({"error": error}), 400 if "Invalid" in error else 404 if "not found" in error else 500
    
    nav_data = df.to_dict(orient="records")
    stats = describe_nav(df)
    return jsonify({"nav_data": nav_data, "stats": stats})

@app.route('/api/fetch_nifty', methods=['POST'])
def fetch_nifty():
    data = request.get_json()
    from_date = data.get("FromDate")
    to_date = data.get("ToDate")

    df, error = get_nifty_data(from_date, to_date)
    if error:
        return jsonify({"error": error}), 500
    
    nifty_data = [{"date": row["Date"], "close": row["Close"]} for _, row in df.iterrows()]
    return jsonify({"nifty_data": nifty_data})

@app.route('/api/compare_mf_nifty', methods=['POST'])
def compare_mf_nifty():
    data = request.get_json()
    mf_name = data.get("MFName")
    from_date = data.get("FromDate")
    to_date = data.get("ToDate")

    # Fetch MF data
    mf_df, mf_error = get_nav_data(mf_name, from_date, to_date)
    if mf_error:
        return jsonify({"error": mf_error}), 500
    
    # Fetch Nifty data
    nifty_df, nifty_error = get_nifty_data(from_date, to_date)
    if nifty_error:
        return jsonify({"error": nifty_error}), 500
    
    # Convert dates to datetime for alignment
    mf_df["date"] = pd.to_datetime(mf_df["date"], format="%d-%b-%Y")
    nifty_df["Date"] = pd.to_datetime(nifty_df["Date"], format="%d-%b-%Y")
    
    # Align datasets by date
    aligned_df = pd.merge(mf_df[["date", "nav"]], nifty_df[["Date", "Close"]], 
                          left_on="date", right_on="Date", how="inner")
    if aligned_df.empty:
        return jsonify({"error": "No overlapping data between MF and Nifty"}), 500
    
    aligned_df = aligned_df.drop(columns=["Date"])
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
    
    # Format Date back to string
    aligned_df["Date"] = aligned_df["Date"].dt.strftime("%d-%b-%Y")
    comparison_data = aligned_df.to_dict(orient="records")
    
    return jsonify({
        "comparison_data": comparison_data,
        "correlation": correlation,
        "plot": f"/plot/{plot_path}"
    })

@app.route('/api/get_aum', methods=['POST'])
def get_aum():
    data = request.get_json()
    mf_name = data.get("MFName")
    year_quarter = data.get("Year_Quarter")
    
    if not mf_name or not year_quarter:
        return jsonify({"error": "MFName and Year_Quarter are required"}), 400
    
    aum_data, error = get_aum_data(mf_name, year_quarter)
    if error:
        return jsonify({"error": error}), 404 if "not found" in error else 400 if "Invalid" in error else 500
    
    return jsonify({"aum_data": aum_data})

@app.route('/api/nav_pred', methods=['POST'])
def nav_pred():
    data = request.get_json()
    mf_name = data.get("MFName")
    from_date = data.get("FromDate")
    to_date = data.get("ToDate")
    
    if not mf_name or not from_date or not to_date:
        return jsonify({"error": "MFName, FromDate, and ToDate are required"}), 400
    
    predictions, metrics, error = predict_nav(mf_name, from_date, to_date)
    if error:
        return jsonify({"error": error}), 404 if "not found" in error else 400 if "Invalid" in error else 500
    
    return jsonify({
        "predictions": predictions,
        "metrics": metrics
    })

@app.route('/plot/<path:filename>', methods=['GET'])
def get_plot(filename):
    return send_file(filename, mimetype='image/png')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)