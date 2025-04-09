from flask import Flask, jsonify, request, send_file
from db import mf_collection
from nav_service import get_nav_data, describe_nav, get_mf_ids, fetch_nifty_data, compare_mf_nifty

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

    df, error = fetch_nifty_data(from_date, to_date)
    if error:
        return jsonify({"error": error}), 500
    
    nifty_data = [{"date": row["Date"], "close": row["Close"]} for _, row in df.iterrows()]
    return jsonify({"nifty_data": nifty_data})

@app.route('/api/compare_mf_nifty', methods=['POST'])
def compare_mf_nifty_endpoint():
    data = request.get_json()
    mf_name = data.get("MFName")
    from_date = data.get("FromDate")
    to_date = data.get("ToDate")

    df, correlation, plot_path, error = compare_mf_nifty(mf_name, from_date, to_date)
    if error:
        return jsonify({"error": error}), 500
    
    comparison_data = df.to_dict(orient="records")
    return jsonify({
        "comparison_data": comparison_data,
        "correlation": correlation,
        "plot": f"/plot/{plot_path}"
    })

@app.route('/plot/<path:filename>', methods=['GET'])
def get_plot(filename):
    return send_file(filename, mimetype='image/png')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)