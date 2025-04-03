from flask import Flask, jsonify, request
from db import mf_collection
from nav_service import get_nav_data, describe_nav, get_mf_ids

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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)