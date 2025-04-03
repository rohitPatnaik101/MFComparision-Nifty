from pymongo import MongoClient
from config import MONGO_URI

client = MongoClient(MONGO_URI)
db = client['MFComparision']
mf_collection = db['mutual_funds']
nav_collection = db['nav_data']

# Initial MF data
MF_DATA = [
    {"company": "Axis Mutual Fund", "fund": "Axis Arbitrage Fund - Regular plan", "mfID": 53, "scID": 130771},
    {"company": "ITI Mutual fund", "fund": "ITI Dynamic Bond Fund - Direct plan", "mfID": 70, "scID": 149029}
]

def init_db():
    if mf_collection.count_documents({}) == 0:
        mf_collection.insert_many(MF_DATA)
    # Create an index on nav_history.date to ensure sorted retrieval
    nav_collection.create_index([("nav_history.date", 1)])

init_db()