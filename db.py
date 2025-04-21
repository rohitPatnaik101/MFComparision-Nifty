from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

# MongoDB Atlas connection string
ATLAS_URI = "mongodb+srv://rohitpatnaik:4r6LA0AZgS2VFNNk@mfcomparision.mlka6gh.mongodb.net/?retryWrites=true&w=majority&appName=MFComparision"

try:
    client = MongoClient(ATLAS_URI, serverSelectionTimeoutMS=5000)
    # Test connection
    client.admin.command('ping')
    print("Successfully connected to MongoDB Atlas")
except ServerSelectionTimeoutError as e:
    print(f"Error: Could not connect to MongoDB Atlas: {e}")
    raise SystemExit("Exiting due to MongoDB connection failure")
except Exception as e:
    print(f"Unexpected error connecting to MongoDB Atlas: {e}")
    raise SystemExit("Exiting due to MongoDB connection failure")

db = client['mf_comparison']

mf_collection = db['mf_data']
nav_collection = db['nav_data']
nifty_collection = db['nifty_data']

# Mutual Fund Data
MF_DATA = [
    {"company": "SBI Mutual Fund", "fund": "SBI Small Cap Fund - Direct Plan - Growth", "mfID": 22, "scID": 125497},
    {"company": "HDFC Mutual Fund", "fund": "HDFC Flexi Cap Fund - Growth Option - Direct Plan", "mfID": 9, "scID": 118955},
    {"company": "ICICI Prudential Mutual Fund", "fund": "ICICI Prudential Bluechip Fund - Direct Plan - Growth", "mfID": 20, "scID": 120586},
    {"company": "UTI Mutual Fund", "fund": "UTI Flexi Cap Fund - Direct Plan - IDCW", "mfID": 28, "scID": 120663}
]

# Initialize mutual fund collection
def init_mf_collection():
    try:
        if mf_collection.count_documents({}) == 0:
            mf_collection.insert_many(MF_DATA)
            print("Initialized mf_collection with MF_DATA")
        else:
            print("mf_collection already initialized")
    except Exception as e:
        print(f"Error initializing mf_collection: {e}")
        raise

init_mf_collection()