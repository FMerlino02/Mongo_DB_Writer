"""
map_booking_ids.py

Script to map booking IDs to MongoDB object IDs from the Properties_HTL and Properties_APT collections.
The output is a dictionary where the keys are booking IDs and the values are MongoDB _id values.
"""

import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

def get_booking_id_map():
    """
    Fetches booking_id to MongoDB _id mappings from both Properties_HTL and Properties_APT collections.
    Returns:
        dict: A dictionary mapping booking_id to MongoDB _id.
    """
    # Load environment variables
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    # Connect to MongoDB
    uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority&appName=DI-TestCluster"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]

    # Initialize the booking_id map
    booking_id_map = {}

    # Access the Properties_HTL collection and map booking_id to _id
    htl_collection = db["Properties_HTL"]
    htl_documents = htl_collection.find({}, {"booking_id": 1})
    for doc in htl_documents:
        booking_id = doc.get("booking_id")
        if booking_id:
            booking_id_map[booking_id] = str(doc["_id"])

    # Access the Properties_APT collection and map booking_id to _id
    apt_collection = db["Properties_APT"]
    apt_documents = apt_collection.find({}, {"booking_id": 1})
    for doc in apt_documents:
        booking_id = doc.get("booking_id")
        if booking_id:
            booking_id_map[booking_id] = str(doc["_id"])

    return booking_id_map


if __name__ == "__main__":
    booking_id_map = get_booking_id_map()
    print(f"Booking ID Map: {booking_id_map}")
