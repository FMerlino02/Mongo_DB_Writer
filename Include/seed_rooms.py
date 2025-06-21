"""
rooms.py

Script to import room data into MongoDB. Each room is mapped to a singular record in the Rooms collection.
"""

from typing import Union, Optional
from datetime import datetime
from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from bson import ObjectId
import logfire
from parsers import parse_int, parse_float, parse_date
from map_booking_ids import get_booking_id_map
import json
import os
import re

# Start logfire session
logfire.configure()

class RoomInfo(BaseModel):
    """
    Pydantic model for room information.
    """
    roomName: str
    roomDesc: str
    roomSize: int
    hasInventory: bool
    OccupancyAdult: int
    OccupancyKid: int
    BedDesc: str
    MainType: str
    SubType: str
    
    DateSearch: datetime
    FullDateSearch: datetime

    PropertyId: Union[str, ObjectId]

    class Config:
        validate_by_name = True  # Updated for Pydantic v2
        arbitrary_types_allowed = True  # Allow ObjectId type

def seed_rooms():
    """
    Main function to import room data from JSON to MongoDB.

    - Loads environment variables.
    - Connects to MongoDB.
    - Reads room data from JSON.
    - Maps property references.
    - Inserts each room as a separate document into the Rooms collection.
    - Logs all actions and prints a summary.
    """
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority&appName=DI-TestCluster"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]

    # Load the JSON file containing room data
    file_path = r"C:\Users\Eiji\Desktop\A_Properties_Lago di Como_PTY_APT_2025-03-20.json"
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    success, errors = 0, 0

    booking_id_map = get_booking_id_map()  # Assuming this function is defined in map_booking_ids.py

    for record in data:
        try:
            booking_id = parse_int(record.get("id"))  # Assuming "id" is the booking_id in the JSON
            if booking_id in booking_id_map:
                record_PropertyId = booking_id_map[booking_id]
            else:
                # Log the issue and write the skipped record to the file
                logfire.error(f"Incorrect mapping for booking_id: {booking_id}")
                with open("skipped_ids.txt", "a", encoding="utf-8") as skipped_file:
                    skipped_file.write(f"{booking_id}\n")
                errors += 1
                continue

            for room in record.get("Rooms", []):
                # Parse maxOccupancy to extract adult and kid occupancy
                max_occupancy = room.get("maxOccupancy", "")
                occupancy_adult, occupancy_kid = 0, 0
                if max_occupancy:
                    parts = max_occupancy.split(",")
                    if len(parts) > 0:
                        occupancy_adult = parse_int(parts[0].split()[0])  # Extract number of adults
                    if len(parts) > 1:
                        occupancy_kid = parse_int(parts[1].split()[0])  # Extract number of kids

                room = RoomInfo(
                    roomName=room.get("name"),
                    roomDesc=room.get("description"),
                    roomSize=parse_int(room.get("roomSize")),
                    hasInventory=room.get("hasRoomInventory", False),
                    OccupancyAdult=occupancy_adult,
                    OccupancyKid=occupancy_kid,
                    BedDesc=room.get("BedsDetails"),
                    MainType=room.get("mainType"),
                    SubType=room.get("subType"),
                    FullDateSearch=parse_date(record.get("DataRicerca")),  # Fixed
                    DateSearch=parse_date(record.get("DataFullRicerca")),  # Fixed
                    PropertyId=ObjectId(record_PropertyId)
                )

            doc = room.model_dump(exclude_none=True)

            # Insert the room document into the Rooms collection
            collection = db["Rooms"]
            result = collection.insert_one(doc)
            logfire.info("Inserted room record", room=doc, mongo_id=str(result.inserted_id))
            success += 1

        except Exception as e:
            logfire.error("Error inserting room record", error=str(e), record=record)
            errors += 1

    logfire.info("Import summary", success=success, errors=errors)
    print(f"Import finished. Success: {success}, Errors: {errors}")


if __name__ == "__main__":
    seed_rooms()