"""
rooms.py

Script to import room data into MongoDB. Each room is mapped to a singular record in the Rooms collection.
"""

from typing import Optional, Union, List
from datetime import datetime
from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from bson import ObjectId
from parsers import parse_int, parse_float, parse_date
import logfire
import os
import json
import re

# Start logfire session
logfire.configure()


def parse_occupancy(accomodation_type: str, occupazione: int):
    """
    Extracts OccupancyAdult and OccupancyKid from AccomodationType string.
    If 'Bambino' is not present, uses 'Occupazione' for adults and sets kids to 0.
    """
    if not accomodation_type or not isinstance(accomodation_type, str):
        return None, None

    # Regex to find "Adulti" and "Bambino" numbers
    adult_match = re.search(r"(\d+)\s*Adulti", accomodation_type)
    kid_match = re.search(r"(\d+)\s*Bambino", accomodation_type)

    occupancy_adult = int(adult_match.group(1)) if adult_match else None
    occupancy_kid = int(kid_match.group(1)) if kid_match else 0

    # If no "Adulti" or "Bambino" found, fallback to Occupazione
    if occupancy_adult is None:
        occupancy_adult = occupazione
        occupancy_kid = 0

    return occupancy_adult, occupancy_kid

class RoomInfo(BaseModel):
    """
    Pydantic model for room information.
    """
    roomName: str
    roomDesc: List[str]
    roomSize: Optional[int]
    hasInventory: bool
    OccupancyAdult: Optional[int]
    OccupancyKid: Optional[int]
    BedDesc: Optional[str]
    MainType: str
    SubType: str
    FullDateSearch: datetime
    Quantity: Optional[int]
    PropertyId: Union[str, ObjectId]

    class Config:
        allow_population_by_field_name = True


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
    file_path = r"C:\Users\Eiji\Desktop\Rooms.json"
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    success, errors = 0, 0

    for record in data:
        try:
            # Map PropertyId using booking_id
            booking_id = record.get("PropertyId")
            if not booking_id:
                logfire.error(f"Missing PropertyId for room: {record}")
                errors += 1
                continue

            # Use parse_occupancy to extract adult/kid occupancy
            occupancy_adult, occupancy_kid = parse_occupancy(
                record.get("AccomodationType"),
                parse_int(record.get("Occupazione"))
            )

            # Create a RoomInfo object
            room = RoomInfo(
                uniqueRoomId=record.get("uniqueRoomId"),
                roomName=record.get("roomName"),
                roomDesc=record.get("roomDesc", []),
                roomSize=parse_int(record.get("roomSize")),
                hasInventory=record.get("hasInventory", False),
                OccupancyAdult=occupancy_adult,
                OccupancyKid=occupancy_kid,
                BedDesc=record.get("BedDesc"),
                MainType=record.get("MainType"),
                SubType=record.get("SubType"),
                FullDateSearch=parse_date(record.get("FullDateSearch")),
                Quantity=parse_int(record.get("Quantity")),
                PropertyId=ObjectId(booking_id) if ObjectId.is_valid(booking_id) else booking_id
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