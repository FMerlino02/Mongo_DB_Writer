"""
reviews.py

Script to import reviews data from a JSON file into MongoDB.
References properties and cities by foreign key, and inserts into the Reviews collection.
All actions are logged with logfire.
"""

import os
import json
from typing import Optional, Union
from datetime import datetime
from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import logfire
from bson import ObjectId
from parsers import parse_int, parse_float, parse_date
from map_booking_ids import get_booking_id_map

# Start logfire session
logfire.configure()

class ReviewInfo(BaseModel):
    """
    Represents a review document to be inserted into MongoDB.

    Attributes:
        NameReviewer (str): The name of the reviewer.
        Nationality (str): The nationality of the reviewer.
        TypeRoom (str): The type of room reviewed.
        LOS (int): Length of stay in days.
        StayingDate (datetime): The date of the stay.
        TypeClient (str): The type of client (e.g., family, solo traveler).
        Vote (float): The review score or vote.
        TitleReview (str): The title of the review.
        Positive (Optional[str]): Positive comments from the review.
        Negative (Optional[str]): Negative comments from the review.
        ReviewDate (Optional[datetime]): The date the review was written.
        PropertyId (Union[str, ObjectId]): The property ID, resolved as an ObjectId.
    """

    NameReviewer: str
    Nationality: str
    TypeRoom: str
    LOS: int
    StayingDate: datetime
    TypeClient: str
    Vote: float
    TitleReview: str
    Positive: Optional[str] = None
    Negative: Optional[str] = None
    ReviewDate: Optional[datetime] = None
    
    PropertyId: Union[str, ObjectId]

    class Config:
        arbitrary_types_allowed = True

def main():
    """
    Main function to import reviews from JSON to MongoDB.
    - Loads environment variables.
    - Connects to MongoDB.
    - Reads reviews data from JSON.
    - Resolves foreign keys for properties and cities.
    - Inserts into the Reviews collection.
    - Logs all actions and prints a summary.
    """
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority&appName=DI-TestCluster"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]

    file_path = r"C:\Users\Eiji\Desktop\LdC_HTL_Reviews_20250201.json"  # Adjust path as needed
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    success, errors = 0, 0

    # Get the booking_id to _id mapping
    booking_id_map = get_booking_id_map()

    for record in data:
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
        try:
            review = ReviewInfo(
                NameReviewer=record.get("Nome"),
                Nationality=record.get("Nazionalità"),
                TypeRoom=record.get("Tipologia Camera"),
                LOS=parse_int(record.get("Durata Soggiorno")),

                StayingDate=parse_date(record.get("Data")),
                
                TypeClient=record.get("Tipologia Cliente"),
                
                Vote=parse_float(record.get("Voto")),
                
                TitleReview=record.get("Titolo Recensione"),
                Positive=record.get("Commento Positivo"),
                Negative=record.get("Commento Negativo"),
                ReviewDate=parse_date(record.get("DataRecensione")),
                
                PropertyId=ObjectId(record_PropertyId)
            )
            doc = review.model_dump(exclude_none=True)

            result = db["Reviews"].insert_one(doc)
            logfire.info("Inserted review", review=doc, mongo_id=str(result.inserted_id))
            success += 1

        except Exception as e:
            logfire.error("Error inserting review", error=str(e), record=record)
            errors += 1

    logfire.info("Import summary", success=success, errors=errors)
    print(f"Import finished. Success: {success}, Errors: {errors}")

if __name__ == "__main__":
    main()