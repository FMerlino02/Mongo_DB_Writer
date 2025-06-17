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

# Start logfire session
logfire.configure()

class ReviewInfo(BaseModel):

    Name: str
    Type: str
    Destination: str
    Stars: int
    
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

    PropertyId: Union[str, ObjectId]
    CityId: Union[str, ObjectId]

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

    file_path = r"C:\Users\Eiji\Desktop\reviews.json"  # Adjust path as needed
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    success, errors = 0, 0

    for record in data:
        try:
            # Resolve PropertyId (FK) by booking_id in Properties_HTL/APT
            booking_id = record.get("PropertyId")
            property_doc = db["Properties_HTL"].find_one({"booking_id": booking_id}) or \
                           db["Properties_APT"].find_one({"booking_id": booking_id})
            property_fk = property_doc["_id"] if property_doc else None

            # Resolve CityId (FK) by city name in Cities
            city_name = record.get("Destination")
            city_doc = db["Cities"].find_one({"City": city_name})
            city_fk = city_doc["_id"] if city_doc else None

            review = ReviewInfo(
                Name=record.get("Name"),
                Type=record.get("Type"),
                Destination=record.get("Destination"),
                Stars=parse_int(record.get("Stars")),
                
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
                PropertyId=property_fk,
                CityId=city_fk
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