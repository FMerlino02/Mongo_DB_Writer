"""
seed_bar.py

Script to import BAR (Best Available Rate) data from a JSON file into MongoDB.
References cities and properties, determines property type (HTL/APT), and inserts into the correct collection.
All actions are logged with logfire.
"""

import os
import json
from typing import Union, Optional
from datetime import datetime, date
from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import logfire
from bson import ObjectId
from pymongo.errors import PyMongoError

# Start logfire session
logfire.configure()

CITY_TRANSLATION = {
    "Milan": "Milano",
    "Rome": "Roma",
    "Florence": "Firenze",
    "Venice": "Venezia",
    "Naples": "Napoli",
    "Turin": "Torino",
    "Genoa": "Genova",
    "Bologna": "Bologna",
    "Palermo": "Palermo",
    "Bari": "Bari",
    # Add more as needed
}

def translate_city(city_name: str) -> str:
    """
    Translate city names from English (or other languages) to Italian using a dictionary.
    If not found, returns the original name.
    """
    return CITY_TRANSLATION.get(city_name, city_name)

class BarInfo(BaseModel):
    """
    Pydantic model for BAR information.
    """
    Type: str
    Stars: int
    CheckIn: datetime
    CheckOut: datetime
    Destination: str
    DemandPressure: int
    SearchRank: int
    SearchPage: int
    CancellationPolicy: str
    Treatment: str
    AccomodationType: str
    AccomodationLevel: str
    Occupation: int
    PriceTot: float
    PriceNight: float
    IsOffer: bool
    OfferDiscountValue: Optional[float]
    OfferDiscountPercent: Optional[float]
    OfferTitle: Optional[str]
    OfferDesc: Optional[str]
    ESG_Rating: str
    ESG_Score: str
    RoomsBARLeft: Optional[int] = None
    DateSearch: date
    FullDateSearch: date
    PropertyId: Union[str, ObjectId]

    class Config:
        arbitrary_types_allowed = True

def parse_float(val):
    """
    Parse a value to float, handling commas and extracting the first number from a string.
    Returns None if parsing fails.
    """
    if isinstance(val, (float, int)):
        return float(val)
    if isinstance(val, str):
        val = val.replace(",", ".")
        import re
        match = re.search(r"[-+]?\d*\.\d+|\d+", val)
        return float(match.group()) if match else None
    return None

def parse_int(val):
    """
    Parse a value to int, returning None if conversion fails.
    """
    if val == "" or val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

    """
    Parse a value to datetime, expects format YYYY-MM-DD.
    Returns None if parsing fails.
    Prints the input and result for debugging.
    """
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        dt = datetime.combine(val, datetime.min.time())
        return dt
    if isinstance(val, str):
        try:
            dt =  val )
            return dt
        except ValueError:
            return None
    return None
def extract_accomodation_level(accomodation_type: str) -> str:
    """
    Extracts the AccomodationLevel from the AccomodationType string using a hashmap for O(1) lookup.
    Handles both English and Italian types, including edge cases for "Junior Suite" and "studio room".
    If no match is found, returns "Other".
    """
    if not accomodation_type or not isinstance(accomodation_type, str):
        return "Other"

    level_map = {
        "camera": "Rooms",
        "camere": "Rooms",
        "room": "Rooms",
        "rooms": "Rooms",
        "studio room": "Rooms",
        "junior suite": "Junior Suite",
        "suite": "Suite",
        "appartamento": "Apartment",
        "appartamenti": "Apartment",
        "apartment": "Apartment",
        "apartments": "Apartment",
        "villa": "Villa",
        "ville": "Villa",
        "villetta": "Villa",
        "castello": "Villa",
        "castelli": "Villa",
        "castelletto": "Villa",
        "chalet": "Villa",
        "depandance": "Dependance",
        "studio": "Studio",
        "bungalow": "Bungalow",
        "dormitory": "Dormitory"
    }

    words = accomodation_type.lower().split()
    for n in range(len(words), 0, -1):
        phrase = " ".join(words[:n])
        if phrase in level_map:
            return level_map[phrase]
    for word in words:
        if word in level_map:
            return level_map[word]
    return "Other"

def convert_dates_to_datetimes(doc):
    """
    Recursively convert all datetime.date values in a dict to datetime.datetime.
    """
    for k, v in doc.items():
        if isinstance(v, date) and not isinstance(v, datetime):
            doc[k] = datetime.combine(v, datetime.min.time())
        elif isinstance(v, dict):
            convert_dates_to_datetimes(v)
    return doc

def main():
    """
    Main function to import BAR data from JSON to MongoDB.

    - Loads environment variables.
    - Connects to MongoDB.
    - Reads BAR data from JSON.
    - Maps city and property references.
    - Determines property type (HTL/APT).
    - Inserts into the correct collection.
    - Logs all actions and prints a summary.
    """
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority&appName=DI-TestCluster"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]


    file_path = r"C:\Users\giova\Desktop\RL_BAR_HTL_LE.json"
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    success, errors = 0, 0

    for record in data:
        # Skip records that don't have at least a city name and property name
        if not record.get("Città") or not record.get("Nome"):
            print(f"Skipping incomplete record: {record}")
            with open("skipped_records.txt", "a", encoding="utf-8") as skipped_file:
                skipped_file.write(f"{record.get('Destinazione','')},{record.get('Città','')}\n")
            errors += 1
            continue
        try:
            # No cityId or propertyId mapping, just use the raw values
            bar = BarInfo(
                Type=record.get("Tipologia"),
                Stars=parse_int(record.get("Stelle")),

                CheckIn= record.get("CheckIn")),
                CheckOut= record.get("CheckOut")),

                Destination=record.get("Destinazione"),
                DemandPressure= record.get("TIN"),

                SearchRank=parse_int(record.get("SearchRank")),
                SearchPage=parse_int(record.get("SearchPage")),
                AccomodationType=record.get("AccomodationType"),
                Treatment=record.get("Trattamento"),
                CancellationPolicy=record.get("CancellationPolicy"),
                AccomodationLevel=extract_accomodation_level(record.get("AccomodationType", "")),
                Occupation=parse_int(record.get("Occupazione")),
                PriceTot=parse_float(record.get("TariffaTOT")),
                PriceNight=parse_float(record.get("TariffaGG")),
                RoomBARLeft=parse_int(record.get("RoomsBARLeft")) or None,
                IsOffer=bool(record.get("IsAnOffer")),
                OfferDiscountValue=parse_float(record.get("OfferDiscountValue")),
                OfferDiscountPercent=parse_float(record.get("OfferDiscountPercent")),
                OfferTitle=record.get("OfferTitle"),
                OfferDesc=record.get("OfferDescription"),
                ESG_Rating=record.get("ESG_Rating"),
                ESG_Score=str(record.get("ESG_Score")),

                DateSearch= record.get("DataRicerca"),
                FullDateSearch=  record.get("FullDateSearch")
                    if record.get("FullDateSearch") else
                    record.get("DataRicerca"),
                
                #PropertyId=record.get("id"),

                RoomsBARLeft=parse_int(record.get("RoomsBARLeft")),
            )
            doc = bar.model_dump(exclude_none=True)
            doc = convert_dates_to_datetimes(doc)

            # Determine the property type (HTL or APT) by looking up the Property_Types collection
            tipologia = record.get("Tipologia")
            if isinstance(tipologia, str):
                property_type_doc = db["Property_Types"].find_one({"property_name": tipologia})
            else:
                property_type_doc = None

            # Insert into the correct collection based on property type
            if property_type_doc and property_type_doc.get("category") == "HTL":
                collection_name = "BAR_HTL"
            else:
                collection_name = "BAR_APT"

            collection = db[collection_name]

            result = collection.insert_one(doc)

        except Exception as e:
            logfire.error("Error inserting record", error=str(e), record=record)
            with open("skipped_records.txt", "a", encoding="utf-8") as skipped_file:
                skipped_file.write(f"{record.get('Destinazione','')},{record.get('Città','')}\n")
            errors += 1

    logfire.info("Import summary", success=success, errors=errors)
    print(f"Import finished. Success: {success}, Errors: {errors}")

if __name__ == "__main__":
    main()