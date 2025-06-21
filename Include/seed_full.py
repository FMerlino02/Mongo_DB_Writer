import os
import json
from typing import Union, Optional
from datetime import datetime, date
from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from datetime import datetime
import logfire
from bson import ObjectId
from map_booking_ids import get_booking_id_map
from parsers import parse_int, parse_float, parse_date
from extract_accommodation_level import extract_accommodation_level

# Start logfire session
logfire.configure()

class FullInfo(BaseModel):
    """
    Pydantic model for FULL information.
    """
    Type: str
    Stars: int

    CheckIn: datetime
    CheckOut: datetime
    
    Destination: str
    DemandPressure: int

    SearchRank: int
    SearchPage: int
    
    
    AccommodationType: str
    CancellationPolicy: str
    Treatment: str
    
    AccomodationLevel: str

    Occupation: int
    minimunStay: int

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
    DateSearch: datetime
    FullDateSearch: datetime
    
    PropertyId: Union[str, ObjectId]

    class Config:
        arbitrary_types_allowed = True

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


    file_path = r"C:\Users\Eiji\Desktop\RL_FULL_HTL_LE.json"
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    success, errors = 0, 0

    property_types = [
        {"propertyIDs": 201, "property_name": "Appartamenti", "category": "APT"},
        {"propertyIDs": 204, "property_name": "Hotel", "category": "HTL"},
        {"propertyIDs": 208, "property_name": "Bed & Breakfast", "category": "APT"},
        {"propertyIDs": 220, "property_name": "Case vacanze", "category": "APT"},
        {"propertyIDs": 216, "property_name": "Affittacamere", "category": "APT"},
        {"propertyIDs": 213, "property_name": "Ville", "category": "APT"},
        {"propertyIDs": 223, "property_name": "Case di campagna", "category": "APT"},
        {"propertyIDs": 203, "property_name": "Ostelli", "category": "APT"},
        {"propertyIDs": 210, "property_name": "Agriturismi", "category": "APT"},
        {"propertyIDs": 228, "property_name": "Chalet", "category": "APT"},
        {"propertyIDs": 222, "property_name": "Alloggi in famiglia/Homestays", "category": "APT"},
        {"propertyIDs": 224, "property_name": "Campeggi di lusso", "category": "APT"},
        {"propertyIDs": 212, "property_name": "Villaggi turistici", "category": "HTL"},
        {"propertyIDs": 205, "property_name": "Motel", "category": "HTL"},
        {"propertyIDs": 206, "property_name": "Resort", "category": "HTL"},
        {"propertyIDs": 219, "property_name": "Residence", "category": "APT"},
        {"propertyIDs": 218, "property_name": "Locande", "category": "HTL"},
    ]

    # Create a lookup dictionary for property types
    property_types_map = {item["property_name"]: item["category"] for item in property_types}

    # Get the booking_id to _id mapping
    booking_id_map = get_booking_id_map()


    for record in data:
        # Skip records that don't have at least a city name and property name
        if not record.get("Città") or not record.get("Nome"):
            print(f"Skipping incomplete record: {record}")
            with open("skipped_records.txt", "a", encoding="utf-8") as skipped_file:
                skipped_file.write(f"{record.get('Destinazione','')},{record.get('Città','')}\n")
            errors += 1
            continue

        booking_id = record.get("id")  # Assuming "id" is the booking_id in the JSON
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
            # Create a document for each room
            full = FullInfo(
                Type=record.get("Tipologia"),
                Stars=parse_int(record.get("Stelle")),
                CheckIn=parse_date(record.get("CheckIn")),
                CheckOut=parse_date(record.get("CheckOut")),
                Destination=record.get("Destinazione"),
                DemandPressure=record.get("TIN"),
                SearchRank=parse_int(record.get("SearchRank")),
                SearchPage=parse_int(record.get("SearchPage")),
                AccommodationType=record.get("AccomodationType"),
                Treatment=record.get("Trattamento"),
                CancellationPolicy=record.get("CancellationType"),
                AccomodationLevel=extract_accommodation_level(record.get("AccomodationType")),
                Occupation=parse_int(record.get("Occupazione")),
                PriceTot=parse_float(record.get("TariffaTOT")),
                PriceNight=parse_float(record.get("TariffaGG")),
                minimunStay=parse_int(record.get("minimunStay")),
                RoomsBARLeft=parse_int(record.get("RoomsLeft")) or None,
                IsOffer=record.get("IsAnOffer") == "YES",
                OfferDiscountValue=parse_float(record.get("OfferDiscount")),
                OfferDiscountPercent=None,  # Adjust if needed
                OfferTitle=record.get("OfferTitle"),
                OfferDesc=record.get("OfferDescription"),
                ESG_Rating=record.get("ESG_Rating"),
                ESG_Score=str(record.get("ESG_Score")),
                DateSearch=parse_date(record.get("DataRicerca")),
                FullDateSearch=parse_date(record.get("FullDataRicerca"))
                    if record.get("FullDataRicerca")
                    else parse_date(record.get("DataRicerca")),
                PropertyId=ObjectId(record_PropertyId)
            )
            doc = full.model_dump(exclude_none=True)

            # Convert all `datetime.date` fields to `datetime.datetime`
            if isinstance(doc.get("DateSearch"), date):
                doc["DateSearch"] = datetime.combine(doc["DateSearch"], datetime.min.time())
            if isinstance(doc.get("FullDateSearch"), date):
                doc["FullDateSearch"] = datetime.combine(doc["FullDateSearch"], datetime.min.time())

            # Determine the property type (HTL or APT) using the in-memory dictionary
            tipologia = record.get("Tipologia")
            property_category = property_types_map.get(tipologia, "APT")  # Default to "APT" if not found

            # Insert into the correct collection based on property type
            collection_name = "FULL_HTL" if property_category == "HTL" else "FULL_APT"
            collection = db[collection_name]

            result = collection.insert_one(doc)
            logfire.info("Inserted record", city=doc, mongo_id=str(result.inserted_id))
            success += 1  # Increment success count

        except Exception as e:
            logfire.error("Error inserting room record", error=str(e), record=record)
            with open("skipped_records.txt", "a", encoding="utf-8") as skipped_file:
                skipped_file.write(f"{record.get('Destinazione')},{record.get('Città')}\n")
            errors += 1

    logfire.info("Import summary", success=success, errors=errors)
    print(f"Import finished. Success: {success}, Errors: {errors}")

if __name__ == "__main__":
    main()