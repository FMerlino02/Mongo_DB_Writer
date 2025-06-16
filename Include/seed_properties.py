"""
seed_properties.py

Script to import property data from a JSON file into MongoDB, referencing cities and property types.
Duplicates are skipped, and all actions are logged with logfire.
"""

import os
import json
from typing import Optional, Union
from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import logfire
from bson import ObjectId

# Start logfire session
logfire.configure()

class PropertiesInfo(BaseModel):
    """
    Pydantic model for property information.
    """
    name: str
    booking_id: int
    type_structure: str
    stars: int
    city: str
    address: str
    distanceCentre: float
    url: str
    latitude: float
    longitude: float
    CirCin: str
    zone: Optional[str] = None
    roomsNum: Optional[int] = None
    seasonality: Optional[str] = None
    totalAccomTypes: Optional[int] = None
    bedsNum: Optional[int] = None
    cityId: Union[str, ObjectId]  # Foreign key to Cities collection

    class Config:
        arbitrary_types_allowed = True

# City name translation mapping (expand as needed)
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

def parse_float(val) -> Optional[float]:
    """
    Parse a value to float, handling commas and extracting the first number from a string.
    """
    if isinstance(val, (float, int)):
        return float(val)
    if isinstance(val, str):
        val = val.replace(",", ".")
        import re
        match = re.search(r"[-+]?\d*\.\d+|\d+", val)
        return float(match.group()) if match else None
    return None

def parse_int(val) -> Optional[int]:
    """
    Parse a value to int, returning None if conversion fails.
    """
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def main():
    """
    Main function to import properties from JSON to MongoDB.
    - Loads environment variables.
    - Connects to MongoDB.
    - Reads property data from JSON.
    - Translates city names and references city ObjectId.
    - Checks for duplicates based on booking_id.
    - Determines property type and inserts into the correct collection.
    - Logs all actions and prints a summary.
    """
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority&appName=DI-TestCluster"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]

    # Path to the JSON file with property data
    file_path = r"C:\Users\Eiji\Desktop\Milano_le.json"
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    success, errors, duplicates = 0, 0, 0

    for record in data:
        # Skip records that don't have at least a city name and property name
        if not record.get("Città") or not record.get("Nome"):
            print(f"Skipping incomplete record: {record}")
            with open("skipped_records.txt", "a", encoding="utf-8") as skipped_file:
                skipped_file.write(json.dumps(record, ensure_ascii=False) + "\n")
            errors += 1
            continue
        try:
            # Translate and reference the city
            city_name_raw = record.get("Città")
            city_name = translate_city(city_name_raw)
            city_doc = db["Cities"].find_one({"City": city_name})
            city_id = city_doc.get("_id") if city_doc else None

            if not city_id:
                warning_msg = (
                    f"Warning: City '{city_name_raw}' (translated: '{city_name}') not found in Cities collection. Skipping record."
                )
                print(warning_msg)
                logfire.warning("City not found for property", city_name=city_name_raw,
                                translated_city=city_name, record=record)
                with open("skipped_records.txt", "a", encoding="utf-8") as skipped_file:
                    skipped_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                errors += 1
                continue

            # Check for duplicate booking_id in both collections
            booking_id = parse_int(record.get("id"))
            if db["Properties_HTL"].find_one({"booking_id": booking_id}) or db["Properties_APT"].find_one({"booking_id": booking_id}):
                print(f"Duplicate booking_id found: {booking_id}, skipping record.")
                logfire.info("Duplicate booking_id skipped", booking_id=booking_id, record=record)
                duplicates += 1
                continue

            # --- Robust distanceCentre parsing ---
            raw_distance = record.get("DistanzaCentro")
            distance_centre = None
            if raw_distance:
                try:
                    # Try to extract a float from the string, e.g. "150 m dal centro" -> 150.0
                    import re
                    match = re.search(r"[-+]?\d*\.\d+|\d+", str(raw_distance).replace(",", "."))
                    distance_centre = float(match.group()) if match else None
                except Exception:
                    distance_centre = None

            # Build the property document using the Pydantic model
            prop = PropertiesInfo(
                name=record.get("Nome"),
                booking_id=booking_id,
                type_structure=record.get("Tipologia"),
                stars=parse_int(record.get("Stelle")),
                address=record.get("Indirizzo"),
                distanceCentre=distance_centre,
                city=city_name,
                url=record.get("url"),
                latitude=parse_float(record.get("LAT")),
                longitude=parse_float(record.get("LNG")),
                CirCin=record.get("Cir"),
                zone=record.get("Zona") if "Zona" in record else None,
                roomsNum=parse_int(record.get("numCamere")) if "numCamere" in record else None,
                seasonality=record.get("stagionalita") if "stagionalita" in record else None,
                totalAccomTypes=parse_int(record.get("totTipiAlloggi")) if "totTipiAlloggi" in record else None,
                bedsNum=parse_int(record.get("numLetti")) if "numLetti" in record else None,
                cityId=city_id,
            )
            doc = prop.model_dump(exclude_none=True)

            # Determine the property type (HTL or APT) by looking up the Property_Types collection
            tipologia = record.get("Tipologia")
            property_type_doc = None
            if isinstance(tipologia, int):
                property_type_doc = db["Property_Types"].find_one({"propertyIDs": tipologia})
            elif isinstance(tipologia, str):
                property_type_doc = db["Property_Types"].find_one({"property_name": tipologia})
            else:
                property_type_doc = None

            # Insert into the correct collection based on property type
            if property_type_doc and property_type_doc.get("category") == "HTL":
                collection_name = "Properties_HTL"
            else:
                collection_name = "Properties_APT"
            collection = db[collection_name]

            result = collection.insert_one(doc)
            logfire.info("Inserted record", property=doc, mongo_id=str(result.inserted_id))
            success += 1
        except Exception as e:
            print(f"Error inserting record: {e}\nRecord: {record}")
            logfire.error("Error inserting record", error=str(e), record=record)
            errors += 1

    # Log and print the import summary
    logfire.info("Import summary", success=success, errors=errors, duplicates=duplicates)
    print(f"Import finished. Success: {success}, Errors: {errors}, Duplicates: {duplicates}")

if __name__ == "__main__":
    main()