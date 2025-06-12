import os
from typing import List
from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import logfire

# Start logfire session
logfire.configure()


# Pydantic model for PropertyType
class PropertyType(BaseModel):
    propertyIDs: int
    property_name: str
    category: str


def main():
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    collection_name = "Property_Types"

    mongo_uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(mongo_uri, server_api=ServerApi("1"))
    db = client[db_name]
    collection = db[collection_name]

    # Data extracted from the notebook
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
        {"propertyIDs": 219, "property_name": "Residence", "category": "HTL"},
        {"propertyIDs": 218, "property_name": "Locande", "category": "HTL"},
    ]

    success, errors = 0, 0

    for entry in property_types:
        try:
            prop = PropertyType(**entry)
            doc = prop.model_dump()
            result = collection.insert_one(doc)
            logfire.info(
                "Inserted property type",
                property_type=doc,
                mongo_id=str(result.inserted_id),
            )
            print(
                f"Inserted: {doc['property_name']} (ID: {doc['propertyIDs']})")
            success += 1
        except Exception as e:
            print(f"Error inserting {entry['property_name']}: {e}")
            logfire.error(
                "Error inserting property type", error=str(e), property_type=entry
            )
            errors += 1

    logfire.info("Import summary", success=success, errors=errors)
    print(f"Import finished. Success: {success}, Errors: {errors}")


if __name__ == "__main__":
    main()
