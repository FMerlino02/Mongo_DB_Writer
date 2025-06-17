"""
seed_reputation.py

Script to import reputation KPI data from a JSON file into MongoDB.
References properties by foreign key, and inserts into the Reputation_KPI collection.
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
from parsers import parse_float, parse_int, parse_date

logfire.configure()

class ReputationKPI(BaseModel):
    WifiScore: str
    QPScore: Optional[float]
    PositionScore: Optional[float]
    CleanScore: Optional[float]
    ComfortScore: Optional[float]
    ServiceScore: Optional[float]
    StaffScore: Optional[float]
    Reviews: Optional[int]
    Score: Optional[int]
    Valuation: Optional[str]
    FullDateSearch: datetime
    DateSearch: datetime
    PropertyId: Union[str, ObjectId]

    class Config:
        arbitrary_types_allowed = True

def main():
    """
    Main function to import reputation KPI data from JSON to MongoDB.
    - Loads environment variables.
    - Connects to MongoDB.
    - Reads reputation data from JSON.
    - Resolves foreign key for property.
    - Inserts into the Reputation_KPI collection.
    - Logs all actions and prints a summary.
    """
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority&appName=DI-TestCluster"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]

    file_path = r"C:\Users\Eiji\Desktop\reputation_kpi.json"  # Adjust path as needed
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

            rep = ReputationKPI(
                WifiScore=str(record.get("WiFi")),
                QPScore=parse_float(record.get("QualitàPrezzo")),
                PositionScore=parse_float(record.get("Posizione")),
                CleanScore=parse_float(record.get("Pulizia")),
                ComfortScore=parse_float(record.get("Comfort")),
                ServiceScore=parse_float(record.get("Servizi")),
                StaffScore=parse_float(record.get("Staff")),
                Reviews=parse_int(record.get("Recensioni")),
                Score=parse_int(record.get("Score")),
                Valuation=record.get("Valutazione"),
                FullDateSearch=parse_date(record.get("DataRicerca")),
                DateSearch=parse_date(record.get("FullDataRicerca")),
                PropertyId=property_fk
            )
            doc = rep.model_dump(exclude_none=True)

            result = db["Reputation_KPI_Table"].insert_one(doc)
            logfire.info("Inserted reputation KPI", reputation=doc, mongo_id=str(result.inserted_id))
            success += 1

        except Exception as e:
            logfire.error("Error inserting reputation KPI", error=str(e), record=record)
            errors += 1

    logfire.info("Import summary", success=success, errors=errors)
    print(f"Import finished. Success: {success}, Errors: {errors}")

if __name__ == "__main__":
    main()