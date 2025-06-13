import os
import json
from typing import Optional
from datetime import datetime
from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import logfire

# Start logfire session
logfire.configure()

# Pydantic model for FullPrices
class FullPrices(BaseModel):
    uniqueId: int
    Type: str
    Stars: int
    Destination: str
    CheckIn: datetime
    CheckOut: datetime

    # TIN
    DemandPressure: Optional[str] = None
    SearchRank: Optional[int] = None
    SearchPage: Optional[int] = None
    AccomodationType: Optional[str] = None
    CancellationPolicy: Optional[str] = None
    Treatment: Optional[str] = None
    AccomodationLevel: Optional[str] = None
    PriceTot: Optional[float] = None
    PriceNight: Optional[float] = None
    MinimumStay: Optional[int] = None
    RoomLeft: Optional[int] = None
    IsOffer: Optional[str] = None
    OfferDiscountValue: Optional[float] = None
    OfferDiscountPercent: Optional[float] = None
    OfferTitle: Optional[str] = None
    OfferDesc: Optional[str] = None
    MainType: Optional[str] = None
    subType: Optional[str] = None
    ESG_Rating: Optional[str] = None
    ESG_Score: Optional[str] = None
    DateSearch: Optional[str] = None

def main():
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    collection_name = "FullPrices"

    uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority&appName=DI-TestCluster"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]
    collection = db[collection_name]

    file_path = input("Enter path to JSON file: ").strip()
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Preview first 3 mapped FullPrices records
    print("\nPreview of first 3 mapped FullPrices records:")
    for record in data[:3]:
        # Map your fields here as needed
        full_price = FullPrices(
            # Example mapping, adjust as needed:
            # uniqueId=record.get("uniqueId"),
            # Type=record.get("Type"),
            # Stars=record.get("Stars"),
            # Destination=record.get("Destination"),
            # CheckIn=datetime.strptime(record["CheckIn"], "%Y-%m-%d") if "CheckIn" in record else None,
            # ...etc...
        )
        print(full_price.model_dump(exclude_none=True))
    proceed = input("\nProceed with import? (y/n): ").strip().lower()
    if proceed != 'y':
        print("Import cancelled.")
        return

    success, errors = 0, 0

    for record in data:
        try:
            full_price = FullPrices(
                # Map your fields here as above
            )
            doc = full_price.model_dump(exclude_none=True)
            result = collection.insert_one(doc)
            logfire.info("Inserted record", full_price=doc, mongo_id=str(result.inserted_id))
            success += 1
        except Exception as e:
            print(f"Error inserting record: {e}\nRecord: {record}")
            logfire.error("Error inserting record", error=str(e), record=record)
            errors += 1

    logfire.info("Import summary", success=success, errors=errors)
    print(f"Import finished. Success: {success}, Errors: {errors}")

if __name__ == "__main__":
    main()