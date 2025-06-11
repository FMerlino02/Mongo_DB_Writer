import os
import json
from typing import Optional
from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import logfire

# Start logfire session
logfire.configure()

# Pydantic model for City
class City(BaseModel):
    uniqueId: Optional[int] = None
    City: Optional[str] = None 
    CityCode: Optional[str] = None 
    CAP: Optional[int] = None 
    Regione: Optional[str] = None 
    Nazione: Optional[str] = None 
    ISTAT: Optional[str] = None 
    LAT: Optional[float] = None
    LNG: Optional[float] = None
    Surface: Optional[float] = None
    Population: Optional[int] = None

def main():
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    
    uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority&appName=DI-TestCluster"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]

    # Insert the data from the JSON file
    # Update the file path to your local JSON file
    file_path =   r"C:"
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    success, errors = 0, 0

    for record in data:
        # Skip records that don't have at least a city name and province code
        if not record.get("denominazione_ita") or not record.get("sigla_provincia"):
            print(f"Skipping incomplete record: {record}")
            errors += 1
            continue
        try:
            city = City(
                City=record.get("denominazione_ita"),
                CityCode=record.get("sigla_provincia"),
                CAP=record.get("cap"),
                Regione=record.get("regione"),
                Nazione="Italia",
                ISTAT=record.get("codice_istat"),
                LAT=record.get("lat"),
                LNG=record.get("lon"),
                Surface=int(float(record["superficie_kmq"])) if "superficie_kmq" in record and record["superficie_kmq"] else None,
                Population=record.get("popolazione")
            )
            doc = city.model_dump(exclude_none=True)

            # Check if the property is a HTL or an APT
            # Determine collection based on "tipologia" field
            tipologia = record.get("tipologia")
            hotel_types = {204, 212, 205, 219, 218}
            if tipologia in hotel_types:
                collection_name = "Properties_HTL"
            else:
                collection_name = "Properties_APT"
            collection = db[collection_name]



            result = collection.insert_one(doc)
            logfire.info("Inserted record", city=doc, mongo_id=str(result.inserted_id))
            success += 1
        except Exception as e:
            print(f"Error inserting record: {e}\nRecord: {record}")
            logfire.error("Error inserting record", error=str(e), record=record)
            errors += 1

    logfire.info("Import summary", success=success, errors=errors)
    print(f"Import finished. Success: {success}, Errors: {errors}")


if __name__ == "__main__":
    main()