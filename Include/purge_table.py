import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import logfire

# Start logfire session
logfire.configure()

def main():
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    uri = f"mongodb+srv://{db_username}:{db_password}@di-testcluster.kf21i.mongodb.net/?retryWrites=true&w=majority&appName=DI-TestCluster"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]
    collection = db["BAR_HTL"]

    confirm = input("Are you sure you want to delete ALL documents from BAR_HTL? Type YES to confirm: ")
    if confirm == "YES":
        result = collection.delete_many({})
        logfire.info("Purged BAR_HTL collection", deleted_count=result.deleted_count)
        print(f"Deleted {result.deleted_count} documents from BAR_HTL.")
    else:
        print("Operation cancelled.")
        logfire.info("Purge cancelled by user")

if __name__ == "__main__":
    main()