import pymongo
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DBConnector:
    _instance = None

    def __new__(cls):
        """
        Singleton pattern: Ensures only one instance of DBConnector is created.
        """
        if cls._instance is None:
            cls._instance = super(DBConnector, cls).__new__(cls)
            try:
                mongo_uri = os.getenv("MONGO_URI")
                if not mongo_uri:
                    raise ValueError("MONGO_URI not found in .env file")
                
                # Establish the connection
                cls._instance.client = pymongo.MongoClient(mongo_uri)
                
                # --- Access your 3 specific databases ---
                cls._instance.db_inventory = cls._instance.client["ShopInventory"]
                cls._instance.db_sales = cls._instance.client["ShopSales"]
                cls._instance.db_member = cls._instance.client["Shopmember"]
                
                print("Successfully connected to MongoDB.")
                print(f"Available databases: {cls._instance.client.list_database_names()}")

            except pymongo.errors.ConnectionFailure as e:
                print(f"FATAL: Could not connect to MongoDB: {e}")
                cls._instance = None
            except ValueError as e:
                print(f"FATAL: Configuration error: {e}")
                cls._instance = None
        return cls._instance

    def get_inventory_db(self):
        """Returns the handle to the ShopInventory database."""
        return self.db_inventory

    def get_sales_db(self):
        """Returns the handle to the ShopSales database."""
        return self.db_sales

    def get_member_db(self):
        """Returns the handle to the Shopmember database."""
        return self.db_member

    def close_connection(self):
        """Closes the MongoDB connection."""
        if hasattr(self, 'client') and self.client:
            self.client.close()
            print("MongoDB connection closed.")

# --- Create a single, importable instance for the whole app ---
db_connection = DBConnector()