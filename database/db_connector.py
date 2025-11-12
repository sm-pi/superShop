import pymongo
from dotenv import load_dotenv
import os

class DBConnection:
    def __init__(self):
        load_dotenv()
        self.connection_string = os.getenv("MONGODB_CONNECTION_STRING")
        if not self.connection_string:
            raise ValueError("MONGODB_CONNECTION_STRING not found in .env file")
        self.client = None
        self.connect()

    def connect(self):
        try:
            self.client = pymongo.MongoClient(self.connection_string)
            self.client.admin.command('ismaster')
            print("Successfully connected to MongoDB.")
        except pymongo.errors.ConnectionFailure as e:
            print(f"FATAL: Could not connect to MongoDB: {e}")
            self.client = None
        except ValueError as e:
             print(f"FATAL: Configuration error: {e}")
             self.client = None
        except Exception as e:
            print(f"An unexpected error occurred during connection: {e}")
            self.client = None

    def list_databases(self):
        if self.client:
            try:
                print(f"Available databases: {self.client.list_database_names()}")
            except Exception as e:
                print(f"Could not list databases (permissions?): {e}")

    def close_connection(self):
        if self.client:
            self.client.close()
            print("MongoDB connection closed.")

    def get_inventory_shard(self, shard_id):
        """
        Connects to a specific inventory shard database (DB1, DB2, DB3).
        shard_id should be 0, 1, or 2.
        """
        if self.client:
            db_name = f"DB{shard_id + 1}"
            return self.client[db_name]
        return None

    def get_sales_db(self):
        """
        Connects to the CENTRAL sales database (for analytics).
        """
        if self.client:
            return self.client["ShopSales"]
        return None

    # --- REMOVED get_member_db() ---
    # The member data is now on the inventory shards

# Create a single instance, check connection after creation
db_connection = DBConnection()
if db_connection.client:
    db_connection.list_databases() # List DBs now
else:
    print("Exiting application due to database connection failure.")
    exit()