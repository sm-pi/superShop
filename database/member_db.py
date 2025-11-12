from .db_connector import db_connection
from bson.objectid import ObjectId
import pymongo
import re
from datetime import datetime


NUM_INVENTORY_SHARDS = 3

def _get_shard_id_for_email(email):
    """
    This is the HASHING FUNCTION for members.
    It returns the shard ID (0, 1, or 2) based on email.
    """
    if not email:
        return 2 # Default to DB3
        
    email = email.lower()
    if email.endswith("@gmail.com"):
        return 0 # Shard 0 -> DB1
    elif email.endswith("@yahoo.com"):
        return 1 # Shard 1 -> DB2
    else:
        return 2 # Shard 2 -> DB3

def _get_member_collection_for_shard(shard_id):
    """Helper to get the 'members' collection from DB1, DB2, or DB3."""
    db_shard = db_connection.get_inventory_shard(shard_id)
    if db_shard is None:
        raise ConnectionError(f"Fatal: Could not connect to inventory shard DB{shard_id + 1}")
    # The 'members' collection lives inside the inventory shard DB
    return db_shard["members"]
# --- END SHARDING ---


def add_member(name, phone, email):
    """
    Adds a new member to the correct shard based on their email.
    Merges 'loyalty' data into the member document.
    """
    # 1. Find the correct shard for this email
    shard_id = _get_shard_id_for_email(email)
    members_coll = _get_member_collection_for_shard(shard_id)
    print(f"Adding member to Shard DB{shard_id + 1} (Email: {email})")

    # 2. Check if phone or email already exists *on this shard*
    existing = members_coll.find_one({"$or": [{"phone": phone}, {"email": email}]})
    if existing:
        print(f"Member with phone {phone} or email {email} already exists on shard {shard_id}")
        return None # Signal that member already exists

    # 3. Create the new member document (merging loyalty)
    member_doc = {
        "name": name,
        "phone": phone,
        "email": email,
        "points": 0, # <-- Loyalty is now part of the member doc
        "created_at": datetime.utcnow()
    }
    
    try:
        result = members_coll.insert_one(member_doc)
        return str(result.inserted_id)
    except Exception as e:
        print(f"Error inserting member: {e}")
        return None


def find_member_by_phone(phone):
    """
    SCATTER-GATHER query.
    Checks all 3 shards for a member by phone number.
    Returns the member document AND the shard_id it was found on.
    """
    print(f"Searching for member with phone: {phone}")
    for shard_id in range(NUM_INVENTORY_SHARDS):
        try:
            members_coll = _get_member_collection_for_shard(shard_id)
            member_doc = members_coll.find_one({"phone": phone})
            
            if member_doc:
                print(f"Found member on Shard DB{shard_id + 1}")
                # Convert ObjectId to string for easier use in GUI
                member_doc["_id"] = str(member_doc["_id"])
                return {
                    "doc": member_doc,
                    "shard_id": shard_id # Return the doc AND the shard ID
                }
        except Exception as e:
            print(f"Error searching shard DB{shard_id + 1} for member: {e}")
            
    print("Member not found on any shard.")
    return None # Not found on any shard