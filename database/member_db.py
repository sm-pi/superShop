from .db_connector import db_connection
from bson.objectid import ObjectId
from datetime import datetime

# Get the database handle
db = db_connection.get_member_db()

# --- Define the "fragmented" collections ---
members_coll = db["members"]
loyalty_coll = db["loyalty"]

def add_member(name, email, phone):
    """
    Adds a new member and initializes their loyalty account.
    """
    # 1. Create the member document
    member_doc = {
        "name": name,
        "email": email,
        "phone": phone,
        "join_date": datetime.utcnow()
    }
    result = members_coll.insert_one(member_doc)
    member_id = result.inserted_id
    
    # 2. Create the associated loyalty document
    loyalty_doc = {
        "member_id": member_id,
        "email": email, # Denormalized for easy lookup
        "points": 0,
        "tier": "bronze"
    }
    loyalty_coll.insert_one(loyalty_doc)
    
    print(f"Added member '{name}' with ID: {member_id}")
    return str(member_id)

def find_member_by_email(email):
    """Finds a member by their email (case-insensitive)."""
    return members_coll.find_one({"email": {"$regex": f"^{email}$", "$options": "i"}})

# --- THIS IS THE NEWLY ADDED FUNCTION ---
def find_member_by_phone(phone):
    """Finds a member by their phone number."""
    return members_coll.find_one({"phone": phone})
# --- END OF NEW FUNCTION ---