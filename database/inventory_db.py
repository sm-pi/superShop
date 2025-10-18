from .db_connector import db_connection
from bson.objectid import ObjectId
import pymongo
from datetime import datetime, timezone
import re

# Get the database handles
db_inventory = db_connection.get_inventory_db()
db_sales = db_connection.get_sales_db() # Get sales DB for fragments

# Define the "fragmented" collections
products_coll = db_inventory["products"]
stock_coll = db_inventory["stock"]
suppliers_coll = db_inventory["suppliers"]

# --- DEFINE THE SINGLE TEMPORARY FRAGMENT COLLECTION ---
temp_fragment_coll = db_sales["FragementedData"]
_temp_index_created = False # Cache

def _ensure_temp_fragment_ttl():
    """
    Ensures the 5-minute TTL index exists on the 'FragementedData' collection.
    """
    global _temp_index_created
    if _temp_index_created:
        return # Don't run this more than once
    try:
        SECONDS_TO_LIVE = 300 # 5 minutes
        temp_fragment_coll.create_index(
            [("createdAt", pymongo.ASCENDING)], 
            expireAfterSeconds=SECONDS_TO_LIVE
        )
        print(f"Ensured TTL index on 'FragementedData'. Data expires in 300s.")
        _temp_index_created = True
    except Exception as e:
        print(f"Error creating TTL index: {e}")

def create_product_fragment(filters={}):
    """
    This is the fragmentation function.
    It queries products and saves the results to the single
    'FragementedData' collection in the ShopSales database.
    """
    
    # 1. Ensure the TTL index exists
    _ensure_temp_fragment_ttl()

    # 2. Build the filter for the 'products' collection
    match_query = {}
    if filters.get("name"):
        match_query["name"] = {"$regex": filters["name"], "$options": "i"}
    if filters.get("category"):
        match_query["category"] = {"$regex": f"^{filters['category']}$", "$options": "i"}
    
    price_query = {}
    if filters.get("min_price"):
        price_query["$gte"] = filters["min_price"]
    if filters.get("max_price"):
        price_query["$lte"] = filters["max_price"]
    if price_query:
        match_query["price"] = price_query

    # 3. Find all products that match the filter
    try:
        products = list(products_coll.find(match_query))
    except Exception as e:
        print(f"Error querying products: {e}")
        return []

    # 4. Loop through products, find their stock, and build the final list
    fragment_docs = []
    for product in products:
        # Find the stock for this product
        stock_item = stock_coll.find_one({"product_id": product["_id"]})
        
        # Only add if stock is found and quantity is greater than 0
        if stock_item and stock_item.get("quantity", 0) > 0:
            fragment_doc = {
                "_id": product["_id"], # Copy the ORIGINAL product ID
                "name": product["name"],
                "price": product["price"],
                "category": product["category"],
                "quantity_in_stock": stock_item["quantity"],
                "createdAt": datetime.now(timezone.utc) # <-- Add TTL field
            }
            fragment_docs.append(fragment_doc)
        
    # 5. Drop (clear) all old data from the temporary fragment
    print("Clearing old data from 'FragementedData'...")
    temp_fragment_coll.delete_many({})
    
    # 6. Insert the new results
    if fragment_docs:
        print(f"Inserting {len(fragment_docs)} new docs into 'FragementedData'...")
        temp_fragment_coll.insert_many(fragment_docs)
    
    # 7. Return the product list to the GUI
    return fragment_docs

def get_product_by_name(name):
    """Finds a product by its name (case-insensitive)."""
    return products_coll.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}})

# --- This function is still needed by inventory_frame.py ---
def add_product(name, price, category, supplier_name, initial_stock):
    """
    Adds a new product to the permanent inventory collections.
    This implements Vertical Fragmentation (products + stock).
    """
    supplier = suppliers_coll.find_one_and_update(
        {"name": supplier_name},
        {"$setOnInsert": {"name": supplier_name, "contact_email": "default@supplier.com"}},
        upsert=True,
        return_document=pymongo.ReturnDocument.AFTER
    )
    
    # --- FIX: Clean the category name before saving ---
    clean_category = "Uncategorized" # Default
    if category and not category.isspace():
        clean_category = category.strip()
    # --- END OF FIX ---

    # 1. Add to 'products' collection
    product_doc = {
        "name": name, 
        "price": price, 
        "category": clean_category, # <-- Use the clean name
        "supplier_id": supplier["_id"], 
        "created_at": datetime.utcnow()
    }
    result = products_coll.insert_one(product_doc)
    product_id = result.inserted_id
    
    # 2. Add to 'stock' collection
    stock_doc = {
        "product_id": product_id, "product_name": name, 
        "quantity": initial_stock, "location": "main_warehouse",
        "last_updated": datetime.utcnow()
    }
    stock_coll.insert_one(stock_doc)
    
    print(f"Added product '{name}' (ID: {product_id}) with stock {initial_stock}")
    return str(product_id)