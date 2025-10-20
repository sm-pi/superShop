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
    global _temp_index_created
    if _temp_index_created: return
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

# --- THIS FUNCTION IS MODIFIED ---
def create_product_fragment(filters={}):
    """
    This is the new fragmentation function.
    It uses an aggregation pipeline to efficiently join products,
    stock, and suppliers, and saves the result to 'FragementedData'.
    
    IT NOW FIXES THE SYNTAX ERROR.
    """
    _ensure_temp_fragment_ttl()

    # 1. Build the initial filter (for non-price fields)
    match_query = {}
    if filters.get("name"):
        match_query["name"] = {"$regex": filters["name"], "$options": "i"}
    if filters.get("category"):
        match_query["category"] = {"$regex": f"^{filters['category']}$", "$options": "i"}

    # 2. Build the price filter (for numeric matching)
    price_match_query = {}
    if filters.get("min_price"):
        price_match_query["$gte"] = filters["min_price"]
    if filters.get("max_price"):
        price_match_query["$lte"] = filters["max_price"]

    # 3. Build the aggregation pipeline
    pipeline = [
        # Stage 1: Match on name and category first
        {"$match": match_query},

        # --- THIS IS THE FIX ---
        # Stage 2: Convert price field to a number, just in case
        # The keys "if", "then", and "else" MUST be strings.
        {
            "$addFields": {
                "numericPrice": {
                    "$cond": {
                        "if": {"$isNumber": "$price"},
                        "then": "$price",
                        "else": {
                             "$convert": {
                                 "input": "$price",
                                 "to": "double",
                                 "onError": 0, # Default to 0 if conversion fails
                                 "onNull": 0   # Default to 0 if null
                             }
                        }
                    }
                }
            }
        },
        # --- END OF FIX ---
    ]

    # Stage 3: Conditionally add the price match stage
    # Now it filters on the new 'numericPrice' field
    if price_match_query:
        pipeline.append({"$match": {"numericPrice": price_match_query}})

    # Stage 4: Continue with the rest of the original pipeline
    pipeline.extend([
        {
            "$lookup": {
                "from": "stock",
                "localField": "_id",
                "foreignField": "product_id",
                "as": "stock_data"
            }
        },
        {"$unwind": "$stock_data"},
        {"$match": {"stock_data.quantity": {"$gt": 0}}},
        {
            "$lookup": {
                "from": "suppliers",
                "localField": "supplier_id",
                "foreignField": "_id",
                "as": "supplier_data"
            }
        },
        {"$unwind": {"path": "$supplier_data", "preserveNullAndEmptyArrays": True}},
        {
            "$project": {
                "_id": 1,
                "name": 1,
                "price": "$numericPrice", # Use the converted price
                "category": 1,
                "quantity_in_stock": "$stock_data.quantity",
                "supplier_name": {"$ifNull": ["$supplier_data.name", "N/A"]},
                "createdAt": datetime.now(timezone.utc)
            }
        }
    ])

    # 4. Run the pipeline
    try:
        fragment_docs = list(products_coll.aggregate(pipeline))
    except Exception as e:
        print(f"Error in aggregation pipeline: {e}")
        return []

    # 5. Clear and insert into the temporary fragment
    print("Clearing old data from 'FragementedData'...")
    temp_fragment_coll.delete_many({})
    if fragment_docs:
        print(f"Inserting {len(fragment_docs)} new docs into 'FragementedData'...")
        temp_fragment_coll.insert_many(fragment_docs)

    return fragment_docs
# --- END MODIFICATION ---

def get_product_by_name_and_supplier(name, supplier_id):
    """Finds a product by its name and supplier ID."""
    return products_coll.find_one({
        "name": {"$regex": f"^{name}$", "$options": "i"},
        "supplier_id": supplier_id
    })

def add_product(name, price, category, supplier_name, initial_stock):

    # 1. Find or create supplier
    supplier = suppliers_coll.find_one_and_update(
        {"name": {"$regex": f"^{supplier_name}$", "$options": "i"}},
        {"$setOnInsert": {"name": supplier_name, "contact_email": "default@supplier.com"}},
        upsert=True,
        return_document=pymongo.ReturnDocument.AFTER
    )
    supplier_id = supplier["_id"]

    # 2. Check if product from this supplier already exists
    existing_product = get_product_by_name_and_supplier(name, supplier_id)
    if existing_product:
        print(f"Error: Product '{name}' from '{supplier_name}' already exists.")
        return None # Return None to signal failure

    # 3. Add to 'products' collection
    clean_category = "Uncategorized"
    if category and not category.isspace():
        clean_category = category.strip()

    product_doc = {
        "name": name,
        "price": price, # Price is saved correctly as a float from the GUI
        "category": clean_category,
        "supplier_id": supplier_id, # Link to supplier
        "created_at": datetime.utcnow()
    }
    result = products_coll.insert_one(product_doc)
    product_id = result.inserted_id

    # 4. Add to 'stock' collection
    stock_doc = {
        "product_id": product_id, "product_name": name,
        "quantity": initial_stock, "location": "main_warehouse",
        "last_updated": datetime.utcnow()
    }
    stock_coll.insert_one(stock_doc)
    print(f"Added product '{name}' (ID: {product_id}) with stock {initial_stock}")
    return str(product_id)

def add_stock_to_product(product_name, supplier_name, amount_to_add):
    """
    Finds a product by name AND supplier, then adds to its stock.
    """
    # 1. Find the supplier
    supplier = suppliers_coll.find_one({"name": {"$regex": f"^{supplier_name}$", "$options": "i"}})
    if not supplier:
        print(f"Error: Supplier '{supplier_name}' not found.")
        return None

    # 2. Find the product by name AND supplier_id
    product = get_product_by_name_and_supplier(product_name, supplier["_id"])
    if not product:
        print(f"Error: Product '{product_name}' from '{supplier_name}' not found.")
        return None

    # 3. Update the stock in the 'stock' collection
    update_result = stock_coll.find_one_and_update(
        {"product_id": product["_id"]},
        {
            "$inc": {"quantity": amount_to_add},
            "$set": {"last_updated": datetime.utcnow()}
        },
        return_document=pymongo.ReturnDocument.AFTER
    )

    return update_result