from .db_connector import db_connection
from bson.objectid import ObjectId
import pymongo
from datetime import datetime, timezone
import re

# --- SHARDING CONFIGURATION ---
NUM_INVENTORY_SHARDS = 3 # DB1, DB2, DB3

# Simple map for hashing - ensuring consistency
CATEGORY_HASH = {
    "Electronics": 1,
    "Self Care": 2,
    "Dairy Products": 3,
    "Bakery": 4,
    "Other": 5,
    "Uncategorized": 0 # Assign 0 explicitly
}

def _get_shard_id_for_category(category):
    """ Hash function: Category name -> Shard ID (0, 1, or 2) """
    # Default to "Uncategorized" hash value if not found or invalid
    hash_value = CATEGORY_HASH.get(category, 0) if category else 0
    shard_id = hash_value % NUM_INVENTORY_SHARDS
    return shard_id

def _get_collections_for_shard(shard_id):
    """ Helper to get collections for a specific inventory shard DB """
    db_shard = db_connection.get_inventory_shard(shard_id)
    if db_shard is None:
        raise ConnectionError(f"Fatal: Could not connect to inventory shard DB{shard_id + 1}")
    # Use consistent collection names within each shard DB
    return db_shard["products"], db_shard["stock"], db_shard["suppliers"]

# --- END SHARDING ---


# Get sales DB (this is not sharded)
db_sales = db_connection.get_sales_db()
if db_sales is None:
     raise ConnectionError("Fatal: Could not connect to ShopSales database")
temp_fragment_coll = db_sales["FragementedData"]
_temp_index_created = False

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
    except pymongo.errors.OperationFailure as e:
         # Ignore index already exists error, raise others
        if "Index already exists" not in str(e):
            print(f"Warning: Could not create TTL index (permissions?): {e}")
    except Exception as e:
        print(f"Error creating TTL index: {e}")


def create_product_fragment(filters={}):
    """
    Scatter-gather fragmentation: Queries ALL shards based on filters,
    merges results, and saves to the temporary 'FragementedData'.
    Includes Brand filter.
    """
    _ensure_temp_fragment_ttl()

    all_fragment_docs = []

    # --- SCATTER PHASE ---
    for shard_id in range(NUM_INVENTORY_SHARDS):
        print(f"Querying Shard DB{shard_id + 1}...")
        try:
            products_coll, stock_coll, suppliers_coll = _get_collections_for_shard(shard_id)

            # 1. Build initial product filter (non-price, non-brand)
            match_query = {}
            if filters.get("name"):
                match_query["name"] = {"$regex": filters["name"], "$options": "i"}
            # Apply category filter ONLY if the category belongs to this shard
            category_filter = filters.get("category")
            if category_filter and _get_shard_id_for_category(category_filter) == shard_id:
                 match_query["category"] = {"$regex": f"^{category_filter}$", "$options": "i"}
            elif category_filter:
                 print(f"Skipping category '{category_filter}' on shard {shard_id}")
                 continue # Skip this shard if category doesn't match

            # Build price filter separately
            price_match_query = {}
            if filters.get("min_price"): price_match_query["$gte"] = filters["min_price"]
            if filters.get("max_price"): price_match_query["$lte"] = filters["max_price"]

            # 2. Build the aggregation pipeline for this shard
            pipeline = [
                {"$match": match_query},
                {"$addFields": {"numericPrice": {"$cond": { "if": {"$isNumber": "$price"}, "then": "$price", "else": {"$convert": {"input": "$price", "to": "double", "onError": 0, "onNull": 0}}}}}},
            ]
            if price_match_query:
                pipeline.append({"$match": {"numericPrice": price_match_query}})

            pipeline.extend([
                # Join with suppliers FIRST to filter by brand early
                {"$lookup": {"from": "suppliers", "localField": "supplier_id", "foreignField": "_id", "as": "supplier_data"}},
                {"$unwind": {"path": "$supplier_data", "preserveNullAndEmptyArrays": True}},
            ])

            # --- NEW: Add Brand filter ---
            brand_filter = filters.get("brand")
            if brand_filter:
                pipeline.append({"$match": {"supplier_data.name": {"$regex": brand_filter, "$options": "i"}}})
            # --- END NEW ---

            pipeline.extend([
                 # Join with stock
                {"$lookup": {"from": "stock", "localField": "_id", "foreignField": "product_id", "as": "stock_data"}},
                {"$unwind": "$stock_data"},
                {"$match": {"stock_data.quantity": {"$gt": 0}}},
                 # Project the final shape
                {"$addFields": {"shard_id_field": shard_id}},
                {"$project": {
                    "_id": 1, "name": 1, "price": "$numericPrice", "category": 1,
                    "quantity_in_stock": "$stock_data.quantity",
                    "supplier_name": {"$ifNull": ["$supplier_data.name", "N/A"]},
                    "shard_id": "$shard_id_field",
                    "createdAt": datetime.now(timezone.utc)
                }}
            ])

            # 3. Run the pipeline for this shard
            shard_docs = list(products_coll.aggregate(pipeline))
            all_fragment_docs.extend(shard_docs)
            print(f"Found {len(shard_docs)} products on Shard DB{shard_id + 1}")

        except Exception as e:
            print(f"Error querying Shard DB{shard_id + 1}: {e}")

    # --- GATHER PHASE ---
    # 4. Clear and insert the merged results into the temporary fragment
    print("Clearing old data from 'FragementedData'...")
    try:
        temp_fragment_coll.delete_many({})
        if all_fragment_docs:
            print(f"Inserting {len(all_fragment_docs)} total new docs into 'FragementedData'...")
            temp_fragment_coll.insert_many(all_fragment_docs)
    except Exception as e:
        print(f"Error writing to FragementedData: {e}")


    return all_fragment_docs

def _get_product_by_name_and_supplier(name, supplier_id, products_coll):
    """ Helper to find product on a specific shard's product collection """
    return products_coll.find_one({
        "name": {"$regex": f"^{name}$", "$options": "i"},
        "supplier_id": supplier_id
    })

def add_product(name, price, category, supplier_name, initial_stock):
    """ Adds product to the correct inventory shard based on category """

    # 1. Determine the correct shard
    shard_id = _get_shard_id_for_category(category)
    print(f"Adding product to Shard DB{shard_id + 1} (Category: {category})")
    try:
        products_coll, stock_coll, suppliers_coll = _get_collections_for_shard(shard_id)
    except ConnectionError as e:
        print(e)
        return None # Cannot proceed if shard connection failed

    # 2. Find or create supplier (on that shard)
    supplier = suppliers_coll.find_one_and_update(
        {"name": {"$regex": f"^{supplier_name}$", "$options": "i"}},
        {"$setOnInsert": {"name": supplier_name, "contact_email": "default@supplier.com"}},
        upsert=True,
        return_document=pymongo.ReturnDocument.AFTER
    )
    supplier_id = supplier["_id"]

    # 3. Check for duplicates (on that shard)
    existing_product = _get_product_by_name_and_supplier(name, supplier_id, products_coll)
    if existing_product:
        print(f"Error: Product '{name}' from '{supplier_name}' already exists on Shard DB{shard_id + 1}.")
        return None

    # 4. Add to 'products' collection (on that shard)
    product_doc = {
        "name": name, "price": price, "category": category, # Save original category
        "supplier_id": supplier_id, "created_at": datetime.utcnow()
    }
    result = products_coll.insert_one(product_doc)
    product_id = result.inserted_id

    # 5. Add to 'stock' collection (on that shard)
    stock_doc = {
        "product_id": product_id, "product_name": name,
        "quantity": initial_stock, "location": "main_warehouse",
        "last_updated": datetime.utcnow()
    }
    stock_coll.insert_one(stock_doc)
    print(f"Added product '{name}' (ID: {product_id}) to Shard DB{shard_id + 1} with stock {initial_stock}")
    return str(product_id)


def add_stock_to_product(product_name, supplier_name, category, amount_to_add):
    """ Adds stock to a product on its correct shard """

    # 1. Determine the correct shard based on category
    shard_id = _get_shard_id_for_category(category)
    print(f"Adding stock on Shard DB{shard_id + 1}...")
    try:
        products_coll, stock_coll, suppliers_coll = _get_collections_for_shard(shard_id)
    except ConnectionError as e:
        print(e)
        return None

    # 2. Find the supplier (on that shard)
    supplier = suppliers_coll.find_one({"name": {"$regex": f"^{supplier_name}$", "$options": "i"}})
    if not supplier:
        print(f"Error: Supplier '{supplier_name}' not found on Shard DB{shard_id + 1}.")
        return None

    # 3. Find the product (on that shard)
    product = _get_product_by_name_and_supplier(product_name, supplier["_id"], products_coll)
    if not product:
        print(f"Error: Product '{product_name}' from '{supplier_name}' not found on Shard DB{shard_id + 1}.")
        return None

    # 4. Update the stock (on that shard's stock collection)
    update_result = stock_coll.find_one_and_update(
        {"product_id": product["_id"]},
        {"$inc": {"quantity": amount_to_add}, "$set": {"last_updated": datetime.utcnow()}},
        return_document=pymongo.ReturnDocument.AFTER # Get the updated doc
    )

    return update_result # Return the updated stock document or None if update failed