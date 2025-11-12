from .db_connector import db_connection
from datetime import datetime, timezone
from bson.objectid import ObjectId
import pymongo
import re

# Get database handles for CENTRAL DBs
db_sales = db_connection.get_sales_db() # For analytics
if db_sales is None: raise ConnectionError("Fatal: Could not connect to ShopSales database")

# --- NO LONGER NEED CENTRAL db_member ---

# --- PERMANENT FRAGMENTATION ---
sold_items_coll = db_sales["Sold_Items"] # Central analytics
# 'transactions_coll' is dynamic and lives on the inventory shards

# --- MODIFIED: Accepts member_info dict ---
def record_sale(member_info, items_sold, discount_applied=0):
    """
    Processes a sale as an ATOMIC TRANSACTION.
    1. Writes receipt to the correct price-based transaction SHARD (DB1 or DB2).
    2. Updates stock on the correct inventory SHARD (DB1, DB2, or DB3).
    3. Updates the CENTRAL 'Sold_Items' analytics collection.
    4. Updates points on the correct member SHARD (DB1, DB2, or DB3).
    """

    client = db_connection.client
    if client is None:
        raise ConnectionError("Fatal: MongoDB client not available")

    if not items_sold:
        raise ValueError("Cannot record a sale with no items.")
        
    with client.start_session() as session:
        with session.start_transaction():
            try:
                subtotal = 0
                permanent_item_docs = [] 

                for item in items_sold:
                    quantity_sold = item["quantity"]
                    product_id_obj = ObjectId(item["product_id"])
                    inventory_shard_id = item["shard_id"] 

                    # 1. Connect to the correct INVENTORY shard DB for *this item*
                    db_inventory_shard_for_item = db_connection.get_inventory_shard(inventory_shard_id)
                    if db_inventory_shard_for_item is None:
                        raise ConnectionError(f"Could not connect to Inventory Shard DB{inventory_shard_id + 1}")
                    shard_products_coll = db_inventory_shard_for_item["products"]
                    shard_stock_coll = db_inventory_shard_for_item["stock"]

                    # 2. Find the product *on its specific inventory shard*
                    product = shard_products_coll.find_one({"_id": product_id_obj}, session=session)
                    if not product:
                        raise ValueError(f"Product ID {product_id_obj} not found on Shard DB{inventory_shard_id + 1}.")

                    price = product["price"]
                    category = product.get("category", "UncategorIZED").strip() or "Uncategorized"
                    subtotal += price * quantity_sold

                    # 3. Update stock *on its specific inventory shard*
                    stock_update_result = shard_stock_coll.update_one(
                        {"product_id": product_id_obj, "quantity": {"$gte": quantity_sold}},
                        {"$inc": {"quantity": -quantity_sold}, "$set": {"last_updated": datetime.now(timezone.utc)}},
                        session=session
                    )
                    if stock_update_result.matched_count == 0:
                        raise ValueError(f"Out of stock for product: {product['name']} on Shard DB{inventory_shard_id + 1}.")

                    # 4. --- UPDATE CENTRAL 'Sold_Items' AGGREGATED FRAGMENT ---
                    update_result = sold_items_coll.update_one(
                        {"category": category, "products_sold.name": product["name"]},
                        {"$inc": { "Total_Sold_in_Category": quantity_sold, "products_sold.$.quantity_sold": quantity_sold }},
                        session=session
                    )
                    if update_result.matched_count == 0:
                        sold_items_coll.update_one(
                            {"category": category},
                            {"$inc": { "Total_Sold_in_Category": quantity_sold },
                             "$push": { "products_sold": {"name": product["name"], "quantity_sold": quantity_sold}}},
                            upsert=True, session=session
                        )

                    item_doc = {
                        "product_id": product_id_obj, "inventory_shard_id": inventory_shard_id,
                        "name": product["name"], "category": category,
                        "price_at_sale": price, "quantity_sold": quantity_sold
                    }
                    permanent_item_docs.append(item_doc)

                # --- Step 5: Calculate final total ---
                final_total = subtotal - discount_applied
                
                # --- Step 6: Determine member ID (if any) ---
                member_id = None
                if member_info:
                    # Use the _id from the member doc
                    member_id = ObjectId(member_info['doc']['_id']) 

                # --- Step 7: Build the final transaction document ---
                transaction_doc = {
                    "_id": ObjectId(), "timestamp": datetime.now(timezone.utc),
                    "subtotal": subtotal, "discount_applied": discount_applied,
                    "total_amount": final_total, "member_id": member_id, # Store the ObjectId or None
                    "payment_method": "cash", "items": permanent_item_docs
                }

                # --- Step 8: TRANSACTION SHARDING LOGIC (Price-based) ---
                if final_total <= 1000:
                    transaction_shard_id = 0 # Corresponds to DB1
                else:
                    transaction_shard_id = 1 # Corresponds to DB2
                
                print(f"Saving transaction to Shard DB{transaction_shard_id + 1} (Total: {final_total})")
                db_transaction_shard = db_connection.get_inventory_shard(transaction_shard_id)
                if db_transaction_shard is None:
                    raise ConnectionError(f"Could not connect to Transaction Shard DB{transaction_shard_id + 1}")
                
                transactions_coll = db_transaction_shard["transactions"]
                trans_result = transactions_coll.insert_one(transaction_doc, session=session)

                # --- Step 9: UPDATE MEMBER LOYALTY ON THE CORRECT SHARD ---
                if member_info: # Check if a member was part of the sale
                    member_shard_id = member_info['shard_id']
                    db_member_shard = db_connection.get_inventory_shard(member_shard_id)
                    if db_member_shard is None:
                        raise ConnectionError(f"Could not connect to Member Shard DB{member_shard_id + 1} for loyalty update.")
                    
                    member_coll = db_member_shard["members"]
                    points_earned = int(final_total)
                    
                    print(f"Updating points for member {member_id} on Shard DB{member_shard_id + 1}")
                    member_coll.update_one(
                        {"_id": member_id}, # Use the ObjectId
                        {"$inc": {"points": points_earned}},
                        session=session
                    )
                # --- END OF LOYALTY UPDATE ---

                return str(trans_result.inserted_id)

            except Exception as e:
                print(f"Transaction aborted: {e}")
                return None