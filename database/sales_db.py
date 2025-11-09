from .db_connector import db_connection
from datetime import datetime, timezone
from bson.objectid import ObjectId
import pymongo
import re

# Get database handles for CENTRAL DBs
db_sales = db_connection.get_sales_db() # For analytics
if db_sales is None: raise ConnectionError("Fatal: Could not connect to ShopSales database")
db_member = db_connection.get_member_db()
if db_member is None: 
    print("Warning: Could not connect to Shopmember database. Member features disabled.")

# --- PERMANENT FRAGMENTATION ---
sold_items_coll = db_sales["Sold_Items"]
# 'transactions_coll' is now dynamic and lives on the inventory shards


def record_sale(member_id, items_sold, discount_applied=0):
    """
    Processes a sale as an ATOMIC TRANSACTION.
    1. Writes receipt to the correct DISTRIBUTED 'DBX' (inventory shard).
    2. Updates stock on the correct DISTRIBUTED 'DBX' inventory shard.
    3. Updates the CENTRAL 'Sold_Items' analytics collection.
    """

    client = db_connection.client
    if client is None:
        raise ConnectionError("Fatal: MongoDB client not available")

    if not items_sold:
        raise ValueError("Cannot record a sale with no items.")
        
    primary_shard_id = items_sold[0]["shard_id"]
    
    db_inventory_shard_for_sale = db_connection.get_inventory_shard(primary_shard_id)
    if db_inventory_shard_for_sale is None:
        raise ConnectionError(f"Could not connect to Sales Shard DB{primary_shard_id + 1}")
        
    transactions_coll = db_inventory_shard_for_sale["transactions"]

    with client.start_session() as session:
        with session.start_transaction():
            try:
                subtotal = 0
                permanent_item_docs = [] 

                for item in items_sold:
                    quantity_sold = item["quantity"]
                    product_id_obj = ObjectId(item["product_id"])
                    inventory_shard_id = item["shard_id"] 

                    db_inventory_shard_for_item = db_connection.get_inventory_shard(inventory_shard_id)
                    if db_inventory_shard_for_item is None:
                        raise ConnectionError(f"Could not connect to Inventory Shard DB{inventory_shard_id + 1}")
                    shard_products_coll = db_inventory_shard_for_item["products"]
                    shard_stock_coll = db_inventory_shard_for_item["stock"]

                    product = shard_products_coll.find_one(
                        {"_id": product_id_obj},
                        session=session
                    )
                    if not product:
                        raise ValueError(f"Product ID {product_id_obj} not found on Shard DB{inventory_shard_id + 1}.")

                    price = product["price"]
                    category = product.get("category", "Uncategorized").strip() or "Uncategorized"
                    subtotal += price * quantity_sold

                    stock_update_result = shard_stock_coll.update_one(
                        {"product_id": product_id_obj, "quantity": {"$gte": quantity_sold}},
                        {"$inc": {"quantity": -quantity_sold}, "$set": {"last_updated": datetime.now(timezone.utc)}},
                        session=session
                    )
                    if stock_update_result.matched_count == 0:
                        raise ValueError(f"Out of stock for product: {product['name']} on Shard DB{inventory_shard_id + 1}.")

                    # --- UPDATE CENTRAL 'Sold_Items' AGGREGATED FRAGMENT ---
                    update_result = sold_items_coll.update_one(
                        {"category": category, "products_sold.name": product["name"]},
                        {"$inc": { "Total_Sold_in_Category": quantity_sold, "products_sold.$.quantity_sold": quantity_sold }},
                        session=session
                    )
                    if update_result.matched_count == 0:
                        sold_items_coll.update_one(
                            {"category": category},
                            {
                                "$inc": { "Total_Sold_in_Category": quantity_sold },
                                "$push": { "products_sold": {"name": product["name"], "quantity_sold": quantity_sold}}
                            },
                            upsert=True, session=session
                        )

                    item_doc = {
                        "product_id": product_id_obj,
                        "inventory_shard_id": inventory_shard_id,
                        "name": product["name"],
                        "category": category,
                        "price_at_sale": price,
                        "quantity_sold": quantity_sold
                    }
                    permanent_item_docs.append(item_doc)

                # --- Step 6: Calculate final total ---
                final_total = subtotal - discount_applied

                # --- Step 7: Build the final transaction document ---
                transaction_doc = {
                    "_id": ObjectId(),
                    "timestamp": datetime.now(timezone.utc),
                    "subtotal": subtotal,
                    "discount_applied": discount_applied,
                    "total_amount": final_total,
                    "member_id": member_id,
                    "payment_method": "cash",
                    "items": permanent_item_docs
                }

                # --- Step 8: Insert the transaction into the correct INVENTORY SHARD ---
                trans_result = transactions_coll.insert_one(transaction_doc, session=session)

                # --- Step 9: Update member loyalty in CENTRAL 'Shopmember' DB ---
                if member_id and db_member is not None:
                    
                    # --- THIS IS THE FIX ---
                    # It was 'final_al' before, now it's 'final_total'
                    points_earned = int(final_total) 
                    # --- END FIX ---
                    
                    db_member["loyalty"].update_one(
                        {"member_id": member_id},
                        {"$inc": {"points": points_earned}},
                        session=session
                    )

                return str(trans_result.inserted_id)

            except Exception as e:
                print(f"Transaction aborted: {e}")
                return None