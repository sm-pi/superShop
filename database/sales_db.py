from .db_connector import db_connection
# No longer need to import inventory_db directly for collections
from datetime import datetime, timezone
from bson.objectid import ObjectId
import pymongo
import re

# Get database handles for central DBs
db_sales = db_connection.get_sales_db()
if db_sales is None: raise ConnectionError("Fatal: Could not connect to ShopSales database")
db_member = db_connection.get_member_db()
if db_member is None: raise ConnectionError("Fatal: Could not connect to Shopmember database")


# --- PERMANENT FRAGMENTATION ---
transactions_coll = db_sales["transactions"]
sold_items_coll = db_sales["Sold_Items"] # Central aggregated collection


def record_sale(member_id, items_sold, discount_applied=0):
    """
    Processes a sale as an ATOMIC TRANSACTION.
    1. Writes receipt to central 'transactions'.
    2. Updates stock on the correct inventory SHARD DB.
    3. Updates central aggregated 'Sold_Items'.
    """

    client = db_connection.client
    if not client: raise ConnectionError("Fatal: MongoDB client not available")

    with client.start_session() as session:
        with session.start_transaction():
            try:
                subtotal = 0
                permanent_item_docs = [] # For the central 'transactions' receipt

                for item in items_sold:
                    quantity_sold = item["quantity"]
                    product_id_obj = ObjectId(item["product_id"])
                    shard_id = item["shard_id"] # Get shard_id from cart item

                    # 1. Connect to the correct inventory shard DB
                    db_shard = db_connection.get_inventory_shard(shard_id)
                    if not db_shard:
                        raise ConnectionError(f"Could not connect to Shard DB{shard_id + 1} during transaction.")
                    shard_products_coll = db_shard["products"]
                    shard_stock_coll = db_shard["stock"]

                    # 2. Find the product *on that specific shard*
                    product = shard_products_coll.find_one(
                        {"_id": product_id_obj},
                        session=session
                    )
                    if not product:
                        raise ValueError(f"Product ID {product_id_obj} not found on Shard DB{shard_id + 1}.")

                    price = product["price"]
                    # Ensure category exists, default to Uncategorized
                    category = product.get("category")
                    if not category or category.isspace():
                        category = "Uncategorized"
                    else:
                        category = category.strip()

                    subtotal += price * quantity_sold

                    # 3. Update stock *on that specific shard's stock collection*
                    stock_update_result = shard_stock_coll.update_one(
                        {"product_id": product_id_obj, "quantity": {"$gte": quantity_sold}},
                        {"$inc": {"quantity": -quantity_sold}, "$set": {"last_updated": datetime.now(timezone.utc)}},
                        session=session
                    )
                    if stock_update_result.matched_count == 0:
                        # This should be rare now with scatter-gather, but keep as safety
                        raise ValueError(f"Out of stock for product: {product['name']} on Shard DB{shard_id + 1}.")

                    # 4. --- UPDATE CENTRAL 'Sold_Items' AGGREGATED FRAGMENT ---
                    # Uses the cleaned category name
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

                    # 5. Prepare the item doc for the permanent 'transactions' receipt
                    item_doc = {
                        "product_id": product_id_obj,
                        "shard_id": shard_id, # Store shard_id for auditing
                        "name": product["name"],
                        "category": category,
                        "price_at_sale": price,
                        "quantity_sold": quantity_sold
                    }
                    permanent_item_docs.append(item_doc)

                # --- Step 6: Calculate final total ---
                final_total = subtotal - discount_applied

                # --- Step 7: Build the final, single transaction document ---
                transaction_doc = {
                    "_id": ObjectId(), # Generate ID manually for consistency
                    "timestamp": datetime.now(timezone.utc),
                    "subtotal": subtotal,
                    "discount_applied": discount_applied,
                    "total_amount": final_total,
                    "member_id": member_id,
                    "payment_method": "cash",
                    "items": permanent_item_docs
                }

                # --- Step 8: Insert the permanent transaction into central ShopSales ---
                trans_result = transactions_coll.insert_one(transaction_doc, session=session)

                # --- Step 9: Update permanent member loyalty in central Shopmember ---
                if member_id and db_member: # Check if db_member connection is valid
                    points_earned = int(final_total)
                    db_member["loyalty"].update_one(
                        {"member_id": member_id},
                        {"$inc": {"points": points_earned}},
                        session=session
                    )

                return str(trans_result.inserted_id)

            except Exception as e:
                print(f"Transaction aborted: {e}")
                # Rollback happens automatically when exiting 'with' block on error
                return None # Signal failure to GUI