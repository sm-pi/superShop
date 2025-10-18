from .db_connector import db_connection
from . import inventory_db 
from datetime import datetime, timezone
from bson.objectid import ObjectId
import pymongo 
import re # For sanitizing names

# Get database handles
db_sales = db_connection.get_sales_db()
db_inventory = db_connection.get_inventory_db()
db_member = db_connection.get_member_db()

# --- PERMANENT FRAGMENTATION ---
transactions_coll = db_sales["transactions"]

# --- THIS FUNCTION IS MODIFIED ---
def _get_aggregated_fragment(category_name):
    """
    Gets or creates an aggregated sales fragment for a category.
    Example: "Electronics" -> "SoldElectronics"
    """
    
    # --- FIX: Check for bad category names ---
    clean_category = "Uncategorized" # Default
    if category_name and not category_name.isspace():
        clean_category = category_name.strip()
    # --- END OF FIX ---
        
    sanitized_name = re.sub(r'[^a_zA_Z0-9]', '_', clean_category)
    collection_name = f"Sold{sanitized_name}"
    
    # Return both the collection handle AND the clean name
    return db_sales[collection_name], clean_category

# --- END OF FRAGMENTATION DEFINITIONS ---


def record_sale(member_id, items_sold, discount_applied=0):
    """
    Processes a sale as an ATOMIC TRANSACTION.
    """
    
    client = db_connection.client
    
    with client.start_session() as session:
        with session.start_transaction():
            try:
                subtotal = 0 
                permanent_item_docs = [] 
                
                for item in items_sold:
                    quantity_sold = item["quantity"]

                    # 1. Get product info
                    product = inventory_db.get_product_by_name(item["name_for_lookup"])
                    if not product:
                        raise Exception(f"Product '{item['name_for_lookup']}' not found in inventory.")
                    
                    product_id_obj = product["_id"]
                    price = product["price"]
                    category = product.get("category") # Get the (potentially bad) category
                    subtotal += price * quantity_sold
                    
                    # 2. Update stock
                    stock_update_result = inventory_db.stock_coll.update_one(
                        {"product_id": product_id_obj, "quantity": {"$gte": quantity_sold}},
                        {"$inc": {"quantity": -quantity_sold}},
                        session=session
                    )
                    if stock_update_result.matched_count == 0:
                        raise Exception(f"Out of stock for product: {product['name']}.")
                    
                    # 3. --- UPDATE AGGREGATED FRAGMENT (MODIFIED) ---
                    # The fixed function is called here
                    agg_coll, clean_category_name = _get_aggregated_fragment(category)
                    
                    # This updates the aggregated data for that category
                    agg_coll.find_one_and_update(
                        {"category_name": clean_category_name}, # Use the clean name
                        {
                            "$inc": {"Number_Product_Sold": quantity_sold},
                            "$addToSet": { 
                                "product_ids": product_id_obj,
                                "product_names": product["name"]
                            }
                        },
                        upsert=True, 
                        session=session
                    )
                    
                    # 4. Prepare the item doc for the permanent 'transactions' receipt
                    item_doc = {
                        "product_id": product_id_obj,
                        "name": product["name"],
                        "category": clean_category_name, # Save the clean name
                        "price_at_sale": price,
                        "quantity_sold": quantity_sold
                    }
                    permanent_item_docs.append(item_doc)

                # --- Step 5: Calculate final total ---
                final_total = subtotal - discount_applied

                # --- Step 6: Build the final, single transaction document ---
                transaction_doc = {
                    "timestamp": datetime.now(timezone.utc),
                    "subtotal": subtotal,
                    "discount_applied": discount_applied,
                    "total_amount": final_total,
                    "member_id": member_id,
                    "payment_method": "cash",
                    "items": permanent_item_docs 
                }
                
                # --- Step 7: Insert the permanent transaction ---
                trans_result = transactions_coll.insert_one(transaction_doc, session=session)
                
                # --- Step 8: Update permanent member loyalty ---
                if member_id:
                    points_earned = int(final_total) 
                    db_member["loyalty"].update_one(
                        {"member_id": member_id},
                        {"$inc": {"points": points_earned}},
                        session=session
                    )
                
                return str(trans_result.inserted_id)

            except Exception as e:
                print(f"Transaction aborted: {e}")
                return None