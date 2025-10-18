from .db_connector import db_connection
from . import inventory_db # <-- We need this import to access the collections
from datetime import datetime, timezone
from bson.objectid import ObjectId
import pymongo 

# Get database handles
db_sales = db_connection.get_sales_db()
db_inventory = db_connection.get_inventory_db()
db_member = db_connection.get_member_db()

# --- PERMANENT TRANSACTION COLLECTION ---
transactions_coll = db_sales["transactions"]

def record_sale(member_id, items_sold, discount_applied=0):
    """
    Processes a sale as an ATOMIC TRANSACTION.
    Saves the ENTIRE sale (header + items) into the
    single, permanent 'transactions' collection.
    """
    
    client = db_connection.client
    
    with client.start_session() as session:
        with session.start_transaction():
            try:
                subtotal = 0 
                permanent_item_docs = [] # A list for the permanent line items
                
                for item in items_sold:
                    quantity_sold = item["quantity"]

                    # --- Step 1: Get product info ---
                    # This line is correct
                    product = inventory_db.get_product_by_name(item["name_for_lookup"])
                    
                    if not product:
                        raise Exception(f"Product '{item['name_for_lookup']}' not found in inventory.")
                    
                    product_id_obj = product["_id"]
                    price = product["price"]
                    subtotal += price * quantity_sold
                    
                    # --- Step 2: Update permanent stock (THIS IS THE FIX) ---
                    
                    # BUGGY CODE was: stock_update_result = db_inventory.stock_coll.update_one(...)
                    # 'db_inventory' is a database, it doesn't have 'stock_coll'.
                    # We must use the imported 'inventory_db' module to find the collection.
                    
                    stock_update_result = inventory_db.stock_coll.update_one(
                        {
                            "product_id": product_id_obj, 
                            "quantity": {"$gte": quantity_sold}
                        },
                        {"$inc": {"quantity": -quantity_sold}},
                        session=session
                    )
                    # --- END OF FIX ---
                    
                    if stock_update_result.matched_count == 0:
                        # This is the error you were (correctly) getting
                        raise Exception(f"Out of stock for product: {product['name']}.")
                    
                    # --- Step 3: Prepare the PERMANENT line item doc ---
                    item_doc = {
                        "product_id": product_id_obj,
                        "name": product["name"],
                        "category": product.get("category", "Uncategorized"),
                        "price_at_sale": price,
                        "quantity_sold": quantity_sold
                    }
                    permanent_item_docs.append(item_doc)

                # --- Step 4: Calculate final total ---
                final_total = subtotal - discount_applied

                # --- Step 5: Build the final, single transaction document ---
                transaction_doc = {
                    "timestamp": datetime.now(timezone.utc),
                    "subtotal": subtotal,
                    "discount_applied": discount_applied,
                    "total_amount": final_total,
                    "member_id": member_id,
                    "payment_method": "cash",
                    "items": permanent_item_docs # <-- Embed the items list
                }
                
                # --- Step 6: Insert the permanent transaction ---
                trans_result = transactions_coll.insert_one(transaction_doc, session=session)
                
                # --- Step 7: Update permanent member loyalty ---
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