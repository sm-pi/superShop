from .db_connector import db_connection
from . import inventory_db 
from datetime import datetime, timezone
from bson.objectid import ObjectId
import pymongo 
import re 

# Get database handles
db_sales = db_connection.get_sales_db()
db_inventory = db_connection.get_inventory_db()
db_member = db_connection.get_member_db()

# --- PERMANENT FRAGMENTATION ---
transactions_coll = db_sales["transactions"]
sold_items_coll = db_sales["Sold_Items"]

# --- THIS IS THE FINAL, CORRECT 'record_sale' FUNCTION ---
def record_sale(member_id, items_sold, discount_applied=0):
    """
    Processes a sale as an ATOMIC TRANSACTION.
    This version reads the unique 'product_id' from the cart.
    """
    
    client = db_connection.client
    
    with client.start_session() as session:
        with session.start_transaction():
            try:
                subtotal = 0 
                permanent_item_docs = [] # For the 'transactions' receipt
                
                for item in items_sold:
                    quantity_sold = item["quantity"]
                    
                    # --- THIS IS THE FIX ---
                    # 1. Get the UNAMBIGUOUS product_id from the cart
                    product_id_obj = ObjectId(item["product_id"])

                    # 2. Find the product using its unique ID
                    product = inventory_db.products_coll.find_one(
                        {"_id": product_id_obj}, 
                        session=session
                    )
                    if not product:
                        raise Exception(f"Product ID {product_id_obj} not found in inventory.")
                    # --- END FIX ---
                    
                    price = product["price"]
                    category = "Uncategorized"
                    if "category" in product and product["category"] and not product["category"].isspace():
                        category = product["category"].strip()

                    subtotal += price * quantity_sold
                    
                    # 3. Update permanent stock in ShopInventory.stock
                    stock_update_result = inventory_db.stock_coll.update_one(
                        {"product_id": product_id_obj, "quantity": {"$gte": quantity_sold}},
                        {"$inc": {"quantity": -quantity_sold}},
                        session=session
                    )
                    if stock_update_result.matched_count == 0:
                        raise Exception(f"Out of stock for product: {product['name']}.")
                    
                    # 4. --- UPDATE 'Sold_Items' AGGREGATED FRAGMENT ---
                    sold_items_coll.find_one_and_update(
                        {"category": category}, # Find the document for this category
                        {
                            "$inc": { "Total_Sold_in_Category": quantity_sold },
                            "$push": { 
                                "products_sold": {
                                    "name": product["name"],
                                    "quantity_sold": quantity_sold
                                }
                            }
                        },
                        upsert=True, 
                        session=session
                    )
                    # --- END OF LOGIC ---
                    
                    # 5. Prepare the item doc for the permanent 'transactions' receipt
                    item_doc = {
                        "product_id": product_id_obj,
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
                    "timestamp": datetime.now(timezone.utc),
                    "subtotal": subtotal,
                    "discount_applied": discount_applied,
                    "total_amount": final_total,
                    "member_id": member_id,
                    "payment_method": "cash",
                    "items": permanent_item_docs 
                }
                
                # --- Step 8: Insert the permanent transaction ---
                trans_result = transactions_coll.insert_one(transaction_doc, session=session)
                
                # --- Step 9: Update permanent member loyalty ---
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