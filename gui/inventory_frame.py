import customtkinter as ctk
from database import inventory_db 

class InventoryFrame(ctk.CTkFrame):
    
    def __init__(self, master, sales_frame=None):
        super().__init__(master, fg_color="transparent")
        self.sales_frame = sales_frame 

        self.grid_columnconfigure(1, weight=1)

        self.label = ctk.CTkLabel(self, text="Inventory Management", font=ctk.CTkFont(size=18, weight="bold"))
        self.label.grid(row=0, column=0, columnspan=2, padx=20, pady=20)

        self.add_label = ctk.CTkLabel(self, text="Add New Product")
        self.add_label.grid(row=1, column=0, padx=10, pady=5, sticky="w")

        self.name_entry = ctk.CTkEntry(self, placeholder_text="Product Name")
        self.name_entry.grid(row=2, column=0, columnspan=2, padx=20, pady=5, sticky="ew")

        self.price_entry = ctk.CTkEntry(self, placeholder_text="Price (e.g., 1200)")
        self.price_entry.grid(row=3, column=0, padx=(20, 5), pady=5, sticky="ew")

        self.stock_entry = ctk.CTkEntry(self, placeholder_text="Initial Stock (e.g., 100)")
        self.stock_entry.grid(row=3, column=1, padx=(5, 20), pady=5, sticky="ew")
        
        self.category_entry = ctk.CTkEntry(self, placeholder_text="Category (e.g., 'Dairy')")
        self.category_entry.grid(row=4, column=0, padx=(20, 5), pady=5, sticky="ew")

        self.supplier_entry = ctk.CTkEntry(self, placeholder_text="Supplier Name")
        self.supplier_entry.grid(row=4, column=1, padx=(5, 20), pady=5, sticky="ew")

        self.add_button = ctk.CTkButton(self, text="Add Product", command=self.add_product_callback)
        self.add_button.grid(row=5, column=0, columnspan=2, padx=20, pady=10)

        self.status_label = ctk.CTkLabel(self, text="", text_color="green")
        self.status_label.grid(row=6, column=0, columnspan=2, padx=20, pady=5)

    # --- THIS FUNCTION IS MODIFIED ---
    def add_product_callback(self):
        name = self.name_entry.get()
        price_str = self.price_entry.get()
        stock_str = self.stock_entry.get()
        category = self.category_entry.get()
        supplier = self.supplier_entry.get()

        if not all([name, price_str, stock_str, supplier]):
            self.status_label.configure(text="All fields (except Category) are required.", text_color="red")
            return
            
        # --- NEW VALIDATION ---
        if not category or category.isspace():
            self.status_label.configure(text="Category field cannot be blank.", text_color="red")
            return
        # --- END OF NEW VALIDATION ---

        try:
            price = float(price_str)
            stock = int(stock_str)
        except ValueError:
            self.status_label.configure(text="Price and Stock must be numbers.", text_color="red")
            return

        try:
            # This 'add_product' function is from the inventory_db.py file
            product_id = inventory_db.add_product(name, price, category, supplier, stock)
            
            self.status_label.configure(text=f"Success! Added '{name}' (ID: {product_id})", text_color="green")
            self.name_entry.delete(0, "end")
            self.price_entry.delete(0, "end")
            self.stock_entry.delete(0, "end")
            self.category_entry.delete(0, "end")
            self.supplier_entry.delete(0, "end")

            if self.sales_frame:
                print("Refreshing sales frame product list...")
                self.sales_frame.apply_filters_callback()
                
        except Exception as e:
            self.status_label.configure(text=f"Error: {e}", text_color="red")