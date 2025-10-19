import customtkinter as ctk
from database import inventory_db

CATEGORIES_LIST = [
    "Electronics", "Self Care", "Dairy Products",
    "Bakery", "Other"
]

class InventoryFrame(ctk.CTkFrame):

    def __init__(self, master, sales_frame=None):
        super().__init__(master, fg_color="transparent")
        self.sales_frame = sales_frame

        self.grid_columnconfigure(0, weight=1) # Focus on left column
        self.grid_columnconfigure(1, weight=0) # Right column takes minimal space

        self.label = ctk.CTkLabel(self, text="Inventory Management", font=ctk.CTkFont(size=18, weight="bold"))
        self.label.grid(row=0, column=0, columnspan=2, padx=20, pady=20)

        # --- Add New Product Section ---
        self.add_label = ctk.CTkLabel(self, text="Add New Product", font=ctk.CTkFont(size=16))
        self.add_label.grid(row=1, column=0, padx=20, pady=(10, 5), sticky="w")
        self.name_entry = ctk.CTkEntry(self, placeholder_text="Product Name (e.g., Milk)")
        self.name_entry.grid(row=2, column=0, columnspan=2, padx=20, pady=5, sticky="ew")
        self.price_entry = ctk.CTkEntry(self, placeholder_text="Price (e.g., 100)")
        self.price_entry.grid(row=3, column=0, padx=(20, 5), pady=5, sticky="ew")
        self.stock_entry = ctk.CTkEntry(self, placeholder_text="Initial Stock (e.g., 50)")
        self.stock_entry.grid(row=3, column=1, padx=(5, 20), pady=5, sticky="ew")
        self.category_var = ctk.StringVar(value=CATEGORIES_LIST[0])
        self.category_menu = ctk.CTkOptionMenu(self, values=CATEGORIES_LIST, variable=self.category_var)
        self.category_menu.grid(row=4, column=0, padx=(20, 5), pady=5, sticky="ew")
        self.supplier_entry = ctk.CTkEntry(self, placeholder_text="Supplier/Brand (e.g., Pran)")
        self.supplier_entry.grid(row=4, column=1, padx=(5, 20), pady=5, sticky="ew")
        self.add_button = ctk.CTkButton(self, text="Add Product", command=self.add_product_callback)
        self.add_button.grid(row=5, column=0, columnspan=2, padx=20, pady=10)

        # --- MODIFIED: Add Stock Section (Left Aligned and Smaller) ---
        self.add_stock_label = ctk.CTkLabel(self, text="Add Stock (Restock)", font=ctk.CTkFont(size=16))
        self.add_stock_label.grid(row=6, column=0, padx=20, pady=(20, 5), sticky="w") # Align left

        # Smaller entry width
        entry_width = 200

        self.stock_name_entry = ctk.CTkEntry(self, placeholder_text="Product Name", width=entry_width)
        self.stock_name_entry.grid(row=7, column=0, padx=20, pady=5, sticky="w") # Align left

        self.stock_supplier_entry = ctk.CTkEntry(self, placeholder_text="Supplier/Brand", width=entry_width)
        self.stock_supplier_entry.grid(row=8, column=0, padx=20, pady=5, sticky="w") # Align left

        self.stock_amount_entry = ctk.CTkEntry(self, placeholder_text="Amount to Add", width=entry_width)
        self.stock_amount_entry.grid(row=9, column=0, padx=20, pady=10, sticky="w") # Align left

        # Smaller button width
        self.add_stock_button = ctk.CTkButton(self, text="Add", width=80, command=self.add_stock_callback)
        self.add_stock_button.grid(row=10, column=0, padx=20, pady=10, sticky="w") # Align left
        # --- END OF MODIFIED SECTION ---

        self.status_label = ctk.CTkLabel(self, text="", text_color="green")
        # Ensure status label is below everything and spans columns if needed
        self.status_label.grid(row=11, column=0, columnspan=2, padx=20, pady=15)

    def add_product_callback(self):
        # ... (rest of the code is unchanged) ...
        name = self.name_entry.get()
        price_str = self.price_entry.get()
        stock_str = self.stock_entry.get()
        category = self.category_var.get()
        supplier = self.supplier_entry.get()

        if not all([name, price_str, stock_str, supplier, category]):
            self.status_label.configure(text="All fields are required.", text_color="red")
            return
        if supplier.isspace():
             self.status_label.configure(text="Supplier/Brand cannot be blank spaces.", text_color="red")
             return

        try:
            price = float(price_str)
            stock = int(stock_str)
            if stock < 0:
                 raise ValueError("Initial stock cannot be negative.")
        except ValueError as e:
            self.status_label.configure(text=f"Invalid input: {e}", text_color="red")
            return

        try:
            product_id = inventory_db.add_product(name, price, category, supplier, stock)
            if not product_id:
                raise Exception("Product with this name and supplier already exists.")

            self.status_label.configure(text=f"Success! Added '{name}' (ID: {product_id})", text_color="green")
            self.name_entry.delete(0, "end")
            self.price_entry.delete(0, "end")
            self.stock_entry.delete(0, "end")
            self.supplier_entry.delete(0, "end")
            self.category_var.set(CATEGORIES_LIST[0])

            if self.sales_frame:
                self.sales_frame.apply_filters_callback()

        except Exception as e:
            self.status_label.configure(text=f"Error adding product: {e}", text_color="red")

    def add_stock_callback(self):
        # ... (rest of the code is unchanged) ...
        product_name = self.stock_name_entry.get()
        supplier_name = self.stock_supplier_entry.get()
        amount_str = self.stock_amount_entry.get()

        if not product_name or not amount_str or not supplier_name:
            self.status_label.configure(text="Product Name, Supplier, and Amount are required.", text_color="red")
            return
        if supplier_name.isspace():
            self.status_label.configure(text="Supplier/Brand cannot be blank spaces.", text_color="red")
            return

        try:
            amount_to_add = int(amount_str)
            if amount_to_add <= 0:
                raise ValueError("Amount to add must be positive.")
        except ValueError as e:
            self.status_label.configure(text=f"Invalid amount: {e}", text_color="red")
            return

        try:
            updated_stock_info = inventory_db.add_stock_to_product(product_name, supplier_name, amount_to_add)

            if updated_stock_info:
                new_quantity = updated_stock_info.get('quantity', 'N/A')
                self.status_label.configure(text=f"Success! '{product_name} ({supplier_name})' now has {new_quantity} stock.", text_color="green")
                self.stock_name_entry.delete(0, "end")
                self.stock_supplier_entry.delete(0, "end")
                self.stock_amount_entry.delete(0, "end")

                if self.sales_frame:
                    self.sales_frame.apply_filters_callback()
            else:
                self.status_label.configure(text=f"Error: Product '{product_name}' from '{supplier_name}' not found.", text_color="red")

        except Exception as e:
            self.status_label.configure(text=f"Error adding stock: {e}", text_color="red")