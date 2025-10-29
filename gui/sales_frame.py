import customtkinter as ctk
from database import sales_db, inventory_db, member_db

# Use the hash map keys for consistency + "All Categories"
CATEGORIES = ["All Categories"] + list(inventory_db.CATEGORY_HASH.keys())

class SalesFrame(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master, fg_color="transparent")
        # (Layout setup...)
        self.cart = []
        self.member_found = None
        self.DISCOUNT_THRESHOLD = 1000
        self.DISCOUNT_PERCENT = 0.05
        self.grid_columnconfigure(0, weight=1, minsize=180)
        self.grid_columnconfigure(1, weight=5)
        self.grid_columnconfigure(2, weight=2, minsize=230)
        self.grid_rowconfigure(1, weight=1)

        # --- Column 0: FILTERS ---
        self.filter_frame = ctk.CTkFrame(self)
        self.filter_frame.grid(row=0, column=0, rowspan=2, padx=(20, 10), pady=20, sticky="nsew")
        self.filter_frame.grid_rowconfigure(7, weight=1) # Add row for brand
        self.filter_label = ctk.CTkLabel(self.filter_frame, text="Filter Products", font=ctk.CTkFont(size=16, weight="bold"))
        self.filter_label.grid(row=0, column=0, padx=15, pady=15, sticky="ew")
        self.name_entry = ctk.CTkEntry(self.filter_frame, placeholder_text="Search by Name")
        self.name_entry.grid(row=1, column=0, padx=15, pady=10, sticky="ew")
        # --- NEW: Brand Filter ---
        self.brand_entry = ctk.CTkEntry(self.filter_frame, placeholder_text="Search by Brand/Supplier")
        self.brand_entry.grid(row=2, column=0, padx=15, pady=10, sticky="ew")
        # --- END NEW ---
        self.category_label = ctk.CTkLabel(self.filter_frame, text="Category:")
        self.category_label.grid(row=3, column=0, padx=15, pady=(10, 0), sticky="w")
        self.category_var = ctk.StringVar(value=CATEGORIES[0])
        self.category_menu = ctk.CTkOptionMenu(self.filter_frame, values=CATEGORIES, variable=self.category_var)
        self.category_menu.grid(row=4, column=0, padx=15, pady=5, sticky="ew")
        self.min_price_entry = ctk.CTkEntry(self.filter_frame, placeholder_text="Min Price")
        self.min_price_entry.grid(row=5, column=0, padx=15, pady=10, sticky="ew")
        self.max_price_entry = ctk.CTkEntry(self.filter_frame, placeholder_text="Max Price")
        self.max_price_entry.grid(row=6, column=0, padx=15, pady=10, sticky="ew")
        self.filter_button = ctk.CTkButton(self.filter_frame, text="Find Products", command=self.apply_filters_callback)
        self.filter_button.grid(row=7, column=0, padx=15, pady=15) # Moved button down

        # (Product List setup...)
        self.product_frame = ctk.CTkFrame(self)
        self.product_frame.grid(row=0, column=1, rowspan=2, padx=10, pady=20, sticky="nsew")
        self.product_frame.grid_rowconfigure(2, weight=1)
        self.product_frame.grid_columnconfigure(0, weight=1)
        self.product_label = ctk.CTkLabel(self.product_frame, text="Product List", font=ctk.CTkFont(size=16, weight="bold"))
        self.product_label.grid(row=0, column=0, padx=15, pady=15, sticky="w")
        self.refresh_button = ctk.CTkButton(self.product_frame, text="Refresh", width=80, command=self.apply_filters_callback)
        self.refresh_button.grid(row=0, column=1, padx=15, pady=15, sticky="e")
        self.header_frame = ctk.CTkFrame(self.product_frame, fg_color="gray20", corner_radius=0)
        self.header_frame.grid(row=1, column=0, columnspan=2, sticky="ew")
        self.header_frame.grid_columnconfigure(0, weight=4)
        self.header_frame.grid_columnconfigure(1, weight=3)
        self.header_frame.grid_columnconfigure(2, minsize=70)
        self.header_frame.grid_columnconfigure(3, minsize=50)
        self.header_frame.grid_columnconfigure(4, minsize=40)
        ctk.CTkLabel(self.header_frame, text="Product Name", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        ctk.CTkLabel(self.header_frame, text="Brand", font=ctk.CTkFont(weight="bold")).grid(row=0, column=1, padx=5, pady=5, sticky="w")
        ctk.CTkLabel(self.header_frame, text="Price (BDT)", font=ctk.CTkFont(weight="bold")).grid(row=0, column=2, padx=5, pady=5, sticky="w")
        ctk.CTkLabel(self.header_frame, text="Stock", font=ctk.CTkFont(weight="bold")).grid(row=0, column=3, padx=5, pady=5, sticky="w")
        self.product_list_frame = ctk.CTkScrollableFrame(self.product_frame, fg_color="transparent", corner_radius=0)
        self.product_list_frame.grid(row=2, column=0, columnspan=2, padx=0, pady=0, sticky="nsew")
        self.product_list_frame.grid_columnconfigure(0, weight=1)

        # (Cart Frame setup...)
        self.cart_frame = ctk.CTkFrame(self)
        self.cart_frame.grid(row=0, column=2, rowspan=2, padx=(10, 20), pady=20, sticky="nsew")
        self.cart_frame.grid_rowconfigure(4, weight=1)
        self.cart_frame.grid_columnconfigure((0, 1), weight=1)
        self.cart_label = ctk.CTkLabel(self.cart_frame, text="Current Sale", font=ctk.CTkFont(size=16, weight="bold"))
        self.cart_label.grid(row=0, column=0, columnspan=2, padx=15, pady=15)
        self.member_phone_entry = ctk.CTkEntry(self.cart_frame, placeholder_text="Member Phone for Discount")
        self.member_phone_entry.grid(row=1, column=0, padx=(15, 5), pady=10, sticky="ew")
        self.check_member_button = ctk.CTkButton(self.cart_frame, text="Check", width=60, command=self.check_member_callback)
        self.check_member_button.grid(row=1, column=1, padx=(5, 15), pady=10)
        self.discount_label = ctk.CTkLabel(self.cart_frame, text="No discount", text_color="gray", font=ctk.CTkFont(size=12))
        self.discount_label.grid(row=2, column=0, columnspan=2, padx=15, pady=(0, 5), sticky="w")
        self.total_label = ctk.CTkLabel(self.cart_frame, text="Total: 0.00 BDT", font=ctk.CTkFont(size=16, weight="bold"))
        self.total_label.grid(row=3, column=0, columnspan=2, padx=15, pady=5, sticky="w")
        self.cart_list_frame = ctk.CTkScrollableFrame(self.cart_frame, fg_color="transparent")
        self.cart_list_frame.grid(row=4, column=0, columnspan=2, padx=15, pady=15, sticky="nsew")
        self.process_sale_button = ctk.CTkButton(self.cart_frame, text="Process Payment", command=self.process_sale_callback)
        self.process_sale_button.grid(row=5, column=0, padx=(15, 5), pady=15, sticky="ew")
        self.clear_sale_button = ctk.CTkButton( self.cart_frame, text="Clear Sale", command=self.clear_sale, fg_color="#D32F2F", hover_color="#B71C1C")
        self.clear_sale_button.grid(row=5, column=1, padx=(5, 15), pady=15, sticky="ew")
        self.status_label = ctk.CTkLabel(self.cart_frame, text="", text_color="green")
        self.status_label.grid(row=6, column=0, columnspan=2, padx=15, pady=5)

        self.apply_filters_callback()

    # --- MODIFIED: Get Brand Filter ---
    def apply_filters_callback(self):
        for widget in self.product_list_frame.winfo_children():
            widget.destroy()

        filters = {}
        name = self.name_entry.get()
        if name: filters["name"] = name
        # --- Get Brand ---
        brand = self.brand_entry.get()
        if brand: filters["brand"] = brand
        # --- End Brand ---
        category = self.category_var.get()
        if category and category != "All Categories":
            filters["category"] = category
        try:
            min_price = self.min_price_entry.get()
            if min_price: filters["min_price"] = float(min_price)
            max_price = self.max_price_entry.get()
            if max_price: filters["max_price"] = float(max_price)
        except ValueError:
            self.status_label.configure(text="Prices must be numbers.", text_color="red")
            return
        else:
            self.status_label.configure(text="")

        products = inventory_db.create_product_fragment(filters) # Pass brand filter

        if not products:
            ctk.CTkLabel(self.product_list_frame, text="No products match filters.").grid(row=0, column=0, padx=10, pady=10)

        for i, product in enumerate(products):
            # (Display logic...)
            brand_display = product.get("supplier_name", "N/A")
            prod_frame = ctk.CTkFrame(self.product_list_frame, corner_radius=0, fg_color=("gray90", "gray10") if i % 2 == 0 else "transparent")
            prod_frame.grid(row=i, column=0, sticky="ew")
            prod_frame.grid_columnconfigure(0, weight=4)
            prod_frame.grid_columnconfigure(1, weight=3)
            prod_frame.grid_columnconfigure(2, minsize=70)
            prod_frame.grid_columnconfigure(3, minsize=50)
            prod_frame.grid_columnconfigure(4, minsize=40)
            ctk.CTkLabel(prod_frame, text=product['name'], anchor="w").grid(row=0, column=0, padx=10, pady=5, sticky="ew")
            ctk.CTkLabel(prod_frame, text=brand_display, anchor="w").grid(row=0, column=1, padx=5, pady=5, sticky="ew")
            ctk.CTkLabel(prod_frame, text=f"{product['price']:.2f}", anchor="w").grid(row=0, column=2, padx=5, pady=5, sticky="ew")
            ctk.CTkLabel(prod_frame, text=product['quantity_in_stock'], anchor="w").grid(row=0, column=3, padx=5, pady=5, sticky="ew")
            ctk.CTkButton(
                prod_frame, text="+", width=30, height=20,
                command=lambda p=product: self.add_to_cart_callback(p)
            ).grid(row=0, column=4, padx=(5, 10), pady=5)

    def check_member_callback(self):
        # (Unchanged)
        phone = self.member_phone_entry.get()
        if not phone:
             self.status_label.configure(text="Enter phone.", text_color="red"); return
        member = member_db.find_member_by_phone(phone)
        self.member_found = member
        status_text = f"Member: {member['name']}" if member else "No member found."
        status_color = "green" if member else "red"
        self.status_label.configure(text=status_text, text_color=status_color)
        self.update_cart_ui()


    # --- MODIFIED: Store shard_id ---
    def add_to_cart_callback(self, product):
        product_id_str = str(product["_id"])
        shard_id = product["shard_id"] # Get shard_id from fragment

        for item in self.cart:
            if item["product_id"] == product_id_str:
                item["quantity"] += 1
                self.update_cart_ui()
                return

        brand_name = product.get("supplier_name", "N/A")
        display_name = f"{product['name']} ({brand_name})"

        cart_item = {
            "product_id": product_id_str,
            "shard_id": shard_id, # Store shard_id
            "name": display_name,
            "price": product["price"],
            "quantity": 1
        }
        self.cart.append(cart_item)
        self.update_cart_ui()

    def update_cart_ui(self):
        # (Unchanged)
        for widget in self.cart_list_frame.winfo_children(): widget.destroy()
        subtotal = 0
        if not self.cart: ctk.CTkLabel(self.cart_list_frame, text="Cart is empty.").pack()
        for item in self.cart:
            item_total = item["price"] * item["quantity"]
            subtotal += item_total
            item_text = f"{item['name']} (x{item['quantity']}) - {item_total:.2f} BDT"
            ctk.CTkLabel(self.cart_list_frame, text=item_text).pack(anchor="w", padx=10)
        discount = 0; final_total = subtotal
        if self.member_found and subtotal >= self.DISCOUNT_THRESHOLD:
            discount = subtotal * self.DISCOUNT_PERCENT
            final_total = subtotal - discount
            self.discount_label.configure(text=f"Discount (5%): -{discount:.2f} BDT", text_color="#1F6AA5")
        else:
            discount_needed = self.DISCOUNT_THRESHOLD - subtotal
            discount_text = f"Spend {discount_needed:.2f} BDT more for 5% off" if self.member_found and discount_needed > 0 else "No discount applied."
            self.discount_label.configure(text=discount_text, text_color="gray")
        self.total_label.configure(text=f"Total: {final_total:.2f} BDT")


    # --- MODIFIED: Pass shard_id ---
    def process_sale_callback(self):
        self.status_label.configure(text="Processing...", text_color="orange"); self.update_idletasks()
        member_id = self.member_found["_id"] if self.member_found else None
        if not self.cart: self.status_label.configure(text="Cart empty.", text_color="red"); return
        subtotal = sum(item["price"] * item["quantity"] for item in self.cart)
        discount_applied = 0
        if self.member_found and subtotal >= self.DISCOUNT_THRESHOLD: discount_applied = subtotal * self.DISCOUNT_PERCENT

        items_sold_db = [
            {"product_id": item["product_id"], "quantity": item["quantity"], "shard_id": item["shard_id"]} # Pass shard_id
            for item in self.cart
        ]
        try:
            transaction_id = sales_db.record_sale(member_id, items_sold_db, discount_applied)
            if transaction_id:
                self.status_label.configure(text=f"Sale complete!", text_color="green"); self.clear_sale()
            else:
                self.status_label.configure(text="Sale Failed (See console).", text_color="red")
        except Exception as e:
            self.status_label.configure(text=f"Error: {e}", text_color="red")

    def clear_sale(self):
        # (Unchanged)
        self.cart = []; self.member_found = None
        self.member_phone_entry.delete(0, "end")
        self.status_label.configure(text="Sale cleared.", text_color="gray")
        self.update_cart_ui(); self.apply_filters_callback()