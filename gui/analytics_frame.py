import customtkinter as ctk
from database import sales_db

class AnalyticsFrame(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master, fg_color="transparent")

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)
        
        # --- 1. TITLE ---
        self.label = ctk.CTkLabel(self, text="Live Sales Fragment Viewer", font=ctk.CTkFont(size=18, weight="bold"))
        self.label.grid(row=0, column=0, columnspan=2, padx=20, pady=20)

        # --- 2. FILTERS ---
        self.filter_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.filter_frame.grid(row=1, column=0, sticky="ew", padx=20)
        self.filter_frame.grid_columnconfigure(0, weight=1)
        self.filter_frame.grid_columnconfigure(1, weight=1)
        self.filter_frame.grid_columnconfigure(2, weight=1)

        # Filter: Fragment Dropdown
        self.fragment_label = ctk.CTkLabel(self.filter_frame, text="Select Fragment:")
        self.fragment_label.grid(row=0, column=0, padx=5, pady=5, sticky="e")
        self.fragment_var = ctk.StringVar(value="budget")
        self.fragment_menu = ctk.CTkOptionMenu(self.filter_frame, values=["budget", "midrange", "premium"], variable=self.fragment_var)
        self.fragment_menu.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        # Filter: Category Entry
        self.category_label = ctk.CTkLabel(self.filter_frame, text="Filter by Category:")
        self.category_label.grid(row=1, column=0, padx=5, pady=5, sticky="e")
        self.category_entry = ctk.CTkEntry(self.filter_frame, placeholder_text="e.g., Dairy")
        self.category_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        # Search Button
        self.search_button = ctk.CTkButton(self.filter_frame, text="Search", command=self.search_sales)
        self.search_button.grid(row=0, column=2, rowspan=2, padx=10, pady=5, sticky="e")

        # --- 3. RESULTS ---
        self.results_frame = ctk.CTkScrollableFrame(self)
        
        # --- THIS IS THE FIXED LINE ---
        # It was 'self.results_.grid(...)', now it's 'self.results_frame.grid(...)'
        self.results_frame.grid(row=2, column=0, padx=20, pady=20, sticky="nsew")
        # --- END OF FIX ---

        # Initial search
        self.search_sales()

    def search_sales(self):
        """
        Called when 'Search' is clicked.
        """
        # 1. Clear old results
        for widget in self.results_frame.winfo_children():
            widget.destroy()

        # 2. Get filter values
        fragment = self.fragment_var.get()
        category = self.category_entry.get()

        # 3. Call database function
        sales = sales_db.get_temp_sales(fragment, category)

        # 4. Display results
        if not sales:
            ctk.CTkLabel(self.results_frame, text="No sales found matching these criteria.").pack(pady=10)
            return

        for sale in sales:
            # Format the output string
            time = sale['createdAt'].strftime("%Y-%m-%d %H:%M:%S")
            text = f"{time} - {sale['name']} (x{sale['quantity_sold']}) at ${sale['price_at_sale']:.2f} [Category: {sale['category']}]"
            
            ctk.CTkLabel(self.results_frame, text=text).pack(anchor="w", padx=10)