import customtkinter as ctk
from .inventory_frame import InventoryFrame
from .sales_frame import SalesFrame
from .member_frame import MemberFrame
from database.db_connector import db_connection 

class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("SuperShop Management System")
        
        # --- 16x8 Inch Window Size ---
        # (Assuming 96 DPI: 16 inches * 96 = 1536px, 8 inches * 96 = 768px)
        self.geometry("1536x768") 

        ctk.set_appearance_mode("System")
        ctk.set_default_color_theme("blue")

        # --- Make the window adjustable/resizable ---
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        # Create Tab View
        self.tab_view = ctk.CTkTabview(self, width=1500, height=750)
        # --- Use .grid() and 'sticky' to make the tab view resizeable ---
        self.tab_view.grid(row=0, column=0, padx=20, pady=20, sticky="nsew")

        # Add tabs
        self.tab_view.add("Point of Sale")
        self.tab_view.add("Inventory")
        self.tab_view.add("Members")

        # --- Populate tabs with frames from other files ---
        self.sales_frame = SalesFrame(self.tab_view.tab("Point of Sale"))
        self.sales_frame.pack(expand=True, fill="both")

        self.inventory_frame = InventoryFrame(self.tab_view.tab("Inventory"), sales_frame=self.sales_frame)
        self.inventory_frame.pack(expand=True, fill="both")

        self.member_frame = MemberFrame(self.tab_view.tab("Members"))
        self.member_frame.pack(expand=True, fill="both")

        # Set default tab
        self.tab_view.set("Point of Sale")

        # Handle window close event
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def on_closing(self):
        """
        Called when the user clicks the 'X' button.
        Ensures the database connection is closed gracefully.
        """
        print("Closing application...")
        db_connection.close_connection()
        self.destroy()