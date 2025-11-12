import customtkinter as ctk
from database import member_db

class MemberFrame(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master, fg_color="transparent")
        
        self.grid_columnconfigure(0, weight=1)

        self.label = ctk.CTkLabel(self, text="Member Management", font=ctk.CTkFont(size=18, weight="bold"))
        self.label.grid(row=0, column=0, padx=20, pady=20, sticky="ew")

        self.name_entry = ctk.CTkEntry(self, placeholder_text="Member Name")
        self.name_entry.grid(row=1, column=0, padx=20, pady=5, sticky="ew")

        self.phone_entry = ctk.CTkEntry(self, placeholder_text="Phone Number")
        self.phone_entry.grid(row=2, column=0, padx=20, pady=5, sticky="ew")

        # --- NEW: Email field is required for sharding ---
        self.email_entry = ctk.CTkEntry(self, placeholder_text="Email (e.g., user@gmail.com)")
        self.email_entry.grid(row=3, column=0, padx=20, pady=5, sticky="ew")
        # --- END NEW ---

        self.add_button = ctk.CTkButton(self, text="Add Member", command=self.add_member_callback)
        self.add_button.grid(row=4, column=0, padx=20, pady=10)

        self.status_label = ctk.CTkLabel(self, text="", text_color="green")
        self.status_label.grid(row=5, column=0, padx=20, pady=10)

    def add_member_callback(self):
        name = self.name_entry.get()
        phone = self.phone_entry.get()
        email = self.email_entry.get() # Get the new email

        if not name or not phone or not email:
            self.status_label.configure(text="Name, Phone, and Email are required.", text_color="red")
            return
            
        try:
            # Pass all three fields to the backend
            member_id = member_db.add_member(name, phone, email)
            if member_id:
                self.status_label.configure(text=f"Success! Added member '{name}'", text_color="green")
                self.name_entry.delete(0, "end")
                self.phone_entry.delete(0, "end")
                self.email_entry.delete(0, "end")
            else:
                # This now checks for duplicates on the specific shard
                self.status_label.configure(text="Error: Phone number or Email already exists on its shard.", text_color="red")
        except Exception as e:
            self.status_label.configure(text=f"Error: {e}", text_color="red")