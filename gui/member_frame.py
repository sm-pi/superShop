import customtkinter as ctk
from database import member_db # Import the backend logic

class MemberFrame(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master, fg_color="transparent")
        
        self.grid_columnconfigure(0, weight=1)

        self.label = ctk.CTkLabel(self, text="Member Management", font=ctk.CTkFont(size=18, weight="bold"))
        self.label.grid(row=0, column=0, columnspan=2, padx=20, pady=20)

        self.name_entry = ctk.CTkEntry(self, placeholder_text="Member Name")
        self.name_entry.grid(row=1, column=0, padx=20, pady=5, sticky="ew")
        
        self.email_entry = ctk.CTkEntry(self, placeholder_text="Email")
        self.email_entry.grid(row=2, column=0, padx=20, pady=5, sticky="ew")
        
        self.phone_entry = ctk.CTkEntry(self, placeholder_text="Phone Number")
        self.phone_entry.grid(row=3, column=0, padx=20, pady=5, sticky="ew")

        self.add_button = ctk.CTkButton(self, text="Add Member", command=self.add_member_callback)
        self.add_button.grid(row=4, column=0, padx=20, pady=10)

        self.status_label = ctk.CTkLabel(self, text="", text_color="green")
        self.status_label.grid(row=5, column=0, padx=20, pady=5)

    def add_member_callback(self):
        """
        Called when the 'Add Member' button is clicked.
        """
        name = self.name_entry.get()
        email = self.email_entry.get()
        phone = self.phone_entry.get()

        if not all([name, email, phone]):
            self.status_label.configure(text="All fields are required.", text_color="red")
            return
        
        try:
            # Check if member already exists
            if member_db.find_member_by_email(email):
                self.status_label.configure(text="Member with this email already exists.", text_color="red")
                return
            
            # --- UI-to-Backend call ---
            member_id = member_db.add_member(name, email, phone)
            
            self.status_label.configure(text=f"Success! Added '{name}' (ID: {member_id})", text_color="green")
            self.name_entry.delete(0, "end")
            self.email_entry.delete(0, "end")
            self.phone_entry.delete(0, "end")
        except Exception as e:
            self.status_label.configure(text=f"Error: {e}", text_color="red")