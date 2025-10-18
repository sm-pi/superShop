from gui.app import App
from database.db_connector import db_connection

if __name__ == "__main__":
    # This checks if the db_connection was successful
    if db_connection:
        
        # --- THESE ARE THE MISSING LINES ---
        # This creates the application window
        app = App()
        
        # This tells the window to open and wait for user input
        app.mainloop()
        # --- END OF MISSING LINES ---
        
    else:
        # If db_connector.py failed, don't start the app.
        print("Application cannot start: Failed to connect to database.")
        print("Please check your .env file and ensure MongoDB is running.")