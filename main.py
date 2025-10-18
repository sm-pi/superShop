from gui.app import App
from database.db_connector import db_connection
# We no longer need to import sales_db here

if __name__ == "__main__":
    # First, check if the database connection was successful
    if db_connection:
        
        # --- THIS LINE IS NOW REMOVED (it caused your crash) ---
        # sales_db.setup_temporary_fragments() 
        
        # --- RUN THE APP ---
        app = App()
        app.mainloop()
    else:
        # If db_connector.py failed, don't start the app.
        print("Application cannot start: Failed to connect to database.")
        print("Please check your .env file and ensure MongoDB is running.")