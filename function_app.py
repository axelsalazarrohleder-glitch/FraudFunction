import azure.functions as func
import logging
import os
import pypyodbc
import random
import uuid
import pandas as pd
from datetime import datetime

# Initialize the Function App
app = func.FunctionApp()

# Define the Timer Trigger to run every minute
@app.schedule(schedule="*/1 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=True) 
def LiveTransactionSimulator(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    try:
        logging.info('LiveTransactionSimulator function started.')

        # --- 1. Get Database Connection String ---
        # Gets the connection string from an environment variable
        conn_str = os.environ["AzureSqlDbConnectionString"]

        with pypyodbc.connect(conn_str) as conn:
            logging.info("Successfully connected to the SQL database.")
            cursor = conn.cursor()

            # --- 2. Generate Realistic Random Data ---
            # (We'll grab a real user_id and account_id from the DB)
            
            # Fetch a random user and their account
            query_user = "SELECT TOP 1 user_id, account_id FROM dbo.accounts ORDER BY NEWID()"
            cursor.execute(query_user)
            row = cursor.fetchone()
            
            if not row:
                logging.warning("No user/account data found in database. Skipping.")
                return

            user_id, account_id = row[0], row[1]

            # Generate a new fake transaction
            new_transaction = {
                "transaction_id": str(uuid.uuid4()),
                "account_id": account_id,
                "user_id": user_id,
                "timestamp": datetime.utcnow().isoformat(),
                "amount": round(random.uniform(500.0, 3000.0), 2), # High-value fraud
                "merchant_category": "Electronics",
                "merchant_name": "ShadyElectronics.com",
                "country": random.choice(["Nigeria", "Romania", "Russia"]), # Atypical country
                "city": "Unknown",
                "transaction_type": "Debit",
                "is_online": True,
                "fraud_flag": True,
                "direction": "out",
                "device_id": None,
                "balance_before": 0.0 # We'll ignore balance for this sim
            }

            # --- 3. Insert the New Transaction into the Database ---
            
            # We need to filter columns to match the 'transactions' table
            # Get the actual columns from the transactions table
            cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'transactions'")
            db_columns = [row[0] for row in cursor.fetchall()]
            
            # Filter our fake transaction to only include columns that exist
            insert_data = {k: v for k, v in new_transaction.items() if k in db_columns}
            
            columns = ', '.join(insert_data.keys())
            placeholders = ', '.join(['?'] * len(insert_data))
            values = list(insert_data.values())

            insert_query = f"INSERT INTO dbo.transactions ({columns}) VALUES ({placeholders})"
            
            cursor.execute(insert_query, values)
            conn.commit()

            logging.info(f"Successfully inserted 1 fraudulent transaction for user {user_id}")

    except pypyodbc.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    logging.info('Python timer trigger function executed successfully.')