import pandas as pd
import sqlite3
import os

def csv_to_sqlite(csv_file: str, db_file: str, table_name: str):
    """
    Convert a CSV file to a SQLite database table with correct column data types,
    storing dates in YYYY-MM-DD format for filtering.
    
    Args:
        csv_file (str): Path to the input CSV file.
        db_file (str): Path to the output SQLite database file.
        table_name (str): Name of the table to create in the SQLite database.
    """
    try:
        # Step 1: Read the CSV file
        print(f"Reading CSV file: {csv_file}")
        df = pd.read_csv(csv_file)
        print(f"CSV loaded with {len(df)} rows and columns: {list(df.columns)}")

        # Step 2: Preprocess data types
        # Convert date columns to YYYY-MM-DD format
        df['order_date'] = pd.to_datetime(df['order_date'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        df['ship_date'] = pd.to_datetime(df['ship_date'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Handle invalid dates (replace NaT with empty string or a default date)
        df['order_date'] = df['order_date'].fillna('1900-01-01')
        df['ship_date'] = df['ship_date'].fillna('1900-01-01')

        # Ensure numeric columns
        df['sales'] = pd.to_numeric(df['sales'], errors='coerce').fillna(0.0)
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
        df['discount'] = pd.to_numeric(df['discount'], errors='coerce').fillna(0.0)
        df['profit'] = pd.to_numeric(df['profit'], errors='coerce').fillna(0.0)
        df['shipping_cost'] = pd.to_numeric(df['shipping_cost'], errors='coerce').fillna(0.0)
        df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(0).astype(int)
        
        # Fill missing text values with empty string
        text_columns = ['order_id', 'ship_mode', 'customer_name', 'segment', 'state', 
                        'country', 'market', 'region', 'product_id', 'category', 
                        'sub_category', 'product_name', 'order_priority']
        df[text_columns] = df[text_columns].fillna('').astype(str)

        # Step 3: Connect to SQLite database
        print(f"Connecting to SQLite database: {db_file}")
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Step 4: Create table with explicit schema
        print(f"Creating table: {table_name}")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                order_id TEXT,
                order_date TEXT,  -- Stored as YYYY-MM-DD
                ship_date TEXT,   -- Stored as YYYY-MM-DD
                ship_mode TEXT,
                customer_name TEXT,
                segment TEXT,
                state TEXT,
                country TEXT,
                market TEXT,
                region TEXT,
                product_id TEXT,
                category TEXT,
                sub_category TEXT,
                product_name TEXT,
                sales REAL,
                quantity INTEGER,
                discount REAL,
                profit REAL,
                shipping_cost REAL,
                order_priority TEXT,
                year INTEGER
            )
        """)
        conn.commit()

        # Step 5: Write DataFrame to SQLite
        print(f"Writing data to table: {table_name}")
        df.to_sql(table_name, conn, if_exists='append', index=False)

        # Step 6: Verify the data
        print("Verifying data in SQLite...")
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Inserted {row_count} rows into {table_name}")


        # Step 7: Close the connection
        conn.close()
        print(f"SQLite database created successfully: {db_file}")

    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Example usage
if __name__ == "__main__":
    csv_file = "data/SuperStoreOrders.csv"  # Replace with your CSV file path
    db_file = "data/SuperStoreOrders.db"  # Name of the output SQLite database
    table_name = "orders"  # Name of the table in SQLite
    
    csv_to_sqlite(csv_file, db_file, table_name)