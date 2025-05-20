#!/usr/bin/env python3
import os
import sys
import psycopg2

def run_sql_file(connection_string, sql_file_path):
    """
    Execute SQL commands from a file on a PostgreSQL database.
    
    Args:
        connection_string (str): PostgreSQL connection string
        sql_file_path (str): Path to the SQL file to execute
    """
    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file '{sql_file_path}' not found.")
        sys.exit(1)
    
    try:
        # Read SQL file
        with open(sql_file_path, 'r') as file:
            sql_script = file.read()
        
        # Connect to the database
        print(f"Connecting to database...")
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        
        # Execute the SQL script
        print(f"Executing SQL from file: {sql_file_path}")
        cursor.execute(sql_script)
        
        # Commit the changes
        conn.commit()
        print("SQL script executed successfully!")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        # Close connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python run_sql.py <sql_file_path>")
        sys.exit(1)
    
    run_sql_file(sys.argv[1], sys.argv[2])