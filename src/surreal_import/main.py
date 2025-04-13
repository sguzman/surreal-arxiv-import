import json
import asyncio
from surrealdb import Surreal

async def load_and_insert_data(file_path: str, database_url: str, namespace: str, database: str):
    """
    Loads data from a JSON file (either JSON Lines or a single JSON array/object),
    connects to SurrealDB, and inserts the data.

    Args:
        file_path (str): The path to the JSON file.
        database_url (str): The URL of the SurrealDB instance (e.g., 'ws://localhost:8000').
        namespace (str): The namespace to use in SurrealDB.
        database (str): The database to use in SurrealDB.
    """
    data = []
    try:
        # 1. Read data from the JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                # Attempt to read as a single JSON array/object first
                content = f.read()
                data = json.loads(content)
                # Ensure data is a list for iteration
                if isinstance(data, dict):
                    data = [data] # Wrap single object in a list
                elif not isinstance(data, list):
                    print(f"Warning: JSON file does not contain a list or object. Found type: {type(data)}. Attempting to process anyway.")
                    # Handle cases where it might be just a value, though unlikely for Kaggle datasets
                    data = [data] if data else []

            except json.JSONDecodeError as e_full:
                print(f"Could not parse as single JSON object/array: {e_full}. Trying JSON Lines format...")
                # If parsing the whole file fails, try reading as JSON Lines
                f.seek(0) # Go back to the start of the file
                try:
                    data = [json.loads(line) for line in f if line.strip()] # Read non-empty lines
                except json.JSONDecodeError as e_lines:
                    print(f"Error decoding JSON Lines: {e_lines}")
                    print("Please ensure the file is valid JSON (either a single object/array or JSON Lines).")
                    return # Stop execution if both parsing methods fail

        if not data:
             print("No data loaded from the file.")
             return

        # 2. Connect to SurrealDB
        print(f"Connecting to SurrealDB at {database_url}...")
        db = Surreal(database_url)
        await db.connect()
        print("Connected.")

        # 3. Select the database and namespace
        print(f"Using namespace '{namespace}' and database '{database}'...")
        await db.use(namespace, database)
        print("Namespace and database selected.")

        # 4. Insert data into SurrealDB (schemaless)
        print(f"Starting data insertion for {len(data)} records...")
        inserted_count = 0
        failed_count = 0
        for i, record in enumerate(data):
            try:
                # SurrealDB is schemaless, so we don't need to define a schema.
                # We'll insert each JSON object as a separate record in a table
                # named "arxiv_data" (more specific than just "data").
                table_name = "arxiv_data" # Use a consistent table name

                # Ensure the record is a dictionary before inserting
                if not isinstance(record, dict):
                    print(f"Skipping record {i+1}: Not a valid JSON object (dictionary). Found type: {type(record)}")
                    failed_count += 1
                    continue

                # print(f"Inserting record {i+1}/{len(data)} into table: {table_name}") # More informative debug
                created = await db.create(table_name, record)
                if created:
                    inserted_count += 1
                else:
                  print(f"Failed to create record {i+1} in {table_name}: {record}")
                  failed_count += 1

            except Exception as e:
                print(f"Error inserting record {i+1}: {e}, record: {record}")
                failed_count += 1

        # 5. Close the connection
        await db.close()
        print(f"Data insertion complete. Inserted: {inserted_count}, Failed: {failed_count}")

    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


async def main():
    """
    Main function to set parameters and call the data loading and insertion function.
    """
    # Configuration
    # Use a more specific path if needed, e.g., 'src/surreal_import/arxivl.json'
    file_path = 'arxivl.json'
    database_url = 'ws://localhost:8000'  # Replace with your SurrealDB URL
    namespace = 'test'  # Replace with your SurrealDB namespace
    database = 'test'  # Replace with your SurrealDB database name

    await load_and_insert_data(file_path, database_url, namespace, database)

if __name__ == "__main__":
    # Ensure the script path in the run command matches this structure
    # e.g., if this file is src/surreal_import/main.py, run from the project root:
    # rye run python src/surreal_import/main.py
    asyncio.run(main())

