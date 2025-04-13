import json
import asyncio
from surrealdb import Surreal

async def load_and_insert_data(file_path: str, database_url: str, namespace: str, database: str):
    """
    Loads data from a JSON file, connects to SurrealDB, and inserts the data.

    Args:
        file_path (str): The path to the JSON file.
        database_url (str): The URL of the SurrealDB instance (e.g., 'ws://localhost:8000').
        namespace (str): The namespace to use in SurrealDB.
        database (str): The database to use in SurrealDB.
    """
    try:
        # 1. Read data from the JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = [json.loads(line) for line in f]  # Read as JSON lines

        # 2. Connect to SurrealDB
        db = Surreal(database_url)
        await db.connect()

        # 3. Select the database and namespace
        await db.use(namespace, database)

        # 4. Insert data into SurrealDB (schemaless)
        for record in data:
            try:
                #  SurrealDB is schemaless, so we don't need to define a schema.
                #  We'll insert each JSON object as a separate record in a table
                #  named after the top-level key in the JSON, or "data" if no
                #  top-level key, or if the data is already a flat object.
                table_name = "data" # default table name

                if isinstance(record, dict):
                  #Gets the first key.
                  first_key = next(iter(record), None)
                  if first_key:
                    table_name = first_key
                # print(f"Inserting into table: {table_name}, record: {record}") # debugging
                created = await db.create(table_name, record)
                if not created:
                  print(f"Failed to create record in {table_name}: {record}")

            except Exception as e:
                print(f"Error inserting record: {e}, record: {record}")

        # 5. Close the connection
        await db.close()
        print("Data insertion complete.")

    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print("Please ensure the file is valid JSON.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")



async def main():
    """
    Main function to set parameters and call the data loading and insertion function.
    """
    # Configuration
    file_path = 'arxivl.json'  # Replace with the actual path to your JSON file
    database_url = 'ws://localhost:8000'  # Replace with your SurrealDB URL
    namespace = 'test'  # Replace with your SurrealDB namespace
    database = 'test'  # Replace with your SurrealDB database name

    await load_and_insert_data(file_path, database_url, namespace, database)

if __name__ == "__main__":
    asyncio.run(main())

