import json
import ijson  # Import the streaming JSON library
import logging
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TaskProgressColumn
from surrealdb import Surreal  # Import the Surreal class
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)]
)
log = logging.getLogger("rich")
# --- End Logging Setup ---

# Function to handle the insertion of a single record
def insert_record(db, table_name: str, record: Dict[str, Any], record_number: int) -> bool:
    """
    Inserts a single record into the database.

    Args:
        db: The database connection object.
        table_name (str): The name of the table to insert into.
        record (Dict[str, Any]): The record to insert.
        record_number (int): The record number for logging.

    Returns:
        bool: True if the insertion was successful, False otherwise.
    """
    try:
        if not isinstance(record, dict):
            log.warning(f"Skipping record {record_number}: Item not a dictionary. Type: {type(record)}")
            return False

        log.debug(f"Attempting to insert record {record_number}...")
        created = db.create(table_name, record)

        if created:
            log.debug(f"Successfully inserted record {record_number}.")
            return True
        else:
            log.error(f"Failed record {record_number}: db.create did not return success. Snippet: {str(record)[:200]}...")
            return False
    except Exception as e:
        log.error(f"Error inserting record {record_number}: {e}", exc_info=True)
        log.debug(f"Problematic record snippet: {str(record)[:200]}...")
        return False


# Function to process records in parallel
def process_records_in_parallel(records: List[Dict[str, Any]], db, table_name: str, max_workers: int = 4):
    """
    Processes records in parallel using a thread pool.

    Args:
        records (List[Dict[str, Any]]): The list of records to process.
        db: The database connection object.
        table_name (str): The name of the table to insert into.
        max_workers (int): The maximum number of worker threads.
    """
    inserted_count = 0
    failed_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_record = {
            executor.submit(insert_record, db, table_name, record, i + 1): record
            for i, record in enumerate(records)
        }

        for future in as_completed(future_to_record):
            try:
                if future.result():
                    inserted_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                log.error(f"Unexpected error during parallel processing: {e}", exc_info=True)
                failed_count += 1

    log.info(f"[bold green]Parallel processing complete.[/bold green] Inserted: {inserted_count}, Failed: {failed_count}")


# Updated load_and_insert_data function
def load_and_insert_data(file_path: str, database_url: str, namespace: str, database: str):
    """
    Loads data by streaming a JSON array using ijson, connects to SurrealDB
    synchronously using a 'with' statement for connection management,
    and inserts the data with progress logging.

    Args:
        file_path (str): The path to the large JSON file containing a single array.
        database_url (str): The URL of the SurrealDB instance.
        namespace (str): The namespace to use in SurrealDB.
        database (str): The database to use in SurrealDB.
    """
    log.info(f"Attempting to stream JSON array from: [cyan]{file_path}[/cyan]")

    table_name = "arxiv_data"  # Use a consistent table name

    try:
        # --- Database Operations Setup (Synchronous using 'with') ---
        log.info(f"Connecting to SurrealDB at [cyan]{database_url}[/cyan]...")
        with Surreal(database_url) as db:
            log.info("[bold green]Successfully connected[/bold green] to SurrealDB.")

            # Sign in using root/root credentials
            log.info("Signing in with root/root credentials...")
            try:
                db.signin({"username": "root", "password": "root"})
                log.info("[bold green]Authentication successful.[/bold green]")
            except Exception as e:
                log.critical(f"Authentication failed: {e}", exc_info=True)
                return  # Quit if authentication fails

            log.info(f"Using namespace '[yellow]{namespace}[/yellow]' and database '[yellow]{database}[/yellow]'...")
            db.use(namespace, database)
            log.info("Namespace and database selected successfully.")

            # --- Streaming Parsing and Insertion ---
            log.info("Starting data streaming and insertion...")

            # Open the file and stream items
            with open(file_path, 'rb') as f:
                parser = ijson.items(f, 'item')  # 'item' targets each element in the array
                records = list(parser)  # Load all records into memory for parallel processing

            log.info(f"Loaded {len(records)} records. Starting parallel processing...")
            process_records_in_parallel(records, db, table_name, max_workers=4)

    except Exception as e:
        log.critical(f"An error occurred during database connection setup: {e}", exc_info=True)


# Synchronous main function
def main():
    """
    Main function to set parameters and call the data loading and insertion function.
    """
    file_path = 'arxiv_array_small.json'
    database_url = 'ws://localhost:8000'
    namespace = 'test'
    database = 'test'

    load_and_insert_data(file_path, database_url, namespace, database)


if __name__ == "__main__":
    main()  # Run main synchronously
