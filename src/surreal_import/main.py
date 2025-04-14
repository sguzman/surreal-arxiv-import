import json
import ijson  # Import the streaming JSON library
import logging
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TaskProgressColumn
from surrealdb import Surreal  # Import the Surreal class
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

num_core = 16

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)]
)
log = logging.getLogger("rich")
# --- End Logging Setup ---

def insert_record(database_url: str, namespace: str, database: str, table_name: str, record: Dict[str, Any], record_number: int) -> bool:
    """
    Inserts a single record into the database.

    Args:
        database_url (str): The URL of the SurrealDB instance.
        namespace (str): The namespace to use in SurrealDB.
        database (str): The database to use in SurrealDB.
        table_name (str): The name of the table to insert into.
        record (Dict[str, Any]): The record to insert.
        record_number (int): The record number for logging.

    Returns:
        bool: True if the insertion was successful, False otherwise.
    """
    log.info(f"[Record {record_number}] Starting insertion...")
    try:
        with Surreal(database_url) as db:
            log.debug(f"[Record {record_number}] Connecting to SurrealDB...")
            db.signin({"username": "root", "password": "root"})
            log.debug(f"[Record {record_number}] Authentication successful.")
            db.use(namespace, database)
            log.debug(f"[Record {record_number}] Using namespace '{namespace}' and database '{database}'.")

            if not isinstance(record, dict):
                log.warning(f"[Record {record_number}] Skipping: Item is not a dictionary. Type: {type(record)}")
                return False

            log.debug(f"[Record {record_number}] Attempting to insert...")
            created = db.create(table_name, record)

            if created:
                log.info(f"[Record {record_number}] Successfully inserted.")
                return True
            else:
                log.error(f"[Record {record_number}] Failed: db.create did not return success. Snippet: {str(record)[:200]}...")
                return False
    except Exception as e:
        error_message = str(e)
        if "already exists" in error_message:
            log.warning(f"[Record {record_number}] Duplicate detected: {error_message}")
            return False  # Treat duplicates as failed inserts but continue
        else:
            log.error(f"[Record {record_number}] Error: {e}", exc_info=True)
            log.debug(f"[Record {record_number}] Problematic record snippet: {str(record)[:200]}...")
            return False


def process_records_in_parallel(database_url: str, namespace: str, database: str, table_name: str, records: List[Dict[str, Any]], max_workers: int = num_core):
    """
    Processes records in parallel using a thread pool.

    Args:
        database_url (str): The URL of the SurrealDB instance.
        namespace (str): The namespace to use in SurrealDB.
        database (str): The database to use in SurrealDB.
        table_name (str): The name of the table to insert into.
        records (List[Dict[str, Any]]): The list of records to process.
        max_workers (int): The maximum number of worker threads.
    """
    log.info(f"Starting parallel processing with {max_workers} workers...")
    inserted_count = 0
    failed_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_record = {
            executor.submit(insert_record, database_url, namespace, database, table_name, record, i + 1): record
            for i, record in enumerate(records)
        }

        for future in as_completed(future_to_record):
            record = future_to_record[future]
            try:
                if future.result():
                    inserted_count += 1
                else:
                    failed_count += 1
                    log.warning(f"[Record Processed] Failed to insert: {record}")
            except Exception as e:
                log.error(f"[Record Processed] Unexpected error: {e}", exc_info=True)
                failed_count += 1

    log.info(f"[bold green]Parallel processing complete.[/bold green] Inserted: {inserted_count}, Failed: {failed_count}")


def load_and_insert_data(file_path: str, database_url: str, namespace: str, database: str):
    """
    Loads data by streaming a JSON array using ijson, connects to SurrealDB,
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
        # Open the file and stream items
        log.info(f"Opening file: {file_path}")
        with open(file_path, 'rb') as f:
            parser = ijson.items(f, 'item')  # 'item' targets each element in the array
            records = list(parser)  # Load all records into memory for parallel processing

        log.info(f"Loaded {len(records)} records. Starting parallel processing...")
        process_records_in_parallel(database_url, namespace, database, table_name, records, max_workers=num_core)

    except FileNotFoundError:
        log.critical(f"File not found: {file_path}")
    except ijson.JSONError as e:
        log.critical(f"JSON parsing error: {e}", exc_info=True)
    except Exception as e:
        log.critical(f"An unexpected error occurred: {e}", exc_info=True)


def main():
    """
    Main function to set parameters and call the data loading and insertion function.
    """
    file_path = 'arxiv_array_small.json'
    database_url = 'ws://localhost:8000'
    namespace = 'test'
    database = 'test'

    log.info("Starting the data import process...")
    load_and_insert_data(file_path, database_url, namespace, database)
    log.info("Data import process completed.")


if __name__ == "__main__":
    main()  # Run main synchronously
