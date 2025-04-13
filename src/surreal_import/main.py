import json
import asyncio
import logging
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from surrealdb import Surreal
import sys # Import sys for exit

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)]
)
log = logging.getLogger("rich")
# --- End Logging Setup ---

async def load_and_insert_data(file_path: str, database_url: str, namespace: str, database: str):
    """
    Loads data from a file containing a single JSON array, connects to SurrealDB,
    and inserts the data with progress logging.

    Args:
        file_path (str): The path to the JSON file (expected to be a single array).
        database_url (str): The URL of the SurrealDB instance.
        namespace (str): The namespace to use in SurrealDB.
        database (str): The database to use in SurrealDB.
    """
    records_to_insert = []
    log.info(f"Attempting to load JSON array from: [cyan]{file_path}[/cyan]")
    try:
        # 1. Read the entire file and parse as a single JSON array
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            data = json.loads(content)

        # 2. Validate that the loaded data is a list (JSON array)
        if not isinstance(data, list):
            log.error(f"Error: File '{file_path}' does not contain a valid JSON array. Found type: {type(data)}.")
            sys.exit(1) # Exit if the format is not the expected array

        records_to_insert = data
        log.info(f"Successfully parsed [bold green]{len(records_to_insert)}[/bold green] records from the JSON array.")

    except FileNotFoundError:
        log.error(f"Error: File not found at [cyan]{file_path}[/cyan]")
        sys.exit(1)
    except json.JSONDecodeError as e:
        log.error(f"Error decoding JSON from file '{file_path}': {e}", exc_info=True)
        log.critical("Please ensure the file contains a single, valid JSON array.")
        sys.exit(1)
    except Exception as e:
        log.critical(f"An unexpected error occurred during file reading/parsing: {e}", exc_info=True)
        sys.exit(1) # Exit on unexpected errors during loading

    if not records_to_insert:
         log.warning("JSON array loaded successfully, but it contains no records. Exiting.")
         return

    # --- Database Operations ---
    db = None # Initialize db to None
    try:
        # 3. Connect to SurrealDB
        log.info(f"Connecting to SurrealDB at [cyan]{database_url}[/cyan]...")
        db = Surreal(database_url)
        await db.connect()
        log.info("[bold green]Successfully connected[/bold green] to SurrealDB.")

        # 4. Select the database and namespace
        log.info(f"Using namespace '[yellow]{namespace}[/yellow]' and database '[yellow]{database}[/yellow]'...")
        await db.use(namespace, database)
        log.info("Namespace and database selected successfully.")

        # 5. Insert data into SurrealDB (schemaless) with progress bar
        log.info("Starting data insertion...")
        inserted_count = 0
        failed_count = 0
        table_name = "arxiv_data" # Use a consistent table name

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("([progress.completed]{task.completed} of {task.total})"),
            TimeElapsedColumn(),
            transient=False,
        ) as progress:
            task = progress.add_task(f"[cyan]Inserting into '{table_name}'...", total=len(records_to_insert))

            for i, record in enumerate(records_to_insert):
                try:
                    # Validate record is a dictionary before inserting
                    if not isinstance(record, dict):
                        log.warning(f"Skipping record {i+1}: Item in JSON array is not a dictionary. Found type: {type(record)}")
                        failed_count += 1
                        progress.update(task, advance=1, description=f"[yellow]Skipping record {i+1}...[/yellow]")
                        continue

                    log.debug(f"Attempting to insert record {i+1}/{len(records_to_insert)}...")
                    created = await db.create(table_name, record)

                    if created:
                        inserted_count += 1
                        log.debug(f"Successfully inserted record {i+1}.")
                        progress.update(task, advance=1, description=f"[cyan]Inserting record {i+1}...[/cyan]")
                    else:
                      # Log failure from SurrealDB (create returns None or potentially raises exception handled below)
                      log.error(f"Failed to create record {i+1} in table '{table_name}'. SurrealDB create command did not return a successful result. Record: {record}")
                      failed_count += 1
                      progress.update(task, advance=1, description=f"[red]Failed record {i+1}...[/red]")

                except Exception as e:
                    # Catch errors during the db.create call specifically
                    log.error(f"Error inserting record {i+1} into SurrealDB: {e}", exc_info=True)
                    log.debug(f"Problematic record data: {record}")
                    failed_count += 1
                    progress.update(task, advance=1, description=f"[red]Error record {i+1}...[/red]")

            # Update progress bar description upon completion
            final_desc = f"[bold green]Insertion finished[/bold green]"
            if failed_count > 0:
                final_desc += f" ([bold red]{failed_count} failed[/bold red])"
            progress.update(task, description=final_desc)

        log.info(f"[bold green]Data insertion complete.[/bold green] Inserted: [bold green]{inserted_count}[/bold green], Failed: [bold {'red' if failed_count > 0 else 'green'}]{failed_count}[/bold {'red' if failed_count > 0 else 'green'}]")

    except Exception as e:
        # Catch errors during connection or namespace selection
        log.critical(f"An error occurred during database operations: {e}", exc_info=True)
    finally:
        # 6. Ensure connection is closed
        if db and db.ws and not db.ws.closed:
             log.info("Closing SurrealDB connection...")
             await db.close()
             log.info("Connection closed.")
        elif db:
             log.info("SurrealDB connection already closed or not fully established.")


async def main():
    """
    Main function to set parameters and call the data loading and insertion function.
    """
    # *** IMPORTANT: Update this path if you used jq to create a new file ***
    file_path = 'arxiv_array.json' # Or 'arxivl.json' if you overwrote the original
    database_url = 'ws://localhost:8000'
    namespace = 'test'
    database = 'test'

    await load_and_insert_data(file_path, database_url, namespace, database)

if __name__ == "__main__":
    asyncio.run(main())
