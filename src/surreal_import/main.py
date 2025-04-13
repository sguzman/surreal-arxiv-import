import json
import asyncio
import logging
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from surrealdb import Surreal

# --- Logging Setup ---
# Configure logging to use RichHandler for pretty, color-coded output
logging.basicConfig(
    level=logging.INFO,  # Set the default logging level (e.g., INFO, DEBUG)
    format="%(message)s", # Keep format simple, RichHandler handles the rest
    datefmt="[%X]",       # Format for timestamps
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)] # Use RichHandler
)
# Get the logger instance
log = logging.getLogger("rich")
# --- End Logging Setup ---

async def load_and_insert_data(file_path: str, database_url: str, namespace: str, database: str):
    """
    Loads data from a JSON file (either JSON Lines or a single JSON array/object),
    connects to SurrealDB, and inserts the data with progress logging.

    Args:
        file_path (str): The path to the JSON file.
        database_url (str): The URL of the SurrealDB instance (e.g., 'ws://localhost:8000').
        namespace (str): The namespace to use in SurrealDB.
        database (str): The database to use in SurrealDB.
    """
    data = []
    log.info(f"Attempting to load data from: [cyan]{file_path}[/cyan]")
    try:
        # 1. Read data from the JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                # Attempt to read as a single JSON array/object first
                log.debug("Trying to parse file as a single JSON object/array...")
                content = f.read()
                data = json.loads(content)
                log.info("Successfully parsed file as a single JSON structure.")
                # Ensure data is a list for iteration
                if isinstance(data, dict):
                    log.debug("Data is a single dictionary, wrapping in a list.")
                    data = [data] # Wrap single object in a list
                elif not isinstance(data, list):
                    log.warning(f"JSON file does not contain a list or object. Found type: {type(data)}. Attempting to process anyway.")
                    data = [data] if data else []

            except json.JSONDecodeError as e_full:
                log.warning(f"Could not parse as single JSON object/array: {e_full}. Trying JSON Lines format...")
                # If parsing the whole file fails, try reading as JSON Lines
                f.seek(0) # Go back to the start of the file
                try:
                    log.debug("Trying to parse file as JSON Lines...")
                    data = [json.loads(line) for line in f if line.strip()] # Read non-empty lines
                    log.info("Successfully parsed file as JSON Lines.")
                except json.JSONDecodeError as e_lines:
                    log.error(f"Error decoding JSON Lines: {e_lines}", exc_info=True)
                    log.critical("Failed to parse JSON in both formats. Please ensure the file is valid JSON.")
                    return # Stop execution if both parsing methods fail

        if not data:
             log.warning("No data loaded from the file. Exiting.")
             return

        log.info(f"Loaded [bold green]{len(data)}[/bold green] records from the file.")

        # 2. Connect to SurrealDB
        log.info(f"Connecting to SurrealDB at [cyan]{database_url}[/cyan]...")
        db = Surreal(database_url)
        try:
            await db.connect()
            log.info("[bold green]Successfully connected[/bold green] to SurrealDB.")
        except Exception as e:
            log.error(f"Failed to connect to SurrealDB: {e}", exc_info=True)
            return

        # 3. Select the database and namespace
        log.info(f"Using namespace '[yellow]{namespace}[/yellow]' and database '[yellow]{database}[/yellow]'...")
        try:
            await db.use(namespace, database)
            log.info("Namespace and database selected successfully.")
        except Exception as e:
            log.error(f"Failed to select namespace/database: {e}", exc_info=True)
            await db.close()
            return

        # 4. Insert data into SurrealDB (schemaless) with progress bar
        log.info("Starting data insertion...")
        inserted_count = 0
        failed_count = 0
        table_name = "arxiv_data" # Use a consistent table name

        # Setup rich progress bar
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("([progress.completed]{task.completed} of {task.total})"),
            TimeElapsedColumn(),
            transient=False, # Keep the progress bar after completion
        ) as progress:
            task = progress.add_task(f"[cyan]Inserting into '{table_name}'...", total=len(data))

            for i, record in enumerate(data):
                try:
                    # Ensure the record is a dictionary before inserting
                    if not isinstance(record, dict):
                        log.warning(f"Skipping record {i+1}: Not a valid JSON object (dictionary). Found type: {type(record)}")
                        failed_count += 1
                        progress.update(task, advance=1, description=f"[yellow]Skipping record {i+1}...[/yellow]")
                        continue

                    log.debug(f"Attempting to insert record {i+1}/{len(data)}...")
                    created = await db.create(table_name, record)

                    if created:
                        inserted_count += 1
                        log.debug(f"Successfully inserted record {i+1}.")
                        progress.update(task, advance=1, description=f"[cyan]Inserting record {i+1}...[/cyan]")
                    else:
                      log.error(f"Failed to create record {i+1} in table '{table_name}'. Record: {record}")
                      failed_count += 1
                      progress.update(task, advance=1, description=f"[red]Failed record {i+1}...[/red]")

                except Exception as e:
                    log.error(f"Error inserting record {i+1}: {e}", exc_info=True) # exc_info=True adds traceback
                    log.debug(f"Problematic record data: {record}")
                    failed_count += 1
                    progress.update(task, advance=1, description=f"[red]Error record {i+1}...[/red]")

            # Final update to progress description after loop finishes
            final_desc = f"[bold green]Insertion finished[/bold green]"
            if failed_count > 0:
                final_desc += f" ([bold red]{failed_count} failed[/bold red])"
            progress.update(task, description=final_desc)


        # 5. Close the connection
        log.info("Closing SurrealDB connection...")
        await db.close()
        log.info("Connection closed.")
        log.info(f"[bold green]Data insertion complete.[/bold green] Inserted: [bold green]{inserted_count}[/bold green], Failed: [bold {'red' if failed_count > 0 else 'green'}]{failed_count}[/bold {'red' if failed_count > 0 else 'green'}]")

    except FileNotFoundError:
        log.error(f"Error: File not found at [cyan]{file_path}[/cyan]")
    except Exception as e:
        log.critical(f"An unexpected critical error occurred: {e}", exc_info=True)


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

