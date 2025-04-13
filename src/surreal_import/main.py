import json # Still needed for potential JSON errors, though ijson has its own
import ijson # Import the streaming JSON library
import asyncio
import logging
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TaskProgressColumn
from surrealdb import Surreal
import sys

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
    Loads data by streaming a JSON array from a file using ijson,
    connects to SurrealDB, and inserts the data with progress logging.

    Args:
        file_path (str): The path to the large JSON file containing a single array.
        database_url (str): The URL of the SurrealDB instance.
        namespace (str): The namespace to use in SurrealDB.
        database (str): The database to use in SurrealDB.
    """
    log.info(f"Attempting to stream JSON array from: [cyan]{file_path}[/cyan]")

    db = None  # Initialize db to None
    inserted_count = 0
    failed_count = 0
    processed_count = 0
    table_name = "arxiv_data" # Use a consistent table name

    try:
        # --- Database Operations Setup ---
        # Connect and select DB/NS before starting the potentially long parsing process
        log.info(f"Connecting to SurrealDB at [cyan]{database_url}[/cyan]...")
        db = Surreal(database_url)
        await db.connect()
        log.info("[bold green]Successfully connected[/bold green] to SurrealDB.")

        log.info(f"Using namespace '[yellow]{namespace}[/yellow]' and database '[yellow]{database}[/yellow]'...")
        await db.use(namespace, database)
        log.info("Namespace and database selected successfully.")

        # --- Streaming Parsing and Insertion ---
        log.info("Starting data streaming and insertion...")

        # Setup progress bar - no total initially, shows count instead
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TaskProgressColumn(show_percentage=False), # Show count/rate instead of %
            TextColumn("Processed: {task.completed} | Failed: {task.fields[failed]}"),
            TimeElapsedColumn(),
            transient=False, # Keep visible after completion
        ) as progress:
            # Add task without total, pass failed_count via fields
            task = progress.add_task(f"[cyan]Streaming from '{file_path}'...", total=None, failed=0)

            try:
                # Open the file and stream items from the top-level array ('item' denotes array elements)
                with open(file_path, 'rb') as f: # Open in binary mode for ijson
                    parser = ijson.items(f, 'item') # 'item' gets objects from the root array
                    for record in parser:
                        processed_count += 1
                        current_failed = progress.tasks[task].fields['failed']
                        progress.update(task, advance=1, description=f"[cyan]Processing record {processed_count}...[/cyan]")

                        try:
                            # Validate record is a dictionary before inserting
                            if not isinstance(record, dict):
                                log.warning(f"Skipping record {processed_count}: Item in JSON stream is not a dictionary. Found type: {type(record)}")
                                failed_count += 1
                                progress.update(task, failed=failed_count) # Update failed count in progress bar
                                continue

                            log.debug(f"Attempting to insert record {processed_count}...")
                            created = await db.create(table_name, record)

                            if created:
                                inserted_count += 1
                                log.debug(f"Successfully inserted record {processed_count}.")
                            else:
                                log.error(f"Failed to create record {processed_count} in table '{table_name}'. SurrealDB create command did not return a successful result. Record snippet: {str(record)[:200]}...")
                                failed_count += 1
                                progress.update(task, failed=failed_count)

                        except Exception as e:
                            # Catch errors during the db.create call specifically
                            log.error(f"Error inserting record {processed_count} into SurrealDB: {e}", exc_info=True)
                            log.debug(f"Problematic record data snippet: {str(record)[:200]}...")
                            failed_count += 1
                            progress.update(task, failed=failed_count)

            except ijson.JSONError as e:
                # Catch JSON parsing errors during streaming
                log.error(f"Fatal JSON parsing error during streaming near record {processed_count+1}: {e}", exc_info=True)
                log.critical("The input file contains invalid JSON syntax. Please check the file structure.")
                # Update progress bar to show failure state
                progress.update(task, description=f"[bold red]JSON Error after {processed_count} records[/bold red]")
                # No sys.exit here, allow finally block to close DB connection
            except FileNotFoundError:
                log.error(f"Error: File not found at [cyan]{file_path}[/cyan]")
                # No sys.exit here, allow finally block if DB was connected
            except Exception as e:
                 # Catch other unexpected errors during file reading/parsing/insertion loop
                log.critical(f"An unexpected error occurred during streaming/insertion near record {processed_count+1}: {e}", exc_info=True)
                progress.update(task, description=f"[bold red]Unexpected Error after {processed_count} records[/bold red]")
                # No sys.exit here

            # Final update to progress bar description after loop finishes (if no fatal error)
            if not progress.tasks[task].description.startswith("[bold red]"):
                 final_desc = f"[bold green]Streaming finished[/bold green]"
                 if failed_count > 0:
                     final_desc += f" ([bold red]{failed_count} failed inserts[/bold red])"
                 progress.update(task, description=final_desc, total=processed_count) # Set total at the end


        log.info(f"[bold green]Data processing complete.[/bold green] Processed: [bold green]{processed_count}[/bold green], Inserted: [bold green]{inserted_count}[/bold green], Failed Inserts: [bold {'red' if failed_count > 0 else 'green'}]{failed_count}[/bold {'red' if failed_count > 0 else 'green'}]")

    except Exception as e:
        # Catch errors during initial DB connection or namespace selection
        log.critical(f"An error occurred during database setup: {e}", exc_info=True)
    finally:
        # Ensure connection is closed
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
    # *** Ensure this path points to your large JSON array file ***
    file_path = 'arxiv_array.json'
    database_url = 'ws://localhost:8000'
    namespace = 'test'
    database = 'test'

    await load_and_insert_data(file_path, database_url, namespace, database)

if __name__ == "__main__":
    asyncio.run(main())
