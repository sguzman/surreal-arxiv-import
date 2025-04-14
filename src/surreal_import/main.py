import json
import ijson # Import the streaming JSON library
# import asyncio # Removed asyncio
import logging
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TaskProgressColumn
from surrealdb import Surreal # Import the Surreal class
# import sys # No longer needed for sys.exit

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)]
)
log = logging.getLogger("rich")
# --- End Logging Setup ---

# Synchronous function using 'with' for connection management
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

    inserted_count = 0
    failed_count = 0
    processed_count = 0
    table_name = "arxiv_data" # Use a consistent table name

    try:
        # --- Database Operations Setup (Synchronous using 'with') ---
        log.info(f"Connecting to SurrealDB at [cyan]{database_url}[/cyan]...")
        # Use a standard 'with' statement for the synchronous client
        # This implicitly handles connect/setup and close/teardown
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
            # Use synchronous use method within the 'with' block
            db.use(namespace, database)
            log.info("Namespace and database selected successfully.")

            # --- Streaming Parsing and Insertion (inside the 'with' block) ---
            log.info("Starting data streaming and insertion...")

            # Setup progress bar
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                # Removed show_percentage=False from TaskProgressColumn for compatibility
                TaskProgressColumn(),
                TextColumn("Processed: {task.completed} | Failed: {task.fields[failed]}"),
                TimeElapsedColumn(),
                transient=False,
            ) as progress:
                task = progress.add_task(f"[cyan]Streaming from '{file_path}'...", total=None, failed=0)

                try:
                    # Open the file and stream items
                    with open(file_path, 'rb') as f:
                        parser = ijson.items(f, 'item')
                        for record in parser:
                            processed_count += 1
                            progress.update(task, advance=1, description=f"[cyan]Processing record {processed_count}...[/cyan]")

                            try:
                                # Validate record
                                if not isinstance(record, dict):
                                    log.warning(f"Skipping record {processed_count}: Item not a dictionary. Type: {type(record)}")
                                    failed_count += 1
                                    progress.update(task, failed=failed_count)
                                    continue

                                log.debug(f"Attempting to insert record {processed_count}...")
                                # Use synchronous create method
                                created = db.create(table_name, record)

                                if created:
                                    inserted_count += 1
                                    log.debug(f"Successfully inserted record {processed_count}.")
                                else:
                                    log.error(f"Failed record {processed_count}: db.create did not return success. Snippet: {str(record)[:200]}...")
                                    failed_count += 1
                                    progress.update(task, failed=failed_count)

                            except Exception as e:
                                log.error(f"Error inserting record {processed_count}: {e}", exc_info=True)
                                log.debug(f"Problematic record snippet: {str(record)[:200]}...")
                                failed_count += 1
                                progress.update(task, failed=failed_count)

                except ijson.JSONError as e:
                    log.error(f"Fatal JSON parsing error near record {processed_count+1}: {e}", exc_info=True)
                    log.critical("Input file contains invalid JSON.")
                    progress.update(task, description=f"[bold red]JSON Error after {processed_count} records[/bold red]")
                except FileNotFoundError:
                    log.error(f"Error: File not found at [cyan]{file_path}[/cyan]")
                except Exception as e:
                    log.critical(f"Unexpected error during streaming/insertion near record {processed_count+1}: {e}", exc_info=True)
                    progress.update(task, description=f"[bold red]Unexpected Error after {processed_count} records[/bold red]")

                # Final update to progress bar
                current_task = progress.tasks[task]
                # Check description to see if a fatal error occurred before setting final state
                if not current_task.description.startswith("[bold red]"):
                     final_desc = f"[bold green]Streaming finished[/bold green]"
                     if failed_count > 0:
                         final_desc += f" ([bold red]{failed_count} failed inserts[/bold red])"
                     # Set total and completed only if finished normally
                     progress.update(task, description=final_desc, total=processed_count, completed=processed_count)

            log.info(f"[bold green]Data processing complete.[/bold green] Processed: [bold green]{processed_count}[/bold green], Inserted: [bold green]{inserted_count}[/bold green], Failed Inserts: [bold {'red' if failed_count > 0 else 'green'}]{failed_count}[/bold {'red' if failed_count > 0 else 'green'}]")

    except Exception as e:
        # Catch errors during the initial 'with Surreal(...)' connection attempt
        log.critical(f"An error occurred during database connection setup: {e}", exc_info=True)
    # No 'finally' block needed for db.close() as 'with' handles it.


# Synchronous main function
def main():
    """
    Main function to set parameters and call the data loading and insertion function.
    """
    file_path = 'arxiv_array.json'
    database_url = 'ws://localhost:8000'
    namespace = 'test'
    database = 'test'

    load_and_insert_data(file_path, database_url, namespace, database)

if __name__ == "__main__":
    main() # Run main synchronously
