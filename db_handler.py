# db_handler.py
"""
Handles all interactions with the SQLite database for tracking
processing status of objects from the OI Import Generator.
"""

import sqlite3
import json
import logging
import datetime
import os

# --- Constants ---
DB_FILE_NAME = "oi_processing_status.db"
DB_PATH = os.path.abspath(DB_FILE_NAME)

# --- Database Initialization ---

def init_db(db_path=DB_PATH):
    """Initializes the SQLite database and creates the 'objects' table if it doesn't exist."""
    try:
        logging.info(f"Initializing database at: {db_path}")
        with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS objects (
                    unique_id TEXT PRIMARY KEY,
                    csv_row_index INTEGER,
                    status TEXT NOT NULL,
                    node_type TEXT,
                    action TEXT,
                    identifier TEXT,                 -- Display identifier (e.g., title or location)
                    generated_xml TEXT,
                    error_message TEXT,
                    output_batch_file TEXT,
                    last_attempt_timestamp TIMESTAMP,
                    csv_data_json TEXT
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON objects (status);')
            # Index added for identifier lookup
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_identifier ON objects (identifier);')
            conn.commit()
        logging.info("Database initialized successfully.")
        return True
    except Exception as e:
        logging.error(f"Database initialization failed for {db_path}: {e}", exc_info=True)
        return False

# --- Core Data Operations (add_pending_objects, get_object_status, update_object_status remain the same) ---

def add_pending_objects(object_list, db_path=DB_PATH):
    """Adds a list of objects to the database with 'pending' status."""
    added_count = 0; skipped_count = 0; rows_to_insert = []
    timestamp = datetime.datetime.now()
    for obj_data in object_list:
        unique_id = obj_data.get('unique_id'); row_index = obj_data.get('csv_row_index'); csv_data = obj_data.get('csv_data', {})
        if not unique_id: logging.warning(f"Skipping object at row {row_index}: Missing 'unique_id'."); skipped_count += 1; continue
        rows_to_insert.append((unique_id, row_index, 'pending', None, None, None, None, None, None, timestamp, json.dumps(csv_data)))
    if not rows_to_insert: logging.info("No new pending objects to add."); return 0, skipped_count
    try:
        with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.executemany('INSERT OR IGNORE INTO objects (unique_id, csv_row_index, status, node_type, action, identifier, generated_xml, error_message, output_batch_file, last_attempt_timestamp, csv_data_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows_to_insert)
            added_count = conn.total_changes; skipped_count += (len(rows_to_insert) - added_count); conn.commit()
        logging.info(f"Added {added_count} pending objects, skipped {skipped_count} existing objects.")
        return added_count, skipped_count
    except Exception as e: logging.error(f"Database error adding pending objects: {e}", exc_info=True); return 0, len(object_list)

def get_object_status(unique_id, db_path=DB_PATH):
    """Retrieves the current status and data for a specific object by unique_id."""
    if not unique_id: return None
    try:
        with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
            conn.row_factory = sqlite3.Row; cursor = conn.cursor()
            cursor.execute("SELECT * FROM objects WHERE unique_id = ?", (unique_id,))
            row = cursor.fetchone()
            if row:
                row_dict = dict(row)
                if row_dict.get('csv_data_json'):
                    try: row_dict['csv_data'] = json.loads(row_dict['csv_data_json'])
                    except json.JSONDecodeError: logging.warning(f"Could not parse csv_data_json for unique_id {unique_id}"); row_dict['csv_data'] = {}
                else: row_dict['csv_data'] = {}
                return row_dict
            else: return None
    except Exception as e: logging.error(f"Database error getting status for {unique_id}: {e}", exc_info=True); return None

def update_object_status(unique_id, status, node_type=None, action=None, identifier=None,
                         generated_xml=None, error_message=None, output_batch_file=None,
                         db_path=DB_PATH):
    """Updates the status and associated data for a specific object."""
    if not unique_id: logging.warning("Attempted to update status for object with no unique_id."); return False
    timestamp = datetime.datetime.now()
    try:
        with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE objects
                SET status = ?, node_type = COALESCE(?, node_type), action = COALESCE(?, action),
                    identifier = COALESCE(?, identifier), generated_xml = COALESCE(?, generated_xml),
                    error_message = ?, output_batch_file = COALESCE(?, output_batch_file),
                    last_attempt_timestamp = ?
                WHERE unique_id = ?
            ''', (status, node_type, action, identifier, generated_xml, error_message,
                  output_batch_file, timestamp, unique_id))
            updated_rows = cursor.rowcount; conn.commit()
            if updated_rows > 0: logging.debug(f"Updated status for {unique_id} to '{status}'."); return True
            else: logging.warning(f"Could not update status for {unique_id}: ID not found."); return False
    except Exception as e: logging.error(f"Database error updating status for {unique_id}: {e}", exc_info=True); return False

# --- Query Functions for Reporting / Reprocessing ---

def get_objects_by_status(status_list, db_path=DB_PATH):
    """Retrieves all objects matching any status in the provided list."""
    if not status_list: return []
    results = []
    try:
        with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
            conn.row_factory = sqlite3.Row; cursor = conn.cursor()
            placeholders = ','.join('?' for status in status_list)
            query = f"SELECT * FROM objects WHERE status IN ({placeholders}) ORDER BY csv_row_index"
            cursor.execute(query, status_list)
            rows = cursor.fetchall()
            for row in rows:
                row_dict = dict(row)
                if row_dict.get('csv_data_json'):
                    try: row_dict['csv_data'] = json.loads(row_dict['csv_data_json'])
                    except json.JSONDecodeError: logging.warning(f"Could not parse csv_data_json for unique_id {row_dict.get('unique_id')}"); row_dict['csv_data'] = {}
                else: row_dict['csv_data'] = {}
                results.append(row_dict)
        logging.info(f"Retrieved {len(results)} objects with status in {status_list}.")
        return results
    except Exception as e: logging.error(f"Database error retrieving objects by status {status_list}: {e}", exc_info=True); return []

def get_object_by_identifier(identifier, db_path=DB_PATH):
    """
    Retrieves the object record matching the given identifier (title/location).
    Assumes identifier should be unique enough for reprocessing matching.
    If multiple matches occur, logs a warning and returns the first one found.

    Args:
        identifier (str): The identifier (title or location) to search for.
        db_path (str, optional): Path to the database file. Defaults to DB_PATH.

    Returns:
        dict: A dictionary containing the object's data, or None if not found or error.
              Includes parsed 'csv_data'.
    """
    if not identifier:
        return None
    try:
        with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
            conn.row_factory = sqlite3.Row # Return rows as dictionary-like objects
            cursor = conn.cursor()
            # Query based on the identifier column
            cursor.execute("SELECT * FROM objects WHERE identifier = ?", (identifier,))
            rows = cursor.fetchall() # Use fetchall to detect duplicates

            if rows:
                if len(rows) > 1:
                    logging.warning(f"Found multiple ({len(rows)}) database entries matching identifier '{identifier}'. Using the first one found (ID: {rows[0]['unique_id']}).")

                # Process the first found row
                row_dict = dict(rows[0])
                # Parse the JSON data back into a Python dict
                if row_dict.get('csv_data_json'):
                    try:
                        row_dict['csv_data'] = json.loads(row_dict['csv_data_json'])
                    except json.JSONDecodeError:
                        logging.warning(f"Could not parse csv_data_json for unique_id {row_dict.get('unique_id')} found via identifier '{identifier}'")
                        row_dict['csv_data'] = {}
                else:
                    row_dict['csv_data'] = {}
                return row_dict
            else:
                logging.warning(f"No database entry found matching identifier: '{identifier}'")
                return None # Not found
    except sqlite3.Error as e:
        logging.error(f"Database error getting object by identifier '{identifier}': {e}", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"Unexpected error getting object by identifier '{identifier}': {e}", exc_info=True)
        return None

def get_status_counts(db_path=DB_PATH):
    """Gets the count of objects for each status."""
    counts = {}
    try:
        with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor(); cursor.execute("SELECT status, COUNT(*) FROM objects GROUP BY status")
            rows = cursor.fetchall();
            for row in rows: counts[row[0]] = row[1]
        return counts
    except Exception as e: logging.error(f"Database error getting status counts: {e}", exc_info=True); return {}

def get_file_type_counts(db_path=DB_PATH):
    """Gets the count of successfully processed objects grouped by node_type."""
    counts = {}
    try:
        with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COALESCE(node_type, 'Unknown'), COUNT(*) FROM objects WHERE status = 'success' GROUP BY COALESCE(node_type, 'Unknown')")
            rows = cursor.fetchall();
            for row in rows: counts[row[0]] = row[1]
        return counts
    except Exception as e: logging.error(f"Database error getting file type counts: {e}", exc_info=True); return {}

# --- Utility Functions (clear_database remains the same) ---
def clear_database(db_path=DB_PATH):
    """Deletes all records from the objects table. Use with caution!"""
    try:
        with sqlite3.connect(db_path) as conn: cursor = conn.cursor(); cursor.execute("DELETE FROM objects"); conn.commit()
        logging.warning(f"Cleared all records from the database: {db_path}"); return True
    except Exception as e: logging.error(f"Database error clearing table: {e}", exc_info=True); return False

# Example Usage (for testing)
if __name__ == '__main__':
    # ... (testing code can be added here, including testing get_object_by_identifier) ...
    if init_db():
        print("DB Initialized.")
        # Add test data if needed...
        # test_id = get_object_by_identifier("My Document Title") # Assuming this was added previously
        # print(f"Found by identifier: {test_id}")
        pass

