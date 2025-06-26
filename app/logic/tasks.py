# =======================================================
# FILE: app/logic/tasks.py
# =======================================================
import os
import json
import tempfile
import sqlite3
from pydantic import BaseModel
from typing import Dict, Any, Optional
from zipfile import ZipFile, is_zipfile
from selectolax.parser import HTMLParser # Using the new, faster parser

# Import the actual parsing functions and the app config
from ..parsers.main_parser import process_single_file, deduplicate_and_sort_messages

DB_FILE = "tasks.db"

def setup_database():
    """Creates the tasks table if it doesn't exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Store task data as JSON text. Set a timestamp for automatic cleanup.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            task_data TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# Pydantic model for response validation (remains the same)
class TaskStatusModel(BaseModel):
    task_id: str
    status: str
    progress: float
    stage: str
    result: Any = None

# --- MODIFIED: Functions now use SQLite ---

def get_task(task_id: str) -> Optional[dict]:
    """Retrieves a task from the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT task_data FROM tasks WHERE task_id = ?", (task_id,))
    row = cursor.fetchone()
    conn.close()
    return json.loads(row[0]) if row else None

def set_task(task_id: str, task_data: dict):
    """Saves or updates a task in the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # INSERT OR REPLACE is a convenient way to handle both new and existing tasks
    cursor.execute(
        "INSERT OR REPLACE INTO tasks (task_id, task_data) VALUES (?, ?)",
        (task_id, json.dumps(task_data))
    )
    conn.commit()
    conn.close()

def update_task_progress(task_id: str, progress: float, stage: str):
    """Updates the progress of a task in SQLite."""
    task = get_task(task_id)
    if task:
        task.update({
            'progress': progress, 'status': 'running', 'stage': stage
        })
        set_task(task_id, task)

# Initialize the database when the module is loaded
setup_database()

# The main parsing job remains the same, as it calls the functions above
def run_parsing_job(file_content: bytes, filename: str, task_id: str):
    """The actual parsing function that runs in the background."""
    try:
        print(f"Task {task_id}: Starting parsing for '{filename}'")
        update_task_progress(task_id, 10.0, "Initializing")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name

        all_unique_messages = []
        seen_hashes = set()

        if is_zipfile(temp_file_path):
            print(f"Task {task_id}: Detected ZIP file. Extracting members...")
            update_task_progress(task_id, 15.0, "Extracting from ZIP archive")
            with ZipFile(temp_file_path, 'r') as archive:
                valid_extensions = ('.json', '.html', '.htm')
                namelist = [
                    m for m in archive.namelist()
                    if not m.endswith('/')
                       and not m.startswith('__MACOSX/')
                       and os.path.basename(m) and not os.path.basename(m).startswith('.')
                       and m.lower().endswith(valid_extensions)
                ]
                print(f"Task {task_id}: Found {len(namelist)} valid files in ZIP.")
                if not namelist:
                    raise ValueError("No valid content files (.json, .html) found in the ZIP archive.")

                for i, member_name in enumerate(namelist):
                    zip_progress = 15 + ((i + 1) / len(namelist)) * 60
                    update_task_progress(task_id, zip_progress, f"Parsing file {i+1}/{len(namelist)}: {member_name}")
                    with archive.open(member_name) as file_obj:
                        if file_obj.peek(1):
                            new_messages = process_single_file(file_obj, seen_hashes)
                            all_unique_messages.extend(new_messages)
                        else:
                            print(f"Task {task_id}: Skipping empty file {member_name}")
        else:
            print(f"Task {task_id}: Detected single file.")
            update_task_progress(task_id, 40.0, "Parsing single file")
            with open(temp_file_path, 'rb') as f:
                new_messages = process_single_file(f, seen_hashes)
                all_unique_messages.extend(new_messages)

        os.remove(temp_file_path)

        update_task_progress(task_id, 80.0, "Finalizing and sorting")
        final_result = deduplicate_and_sort_messages(all_unique_messages)

        task = get_task(task_id) or {}
        task.update({
            "status": "completed", "stage": "Completed",
            "progress": 100.0, "result": final_result
        })
        set_task(task_id, task)
        print(f"Task {task_id}: Completed successfully. Found {len(final_result)} total messages.")

    except Exception as e:
        error_message = f"Task {task_id}: Failed with error: {e}"
        print(error_message)
        task = get_task(task_id) or {}
        task.update({
            "status": "failed", "stage": "Failed",
            "result": {"error": str(e)}
        })
        set_task(task_id, task)
