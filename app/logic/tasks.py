import os
import json
import tempfile
import sqlite3
from pydantic import BaseModel
from typing import Any, Optional
from zipfile import ZipFile, is_zipfile
from selectolax.parser import HTMLParser

# Import the actual parsing functions from the parsers module
from ..parsers.main_parser import process_single_file, deduplicate_and_sort_messages

# Use an absolute path for the database for reliability on the server
DB_FILE = "/home/spnnnnn/parser/tasks.db"

def setup_database():
    """Creates the tasks table if it doesn't exist."""
    # check_same_thread=False is essential for multi-threaded access in Flask
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS tasks
                   (
                       task_id TEXT PRIMARY KEY,
                       task_data TEXT,
                       created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                   )
                   ''')
    conn.commit()
    conn.close()

class TaskStatusModel(BaseModel):
    task_id: str
    status: str
    progress: float
    stage: str
    result: Any = None

def get_task(task_id: str) -> Optional[dict]:
    """Retrieves a task from the SQLite database."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("SELECT task_data FROM tasks WHERE task_id = ?", (task_id,))
    row = cursor.fetchone()
    conn.close()
    return json.loads(row[0]) if row else None

def set_task(task_id: str, task_data: dict):
    """Saves or updates a task in the SQLite database."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
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

def count_valid_files_in_zip(zip_path):
    """Count valid files in ZIP archive for accurate progress tracking"""
    valid_extensions = ('.json', '.html', '.htm')
    try:
        with ZipFile(zip_path, 'r') as archive:
            valid_files = [
                m for m in archive.namelist()
                if not m.endswith('/')
                   and not m.startswith('__MACOSX/')
                   and os.path.basename(m) and not os.path.basename(m).startswith('.')
                   and m.lower().endswith(valid_extensions)
            ]
            return len(valid_files), valid_files
    except Exception as e:
        print(f"Error counting files in ZIP: {e}")
        return 0, []

def run_parsing_job(file_content: bytes, filename: str, task_id: str):
    """
    This function is now the main background task, run directly in a thread.
    We've removed the redundant multiprocessing layer.
    """
    try:
        print(f"Background thread started for Task {task_id}: Parsing '{filename}'")
        update_task_progress(task_id, 5.0, "Initializing parser")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name

        all_unique_messages = []
        seen_hashes = set()

        update_task_progress(task_id, 10.0, "Analyzing file structure")

        if is_zipfile(temp_file_path):
            file_count, valid_files = count_valid_files_in_zip(temp_file_path)

            if file_count == 0:
                raise ValueError("No valid content files (.json, .html, .htm) found in the ZIP archive.")

            print(f"Task {task_id}: Detected ZIP file with {file_count} valid files")
            update_task_progress(task_id, 15.0, f"Processing ZIP archive ({file_count} files)")

            with ZipFile(temp_file_path, 'r') as archive:
                for i, member_name in enumerate(valid_files):
                    file_progress = 15 + ((i + 1) / file_count) * 70
                    stage_msg = f"Processing file {i + 1}/{file_count}: {os.path.basename(member_name)}"
                    update_task_progress(task_id, file_progress, stage_msg)

                    try:
                        with archive.open(member_name) as file_obj:
                            file_obj.filename = member_name
                            if file_obj.peek(1):
                                new_messages = process_single_file(file_obj, seen_hashes)
                                all_unique_messages.extend(new_messages)
                                print(f"  -> File {i + 1}: '{os.path.basename(member_name)}' - {len(new_messages)} messages")
                            else:
                                print(f"  -> File {i + 1}: '{os.path.basename(member_name)}' - Empty file, skipped")
                    except Exception as file_error:
                        print(f"  -> Error processing {member_name}: {file_error}")
                        continue
        else:
            # Handle single file
            print(f"Task {task_id}: Processing single file '{filename}'")
            update_task_progress(task_id, 30.0, f"Processing single file: {filename}")
            with open(temp_file_path, 'rb') as f:
                f.filename = filename
                new_messages = process_single_file(f, seen_hashes)
                all_unique_messages.extend(new_messages)

        os.remove(temp_file_path)

        update_task_progress(task_id, 90.0, "Deduplicating and sorting messages")
        final_result = deduplicate_and_sort_messages(all_unique_messages)

        total_messages = len(final_result)
        unique_senders = len(set(msg.get('sender', 'Unknown') for msg in final_result))
        platforms = list(set(msg.get('source', 'Unknown') for msg in final_result))

        task = get_task(task_id) or {}
        task.update({
            "status": "completed",
            "stage": f"Completed - {total_messages} messages from {unique_senders} senders",
            "progress": 100.0,
            "result": {
                "messages": final_result,
                "statistics": {
                    "total_messages": total_messages,
                    "unique_senders": unique_senders,
                    "platforms_detected": platforms,
                    "file_processed": filename
                }
            }
        })
        set_task(task_id, task)
        print(f"Task {task_id}: Completed successfully!")

    except Exception as e:
        error_message = f"Task {task_id}: Failed with error: {e}"
        print(error_message)
        task = get_task(task_id) or {}
        task.update({
            "status": "failed", "stage": f"Failed: {str(e)}", "progress": 0.0,
            "result": {"error": str(e)}
        })
        set_task(task_id, task)
