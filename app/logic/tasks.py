import os
import tempfile
from pydantic import BaseModel
from typing import Dict, Any

# Import the actual parsing functions
from ..parsers.main_parser import process_single_file, deduplicate_and_sort_messages

# --- In-memory storage for task status and results ---
tasks: Dict[str, Dict[str, Any]] = {}

# Pydantic model for response validation
class TaskStatusModel(BaseModel):
    task_id: str
    status: str
    progress: float
    stage: str
    result: Any = None

def update_task_progress(task_id: str, progress: float, stage: str):
    """Updates the progress of a task in the in-memory store."""
    if task_id in tasks:
        tasks[task_id].update({
            'progress': progress,
            'status': 'running',
            'stage': stage
        })

def run_parsing_job(file_content: bytes, filename: str, task_id: str):
    """The actual parsing function that runs in the background."""
    try:
        print(f"Task {task_id}: Starting parsing for '{filename}'")
        update_task_progress(task_id, 10.0, "Initializing")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name

        update_task_progress(task_id, 20.0, "Parsing file content")

        seen_hashes = set()
        with open(temp_file_path, 'rb') as f:
            setattr(f, 'filename', filename)
            unique_messages = process_single_file(f, seen_hashes)

        os.remove(temp_file_path)

        update_task_progress(task_id, 80.0, "Finalizing and sorting")
        final_result = deduplicate_and_sort_messages(unique_messages)

        tasks[task_id].update({
            "status": "completed", "stage": "Completed",
            "progress": 100.0, "result": final_result
        })
        print(f"Task {task_id}: Completed successfully.")

    except Exception as e:
        error_message = f"Task {task_id}: Failed with error: {e}"
        print(error_message)
        tasks[task_id].update({
            "status": "failed", "stage": "Failed",
            "result": {"error": str(e)}
        })
