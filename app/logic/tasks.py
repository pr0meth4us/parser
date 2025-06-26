import os
import json
import tempfile
import redis
from pydantic import BaseModel
from typing import Dict, Any, Optional
from zipfile import ZipFile, is_zipfile

# Import the actual parsing functions and the app config
from ..parsers.main_parser import process_single_file, deduplicate_and_sort_messages
from ..config import settings

# --- MODIFIED: Use Redis instead of an in-memory dictionary ---
# This client will connect to the Redis instance specified in the config.
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

# Pydantic model for response validation
class TaskStatusModel(BaseModel):
    task_id: str
    status: str
    progress: float
    stage: str
    result: Any = None

def get_task(task_id: str) -> Optional[dict]:
    """Retrieves a task from Redis."""
    task_json = redis_client.get(f"task:{task_id}")
    return json.loads(task_json) if task_json else None

def set_task(task_id: str, task_data: dict):
    """Saves a task to Redis with a 24-hour expiration."""
    redis_client.set(f"task:{task_id}", json.dumps(task_data), ex=86400) # 24 hours

def update_task_progress(task_id: str, progress: float, stage: str):
    """Updates the progress of a task in Redis."""
    task = get_task(task_id)
    if task:
        task.update({
            'progress': progress,
            'status': 'running',
            'stage': stage
        })
        set_task(task_id, task)

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
                            setattr(file_obj, 'filename', member_name)
                            new_messages = process_single_file(file_obj, seen_hashes)
                            all_unique_messages.extend(new_messages)
                        else:
                            print(f"Task {task_id}: Skipping empty file {member_name}")
        else:
            print(f"Task {task_id}: Detected single file.")
            update_task_progress(task_id, 40.0, "Parsing single file")
            with open(temp_file_path, 'rb') as f:
                setattr(f, 'filename', filename)
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
