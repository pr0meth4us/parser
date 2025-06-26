import os
import tempfile
from pydantic import BaseModel
from typing import Dict, Any
from zipfile import ZipFile, is_zipfile

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

        # Use a temporary file to handle the uploaded content
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name

        all_unique_messages = []
        seen_hashes = set()

        if is_zipfile(temp_file_path):
            print(f"Task {task_id}: Detected ZIP file. Extracting members...")
            update_task_progress(task_id, 15.0, "Extracting from ZIP archive")
            with ZipFile(temp_file_path, 'r') as archive:
                # --- CORRECTED: Stricter filtering for valid files ---
                valid_extensions = ('.json', '.html', '.htm')
                namelist = [
                    m for m in archive.namelist()
                    if not m.endswith('/')                               # Ignore directories
                       and not m.startswith('__MACOSX/')                    # Ignore macOS metadata folder
                       and os.path.basename(m) and not os.path.basename(m).startswith('.') # Ignore hidden files like .DS_Store
                       and m.lower().endswith(valid_extensions)
                ]

                print(f"Task {task_id}: Found {len(namelist)} valid files in ZIP.")

                if not namelist:
                    raise ValueError("No valid content files (.json, .html) found in the ZIP archive.")

                # Process each valid file inside the zip
                for i, member_name in enumerate(namelist):
                    zip_progress = 15 + ((i + 1) / len(namelist)) * 60
                    update_task_progress(task_id, zip_progress, f"Parsing file {i+1}/{len(namelist)}: {member_name}")

                    with archive.open(member_name) as file_obj:
                        # Ensure the file is not empty before processing
                        if file_obj.peek(1):
                            setattr(file_obj, 'filename', member_name)
                            new_messages = process_single_file(file_obj, seen_hashes)
                            all_unique_messages.extend(new_messages)
                        else:
                            print(f"Task {task_id}: Skipping empty file {member_name}")

        else:
            # Logic for single file uploads remains the same
            print(f"Task {task_id}: Detected single file.")
            update_task_progress(task_id, 40.0, "Parsing single file")
            with open(temp_file_path, 'rb') as f:
                setattr(f, 'filename', filename)
                new_messages = process_single_file(f, seen_hashes)
                all_unique_messages.extend(new_messages)

        os.remove(temp_file_path)

        update_task_progress(task_id, 80.0, "Finalizing and sorting")
        final_result = deduplicate_and_sort_messages(all_unique_messages)

        tasks[task_id].update({
            "status": "completed", "stage": "Completed",
            "progress": 100.0, "result": final_result
        })
        print(f"Task {task_id}: Completed successfully. Found {len(final_result)} total messages.")

    except Exception as e:
        error_message = f"Task {task_id}: Failed with error: {e}"
        print(error_message)
        tasks[task_id].update({
            "status": "failed", "stage": "Failed",
            "result": {"error": str(e)}
        })

