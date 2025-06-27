# This file is now a simple logic module, with all database and task logic removed.

import os
import json
import tempfile
from zipfile import ZipFile, is_zipfile

# Import the actual parsing functions that do the real work.
from ..parsers.main_parser import process_single_file, deduplicate_and_sort_messages

def parse_file_and_get_results(file_content: bytes, filename: str) -> dict:
    """
    This is the main function. It takes a file, processes it completely,
    and returns the final result as a dictionary. It is a single, blocking operation.
    """
    try:
        print(f"Stateless worker received file: {filename}")

        # Use a temporary file to handle both single files and zips uniformly
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name

        all_unique_messages = []
        seen_hashes = set()

        if is_zipfile(temp_file_path):
            print("Detected ZIP file. Extracting and processing...")
            with ZipFile(temp_file_path, 'r') as archive:
                for member_name in archive.namelist():
                    if member_name.endswith('/'): continue # Skip directories
                    with archive.open(member_name) as file_obj:
                        file_obj.filename = member_name
                        if file_obj.peek(1): # Check if file has content
                            new_messages = process_single_file(file_obj, seen_hashes)
                            all_unique_messages.extend(new_messages)
        else:
            print("Processing single file...")
            with open(temp_file_path, 'rb') as f:
                f.filename = filename
                new_messages = process_single_file(f, seen_hashes)
                all_unique_messages.extend(new_messages)

        # Clean up the temporary file
        os.remove(temp_file_path)

        print("Deduplicating and sorting final messages...")
        final_messages = deduplicate_and_sort_messages(all_unique_messages)

        # Build the final result object to be returned
        result = {
            "messages": final_messages,
            "statistics": {
                "total_messages": len(final_messages),
                "unique_senders": len(set(msg.get('sender', 'Unknown') for msg in final_messages)),
                "file_processed": filename
            }
        }
        print("Processing complete. Returning results.")
        return result

    except Exception as e:
        print(f"ERROR during stateless parsing: {e}")
        # Return an error object in the same format
        return {"error": str(e)}

