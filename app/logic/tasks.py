import os
import json
import tempfile
from zipfile import ZipFile, is_zipfile
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from ..parsers.main_parser import process_single_file, deduplicate_and_sort_messages
from ..config import settings

try:
    connection_pool = pool.SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        dsn=settings.DATABASE_URL
    )
    print("Successfully created PostgreSQL connection pool.")
except (Exception, psycopg2.DatabaseError) as error:
    print("Error while creating PostgreSQL connection pool", error)
    raise

@contextmanager
def get_db_connection():
    """Context manager to get a connection from the pool and ensure it's returned."""
    conn = None
    try:
        conn = connection_pool.getconn()
        yield conn
    finally:
        if conn:
            connection_pool.putconn(conn)

def setup_database():
    """
    Creates the tasks table with a JSONB column to store flexible data.
    Also creates a function and trigger to handle automatic expiration.
    """
    create_table_command = """
    CREATE TABLE IF NOT EXISTS parsing_tasks (
        id SERIAL PRIMARY KEY,
        task_id UUID NOT NULL UNIQUE,
        task_data JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    create_cleanup_function_command = """
    CREATE OR REPLACE FUNCTION delete_old_parsing_tasks() RETURNS void AS $$
    BEGIN
        DELETE FROM parsing_tasks WHERE created_at < NOW() - INTERVAL '24 hours';
    END;
    $$ LANGUAGE plpgsql;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_command)
                cur.execute(create_cleanup_function_command)
                conn.commit()
        print("Database table and cleanup function ensured.")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error during database setup", error)

setup_database()

def get_task(task_id: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT task_data FROM parsing_tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            return row[0] if row else None

def set_task(task_id: str, task_data: dict):
    sql = """
    INSERT INTO parsing_tasks (task_id, task_data)
    VALUES (%s, %s)
    ON CONFLICT (task_id) DO UPDATE SET task_data = EXCLUDED.task_data;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (task_id, json.dumps(task_data)))
            conn.commit()

def update_task_progress(task_id: str, progress: float, stage: str):
    """Updates the progress of a task in PostgreSQL using JSONB functions."""
    task = get_task(task_id)
    if task:
        task.update({
            'progress': progress,
            'status': 'running',
            'stage': stage
        })
        set_task(task_id, task)

def run_parsing_job(file_content: bytes, filename: str, task_id: str):
    try:
        print(f"Background thread started for Task {task_id}: Parsing '{filename}'")
        update_task_progress(task_id, 5.0, "Initializing parser")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name

        # The entire file processing logic is identical to the previous versions
        all_unique_messages = []
        seen_hashes = set()
        update_task_progress(task_id, 10.0, "Analyzing file structure")
        if is_zipfile(temp_file_path):
            file_count, valid_files = count_valid_files_in_zip(temp_file_path)
            if file_count == 0:
                raise ValueError("No valid content files found in ZIP.")
            update_task_progress(task_id, 15.0, f"Processing ZIP archive ({file_count} files)")
            with ZipFile(temp_file_path, 'r') as archive:
                for i, member_name in enumerate(valid_files):
                    file_progress = 15 + ((i + 1) / file_count) * 70
                    update_task_progress(task_id, file_progress, f"Processing file {i + 1}/{file_count}")
                    with archive.open(member_name) as file_obj:
                        file_obj.filename = member_name
                        if file_obj.peek(1):
                            all_unique_messages.extend(process_single_file(file_obj, seen_hashes))
        else:
            update_task_progress(task_id, 30.0, f"Processing single file: {filename}")
            with open(temp_file_path, 'rb') as f:
                f.filename = filename
                all_unique_messages.extend(process_single_file(f, seen_hashes))

        os.remove(temp_file_path)
        update_task_progress(task_id, 90.0, "Deduplicating and sorting messages")
        final_result = deduplicate_and_sort_messages(all_unique_messages)
        total_messages = len(final_result)
        unique_senders = len(set(msg.get('sender', 'Unknown') for msg in final_result))
        platforms = list(set(msg.get('source', 'Unknown') for msg in final_result))

        completed_task_data = {
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
        }
        set_task(task_id, completed_task_data)
        print(f"Task {task_id}: Completed and result saved to Neon.")

    except Exception as e:
        error_message = f"Task {task_id}: Failed with error: {e}"
        print(error_message)
        failure_data = {
            "status": "failed",
            "stage": f"Failed: {str(e)}",
            "progress": 0.0,
            "result": {"error": str(e)}
        }
        set_task(task_id, failure_data)

def count_valid_files_in_zip(zip_path):
    """Helper function to count valid files in a ZIP archive."""
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
