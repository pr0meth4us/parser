# This is your new app/api/endpoints.py file, rewritten for Flask.

import uuid
import threading
from flask import Blueprint, request, jsonify, current_app
from ..logic.tasks import run_parsing_job, set_task, get_task, TaskStatusModel

# In Flask, we use a 'Blueprint' instead of an 'APIRouter'.
# The first argument, 'api', is the name of the blueprint.
# The second, __name__, is standard practice.
# The url_prefix will make all routes in this file start with '/api'.
api_blueprint = Blueprint('api', __name__, url_prefix='/api')

def start_background_task(app, file_content, filename, task_id):
    """
    Helper function to run the parsing job in a background thread.
    This is the Flask equivalent of FastAPI's BackgroundTasks.
    """
    def run_job():
        # The background thread needs the application context to work correctly
        with app.app_context():
            run_parsing_job(file_content, filename, task_id)

    thread = threading.Thread(target=run_job)
    thread.daemon = True
    thread.start()

@api_blueprint.route("/parse", methods=['POST'])
def create_parsing_task():
    """ The /parse endpoint, rewritten for Flask. """
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']

    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file:
        task_id = str(uuid.uuid4())
        file_content = file.read()
        filename = file.filename

        initial_task_data = {
            "task_id": task_id, "status": "pending", "progress": 0.0,
            "stage": "Queued", "result": None
        }
        set_task(task_id, initial_task_data)

        # We start the background task, passing it the current Flask app context
        start_background_task(current_app._get_current_object(), file_content, filename, task_id)

        response_data = {"task_id": task_id, "status": "pending", "message": "Parsing task has been queued."}
        return jsonify(response_data), 202

@api_blueprint.route("/status/<task_id>", methods=['GET'])
def get_task_status(task_id: str):
    """ The /status endpoint, rewritten for Flask. """
    task = get_task(task_id)
    if not task:
        return jsonify({"detail": "Task not found"}), 404
    # We can directly return the task dictionary; Flask will jsonify it.
    return jsonify(task)
