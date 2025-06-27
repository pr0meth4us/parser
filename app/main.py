# This is the correct version of app/main.py for your Flask application.

from flask import Flask, jsonify
from flask_cors import CORS
# This import is correct because endpoints.py is in the 'api' subdirectory.
from .api.endpoints import api_blueprint
from .config import settings

# Initialize the main Flask (WSGI) application
app = Flask(__name__)

# --- Add CORS Middleware ---
CORS(app, origins=settings.ALLOWED_ORIGINS, supports_credentials=True)

# --- Register the API routes ---
# This line tells our main app about all the routes in the endpoints.py file.
app.register_blueprint(api_blueprint)

@app.route("/")
def read_root():
    """A simple health check endpoint."""
    return jsonify({"status": "ok", "message": "Parsing Service is running."})
