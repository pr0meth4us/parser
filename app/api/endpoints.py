# This file is now much simpler. It only has one endpoint.

from flask import Blueprint, request, jsonify
from ..logic.tasks import parse_file_and_get_results

# We no longer have a url_prefix, the endpoint will be directly at /parse
api_blueprint = Blueprint('api', __name__)


@api_blueprint.route("/parse", methods=['POST'])
def parse_endpoint():
    """
    This single endpoint receives a file, calls the synchronous parsing function,
    and returns the complete result in the response.
    The /status endpoint is no longer needed.
    """
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file:
        file_content = file.read()
        filename = file.filename

        # This is now a direct, blocking call. The request will wait here
        # until the parsing is finished.
        result_data = parse_file_and_get_results(file_content, filename)

        if "error" in result_data:
            return jsonify(result_data), 500

        # Return the final, complete JSON data.
        return jsonify(result_data), 200

