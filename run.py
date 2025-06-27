# This is the new content for your run.py file.
# It is designed to run your Flask application for local development.

from app.main import app

# The __name__ == "__main__" check is standard Python practice.
# It ensures this server only runs when you execute "python run.py" directly.
if __name__ == "__main__":
    # Instead of uvicorn.run(), we use app.run() which is the built-in
    # way to start the Flask development server.
    #
    # debug=True will automatically reload the server when you save changes,
    # just like uvicorn did.
    app.run(host="0.0.0.0", port=8000, debug=True)
