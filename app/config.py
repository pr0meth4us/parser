import os

class Settings:
    # A comma-separated list of allowed origins for CORS.
    # Your main application's URL should be in this list.
    # Example: "http://localhost:5000,https://my-frontend.com"
    ALLOWED_ORIGINS: list[str] = os.environ.get("ALLOWED_ORIGINS", "http://localhost:5000,http://127.0.0.1:5000").split(",")

settings = Settings()
