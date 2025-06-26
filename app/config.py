# app/config.py

import os

class Settings:
    # We hardcode the list here because free accounts don't have environment variables
    ALLOWED_ORIGINS: list[str] = [
        "http://localhost:5000",
        "https://apparent-nadeen-aupp-54d2fac0.koyeb.app",
        "https://spnnnnn.pythonanywhere.com"  # Your PythonAnywhere domain
    ]
    # REDIS_URL is not used by your current code, so we can ignore it.

settings = Settings()