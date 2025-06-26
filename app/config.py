import os

class Settings:
    ALLOWED_ORIGINS: list[str] = os.environ.get("ALLOWED_ORIGINS", "http://localhost:5000,https://apparent-nadeen-aupp-54d2fac0.koyeb.app").split(",")

settings = Settings()
