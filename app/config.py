import os
from dotenv import load_dotenv
load_dotenv()
class Settings:
    ALLOWED_ORIGINS: list[str] = os.environ.get("ALLOWED_ORIGINS", "http://localhost:5000,https://apparent-nadeen-aupp-54d2fac0.koyeb.app").split(",")
    REDIS_URL: str = os.getenv("REDIS_URL")


settings = Settings()