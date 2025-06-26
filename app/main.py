from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api import endpoints
from .config import settings

# Initialize the main FastAPI application
app = FastAPI(
    title="Chat Parsing Service",
    description="A dedicated microservice to parse chat history files.",
    version="1.0.0"
)

# --- Add CORS Middleware ---
# This is crucial for allowing your gateway app to communicate with this service.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,  # Origins that are allowed to make requests
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Include the API routes from the endpoints module
app.include_router(endpoints.router)

@app.get("/", tags=["Root"])
def read_root():
    """A simple health check endpoint."""
    return {"status": "ok", "message": "Parsing Service is running."}
