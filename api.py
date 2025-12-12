from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "healthy", "message": "API is running"}


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=5000, reload=True)

