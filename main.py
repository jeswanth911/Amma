import os
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from backend.query_router import router as query_router
from backend.router import router as data_router
from utils.logger import logger
from controller.predictor import predict_router

# ✅ Ensure required folders exist before app runs
required_folders = [
    "data/uploaded", "data/cleaned", "data/analyzed",
    "data/output", "data/exports", "data/temp"
]
for folder in required_folders:
    os.makedirs(folder, exist_ok=True)

# ✅ FastAPI App Initialization
app = FastAPI(
    title="My AI Data Agent",
    description="A FastAPI backend that automates data ingestion, cleaning, analysis, and querying with natural language.",
    version="1.0.0"
)

app = FastAPI(
    title="MyBAI - AI Data Agent",
    description="Upload files → Clean → Analyze → Convert to SQLite → Ask questions in natural language",
    version="1.0.0"
)




# ✅ CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update to your frontend domain in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Global Exception Handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred. Please try again later."}
    )

# ✅ Logging Middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code}")
    return response

# ✅ Add Router
app.include_router(data_router, prefix="/api", tags=["Data Ingestion"])
app.include_router(query_router)  # /api/ask
app.include_router(predict_router, prefix="/api", tags=["Prediction"])
app.include_router(data_router, prefix="/data", tags=["Data Ingestion"])

# ✅ Local Run Entrypoint
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    
@app.get("/")
def root():
    return {"message": "🚀 My AI Data Agent is running!"}
