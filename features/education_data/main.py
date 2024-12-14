"""
Main application file for the Education Data Analysis System
"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.requests import Request
import uvicorn
import os

from features.education_data.router.analysis_router import router as analysis_router

app = FastAPI(title="Education Data Analysis System")

# Mount static files
app.mount("/static", StaticFiles(directory="features/education_data/static"), name="static")

# Configure templates
templates = Jinja2Templates(directory="features/education_data/templates")

# Include routers
app.include_router(analysis_router)

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """
    Serve the main dashboard page
    """
    return templates.TemplateResponse("analysis_dashboard.html", {"request": request})

if __name__ == "__main__":
    # Create static directory if it doesn't exist
    os.makedirs("features/education_data/static", exist_ok=True)
    
    # Run the application
    uvicorn.run("features.education_data.main:app", host="0.0.0.0", port=8000, reload=True)
