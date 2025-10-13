from fastapi import FastAPI
from app.routers import report

app = FastAPI(title="Shift Report API")
app.include_router(report.router)

@app.get("/")
def root():
    return {"status":"ok","message":"Shift Report API running"}
