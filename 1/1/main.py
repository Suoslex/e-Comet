import uvicorn

if __name__ == "__main__":
    uvicorn.run("db_version_app.app:create_app", factory=True)
