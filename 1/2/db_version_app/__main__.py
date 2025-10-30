import uvicorn


if __name__ == "__main__":
    uvicorn.run("db_version_app.web.app:create_app", factory=True)
