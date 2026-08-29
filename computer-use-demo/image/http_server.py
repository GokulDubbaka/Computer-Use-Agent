from uvicorn import run


if __name__ == "__main__":
    run("computer_use_demo.server:app", host="0.0.0.0", port=8080, log_level="info")
