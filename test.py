import time
import uvicorn

from fastapi import FastAPI
from fastotp.supervisor import setup_otp_supervisor, queue_task, run_blocking_task, queue_service_request
from fastotp.service import HelloWorldService
from uvicorn.config import LOGGING_CONFIG

app = FastAPI()
setup_otp_supervisor(app, services=[HelloWorldService()], worker_cores=1)




def wasteful_computation(log=None):
    time.sleep(5)
    log.info(f"5^3 = {5**3}")
    return 5**3

@app.get("/sync")
def root():
    return {"result": run_blocking_task(wasteful_computation)}

    

@app.get("/async")
def root():
    queue_service_request(service_name="HelloWorld", priority=5, args=("Hello Ether",), kwargs={})
    return {"message": "Hello World"}

if __name__ == '__main__':
	print(list(LOGGING_CONFIG["formatters"].keys()))
	LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(levelprefix)s] %(message)s service=api"
	LOGGING_CONFIG["formatters"]["access"]["fmt"] = "%(asctime)s [%(levelprefix)s] %(message)s"
	uvicorn.run(app, workers=1)