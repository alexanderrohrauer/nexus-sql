from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.controllers import (
    dashboard_controller, import_task_controller, researchers_controller,
    institutions_controller, works_controller, chart_controller,
)
from app.db import db
from app.middlewares import cors_middleware
from app.scheduled.scheduler import scheduler


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await db.init()
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)

cors_middleware.add(app)

app.include_router(dashboard_controller.router)
app.include_router(import_task_controller.router)
app.include_router(researchers_controller.router)
app.include_router(institutions_controller.router)
app.include_router(works_controller.router)
app.include_router(chart_controller.router)
