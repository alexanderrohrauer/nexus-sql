import logging
from datetime import datetime

from apscheduler.job import Job
from apscheduler.triggers.cron import CronTrigger
from fastapi import APIRouter, HTTPException, BackgroundTasks
from sqlalchemy import delete

from app.db.db import get_session
from app.dtos.import_task import (
    ImportJob, ImportTask, CreateImportTaskRequest, UpdateImportTaskRequest, ResetCursorsRequest,
)
from app.models import Researcher, Work, Institution
from app.scheduled.import_jobs import import_job_map
from app.scheduled.models import ImportJobId, ImportCursor
from app.scheduled.scheduler import scheduler
from app.services import duplicate_detection_service, duplicate_elimination_service

router = APIRouter(prefix="/import-task", tags=["import-task"])

logger = logging.getLogger("uvicorn.error")


@router.get("")
def get_import_task():
    jobs: list[Job] = list(filter(lambda job: job.id in ImportJobId._value2member_map_, scheduler.get_jobs()))
    if len(jobs) > 0:
        job_dtos = [
            ImportJob(
                job_id=ImportJobId(job.id),
                cursor=ImportCursor(**job.kwargs["cursor"]),
                enabled=job.next_run_time is not None,
            )
            for job in jobs
        ]
        return ImportTask(
            cron_expr=str(jobs[0].kwargs["cron_expr"]),
            n_batches=jobs[0].kwargs["n_batches"],
            keywords=jobs[0].kwargs["keywords"],
            jobs=job_dtos,
        )
    return None


def find_job_by_id(id: ImportJobId):
    return scheduler.get_job(id.value)


@router.post("")
def create_import_task(dto: CreateImportTaskRequest):
    jobs = []
    for job_id in [e.value for e in ImportJobId]:
        job_id_enum = ImportJobId(job_id)
        if find_job_by_id(job_id_enum) is not None:
            raise HTTPException(status_code=400, detail="Jobs already created!")
        func = import_job_map.get(job_id_enum)
        try:
            trigger = CronTrigger.from_crontab(dto.cron_expr)
        except ValueError:
            raise HTTPException(status_code=400, detail="Cron expression has wrong format!")
        cursor = ImportCursor()
        args = {"n_batches": dto.n_batches, "keywords": dto.keywords, "cursor": cursor.model_dump(), "cron_expr": dto.cron_expr}
        job = scheduler.add_job(func, trigger, None, args, job_id, replace_existing=True)
        jobs.append(ImportJob(job_id=ImportJobId(job.id), cursor=cursor, enabled=job.next_run_time is not None))
    return ImportTask(cron_expr=dto.cron_expr, n_batches=dto.n_batches, keywords=dto.keywords, jobs=jobs)


@router.put("")
def update_import_task(dto: UpdateImportTaskRequest):
    for job_id in [e.value for e in ImportJobId]:
        job_id_enum = ImportJobId(job_id)
        found_job = find_job_by_id(job_id_enum)
        args = {"n_batches": dto.n_batches, "keywords": dto.keywords, "cron_expr": dto.cron_expr, "cursor": found_job.kwargs["cursor"]}
        try:
            trigger = CronTrigger.from_crontab(dto.cron_expr)
        except ValueError:
            raise HTTPException(status_code=400, detail="Cron expression has wrong format!")
        found_job.modify(trigger=trigger, kwargs=args)
        if job_id_enum not in dto.jobs:
            found_job.pause()
        else:
            found_job.resume()


@router.put("/reset-cursors")
def reset_cursors(dto: ResetCursorsRequest):
    for job_id in dto.jobs:
        found_job = find_job_by_id(job_id)
        found_job.kwargs["cursor"] = ImportCursor().model_dump()
        found_job.modify(kwargs=found_job.kwargs)


@router.post("/run")
def run_all_import_jobs():
    for job_id in [e.value for e in ImportJobId]:
        job_id_enum = ImportJobId(job_id)
        found_job = find_job_by_id(job_id_enum)
        if found_job.next_run_time is not None:
            found_job.modify(next_run_time=datetime.now())


@router.delete("")
def delete_import_task():
    for job_id in [e.value for e in ImportJobId]:
        job_id_enum = ImportJobId(job_id)
        found_job = find_job_by_id(job_id_enum)
        found_job.remove()


async def deduplication():
    logger.info("Starting deduplication.")
    await duplicate_detection_service.deduplicate_works()
    await duplicate_detection_service.deduplicate_researchers()
    await duplicate_detection_service.deduplicate_institutions()
    logger.info("Deduplication finished.")


@router.post("/run-duplicate-detection")
async def run_duplicate_detection(background_tasks: BackgroundTasks):
    background_tasks.add_task(deduplication)


async def deduplication_elimination():
    logger.info("Starting deduplication elimination.")
    await duplicate_elimination_service.eliminate_institutions_duplicates()
    await duplicate_elimination_service.eliminate_researcher_duplicates()
    await duplicate_elimination_service.eliminate_work_duplicates()
    async with get_session() as session:
        await session.execute(delete(Institution).where(Institution.marked_for_removal == True))
        await session.execute(delete(Researcher).where(Researcher.marked_for_removal == True))
        await session.execute(delete(Work).where(Work.marked_for_removal == True))
        await session.commit()
    logger.info("Deduplication elimination finished.")


@router.post("/run-duplicate-elimination")
async def run_deduplication_elimination(background_tasks: BackgroundTasks):
    background_tasks.add_task(deduplication_elimination)