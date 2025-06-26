import uuid
from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException, status
from ..logic.tasks import run_parsing_job, tasks, TaskStatusModel

# APIRouter allows organizing routes into modules
router = APIRouter()

@router.post("/parse", status_code=status.HTTP_202_ACCEPTED, tags=["Parsing"])
async def create_parsing_task(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """
    Accepts a chat file, starts a background parsing job,
    and immediately returns a task ID for status polling.
    """
    task_id = str(uuid.uuid4())

    file_content = await file.read()
    filename = file.filename

    # Store initial task state
    tasks[task_id] = {
        "task_id": task_id, "status": "pending", "progress": 0.0,
        "stage": "Queued", "result": None
    }

    # Add the long-running job to the background
    background_tasks.add_task(run_parsing_job, file_content, filename, task_id)

    return {"task_id": task_id, "status": "pending", "message": "Parsing task has been queued."}


@router.get("/status/{task_id}", response_model=TaskStatusModel, tags=["Parsing"])
async def get_task_status(task_id: str):
    """
    Poll this endpoint with a task ID to get the current status,
    progress, and final result of the parsing job.
    """
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task
