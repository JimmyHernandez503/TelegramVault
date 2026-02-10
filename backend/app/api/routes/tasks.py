from fastapi import APIRouter, Depends, HTTPException
from typing import Optional

from backend.app.api.deps import get_current_user
from backend.app.models.user import AppUser
from backend.app.services.task_queue import task_queue, TaskStatus

router = APIRouter()


@router.get("/")
async def list_tasks(
    status: Optional[str] = None,
    limit: int = 50,
    current_user: AppUser = Depends(get_current_user)
):
    tasks = task_queue.get_all_tasks()
    
    if status:
        try:
            filter_status = TaskStatus(status)
            tasks = [t for t in tasks if t.status == filter_status]
        except ValueError:
            pass
    
    tasks = sorted(tasks, key=lambda t: t.created_at, reverse=True)[:limit]
    
    return {
        "tasks": [
            {
                "id": t.id,
                "name": t.name,
                "status": t.status.value,
                "progress": t.progress,
                "error": t.error,
                "created_at": t.created_at.isoformat(),
                "started_at": t.started_at.isoformat() if t.started_at else None,
                "completed_at": t.completed_at.isoformat() if t.completed_at else None,
                "metadata": t.metadata
            }
            for t in tasks
        ],
        "total": len(tasks)
    }


@router.get("/{task_id}")
async def get_task(
    task_id: str,
    current_user: AppUser = Depends(get_current_user)
):
    task = task_queue.get_task(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "id": task.id,
        "name": task.name,
        "status": task.status.value,
        "progress": task.progress,
        "result": task.result,
        "error": task.error,
        "created_at": task.created_at.isoformat(),
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "metadata": task.metadata
    }


@router.post("/{task_id}/cancel")
async def cancel_task(
    task_id: str,
    current_user: AppUser = Depends(get_current_user)
):
    success = await task_queue.cancel_task(task_id)
    
    if not success:
        raise HTTPException(status_code=400, detail="Cannot cancel task")
    
    return {"success": True, "message": "Task cancelled"}


@router.get("/stats/summary")
async def get_task_stats(
    current_user: AppUser = Depends(get_current_user)
):
    all_tasks = task_queue.get_all_tasks()
    
    stats = {
        "total": len(all_tasks),
        "pending": len([t for t in all_tasks if t.status == TaskStatus.PENDING]),
        "running": len([t for t in all_tasks if t.status == TaskStatus.RUNNING]),
        "completed": len([t for t in all_tasks if t.status == TaskStatus.COMPLETED]),
        "failed": len([t for t in all_tasks if t.status == TaskStatus.FAILED]),
        "cancelled": len([t for t in all_tasks if t.status == TaskStatus.CANCELLED])
    }
    
    return stats


@router.post("/cleanup")
async def cleanup_old_tasks(
    max_age_hours: int = 24,
    current_user: AppUser = Depends(get_current_user)
):
    removed = task_queue.cleanup_old_tasks(max_age_hours)
    return {"removed": removed}
