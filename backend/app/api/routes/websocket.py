from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional
import json

from backend.app.services.websocket_manager import ws_manager, WSMessage
from backend.app.services.task_queue import task_queue, TaskStatus

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    channel: str = Query(default="general"),
    token: Optional[str] = Query(default=None)
):
    user_id = None
    if token:
        try:
            from backend.app.core.security import decode_token
            payload = decode_token(token)
            user_id = payload.get("sub")
        except Exception:
            pass
    
    await ws_manager.connect(websocket, channel, user_id)
    
    try:
        while True:
            data = await websocket.receive_text()
            
            try:
                message = json.loads(data)
                event = message.get("event", "")
                payload = message.get("data", {})
                
                if event == "ping":
                    await ws_manager.send_personal(
                        websocket,
                        WSMessage(event="pong", data={})
                    )
                
                elif event == "subscribe":
                    new_channel = payload.get("channel")
                    if new_channel:
                        ws_manager.disconnect(websocket)
                        await ws_manager.connect(websocket, new_channel, user_id)
                
                elif event == "get_tasks":
                    tasks = task_queue.get_all_tasks()
                    await ws_manager.send_personal(
                        websocket,
                        WSMessage(
                            event="tasks_list",
                            data=[{
                                "id": t.id,
                                "name": t.name,
                                "status": t.status.value,
                                "progress": t.progress,
                                "created_at": t.created_at.isoformat()
                            } for t in tasks[-50:]]
                        )
                    )
                
                elif event == "get_stats":
                    stats = {
                        "connections": ws_manager.get_total_connections(),
                        "channels": ws_manager.get_channels(),
                        "tasks": {
                            "pending": len(task_queue.get_pending_tasks()),
                            "running": len(task_queue.get_running_tasks())
                        }
                    }
                    await ws_manager.send_personal(
                        websocket,
                        WSMessage(event="stats", data=stats)
                    )
                
            except json.JSONDecodeError:
                await ws_manager.send_personal(
                    websocket,
                    WSMessage(event="error", data={"message": "Invalid JSON"})
                )
    
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


@router.get("/ws/stats")
async def get_ws_stats():
    return {
        "total_connections": ws_manager.get_total_connections(),
        "channels": ws_manager.get_channels()
    }
