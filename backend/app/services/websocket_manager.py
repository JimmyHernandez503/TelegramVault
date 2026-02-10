import asyncio
import json
from typing import Dict, Set, Any, Optional
from datetime import datetime
from fastapi import WebSocket
from dataclasses import dataclass, asdict


@dataclass
class WSMessage:
    event: str
    data: Any
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))


class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self.user_connections: Dict[int, Set[WebSocket]] = {}
        self.connection_metadata: Dict[WebSocket, Dict[str, Any]] = {}
    
    async def connect(
        self, 
        websocket: WebSocket, 
        channel: str = "general",
        user_id: Optional[int] = None
    ):
        await websocket.accept()
        
        if channel not in self.active_connections:
            self.active_connections[channel] = set()
        self.active_connections[channel].add(websocket)
        
        if user_id:
            if user_id not in self.user_connections:
                self.user_connections[user_id] = set()
            self.user_connections[user_id].add(websocket)
        
        self.connection_metadata[websocket] = {
            "channel": channel,
            "user_id": user_id,
            "connected_at": datetime.utcnow().isoformat()
        }
        
        await self.broadcast(
            channel,
            WSMessage(
                event="user_connected",
                data={"user_id": user_id, "channel": channel}
            )
        )
    
    def disconnect(self, websocket: WebSocket):
        metadata = self.connection_metadata.get(websocket, {})
        channel = metadata.get("channel", "general")
        user_id = metadata.get("user_id")
        
        if channel in self.active_connections:
            self.active_connections[channel].discard(websocket)
        
        if user_id and user_id in self.user_connections:
            self.user_connections[user_id].discard(websocket)
        
        if websocket in self.connection_metadata:
            del self.connection_metadata[websocket]
    
    async def send_personal(self, websocket: WebSocket, message: WSMessage):
        try:
            await websocket.send_text(message.to_json())
        except Exception:
            self.disconnect(websocket)
    
    async def send_to_user(self, user_id: int, message: WSMessage):
        connections = self.user_connections.get(user_id, set())
        for connection in list(connections):
            await self.send_personal(connection, message)
    
    async def broadcast(self, channel: str, message: WSMessage, exclude: WebSocket = None):
        connections = self.active_connections.get(channel, set())
        for connection in list(connections):
            if connection != exclude:
                await self.send_personal(connection, message)
    
    async def broadcast_all(self, message: WSMessage):
        for channel in self.active_connections:
            await self.broadcast(channel, message)
    
    def get_channel_count(self, channel: str) -> int:
        return len(self.active_connections.get(channel, set()))
    
    def get_total_connections(self) -> int:
        return sum(len(conns) for conns in self.active_connections.values())
    
    def get_channels(self) -> Dict[str, int]:
        return {
            channel: len(conns) 
            for channel, conns in self.active_connections.items()
        }


ws_manager = ConnectionManager()


async def notify_new_message(message_data: Dict[str, Any]):
    await ws_manager.broadcast(
        "messages",
        WSMessage(event="new_message", data=message_data)
    )


async def notify_new_detection(detection_data: Dict[str, Any]):
    await ws_manager.broadcast(
        "detections",
        WSMessage(event="new_detection", data=detection_data)
    )


async def notify_task_update(task_data: Dict[str, Any]):
    await ws_manager.broadcast(
        "tasks",
        WSMessage(event="task_update", data=task_data)
    )


async def notify_account_status(account_id: int, status: str):
    await ws_manager.broadcast(
        "accounts",
        WSMessage(
            event="account_status_changed",
            data={"account_id": account_id, "status": status}
        )
    )


async def notify_enrichment_complete(enrichment_data: Dict[str, Any]):
    """
    Notify clients that user enrichment has completed.
    
    Args:
        enrichment_data: Dictionary containing enriched user information
            - telegram_id: User's Telegram ID
            - user_id: Database user ID
            - display_name: User's display name
            - username: User's username
            - photo_path: Path to profile photo
            - is_premium: Premium status
            - is_bot: Bot status
    """
    await ws_manager.broadcast(
        "user_enrichment",
        WSMessage(event="enrichment_complete", data=enrichment_data)
    )


async def notify_enrichment_failed(telegram_id: int, error: str):
    """
    Notify clients that user enrichment has failed.
    
    Args:
        telegram_id: User's Telegram ID
        error: Error message describing the failure
    """
    await ws_manager.broadcast(
        "user_enrichment",
        WSMessage(
            event="enrichment_failed",
            data={"telegram_id": telegram_id, "error": error}
        )
    )
