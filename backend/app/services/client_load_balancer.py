import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from telethon import TelegramClient
from telethon.errors import FloodWaitError

logger = logging.getLogger("load_balancer")


class ClientStats:
    def __init__(self, account_id: int, client: TelegramClient):
        self.account_id = account_id
        self.client = client
        self.requests_count = 0
        self.errors_count = 0
        self.flood_wait_until: Optional[datetime] = None
        self.last_request: Optional[datetime] = None
        self.stories_downloaded = 0
        self.members_scraped = 0
    
    def is_available(self) -> bool:
        if self.flood_wait_until:
            if datetime.utcnow() < self.flood_wait_until:
                return False
            self.flood_wait_until = None
        return self.client and self.client.is_connected()
    
    def set_flood_wait(self, seconds: int):
        wait_time = min(seconds + 5, 300)
        self.flood_wait_until = datetime.utcnow() + timedelta(seconds=wait_time)
        logger.warning(f"[LoadBalancer] Account {self.account_id} FloodWait for {wait_time}s")
    
    def record_request(self):
        self.requests_count += 1
        self.last_request = datetime.utcnow()
    
    def record_error(self):
        self.errors_count += 1
    
    def get_flood_wait_remaining(self) -> int:
        if not self.flood_wait_until:
            return 0
        remaining = (self.flood_wait_until - datetime.utcnow()).total_seconds()
        return max(0, int(remaining))


class ClientLoadBalancer:
    def __init__(self):
        self._clients: Dict[int, ClientStats] = {}
        self._round_robin_index = 0
        self._lock = asyncio.Lock()
    
    def register_clients(self, clients_dict: Dict[int, TelegramClient]):
        for account_id, client in clients_dict.items():
            if client and client.is_connected():
                if account_id not in self._clients:
                    self._clients[account_id] = ClientStats(account_id, client)
                else:
                    self._clients[account_id].client = client
        
        disconnected = [aid for aid, stats in self._clients.items() 
                       if not stats.client or not stats.client.is_connected()]
        for aid in disconnected:
            del self._clients[aid]
        
        logger.info(f"[LoadBalancer] Registered {len(self._clients)} active clients")
    
    def get_available_clients(self) -> List[ClientStats]:
        return [stats for stats in self._clients.values() if stats.is_available()]
    
    async def get_next_client(self) -> Optional[Tuple[int, TelegramClient]]:
        async with self._lock:
            available = self.get_available_clients()
            if not available:
                return None
            
            available.sort(key=lambda s: s.requests_count)
            best = available[0]
            best.record_request()
            return best.account_id, best.client
    
    async def get_client_round_robin(self) -> Optional[Tuple[int, TelegramClient]]:
        async with self._lock:
            available = self.get_available_clients()
            if not available:
                return None
            
            self._round_robin_index = (self._round_robin_index + 1) % len(available)
            selected = available[self._round_robin_index]
            selected.record_request()
            return selected.account_id, selected.client
    
    def report_flood_wait(self, account_id: int, seconds: int):
        if account_id in self._clients:
            self._clients[account_id].set_flood_wait(seconds)
    
    def report_error(self, account_id: int):
        if account_id in self._clients:
            self._clients[account_id].record_error()
    
    def report_success(self, account_id: int, stories: int = 0, members: int = 0):
        if account_id in self._clients:
            self._clients[account_id].stories_downloaded += stories
            self._clients[account_id].members_scraped += members
    
    def get_stats(self) -> Dict[str, Any]:
        stats = {
            "total_clients": len(self._clients),
            "available_clients": len(self.get_available_clients()),
            "accounts": []
        }
        
        for account_id, client_stats in self._clients.items():
            stats["accounts"].append({
                "account_id": account_id,
                "available": client_stats.is_available(),
                "requests": client_stats.requests_count,
                "errors": client_stats.errors_count,
                "stories_downloaded": client_stats.stories_downloaded,
                "members_scraped": client_stats.members_scraped,
                "flood_wait_remaining": client_stats.get_flood_wait_remaining()
            })
        
        return stats
    
    def all_clients_blocked(self) -> bool:
        return len(self.get_available_clients()) == 0
    
    def get_min_flood_wait_remaining(self) -> int:
        min_wait = float('inf')
        for stats in self._clients.values():
            remaining = stats.get_flood_wait_remaining()
            if remaining > 0:
                min_wait = min(min_wait, remaining)
        return int(min_wait) if min_wait != float('inf') else 0


load_balancer = ClientLoadBalancer()
