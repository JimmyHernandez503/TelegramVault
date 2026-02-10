import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger("rate_limit_manager")


@dataclass
class AccountRateLimitState:
    account_id: int
    flood_wait_until: Optional[datetime] = None
    consecutive_flood_waits: int = 0
    total_flood_waits: int = 0
    last_flood_wait_seconds: int = 0
    last_successful_request: Optional[datetime] = None
    total_requests: int = 0
    total_errors: int = 0
    is_potentially_banned: bool = False
    ban_detection_reason: Optional[str] = None
    adaptive_delay_seconds: float = 0.5
    
    def record_success(self):
        self.last_successful_request = datetime.utcnow()
        self.total_requests += 1
        self.consecutive_flood_waits = 0
        if self.adaptive_delay_seconds > 0.5:
            self.adaptive_delay_seconds = max(0.5, self.adaptive_delay_seconds * 0.9)
    
    def record_flood_wait(self, seconds: int):
        self.flood_wait_until = datetime.utcnow() + timedelta(seconds=seconds)
        self.consecutive_flood_waits += 1
        self.total_flood_waits += 1
        self.last_flood_wait_seconds = seconds
        self.adaptive_delay_seconds = min(30.0, self.adaptive_delay_seconds * 1.5 + 1.0)
        
        if seconds >= 3600:
            self.is_potentially_banned = True
            self.ban_detection_reason = f"FloodWait de {seconds}s (>1 hora) indica posible ban temporal"
        elif self.consecutive_flood_waits >= 5:
            self.is_potentially_banned = True
            self.ban_detection_reason = f"{self.consecutive_flood_waits} FloodWaits consecutivos"
    
    def record_error(self, error_type: str = "unknown"):
        self.total_errors += 1
        if error_type in ["UserBannedInChannel", "ChannelPrivate", "ChatWriteForbidden"]:
            self.is_potentially_banned = True
            self.ban_detection_reason = f"Error de acceso: {error_type}"
    
    def is_blocked(self) -> bool:
        if self.flood_wait_until:
            return datetime.utcnow() < self.flood_wait_until
        return False
    
    def get_wait_time(self) -> int:
        if self.flood_wait_until:
            remaining = (self.flood_wait_until - datetime.utcnow()).total_seconds()
            return max(0, int(remaining))
        return 0
    
    def clear_ban_status(self):
        self.is_potentially_banned = False
        self.ban_detection_reason = None
        self.consecutive_flood_waits = 0


class RateLimitManager:
    def __init__(self):
        self._accounts: Dict[int, AccountRateLimitState] = {}
        self._global_slowdown = False
        self._global_slowdown_until: Optional[datetime] = None
        self._stats = {
            "total_flood_waits": 0,
            "total_bans_detected": 0,
            "global_slowdowns": 0
        }
    
    def get_or_create_account(self, account_id: int) -> AccountRateLimitState:
        if account_id not in self._accounts:
            self._accounts[account_id] = AccountRateLimitState(account_id=account_id)
        return self._accounts[account_id]
    
    def record_success(self, account_id: int):
        account = self.get_or_create_account(account_id)
        account.record_success()
    
    def record_flood_wait(self, account_id: int, seconds: int):
        account = self.get_or_create_account(account_id)
        account.record_flood_wait(seconds)
        self._stats["total_flood_waits"] += 1
        
        if account.is_potentially_banned:
            self._stats["total_bans_detected"] += 1
            logger.warning(f"[RateLimitManager] Posible ban detectado para cuenta {account_id}: {account.ban_detection_reason}")
        
        blocked_count = sum(1 for a in self._accounts.values() if a.is_blocked())
        total_count = len(self._accounts)
        
        if total_count > 0 and blocked_count / total_count >= 0.5:
            self._trigger_global_slowdown(seconds)
    
    def record_error(self, account_id: int, error_type: str):
        account = self.get_or_create_account(account_id)
        account.record_error(error_type)
        
        if account.is_potentially_banned:
            self._stats["total_bans_detected"] += 1
            logger.warning(f"[RateLimitManager] Posible ban por error para cuenta {account_id}: {account.ban_detection_reason}")
    
    def _trigger_global_slowdown(self, base_seconds: int):
        slowdown_duration = min(base_seconds * 2, 3600)
        self._global_slowdown = True
        self._global_slowdown_until = datetime.utcnow() + timedelta(seconds=slowdown_duration)
        self._stats["global_slowdowns"] += 1
        logger.warning(f"[RateLimitManager] Activando slowdown global por {slowdown_duration}s - 50%+ de cuentas bloqueadas")
    
    def is_global_slowdown(self) -> bool:
        if self._global_slowdown and self._global_slowdown_until:
            if datetime.utcnow() >= self._global_slowdown_until:
                self._global_slowdown = False
                self._global_slowdown_until = None
                logger.info("[RateLimitManager] Slowdown global terminado")
                return False
            return True
        return False
    
    def get_recommended_delay(self, account_id: int) -> float:
        account = self.get_or_create_account(account_id)
        base_delay = account.adaptive_delay_seconds
        
        if self.is_global_slowdown():
            base_delay *= 3.0
        
        if account.is_potentially_banned:
            base_delay *= 5.0
        
        return base_delay
    
    async def wait_if_needed(self, account_id: int) -> bool:
        account = self.get_or_create_account(account_id)
        
        if account.is_blocked():
            wait_time = account.get_wait_time()
            if wait_time > 0:
                logger.info(f"[RateLimitManager] Cuenta {account_id} bloqueada, esperando {wait_time}s")
                await asyncio.sleep(min(wait_time, 60))
                return True
        
        recommended_delay = self.get_recommended_delay(account_id)
        if recommended_delay > 0.5:
            await asyncio.sleep(recommended_delay)
        
        return False
    
    def get_available_account(self) -> Optional[int]:
        available = [
            (acc_id, acc) for acc_id, acc in self._accounts.items()
            if not acc.is_blocked() and not acc.is_potentially_banned
        ]
        
        if not available:
            non_banned = [
                (acc_id, acc) for acc_id, acc in self._accounts.items()
                if not acc.is_blocked()
            ]
            if non_banned:
                return min(non_banned, key=lambda x: x[1].adaptive_delay_seconds)[0]
            return None
        
        return min(available, key=lambda x: x[1].adaptive_delay_seconds)[0]
    
    def clear_ban_status(self, account_id: int):
        if account_id in self._accounts:
            self._accounts[account_id].clear_ban_status()
            logger.info(f"[RateLimitManager] Estado de ban limpiado para cuenta {account_id}")
    
    def get_status(self) -> Dict[str, Any]:
        accounts_status = []
        for acc_id, acc in self._accounts.items():
            accounts_status.append({
                "account_id": acc_id,
                "is_blocked": acc.is_blocked(),
                "wait_time_remaining": acc.get_wait_time(),
                "is_potentially_banned": acc.is_potentially_banned,
                "ban_reason": acc.ban_detection_reason,
                "consecutive_flood_waits": acc.consecutive_flood_waits,
                "total_flood_waits": acc.total_flood_waits,
                "adaptive_delay": round(acc.adaptive_delay_seconds, 2),
                "total_requests": acc.total_requests,
                "total_errors": acc.total_errors
            })
        
        blocked_count = sum(1 for a in self._accounts.values() if a.is_blocked())
        banned_count = sum(1 for a in self._accounts.values() if a.is_potentially_banned)
        
        return {
            "global_slowdown": self.is_global_slowdown(),
            "global_slowdown_remaining": (
                int((self._global_slowdown_until - datetime.utcnow()).total_seconds())
                if self._global_slowdown_until and self._global_slowdown else 0
            ),
            "total_accounts": len(self._accounts),
            "blocked_accounts": blocked_count,
            "potentially_banned_accounts": banned_count,
            "stats": self._stats,
            "accounts": accounts_status
        }


rate_limit_manager = RateLimitManager()
