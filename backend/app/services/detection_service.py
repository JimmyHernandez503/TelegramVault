import re
from typing import List, Dict, Any, Optional
from functools import lru_cache
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.app.models.detection import Detection, RegexDetector
from backend.app.services.live_stats import live_stats
from backend.app.core.config_manager import get_config_manager
from backend.app.core.enhanced_logging_system import enhanced_logging


BUILTIN_PATTERNS = [
    {
        "name": "Email Standard",
        "pattern": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        "category": "email",
        "description": "Standard email addresses",
        "priority": 5
    },
    {
        "name": "Email Obfuscated At",
        "pattern": r"[a-zA-Z0-9._%+-]+\s*[\[\(\{]?\s*(?:@|at|arroba|AT)\s*[\]\)\}]?\s*[a-zA-Z0-9.-]+\s*[\[\(\{]?\s*(?:\.|dot|punto|DOT)\s*[\]\)\}]?\s*[a-zA-Z]{2,}",
        "category": "email",
        "description": "Obfuscated emails (user [at] domain [dot] com)",
        "priority": 4
    },
    {
        "name": "Phone International",
        "pattern": r"\+[1-9]\d{6,14}",
        "category": "phone",
        "description": "International phone numbers with + prefix",
        "priority": 5
    },
    {
        "name": "Phone International Spaced",
        "pattern": r"\+[1-9][\d\s\-\.]{7,18}\d",
        "category": "phone",
        "description": "International phones with spaces/dashes",
        "priority": 5
    },
    {
        "name": "Phone Local 8-10 digits",
        "pattern": r"\b[2-9]\d{7,9}\b",
        "category": "phone",
        "description": "Local phone numbers 8-10 digits",
        "priority": 3
    },
    {
        "name": "Phone with Parentheses",
        "pattern": r"\(?\d{2,4}\)?[\s\.\-]?\d{3,4}[\s\.\-]?\d{3,4}",
        "category": "phone",
        "description": "Phones with parentheses (xxx) xxx-xxxx",
        "priority": 4
    },
    {
        "name": "Phone LATAM Format",
        "pattern": r"\+?(?:502|503|504|505|506|507|51|52|53|54|55|56|57|58|591|592|593|595|598)[\s\-\.]?\d{4}[\s\-\.]?\d{4}",
        "category": "phone",
        "description": "Latin American phone formats",
        "priority": 5
    },
    {
        "name": "Phone US Format",
        "pattern": r"\+?1?[\s\.\-]?\(?\d{3}\)?[\s\.\-]?\d{3}[\s\.\-]?\d{4}",
        "category": "phone",
        "description": "US phone format +1 (xxx) xxx-xxxx",
        "priority": 4
    },
    {
        "name": "Phone WhatsApp Mention",
        "pattern": r"(?:whatsapp|wsp|wa|whats)\s*:?\s*\+?[\d\s\-\.\(\)]{8,18}",
        "category": "phone",
        "description": "WhatsApp mentions with numbers",
        "priority": 5
    },
    {
        "name": "Bitcoin Legacy",
        "pattern": r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b",
        "category": "crypto",
        "description": "Bitcoin legacy addresses (1... or 3...)",
        "priority": 5
    },
    {
        "name": "Bitcoin Bech32",
        "pattern": r"\bbc1[a-zA-HJ-NP-Z0-9]{39,59}\b",
        "category": "crypto",
        "description": "Bitcoin bech32 addresses (bc1...)",
        "priority": 5
    },
    {
        "name": "Ethereum",
        "pattern": r"\b0x[a-fA-F0-9]{40}\b",
        "category": "crypto",
        "description": "Ethereum addresses",
        "priority": 5
    },
    {
        "name": "USDT TRC20",
        "pattern": r"\bT[a-zA-HJ-NP-Z1-9]{33}\b",
        "category": "crypto",
        "description": "USDT TRC20 (TRON) addresses",
        "priority": 5
    },
    {
        "name": "Litecoin",
        "pattern": r"\b[LM][a-km-zA-HJ-NP-Z1-9]{26,33}\b",
        "category": "crypto",
        "description": "Litecoin addresses",
        "priority": 5
    },
    {
        "name": "Monero",
        "pattern": r"\b4[0-9AB][1-9A-HJ-NP-Za-km-z]{93}\b",
        "category": "crypto",
        "description": "Monero addresses",
        "priority": 5
    },
    {
        "name": "Telegram Username",
        "pattern": r"@[a-zA-Z][a-zA-Z0-9_]{4,31}",
        "category": "telegram_username",
        "description": "Telegram @usernames",
        "priority": 5
    },
    {
        "name": "Telegram Username Mention",
        "pattern": r"(?:telegram|tg|user|contact|dm|ig)\s*:?\s*@?[a-zA-Z][a-zA-Z0-9_]{4,31}",
        "category": "telegram_username",
        "description": "Username mentions with prefix",
        "priority": 4
    },
    {
        "name": "Telegram t.me Link",
        "pattern": r"(?:https?://)?t\.me/[a-zA-Z0-9_]+(?:/\d+)?",
        "category": "telegram_link",
        "description": "Telegram t.me links to users/channels",
        "priority": 5
    },
    {
        "name": "Telegram Invite Link New",
        "pattern": r"(?:https?://)?t\.me/\+[a-zA-Z0-9_-]+",
        "category": "invite_link",
        "description": "New format Telegram invite links (t.me/+...)",
        "priority": 6
    },
    {
        "name": "Telegram Invite Link Legacy",
        "pattern": r"(?:https?://)?(?:t\.me|telegram\.me)/joinchat/[a-zA-Z0-9_-]+",
        "category": "invite_link",
        "description": "Legacy Telegram invite links (joinchat/...)",
        "priority": 6
    },
    {
        "name": "Telegram tg:// Link",
        "pattern": r"tg://(?:resolve\?domain=|user\?id=|openmessage\?user_id=|join\?invite=|privatepost\?)[a-zA-Z0-9_=&]+",
        "category": "telegram_link",
        "description": "Telegram deep links (tg://...)",
        "priority": 5
    },
    {
        "name": "Telegram Channel ID",
        "pattern": r"-100\d{10,13}",
        "category": "telegram_link",
        "description": "Telegram channel IDs (-100...)",
        "priority": 4
    },
    {
        "name": "Telegram Private Link",
        "pattern": r"(?:https?://)?t\.me/c/\d+/\d+",
        "category": "telegram_link",
        "description": "Links to specific messages in private channels",
        "priority": 5
    },
    {
        "name": "URL HTTP",
        "pattern": r"https?://[^\s<>\"'\]\)]+",
        "category": "url",
        "description": "HTTP/HTTPS URLs",
        "priority": 5
    },
    {
        "name": "URL WWW",
        "pattern": r"\bwww\.[a-zA-Z0-9][a-zA-Z0-9\-]*\.[a-zA-Z]{2,}[^\s<>\"'\]\)]*",
        "category": "url",
        "description": "URLs starting with www.",
        "priority": 4
    },
    {
        "name": "URL Common Domains",
        "pattern": r"\b[a-zA-Z0-9][a-zA-Z0-9\-]*\.(?:com|net|org|io|co|info|biz|me|tv|cc|xyz|online|site|store|shop|app|dev|link)(?:/[^\s<>\"'\]\)]*)?",
        "category": "url",
        "description": "Common domain URLs without protocol",
        "priority": 3
    },
    {
        "name": "URL Shortened",
        "pattern": r"\b(?:bit\.ly|goo\.gl|tinyurl\.com|t\.co|ow\.ly|is\.gd|buff\.ly|adf\.ly|j\.mp|tr\.im|v\.gd|cutt\.ly|rb\.gy|short\.link)/[a-zA-Z0-9]+",
        "category": "url",
        "description": "Shortened URLs",
        "priority": 5
    },
    {
        "name": "Credit Card Visa",
        "pattern": r"\b4[0-9]{12}(?:[0-9]{3})?\b",
        "category": "credit_card",
        "description": "Visa card numbers",
        "priority": 6
    },
    {
        "name": "Credit Card Mastercard",
        "pattern": r"\b(?:5[1-5][0-9]{2}|222[1-9]|22[3-9][0-9]|2[3-6][0-9]{2}|27[01][0-9]|2720)[0-9]{12}\b",
        "category": "credit_card",
        "description": "Mastercard card numbers",
        "priority": 6
    },
    {
        "name": "MD5 Hash",
        "pattern": r"\b[a-fA-F0-9]{32}\b",
        "category": "hash",
        "description": "MD5 hashes",
        "priority": 3
    },
    {
        "name": "SHA256 Hash",
        "pattern": r"\b[a-fA-F0-9]{64}\b",
        "category": "hash",
        "description": "SHA256 hashes",
        "priority": 3
    },
    {
        "name": "IP Address",
        "pattern": r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b",
        "category": "ip_address",
        "description": "IPv4 addresses",
        "priority": 4
    },
    {
        "name": "Obfuscated Telegram Link Dot",
        "pattern": r"t\s*[\.\(\[\{\|]\s*me\s*/\s*[a-zA-Z0-9_+]+",
        "category": "telegram_link",
        "description": "Obfuscated t.me links with spaces/brackets",
        "priority": 5
    },
    {
        "name": "Instagram Handle",
        "pattern": r"(?:instagram|ig|insta)\s*:?\s*@?[a-zA-Z][a-zA-Z0-9_.]{2,29}",
        "category": "social_media",
        "description": "Instagram handles and mentions",
        "priority": 4
    },
    {
        "name": "Twitter/X Handle",
        "pattern": r"(?:twitter|x|tw)\s*:?\s*@[a-zA-Z][a-zA-Z0-9_]{1,14}",
        "category": "social_media",
        "description": "Twitter/X handles",
        "priority": 4
    },
    {
        "name": "Facebook Profile",
        "pattern": r"(?:facebook|fb)\s*:?\s*(?:facebook\.com/)?[a-zA-Z0-9.]+",
        "category": "social_media",
        "description": "Facebook profiles",
        "priority": 4
    },
    {
        "name": "OnlyFans Link",
        "pattern": r"(?:https?://)?(?:www\.)?onlyfans\.com/[a-zA-Z0-9_]+",
        "category": "social_media",
        "description": "OnlyFans profiles",
        "priority": 5
    },
    {
        "name": "Discord Invite",
        "pattern": r"(?:https?://)?(?:www\.)?discord\.(?:gg|com/invite)/[a-zA-Z0-9]+",
        "category": "invite_link",
        "description": "Discord invite links",
        "priority": 5
    },
    {
        "name": "Signal Group",
        "pattern": r"(?:https?://)?signal\.group/[a-zA-Z0-9#]+",
        "category": "invite_link",
        "description": "Signal group links",
        "priority": 5
    },
    {
        "name": "PayPal Handle",
        "pattern": r"(?:paypal|paypal\.me)\s*:?\s*[a-zA-Z0-9]+",
        "category": "payment",
        "description": "PayPal handles",
        "priority": 4
    },
    {
        "name": "CashApp Handle",
        "pattern": r"\$[a-zA-Z][a-zA-Z0-9]{1,19}",
        "category": "payment",
        "description": "CashApp $tags",
        "priority": 4
    },
    {
        "name": "Venmo Handle",
        "pattern": r"(?:venmo)\s*:?\s*@?[a-zA-Z][a-zA-Z0-9_-]{1,29}",
        "category": "payment",
        "description": "Venmo handles",
        "priority": 4
    },
    {
        "name": "Zelle Info",
        "pattern": r"(?:zelle)\s*:?\s*(?:\+?[\d\s\-\.]+|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+)",
        "category": "payment",
        "description": "Zelle phone/email",
        "priority": 4
    },
    {
        "name": "Bank Account IBAN",
        "pattern": r"\b[A-Z]{2}\d{2}[A-Z0-9]{4,30}\b",
        "category": "bank_account",
        "description": "IBAN bank accounts",
        "priority": 4
    },
    {
        "name": "SSN Format",
        "pattern": r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b",
        "category": "pii",
        "description": "Social Security Number format",
        "priority": 6
    },
    {
        "name": "Document ID Generic",
        "pattern": r"(?:cedula|dni|nit|rut|rfc|curp|passport|pasaporte|id)\s*:?\s*[a-zA-Z0-9\-]{5,20}",
        "category": "pii",
        "description": "Document IDs (cedula, DNI, passport, etc)",
        "priority": 5
    },
]


class DetectionService:
    def __init__(self):
        self._compiled_patterns: Dict[int, re.Pattern] = {}
        self.config = get_config_manager()
        self.logger = enhanced_logging
        
        # Load configuration
        self.cache_size = self.config.get_int("DETECTION_CACHE_SIZE", 1000)
        self.validate_patterns = self.config.get_bool("DETECTION_VALIDATE_PATTERNS", True)
        self.log_compilation_errors = self.config.get_bool("DETECTION_LOG_COMPILATION_ERRORS", True)
    
    def validate_regex_pattern(self, pattern: str) -> bool:
        """
        Validate regex pattern syntax before compilation.
        
        Args:
            pattern: Regex pattern string to validate
            
        Returns:
            bool: True if pattern is valid, False otherwise
        """
        if not pattern:
            return False
        
        try:
            re.compile(pattern)
            return True
        except re.error as e:
            if self.log_compilation_errors:
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(
                            self.logger.log_warning(
                                "DetectionService",
                                "validate_regex_pattern",
                                f"Invalid regex pattern syntax: {pattern[:100]}",
                                details={
                                    "pattern": pattern[:200],
                                    "error": str(e),
                                    "error_type": type(e).__name__
                                }
                            )
                        )
                except Exception:
                    pass  # Fallback if async logging fails
            return False
    
    def _compile_pattern(self, pattern: str) -> Optional[re.Pattern]:
        """
        Compile regex pattern with validation and error logging.
        
        Args:
            pattern: Regex pattern string to compile
            
        Returns:
            Compiled pattern or None if compilation fails
        """
        # Validate pattern first if enabled
        if self.validate_patterns:
            if not self.validate_regex_pattern(pattern):
                return None
        
        try:
            return re.compile(pattern, re.IGNORECASE | re.MULTILINE)
        except re.error as e:
            if self.log_compilation_errors:
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(
                            self.logger.log_error(
                                "DetectionService",
                                "compile_pattern",
                                f"Failed to compile regex pattern: {pattern[:100]}",
                                error=e,
                                details={
                                    "pattern": pattern[:200],
                                    "error": str(e),
                                    "error_type": type(e).__name__
                                }
                            )
                        )
                except Exception:
                    pass  # Fallback if async logging fails
            return None
    
    def compile_pattern(self, detector_id: int, pattern: str) -> Optional[re.Pattern]:
        """
        Compile pattern with LRU cache management.
        
        Args:
            detector_id: ID of the detector
            pattern: Regex pattern string
            
        Returns:
            Compiled pattern or None if compilation fails
        """
        # Check if pattern is already cached
        if detector_id in self._compiled_patterns:
            return self._compiled_patterns[detector_id]
        
        # Compile the pattern
        compiled = self._compile_pattern(pattern)
        
        if compiled:
            # Manage cache size (simple LRU: remove oldest if at capacity)
            if len(self._compiled_patterns) >= self.cache_size:
                # Remove the first (oldest) entry
                oldest_key = next(iter(self._compiled_patterns))
                del self._compiled_patterns[oldest_key]
            
            # Add to cache
            self._compiled_patterns[detector_id] = compiled
        
        return compiled
    
    def is_duplicate_detection(
        self,
        seen_matches: set,
        category: str,
        matched_text: str
    ) -> bool:
        """
        Check if a detection is a duplicate.
        
        Args:
            seen_matches: Set of already seen (category, matched_text) tuples
            category: Detection category
            matched_text: Matched text
            
        Returns:
            bool: True if duplicate, False otherwise
        """
        match_key = (category, matched_text.lower())
        if match_key in seen_matches:
            return True
        seen_matches.add(match_key)
        return False
    
    async def get_active_detectors(self, db: AsyncSession) -> List[RegexDetector]:
        """Get all active regex detectors from database."""
        result = await db.execute(
            select(RegexDetector)
            .where(RegexDetector.is_active == True)
            .order_by(RegexDetector.priority.desc())
        )
        return result.scalars().all()
    
    async def scan_text(
        self,
        db: AsyncSession,
        text: str,
        message_id: Optional[int] = None,
        media_id: Optional[int] = None,
        user_id: Optional[int] = None,
        group_id: Optional[int] = None,
        source: str = "text",
        auto_commit: bool = True,
        skip_existing: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Scan text for pattern matches and create detections.
        
        Args:
            db: Database session
            text: Text to scan
            message_id: Optional message ID
            media_id: Optional media ID
            user_id: Optional user ID
            group_id: Optional group ID
            source: Source of the text (default: "text")
            auto_commit: Whether to auto-commit detections
            skip_existing: Whether to skip existing detections
            
        Returns:
            List of detection dictionaries
        """
        if not text:
            return []
        
        try:
            # Get existing detections if skip_existing is enabled
            existing_detections = set()
            if skip_existing and message_id:
                result = await db.execute(
                    select(Detection.detector_id, Detection.matched_text)
                    .where(Detection.message_id == message_id)
                )
                for row in result.all():
                    existing_detections.add((row[0], row[1].lower() if row[1] else ""))
            
            # Get active detectors
            detectors = await self.get_active_detectors(db)
            detections = []
            seen_matches = set()
            
            for detector in detectors:
                # Compile pattern with cache
                if detector.id not in self._compiled_patterns:
                    compiled = self.compile_pattern(detector.id, detector.pattern)
                    if not compiled:
                        continue
                
                pattern = self._compiled_patterns.get(detector.id)
                if not pattern:
                    continue
                
                try:
                    # Find all matches
                    for match in pattern.finditer(text):
                        matched_text = match.group()
                        
                        # Check for duplicates
                        if self.is_duplicate_detection(seen_matches, detector.category, matched_text):
                            continue
                        
                        # Skip if already exists in database
                        if skip_existing and (detector.id, matched_text.lower()) in existing_detections:
                            continue
                        
                        # Extract context (50 characters before and after)
                        start = max(0, match.start() - 50)
                        end = min(len(text), match.end() + 50)
                        context_before = text[start:match.start()]
                        context_after = text[match.end():end]
                        
                        # Create detection
                        detection = Detection(
                            message_id=message_id,
                            media_id=media_id,
                            user_id=user_id,
                            group_id=group_id,
                            detector_id=detector.id,
                            detection_type=detector.category,
                            matched_text=matched_text,
                            context_before=context_before,
                            context_after=context_after,
                            source=source
                        )
                        db.add(detection)
                        live_stats.record("detections_found")
                        
                        detections.append({
                            "detector": detector.name,
                            "category": detector.category,
                            "matched_text": matched_text,
                            "context_before": context_before,
                            "context_after": context_after,
                            "source": source
                        })
                        
                        # Queue invite links for autojoin
                        if detector.category == "invite_link":
                            import asyncio
                            asyncio.create_task(
                                self._queue_invite_link(matched_text, group_id, user_id, message_id)
                            )
                
                except Exception as e:
                    # Log regex execution errors
                    await self.logger.log_error(
                        "DetectionService",
                        "scan_text",
                        f"Error executing regex pattern for detector {detector.name}",
                        error=e,
                        details={
                            "detector_id": detector.id,
                            "detector_name": detector.name,
                            "pattern": detector.pattern[:200],
                            "text_length": len(text)
                        }
                    )
                    continue
            
            # Commit detections if requested
            if detections and auto_commit:
                await db.commit()
            
            return detections
            
        except Exception as e:
            await self.logger.log_error(
                "DetectionService",
                "scan_text",
                "Error scanning text for detections",
                error=e,
                details={
                    "text_length": len(text) if text else 0,
                    "message_id": message_id,
                    "source": source
                }
            )
            return []
    
    async def scan_text_no_save(
        self,
        db: AsyncSession,
        text: str
    ) -> List[Dict[str, Any]]:
        """
        Scan text for pattern matches without saving to database.
        
        Args:
            db: Database session
            text: Text to scan
            
        Returns:
            List of detection dictionaries
        """
        if not text:
            return []
        
        try:
            detectors = await self.get_active_detectors(db)
            detections = []
            seen_matches = set()
            
            for detector in detectors:
                # Compile pattern with cache
                if detector.id not in self._compiled_patterns:
                    compiled = self.compile_pattern(detector.id, detector.pattern)
                    if not compiled:
                        continue
                
                pattern = self._compiled_patterns.get(detector.id)
                if not pattern:
                    continue
                
                try:
                    for match in pattern.finditer(text):
                        matched_text = match.group()
                        
                        # Check for duplicates
                        if self.is_duplicate_detection(seen_matches, detector.category, matched_text):
                            continue
                        
                        # Extract context
                        start = max(0, match.start() - 50)
                        end = min(len(text), match.end() + 50)
                        
                        detections.append({
                            "detector": detector.name,
                            "category": detector.category,
                            "matched_text": matched_text,
                            "start": match.start(),
                            "end": match.end(),
                            "context_before": text[start:match.start()],
                            "context_after": text[match.end():end]
                        })
                
                except Exception as e:
                    # Log regex execution errors
                    await self.logger.log_error(
                        "DetectionService",
                        "scan_text_no_save",
                        f"Error executing regex pattern for detector {detector.name}",
                        error=e,
                        details={
                            "detector_id": detector.id,
                            "detector_name": detector.name,
                            "pattern": detector.pattern[:200]
                        }
                    )
                    continue
            
            return detections
            
        except Exception as e:
            await self.logger.log_error(
                "DetectionService",
                "scan_text_no_save",
                "Error scanning text",
                error=e,
                details={"text_length": len(text) if text else 0}
            )
            return []
    
    async def seed_builtin_detectors(self, db: AsyncSession) -> int:
        existing = await db.execute(
            select(RegexDetector).where(RegexDetector.is_builtin == True)
        )
        existing_names = {d.name for d in existing.scalars().all()}
        
        created = 0
        for pattern_def in BUILTIN_PATTERNS:
            if pattern_def["name"] in existing_names:
                continue
            
            detector = RegexDetector(
                name=pattern_def["name"],
                pattern=pattern_def["pattern"],
                category=pattern_def["category"],
                description=pattern_def["description"],
                priority=pattern_def["priority"],
                is_builtin=True,
                is_active=True
            )
            db.add(detector)
            created += 1
        
        if created > 0:
            await db.commit()
        
        return created


    async def process_message(
        self,
        message_id: int,
        text: str,
        group_id: Optional[int] = None,
        sender_id: Optional[int] = None,
        db: Optional[AsyncSession] = None,
        account_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        from backend.app.db.database import async_session_maker
        from backend.app.services.websocket_manager import ws_manager
        
        should_close = db is None
        if db is None:
            db = async_session_maker()
            await db.__aenter__()
        
        external_session = not should_close
        try:
            detections = await self.scan_text(
                db=db,
                text=text,
                message_id=message_id,
                group_id=group_id,
                user_id=sender_id,
                source="message",
                auto_commit=not external_session
            )
            
            if detections:
                from backend.app.services.websocket_manager import WSMessage
                
                from backend.app.models.telegram_message import TelegramMessage
                from backend.app.models.telegram_group import TelegramGroup
                from backend.app.models.telegram_user import TelegramUser
                
                msg_info = await db.execute(
                    select(TelegramMessage).where(TelegramMessage.id == message_id)
                )
                msg = msg_info.scalar_one_or_none()
                
                group_name = "Unknown"
                sender_name = "Unknown"
                if msg:
                    grp_info = await db.execute(
                        select(TelegramGroup.title).where(TelegramGroup.id == msg.group_id)
                    )
                    group_name = grp_info.scalar() or "Unknown"
                    
                    if msg.sender_id:
                        usr_info = await db.execute(
                            select(TelegramUser.first_name, TelegramUser.username, TelegramUser.telegram_id).where(TelegramUser.id == msg.sender_id)
                        )
                        usr = usr_info.one_or_none()
                        if usr:
                            sender_name = usr[0] or usr[1] or "Unknown"
                            
                            # Trigger enrichment if sender is unknown
                            if sender_name == "Unknown":
                                from backend.app.services.enrichment_utils import trigger_user_enrichment
                                from backend.app.services.telegram_service import telegram_manager
                                
                                if account_id:
                                    client = telegram_manager.clients.get(account_id)
                                    if client:
                                        await trigger_user_enrichment(
                                            client=client,
                                            telegram_id=usr[2],  # telegram_id
                                            group_id=group_id,
                                            source="detection_service"
                                        )
                
                from datetime import datetime
                
                for detection in detections:
                    await ws_manager.broadcast("detections", WSMessage(
                        event="new_detection",
                        data={
                            "pattern_name": detection.get("detector", ""),
                            "type": detection.get("category", ""),
                            "matched_text": detection.get("matched_text", ""),
                            "group_id": group_id,
                            "group_name": group_name,
                            "sender_name": sender_name,
                            "message_id": message_id,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    ))
                
                if account_id:
                    await self._queue_invite_links(detections, account_id, message_id, group_id, sender_id)
            
            return detections
        finally:
            if should_close:
                await db.__aexit__(None, None, None)
    
    async def _queue_invite_links(
        self, 
        detections: List[Dict[str, Any]], 
        account_id: int, 
        message_id: int,
        group_id: Optional[int] = None,
        sender_id: Optional[int] = None
    ):
        """Queue invite links for autojoin service."""
        try:
            from backend.app.services.autojoin_service import autojoin_service
            
            for detection in detections:
                if detection.get("category") == "invite_link":
                    link = detection.get("matched_text", "")
                    if link:
                        await autojoin_service.add_from_detection(
                            link,
                            source_group_id=group_id,
                            source_user_id=sender_id,
                            source_message_id=message_id
                        )
        except Exception as e:
            await self.logger.log_error(
                "DetectionService",
                "queue_invite_links",
                "Error queueing invite links",
                error=e,
                details={
                    "account_id": account_id,
                    "message_id": message_id,
                    "group_id": group_id,
                    "detection_count": len(detections)
                }
            )
    
    async def _queue_invite_link(
        self,
        link: str,
        group_id: Optional[int] = None,
        user_id: Optional[int] = None,
        message_id: Optional[int] = None
    ):
        """Queue a single invite link for autojoin service."""
        try:
            from backend.app.services.autojoin_service import autojoin_service
            await autojoin_service.add_from_detection(
                link,
                source_group_id=group_id,
                source_user_id=user_id,
                source_message_id=message_id
            )
        except Exception as e:
            await self.logger.log_error(
                "DetectionService",
                "queue_invite_link",
                f"Error queueing invite link: {link}",
                error=e,
                details={
                    "link": link,
                    "group_id": group_id,
                    "user_id": user_id,
                    "message_id": message_id
                }
            )


detection_service = DetectionService()
