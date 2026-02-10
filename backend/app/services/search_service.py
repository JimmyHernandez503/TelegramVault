import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func, or_, and_
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import DatabaseError, ProgrammingError

from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.detection import Detection
from backend.app.core.config_manager import ConfigManager, get_config_manager
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem, enhanced_logging


class SearchService:
    """
    Full-Text Search Service with PostgreSQL FTS and ILIKE fallback.
    
    Features:
    - PostgreSQL full-text search using tsvector and plainto_tsquery
    - Automatic fallback to ILIKE pattern matching on FTS failures
    - Comprehensive error logging and diagnostics
    - Query validation and sanitization
    - Configurable search language and behavior
    """
    
    def __init__(
        self,
        config_manager: Optional[ConfigManager] = None,
        logger: Optional[EnhancedLoggingSystem] = None
    ):
        """
        Initialize SearchService with configuration and logging.
        
        Args:
            config_manager: Configuration manager instance
            logger: Enhanced logging system instance
        """
        self.config = config_manager or get_config_manager()
        self.logger = logger or enhanced_logging
        
        # Load configuration
        self.fts_language = self.config.get("SEARCH_FTS_LANGUAGE", "spanish")
        self.fallback_enabled = self.config.get_bool("SEARCH_FALLBACK_TO_ILIKE", True)
        self.log_failures = self.config.get_bool("SEARCH_LOG_FAILURES", True)
        
        # Statistics
        self._stats = {
            "total_searches": 0,
            "fts_successes": 0,
            "fts_failures": 0,
            "fallback_uses": 0,
            "validation_failures": 0
        }
    
    def _validate_query(self, query: str) -> Tuple[bool, Optional[str]]:
        """
        Validate and sanitize search query.
        
        Args:
            query: Raw search query
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not query:
            return False, "Query is empty"
        
        query = query.strip()
        
        if len(query) < 2:
            return False, "Query too short (minimum 2 characters)"
        
        if len(query) > 500:
            return False, "Query too long (maximum 500 characters)"
        
        # Check for SQL injection patterns
        dangerous_patterns = [
            r";\s*drop\s+table",
            r";\s*delete\s+from",
            r";\s*update\s+",
            r";\s*insert\s+into",
            r"--",
            r"/\*.*\*/",
            r"xp_cmdshell",
            r"exec\s*\(",
        ]
        
        query_lower = query.lower()
        for pattern in dangerous_patterns:
            if re.search(pattern, query_lower, re.IGNORECASE):
                return False, f"Query contains potentially dangerous pattern: {pattern}"
        
        return True, None
    
    def _sanitize_query(self, query: str) -> str:
        """
        Sanitize search query for safe use.
        
        Args:
            query: Raw search query
            
        Returns:
            Sanitized query string
        """
        # Strip whitespace
        query = query.strip()
        
        # Remove control characters
        query = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', query)
        
        # Normalize whitespace
        query = re.sub(r'\s+', ' ', query)
        
        return query
    
    def _normalize_query(self, query: str) -> str:
        """
        Normalize query for FTS by creating tsquery format.
        
        Args:
            query: Sanitized search query
            
        Returns:
            Normalized query for FTS
        """
        query = query.strip()
        query = re.sub(r'[^\w\s@#\-\+\.]', ' ', query)
        words = query.split()
        if len(words) > 1:
            return ' & '.join(words)
        return query
    
    def _diagnose_fts_failure(self, error: Exception, query: str) -> Dict[str, Any]:
        """
        Diagnose FTS failure and provide detailed information.
        
        Args:
            error: Exception that occurred
            query: Query that failed
            
        Returns:
            Dictionary with diagnostic information
        """
        diagnosis = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "query": query,
            "query_length": len(query),
            "fts_language": self.fts_language,
            "possible_causes": [],
            "suggested_fixes": []
        }
        
        error_str = str(error).lower()
        
        # Diagnose specific error types
        if "plainto_tsquery" in error_str or "to_tsquery" in error_str:
            diagnosis["possible_causes"].append("Invalid tsquery syntax")
            diagnosis["suggested_fixes"].append("Check query for special characters")
            diagnosis["suggested_fixes"].append("Verify FTS language configuration")
        
        if "language" in error_str or "dictionary" in error_str:
            diagnosis["possible_causes"].append(f"FTS language '{self.fts_language}' not available")
            diagnosis["suggested_fixes"].append("Install required PostgreSQL text search dictionary")
            diagnosis["suggested_fixes"].append("Change SEARCH_FTS_LANGUAGE configuration")
        
        if "syntax" in error_str:
            diagnosis["possible_causes"].append("SQL syntax error in query")
            diagnosis["suggested_fixes"].append("Review query construction logic")
        
        if "column" in error_str and "search_vector" in error_str:
            diagnosis["possible_causes"].append("search_vector column missing or not indexed")
            diagnosis["suggested_fixes"].append("Run database migrations to create search_vector columns")
            diagnosis["suggested_fixes"].append("Rebuild search index")
        
        if not diagnosis["possible_causes"]:
            diagnosis["possible_causes"].append("Unknown FTS error")
            diagnosis["suggested_fixes"].append("Check PostgreSQL logs for details")
            diagnosis["suggested_fixes"].append("Verify database connection and permissions")
        
        return diagnosis
    
    def build_search_query(
        self,
        table_alias: str,
        search_vector_column: str,
        query_param: str,
        additional_conditions: List[str],
        select_columns: List[str],
        joins: List[str],
        order_by: str = "relevance DESC"
    ) -> str:
        """
        Build a consolidated FTS search query.
        
        Args:
            table_alias: Alias for the main table (e.g., 'm' for messages)
            search_vector_column: Name of the search_vector column
            query_param: Parameter name for the query (e.g., ':query')
            additional_conditions: List of WHERE conditions
            select_columns: List of columns to select
            joins: List of JOIN clauses
            order_by: ORDER BY clause
            
        Returns:
            SQL query string
        """
        # Build WHERE clause
        where_conditions = [
            f"{table_alias}.{search_vector_column} @@ plainto_tsquery('{self.fts_language}', {query_param})"
        ]
        where_conditions.extend(additional_conditions)
        where_clause = " AND ".join(where_conditions)
        
        # Build SELECT clause
        select_clause = ",\n                    ".join(select_columns)
        
        # Build JOIN clause
        join_clause = "\n                ".join(joins) if joins else ""
        
        # Construct full query
        query = f"""
                SELECT 
                    {select_clause}
                FROM {table_alias}
                {join_clause}
                WHERE {where_clause}
                ORDER BY {order_by}
                LIMIT :limit OFFSET :offset
            """
        
        return query
    
    async def search_all(
        self,
        db: AsyncSession,
        query: str,
        group_id: Optional[int] = None,
        user_id: Optional[int] = None,
        source_types: Optional[List[str]] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Search across all content types with FTS and fallback.
        
        Args:
            db: Database session
            query: Search query
            group_id: Optional group filter
            user_id: Optional user filter
            source_types: Types to search (messages, users, detections)
            date_from: Start date filter
            date_to: End date filter
            limit: Maximum results
            offset: Result offset for pagination
            
        Returns:
            Dictionary with results, total count, and metadata
        """
        self._stats["total_searches"] += 1
        
        # Validate query
        is_valid, error_message = self._validate_query(query)
        if not is_valid:
            self._stats["validation_failures"] += 1
            await self.logger.log_warning(
                "SearchService",
                "search_all",
                f"Query validation failed: {error_message}",
                details={"query": query, "error": error_message}
            )
            return {
                "results": [],
                "total": 0,
                "query": query,
                "error": error_message
            }
        
        # Sanitize query
        sanitized_query = self._sanitize_query(query)
        ts_query = self._normalize_query(sanitized_query)
        
        results = []
        total_count = 0
        
        types_to_search = source_types or ["messages", "users", "detections"]
        
        if "messages" in types_to_search:
            msg_results, msg_count = await self._search_messages(
                db, ts_query, sanitized_query, group_id, user_id, date_from, date_to, limit, offset
            )
            results.extend(msg_results)
            total_count += msg_count
        
        if "users" in types_to_search:
            user_results, user_count = await self._search_users(
                db, ts_query, sanitized_query, group_id, limit, offset
            )
            results.extend(user_results)
            total_count += user_count
        
        if "detections" in types_to_search:
            det_results, det_count = await self._search_detections(
                db, ts_query, sanitized_query, group_id, user_id, date_from, date_to, limit, offset
            )
            results.extend(det_results)
            total_count += det_count
        
        results.sort(key=lambda x: x.get("relevance", 0), reverse=True)
        
        return {
            "results": results[:limit],
            "total": total_count,
            "query": sanitized_query,
            "filters": {
                "group_id": group_id,
                "user_id": user_id,
                "source_types": types_to_search,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None
            }
        }
    
    async def _search_messages(
        self,
        db: AsyncSession,
        ts_query: str,
        raw_query: str,
        group_id: Optional[int],
        user_id: Optional[int],
        date_from: Optional[datetime],
        date_to: Optional[datetime],
        limit: int,
        offset: int
    ) -> tuple[List[Dict], int]:
        """
        Search messages using FTS with fallback to ILIKE.
        
        Args:
            db: Database session
            ts_query: Normalized tsquery string
            raw_query: Original sanitized query
            group_id: Optional group filter
            user_id: Optional user filter
            date_from: Start date filter
            date_to: End date filter
            limit: Maximum results
            offset: Result offset
            
        Returns:
            Tuple of (results list, total count)
        """
        try:
            base_conditions = []
            
            if group_id:
                base_conditions.append(f"m.group_id = {group_id}")
            if user_id:
                base_conditions.append(f"m.sender_id = {user_id}")
            if date_from:
                base_conditions.append(f"m.date >= '{date_from.isoformat()}'")
            if date_to:
                base_conditions.append(f"m.date <= '{date_to.isoformat()}'")
            
            where_clause = " AND ".join(base_conditions) if base_conditions else "1=1"
            
            sql = text(f"""
                SELECT 
                    m.id,
                    m.text,
                    m.date,
                    m.group_id,
                    m.sender_id,
                    g.title as group_title,
                    u.first_name as sender_name,
                    u.username as sender_username,
                    ts_rank(m.search_vector, plainto_tsquery('{self.fts_language}', :query)) as relevance,
                    ts_headline('{self.fts_language}', m.text, plainto_tsquery('{self.fts_language}', :query), 
                        'StartSel=<mark>, StopSel=</mark>, MaxWords=50, MinWords=20') as highlight
                FROM telegram_messages m
                LEFT JOIN telegram_groups g ON m.group_id = g.id
                LEFT JOIN telegram_users u ON m.sender_id = u.id
                WHERE m.search_vector @@ plainto_tsquery('{self.fts_language}', :query)
                AND {where_clause}
                ORDER BY relevance DESC, m.date DESC
                LIMIT :limit OFFSET :offset
            """)
            
            result = await db.execute(sql, {"query": raw_query, "limit": limit, "offset": offset})
            rows = result.fetchall()
            
            count_sql = text(f"""
                SELECT COUNT(*) FROM telegram_messages m
                WHERE m.search_vector @@ plainto_tsquery('{self.fts_language}', :query)
                AND {where_clause}
            """)
            count_result = await db.execute(count_sql, {"query": raw_query})
            total = count_result.scalar() or 0
            
            results = []
            for row in rows:
                results.append({
                    "type": "message",
                    "id": row.id,
                    "text": row.text,
                    "highlight": row.highlight,
                    "date": row.date.isoformat() if row.date else None,
                    "group_id": row.group_id,
                    "group_title": row.group_title,
                    "sender_id": row.sender_id,
                    "sender_name": row.sender_name,
                    "sender_username": row.sender_username,
                    "relevance": float(row.relevance) if row.relevance else 0
                })
            
            self._stats["fts_successes"] += 1
            return results, total
            
        except Exception as e:
            self._stats["fts_failures"] += 1
            
            # Diagnose FTS failure
            diagnosis = self._diagnose_fts_failure(e, raw_query)
            
            # Log failure if enabled
            if self.log_failures:
                await self.logger.log_error(
                    "SearchService",
                    "_search_messages",
                    f"FTS search failed for messages: {str(e)}",
                    error=e,
                    details={
                        "query": raw_query,
                        "ts_query": ts_query,
                        "diagnosis": diagnosis,
                        "fallback_enabled": self.fallback_enabled
                    }
                )
            
            # Fallback to ILIKE if enabled
            if self.fallback_enabled:
                self._stats["fallback_uses"] += 1
                
                await self.logger.log_info(
                    "SearchService",
                    "_search_messages",
                    "Falling back to ILIKE search for messages",
                    details={
                        "query": raw_query,
                        "reason": "FTS failure",
                        "error_type": type(e).__name__
                    }
                )
                
                like_query = f"%{raw_query}%"
                result = await db.execute(
                    select(TelegramMessage)
                    .where(TelegramMessage.text.ilike(like_query))
                    .limit(limit)
                )
                messages = result.scalars().all()
                return [{"type": "message", "id": m.id, "text": m.text, "relevance": 0.5} for m in messages], len(messages)
            else:
                # No fallback, return empty results
                return [], 0
    
    async def _search_users(
        self,
        db: AsyncSession,
        ts_query: str,
        raw_query: str,
        group_id: Optional[int],
        limit: int,
        offset: int
    ) -> tuple[List[Dict], int]:
        """
        Search users using FTS with fallback to ILIKE.
        
        Args:
            db: Database session
            ts_query: Normalized tsquery string
            raw_query: Original sanitized query
            group_id: Optional group filter
            limit: Maximum results
            offset: Result offset
            
        Returns:
            Tuple of (results list, total count)
        """
        try:
            group_filter = ""
            if group_id:
                group_filter = f"""
                    AND u.id IN (
                        SELECT user_id FROM group_memberships WHERE group_id = {group_id}
                    )
                """
            
            sql = text(f"""
                SELECT 
                    u.id,
                    u.telegram_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.phone,
                    u.bio,
                    u.current_photo_path,
                    u.messages_count,
                    u.is_watchlist,
                    u.is_favorite,
                    ts_rank(u.search_vector, plainto_tsquery('{self.fts_language}', :query)) as relevance,
                    ts_headline('{self.fts_language}', 
                        COALESCE(u.first_name, '') || ' ' || COALESCE(u.last_name, '') || ' ' || COALESCE(u.username, '') || ' ' || COALESCE(u.bio, ''),
                        plainto_tsquery('{self.fts_language}', :query),
                        'StartSel=<mark>, StopSel=</mark>, MaxWords=30') as highlight
                FROM telegram_users u
                WHERE u.search_vector @@ plainto_tsquery('{self.fts_language}', :query)
                {group_filter}
                ORDER BY relevance DESC, u.messages_count DESC
                LIMIT :limit OFFSET :offset
            """)
            
            result = await db.execute(sql, {"query": raw_query, "limit": limit, "offset": offset})
            rows = result.fetchall()
            
            count_sql = text(f"""
                SELECT COUNT(*) FROM telegram_users u
                WHERE u.search_vector @@ plainto_tsquery('{self.fts_language}', :query)
                {group_filter}
            """)
            count_result = await db.execute(count_sql, {"query": raw_query})
            total = count_result.scalar() or 0
            
            results = []
            for row in rows:
                results.append({
                    "type": "user",
                    "id": row.id,
                    "telegram_id": row.telegram_id,
                    "username": row.username,
                    "first_name": row.first_name,
                    "last_name": row.last_name,
                    "phone": row.phone,
                    "bio": row.bio,
                    "photo_path": row.current_photo_path,
                    "messages_count": row.messages_count,
                    "is_watchlist": row.is_watchlist,
                    "is_favorite": row.is_favorite,
                    "highlight": row.highlight,
                    "relevance": float(row.relevance) if row.relevance else 0
                })
            
            self._stats["fts_successes"] += 1
            return results, total
            
        except Exception as e:
            self._stats["fts_failures"] += 1
            
            # Diagnose FTS failure
            diagnosis = self._diagnose_fts_failure(e, raw_query)
            
            # Log failure if enabled
            if self.log_failures:
                await self.logger.log_error(
                    "SearchService",
                    "_search_users",
                    f"FTS search failed for users: {str(e)}",
                    error=e,
                    details={
                        "query": raw_query,
                        "ts_query": ts_query,
                        "diagnosis": diagnosis,
                        "fallback_enabled": self.fallback_enabled
                    }
                )
            
            # Fallback to ILIKE if enabled
            if self.fallback_enabled:
                self._stats["fallback_uses"] += 1
                
                await self.logger.log_info(
                    "SearchService",
                    "_search_users",
                    "Falling back to ILIKE search for users",
                    details={
                        "query": raw_query,
                        "reason": "FTS failure",
                        "error_type": type(e).__name__
                    }
                )
                
                like_query = f"%{raw_query}%"
                result = await db.execute(
                    select(TelegramUser)
                    .where(or_(
                        TelegramUser.username.ilike(like_query),
                        TelegramUser.first_name.ilike(like_query),
                        TelegramUser.last_name.ilike(like_query)
                    ))
                    .limit(limit)
                )
                users = result.scalars().all()
                return [{"type": "user", "id": u.id, "username": u.username, "relevance": 0.5} for u in users], len(users)
            else:
                # No fallback, return empty results
                return [], 0
    
    async def _search_detections(
        self,
        db: AsyncSession,
        ts_query: str,
        raw_query: str,
        group_id: Optional[int],
        user_id: Optional[int],
        date_from: Optional[datetime],
        date_to: Optional[datetime],
        limit: int,
        offset: int
    ) -> tuple[List[Dict], int]:
        """
        Search detections using FTS with fallback to ILIKE.
        
        Args:
            db: Database session
            ts_query: Normalized tsquery string
            raw_query: Original sanitized query
            group_id: Optional group filter
            user_id: Optional user filter
            date_from: Start date filter
            date_to: End date filter
            limit: Maximum results
            offset: Result offset
            
        Returns:
            Tuple of (results list, total count)
        """
        try:
            base_conditions = []
            
            if group_id:
                base_conditions.append(f"d.group_id = {group_id}")
            if user_id:
                base_conditions.append(f"d.user_id = {user_id}")
            if date_from:
                base_conditions.append(f"d.created_at >= '{date_from.isoformat()}'")
            if date_to:
                base_conditions.append(f"d.created_at <= '{date_to.isoformat()}'")
            
            where_clause = " AND ".join(base_conditions) if base_conditions else "1=1"
            
            sql = text(f"""
                SELECT 
                    d.id,
                    d.detection_type,
                    d.matched_text,
                    d.context_before,
                    d.context_after,
                    d.source,
                    d.created_at,
                    d.group_id,
                    d.user_id,
                    d.message_id,
                    g.title as group_title,
                    r.name as detector_name,
                    ts_rank(d.search_vector, plainto_tsquery('{self.fts_language}', :query)) as relevance,
                    ts_headline('{self.fts_language}', 
                        COALESCE(d.matched_text, '') || ' ' || COALESCE(d.context_before, '') || ' ' || COALESCE(d.context_after, ''),
                        plainto_tsquery('{self.fts_language}', :query),
                        'StartSel=<mark>, StopSel=</mark>, MaxWords=40') as highlight
                FROM detections d
                LEFT JOIN telegram_groups g ON d.group_id = g.id
                LEFT JOIN regex_detectors r ON d.detector_id = r.id
                WHERE d.search_vector @@ plainto_tsquery('{self.fts_language}', :query)
                AND {where_clause}
                ORDER BY relevance DESC, d.created_at DESC
                LIMIT :limit OFFSET :offset
            """)
            
            result = await db.execute(sql, {"query": raw_query, "limit": limit, "offset": offset})
            rows = result.fetchall()
            
            count_sql = text(f"""
                SELECT COUNT(*) FROM detections d
                WHERE d.search_vector @@ plainto_tsquery('{self.fts_language}', :query)
                AND {where_clause}
            """)
            count_result = await db.execute(count_sql, {"query": raw_query})
            total = count_result.scalar() or 0
            
            results = []
            for row in rows:
                results.append({
                    "type": "detection",
                    "id": row.id,
                    "detection_type": row.detection_type,
                    "matched_text": row.matched_text,
                    "context_before": row.context_before,
                    "context_after": row.context_after,
                    "highlight": row.highlight,
                    "source": row.source,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "group_id": row.group_id,
                    "group_title": row.group_title,
                    "user_id": row.user_id,
                    "message_id": row.message_id,
                    "detector_name": row.detector_name,
                    "relevance": float(row.relevance) if row.relevance else 0
                })
            
            self._stats["fts_successes"] += 1
            return results, total
            
        except Exception as e:
            self._stats["fts_failures"] += 1
            
            # Diagnose FTS failure
            diagnosis = self._diagnose_fts_failure(e, raw_query)
            
            # Log failure if enabled
            if self.log_failures:
                await self.logger.log_error(
                    "SearchService",
                    "_search_detections",
                    f"FTS search failed for detections: {str(e)}",
                    error=e,
                    details={
                        "query": raw_query,
                        "ts_query": ts_query,
                        "diagnosis": diagnosis,
                        "fallback_enabled": self.fallback_enabled
                    }
                )
            
            # Fallback to ILIKE if enabled
            if self.fallback_enabled:
                self._stats["fallback_uses"] += 1
                
                await self.logger.log_info(
                    "SearchService",
                    "_search_detections",
                    "Falling back to ILIKE search for detections",
                    details={
                        "query": raw_query,
                        "reason": "FTS failure",
                        "error_type": type(e).__name__
                    }
                )
                
                like_query = f"%{raw_query}%"
                result = await db.execute(
                    select(Detection)
                    .where(Detection.matched_text.ilike(like_query))
                    .limit(limit)
                )
                detections = result.scalars().all()
                return [{"type": "detection", "id": d.id, "matched_text": d.matched_text, "relevance": 0.5} for d in detections], len(detections)
            else:
                # No fallback, return empty results
                return [], 0
    
    async def rebuild_search_index(self, db: AsyncSession) -> Dict[str, int]:
        """
        Rebuild search index for all tables.
        
        Args:
            db: Database session
            
        Returns:
            Dictionary with counts of updated records per table
        """
        try:
            await self.logger.log_info(
                "SearchService",
                "rebuild_search_index",
                "Starting search index rebuild",
                details={"fts_language": self.fts_language}
            )
            
            counts = {"messages": 0, "users": 0, "detections": 0}
            
            # Rebuild messages search_vector
            await db.execute(text(f"""
                UPDATE telegram_messages 
                SET search_vector = to_tsvector('{self.fts_language}', COALESCE(text, ''))
                WHERE text IS NOT NULL
            """))
            counts["messages"] = (await db.execute(text("SELECT COUNT(*) FROM telegram_messages WHERE text IS NOT NULL"))).scalar()
            
            # Rebuild users search_vector
            await db.execute(text(f"""
                UPDATE telegram_users 
                SET search_vector = to_tsvector('{self.fts_language}', 
                    COALESCE(first_name, '') || ' ' || 
                    COALESCE(last_name, '') || ' ' || 
                    COALESCE(username, '') || ' ' ||
                    COALESCE(bio, '')
                )
            """))
            counts["users"] = (await db.execute(text("SELECT COUNT(*) FROM telegram_users"))).scalar()
            
            # Rebuild detections search_vector
            await db.execute(text(f"""
                UPDATE detections 
                SET search_vector = to_tsvector('{self.fts_language}', 
                    COALESCE(matched_text, '') || ' ' || 
                    COALESCE(context_before, '') || ' ' || 
                    COALESCE(context_after, '')
                )
            """))
            counts["detections"] = (await db.execute(text("SELECT COUNT(*) FROM detections"))).scalar()
            
            await db.commit()
            
            await self.logger.log_info(
                "SearchService",
                "rebuild_search_index",
                "Search index rebuild completed",
                details=counts
            )
            
            return counts
            
        except Exception as e:
            await self.logger.log_error(
                "SearchService",
                "rebuild_search_index",
                "Failed to rebuild search index",
                error=e
            )
            raise
    
    async def get_statistics(self) -> Dict[str, Any]:
        """
        Get search service statistics.
        
        Returns:
            Dictionary with service statistics
        """
        total_searches = self._stats["total_searches"]
        fts_success_rate = (
            self._stats["fts_successes"] / total_searches * 100
            if total_searches > 0 else 0
        )
        fallback_rate = (
            self._stats["fallback_uses"] / total_searches * 100
            if total_searches > 0 else 0
        )
        
        return {
            "total_searches": total_searches,
            "fts_successes": self._stats["fts_successes"],
            "fts_failures": self._stats["fts_failures"],
            "fts_success_rate": round(fts_success_rate, 2),
            "fallback_uses": self._stats["fallback_uses"],
            "fallback_rate": round(fallback_rate, 2),
            "validation_failures": self._stats["validation_failures"],
            "configuration": {
                "fts_language": self.fts_language,
                "fallback_enabled": self.fallback_enabled,
                "log_failures": self.log_failures
            }
        }


search_service = SearchService()
