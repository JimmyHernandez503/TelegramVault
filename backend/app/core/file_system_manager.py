import os
import shutil
import logging
import asyncio
import aiofiles
import aiofiles.os
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import psutil
import gzip
import tarfile
from concurrent.futures import ThreadPoolExecutor


class StorageStatus(Enum):
    """Storage status indicators"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    FULL = "full"


class CompressionType(Enum):
    """Supported compression types"""
    NONE = "none"
    GZIP = "gzip"
    TAR_GZ = "tar_gz"


@dataclass
class StorageInfo:
    """Storage information structure"""
    total_bytes: int
    used_bytes: int
    free_bytes: int
    usage_percent: float
    status: StorageStatus
    warning_threshold: float = 80.0
    critical_threshold: float = 90.0


@dataclass
class DirectoryStats:
    """Directory statistics structure"""
    path: str
    file_count: int
    total_size: int
    last_modified: datetime
    permissions: str


class FileSystemManager:
    """
    Enhanced file system manager for TelegramVault media storage.
    
    Handles directory structure management, permission validation,
    disk space monitoring, file organization, and cleanup operations.
    """
    
    def __init__(self, base_media_dir: str = "media"):
        self.logger = logging.getLogger(__name__)
        self.base_media_dir = Path(base_media_dir)
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # Storage monitoring settings
        self.warning_threshold = 80.0  # Percentage
        self.critical_threshold = 90.0  # Percentage
        self.cleanup_threshold = 85.0   # Percentage
        
        # Directory structure
        self.subdirectories = {
            'photos': 'photos',
            'videos': 'videos', 
            'documents': 'documents',
            'audio': 'audio',
            'voice': 'voice',
            'stickers': 'stickers',
            'group_photos': 'group_photos',
            'profile_photos': 'profile_photos',
            'stories': 'stories',
            'dialogs': 'dialogs',
            'invite_previews': 'invite_previews',
            'temp': 'temp',
            'archive': 'archive'
        }
        
        # File organization settings
        self.max_files_per_directory = 1000
        self.archive_after_days = 365
        self.temp_cleanup_hours = 24
        
        self._initialized = False
    
    async def initialize(self) -> bool:
        """
        Initialize the file system manager.
        
        Returns:
            bool: True if initialization successful
        """
        try:
            self.logger.info("Initializing FileSystemManager")
            
            # Create base directory structure
            await self._create_directory_structure()
            
            # Validate permissions
            if not await self._validate_permissions():
                self.logger.error("Permission validation failed")
                return False
            
            # Check initial storage status
            storage_info = await self.get_storage_info()
            self.logger.info(f"Storage status: {storage_info.status.value} "
                           f"({storage_info.usage_percent:.1f}% used)")
            
            if storage_info.status == StorageStatus.CRITICAL:
                self.logger.warning("Storage is in critical state - cleanup recommended")
            
            self._initialized = True
            self.logger.info("FileSystemManager initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize FileSystemManager: {e}")
            return False
    
    async def _create_directory_structure(self) -> None:
        """Create the complete directory structure for media storage."""
        try:
            # Create base media directory
            await aiofiles.os.makedirs(self.base_media_dir, exist_ok=True)
            
            # Create all subdirectories
            for subdir_key, subdir_name in self.subdirectories.items():
                subdir_path = self.base_media_dir / subdir_name
                await aiofiles.os.makedirs(subdir_path, exist_ok=True)
                
                # Create numbered subdirectories for high-volume types
                if subdir_key in ['photos', 'videos', 'documents', 'profile_photos']:
                    for i in range(1, 101):  # Create 1-100 subdirectories
                        numbered_dir = subdir_path / str(i)
                        await aiofiles.os.makedirs(numbered_dir, exist_ok=True)
            
            self.logger.info("Directory structure created successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to create directory structure: {e}")
            raise
    
    async def _validate_permissions(self) -> bool:
        """
        Validate that we have proper permissions for all operations.
        
        Returns:
            bool: True if all permissions are valid
        """
        try:
            test_file = self.base_media_dir / "temp" / ".permission_test"
            
            # Test write permission
            async with aiofiles.open(test_file, 'w') as f:
                await f.write("test")
            
            # Test read permission
            async with aiofiles.open(test_file, 'r') as f:
                content = await f.read()
                if content != "test":
                    return False
            
            # Test delete permission
            await aiofiles.os.remove(test_file)
            
            # Test directory creation/deletion
            test_dir = self.base_media_dir / "temp" / ".test_dir"
            await aiofiles.os.makedirs(test_dir, exist_ok=True)
            await aiofiles.os.rmdir(test_dir)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Permission validation failed: {e}")
            return False
    
    async def get_storage_info(self) -> StorageInfo:
        """
        Get current storage information and status.
        
        Returns:
            StorageInfo: Current storage information
        """
        try:
            # Get disk usage for the media directory
            usage = await asyncio.get_event_loop().run_in_executor(
                self.executor, shutil.disk_usage, self.base_media_dir
            )
            
            total_bytes = usage.total
            free_bytes = usage.free
            used_bytes = total_bytes - free_bytes
            usage_percent = (used_bytes / total_bytes) * 100
            
            # Determine status
            if usage_percent >= self.critical_threshold:
                status = StorageStatus.CRITICAL
            elif usage_percent >= self.warning_threshold:
                status = StorageStatus.WARNING
            else:
                status = StorageStatus.HEALTHY
            
            return StorageInfo(
                total_bytes=total_bytes,
                used_bytes=used_bytes,
                free_bytes=free_bytes,
                usage_percent=usage_percent,
                status=status,
                warning_threshold=self.warning_threshold,
                critical_threshold=self.critical_threshold
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get storage info: {e}")
            # Return default critical status on error
            return StorageInfo(
                total_bytes=0,
                used_bytes=0,
                free_bytes=0,
                usage_percent=100.0,
                status=StorageStatus.CRITICAL
            )
    
    async def get_directory_stats(self, subdir_key: str) -> Optional[DirectoryStats]:
        """
        Get statistics for a specific subdirectory.
        
        Args:
            subdir_key: Key for the subdirectory (e.g., 'photos', 'videos')
            
        Returns:
            DirectoryStats: Directory statistics or None if error
        """
        try:
            if subdir_key not in self.subdirectories:
                self.logger.error(f"Unknown subdirectory key: {subdir_key}")
                return None
            
            dir_path = self.base_media_dir / self.subdirectories[subdir_key]
            
            if not await aiofiles.os.path.exists(dir_path):
                return None
            
            # Get directory stats using executor
            stats = await asyncio.get_event_loop().run_in_executor(
                self.executor, self._get_dir_stats_sync, dir_path
            )
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get directory stats for {subdir_key}: {e}")
            return None
    
    def _get_dir_stats_sync(self, dir_path: Path) -> DirectoryStats:
        """Synchronous helper for getting directory statistics."""
        file_count = 0
        total_size = 0
        last_modified = datetime.fromtimestamp(0)
        
        for root, dirs, files in os.walk(dir_path):
            file_count += len(files)
            for file in files:
                file_path = Path(root) / file
                try:
                    stat = file_path.stat()
                    total_size += stat.st_size
                    file_modified = datetime.fromtimestamp(stat.st_mtime)
                    if file_modified > last_modified:
                        last_modified = file_modified
                except (OSError, IOError):
                    continue
        
        # Get directory permissions
        try:
            permissions = oct(dir_path.stat().st_mode)[-3:]
        except (OSError, IOError):
            permissions = "unknown"
        
        return DirectoryStats(
            path=str(dir_path),
            file_count=file_count,
            total_size=total_size,
            last_modified=last_modified,
            permissions=permissions
        )
    
    async def get_optimal_subdirectory(self, subdir_key: str, file_size: int = 0) -> Optional[Path]:
        """
        Get the optimal subdirectory for storing a file.
        
        Args:
            subdir_key: Key for the subdirectory type
            file_size: Size of the file to be stored (for space checking)
            
        Returns:
            Path: Optimal subdirectory path or None if error
        """
        try:
            if subdir_key not in self.subdirectories:
                return None
            
            base_dir = self.base_media_dir / self.subdirectories[subdir_key]
            
            # For non-numbered directories, return base directory
            if subdir_key not in ['photos', 'videos', 'documents', 'profile_photos']:
                return base_dir
            
            # Find subdirectory with least files (load balancing)
            best_subdir = None
            min_file_count = float('inf')
            
            for i in range(1, 101):
                subdir = base_dir / str(i)
                if not await aiofiles.os.path.exists(subdir):
                    continue
                
                try:
                    # Count files in subdirectory
                    file_count = await asyncio.get_event_loop().run_in_executor(
                        self.executor, self._count_files_sync, subdir
                    )
                    
                    # If under max files per directory, consider this subdirectory
                    if file_count < self.max_files_per_directory and file_count < min_file_count:
                        min_file_count = file_count
                        best_subdir = subdir
                        
                        # If very low count, use immediately
                        if file_count < 100:
                            break
                            
                except Exception:
                    continue
            
            # If no suitable subdirectory found, use first available
            if best_subdir is None:
                best_subdir = base_dir / "1"
            
            return best_subdir
            
        except Exception as e:
            self.logger.error(f"Failed to get optimal subdirectory for {subdir_key}: {e}")
            return None
    
    def _count_files_sync(self, directory: Path) -> int:
        """Synchronous helper for counting files in a directory."""
        try:
            return len([f for f in directory.iterdir() if f.is_file()])
        except (OSError, IOError):
            return 0
    
    async def ensure_file_path(self, file_path: Path) -> bool:
        """
        Ensure that the directory structure exists for a file path.
        
        Args:
            file_path: Full path to the file
            
        Returns:
            bool: True if directory structure exists/created successfully
        """
        try:
            directory = file_path.parent
            await aiofiles.os.makedirs(directory, exist_ok=True)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to ensure file path {file_path}: {e}")
            return False
    
    async def move_file(self, source: Path, destination: Path) -> bool:
        """
        Move a file from source to destination.
        
        Args:
            source: Source file path
            destination: Destination file path
            
        Returns:
            bool: True if move successful
        """
        try:
            # Ensure destination directory exists
            if not await self.ensure_file_path(destination):
                return False
            
            # Move file using executor
            await asyncio.get_event_loop().run_in_executor(
                self.executor, shutil.move, str(source), str(destination)
            )
            
            self.logger.debug(f"Moved file from {source} to {destination}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to move file from {source} to {destination}: {e}")
            return False
    
    async def copy_file(self, source: Path, destination: Path) -> bool:
        """
        Copy a file from source to destination.
        
        Args:
            source: Source file path
            destination: Destination file path
            
        Returns:
            bool: True if copy successful
        """
        try:
            # Ensure destination directory exists
            if not await self.ensure_file_path(destination):
                return False
            
            # Copy file using executor
            await asyncio.get_event_loop().run_in_executor(
                self.executor, shutil.copy2, str(source), str(destination)
            )
            
            self.logger.debug(f"Copied file from {source} to {destination}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to copy file from {source} to {destination}: {e}")
            return False
    
    async def delete_file(self, file_path: Path) -> bool:
        """
        Delete a file safely.
        
        Args:
            file_path: Path to the file to delete
            
        Returns:
            bool: True if deletion successful
        """
        try:
            if await aiofiles.os.path.exists(file_path):
                await aiofiles.os.remove(file_path)
                self.logger.debug(f"Deleted file: {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete file {file_path}: {e}")
            return False
    
    async def compress_file(self, file_path: Path, compression_type: CompressionType = CompressionType.GZIP) -> Optional[Path]:
        """
        Compress a file using the specified compression type.
        
        Args:
            file_path: Path to the file to compress
            compression_type: Type of compression to use
            
        Returns:
            Path: Path to compressed file or None if failed
        """
        try:
            if not await aiofiles.os.path.exists(file_path):
                return None
            
            if compression_type == CompressionType.GZIP:
                compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
                await self._compress_gzip(file_path, compressed_path)
                return compressed_path
            elif compression_type == CompressionType.TAR_GZ:
                compressed_path = file_path.with_suffix('.tar.gz')
                await self._compress_tar_gz(file_path, compressed_path)
                return compressed_path
            else:
                self.logger.error(f"Unsupported compression type: {compression_type}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to compress file {file_path}: {e}")
            return None
    
    async def _compress_gzip(self, source: Path, destination: Path) -> None:
        """Compress file using gzip."""
        def compress_sync():
            with open(source, 'rb') as f_in:
                with gzip.open(destination, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        await asyncio.get_event_loop().run_in_executor(self.executor, compress_sync)
    
    async def _compress_tar_gz(self, source: Path, destination: Path) -> None:
        """Compress file using tar.gz."""
        def compress_sync():
            with tarfile.open(destination, 'w:gz') as tar:
                tar.add(source, arcname=source.name)
        
        await asyncio.get_event_loop().run_in_executor(self.executor, compress_sync)
    
    async def cleanup_temp_files(self) -> int:
        """
        Clean up temporary files older than the configured threshold.
        
        Returns:
            int: Number of files cleaned up
        """
        try:
            temp_dir = self.base_media_dir / "temp"
            if not await aiofiles.os.path.exists(temp_dir):
                return 0
            
            cutoff_time = datetime.now() - timedelta(hours=self.temp_cleanup_hours)
            cleaned_count = 0
            
            # Use executor for file system operations
            cleaned_count = await asyncio.get_event_loop().run_in_executor(
                self.executor, self._cleanup_temp_files_sync, temp_dir, cutoff_time
            )
            
            if cleaned_count > 0:
                self.logger.info(f"Cleaned up {cleaned_count} temporary files")
            
            return cleaned_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup temp files: {e}")
            return 0
    
    def _cleanup_temp_files_sync(self, temp_dir: Path, cutoff_time: datetime) -> int:
        """Synchronous helper for cleaning up temp files."""
        cleaned_count = 0
        
        try:
            for file_path in temp_dir.rglob('*'):
                if file_path.is_file():
                    try:
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_mtime < cutoff_time:
                            file_path.unlink()
                            cleaned_count += 1
                    except (OSError, IOError):
                        continue
        except Exception:
            pass
        
        return cleaned_count
    
    async def archive_old_files(self, subdir_key: str, days_old: int = None) -> int:
        """
        Archive files older than the specified number of days.
        
        Args:
            subdir_key: Subdirectory to archive from
            days_old: Number of days old (uses default if None)
            
        Returns:
            int: Number of files archived
        """
        try:
            if days_old is None:
                days_old = self.archive_after_days
            
            if subdir_key not in self.subdirectories:
                return 0
            
            source_dir = self.base_media_dir / self.subdirectories[subdir_key]
            archive_dir = self.base_media_dir / "archive" / self.subdirectories[subdir_key]
            
            if not await aiofiles.os.path.exists(source_dir):
                return 0
            
            await aiofiles.os.makedirs(archive_dir, exist_ok=True)
            
            cutoff_time = datetime.now() - timedelta(days=days_old)
            
            # Use executor for file system operations
            archived_count = await asyncio.get_event_loop().run_in_executor(
                self.executor, self._archive_old_files_sync, source_dir, archive_dir, cutoff_time
            )
            
            if archived_count > 0:
                self.logger.info(f"Archived {archived_count} files from {subdir_key}")
            
            return archived_count
            
        except Exception as e:
            self.logger.error(f"Failed to archive old files from {subdir_key}: {e}")
            return 0
    
    def _archive_old_files_sync(self, source_dir: Path, archive_dir: Path, cutoff_time: datetime) -> int:
        """Synchronous helper for archiving old files."""
        archived_count = 0
        
        try:
            for file_path in source_dir.rglob('*'):
                if file_path.is_file():
                    try:
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_mtime < cutoff_time:
                            # Create relative path structure in archive
                            relative_path = file_path.relative_to(source_dir)
                            archive_path = archive_dir / relative_path
                            
                            # Ensure archive directory exists
                            archive_path.parent.mkdir(parents=True, exist_ok=True)
                            
                            # Move file to archive
                            shutil.move(str(file_path), str(archive_path))
                            archived_count += 1
                            
                    except (OSError, IOError):
                        continue
        except Exception:
            pass
        
        return archived_count
    
    async def get_storage_usage_by_type(self) -> Dict[str, Dict[str, Any]]:
        """
        Get storage usage breakdown by media type.
        
        Returns:
            Dict: Storage usage information by type
        """
        try:
            usage_info = {}
            
            for subdir_key, subdir_name in self.subdirectories.items():
                stats = await self.get_directory_stats(subdir_key)
                if stats:
                    usage_info[subdir_key] = {
                        'directory': subdir_name,
                        'file_count': stats.file_count,
                        'total_size': stats.total_size,
                        'size_mb': round(stats.total_size / (1024 * 1024), 2),
                        'last_modified': stats.last_modified.isoformat(),
                        'permissions': stats.permissions
                    }
                else:
                    usage_info[subdir_key] = {
                        'directory': subdir_name,
                        'file_count': 0,
                        'total_size': 0,
                        'size_mb': 0.0,
                        'last_modified': None,
                        'permissions': 'unknown'
                    }
            
            return usage_info
            
        except Exception as e:
            self.logger.error(f"Failed to get storage usage by type: {e}")
            return {}
    
    async def perform_maintenance(self) -> Dict[str, Any]:
        """
        Perform routine maintenance operations.
        
        Returns:
            Dict: Maintenance operation results
        """
        try:
            self.logger.info("Starting file system maintenance")
            
            results = {
                'temp_files_cleaned': 0,
                'files_archived': 0,
                'storage_info': None,
                'errors': []
            }
            
            # Clean up temporary files
            try:
                results['temp_files_cleaned'] = await self.cleanup_temp_files()
            except Exception as e:
                results['errors'].append(f"Temp cleanup failed: {e}")
            
            # Archive old files if storage is getting full
            storage_info = await self.get_storage_info()
            results['storage_info'] = {
                'usage_percent': storage_info.usage_percent,
                'status': storage_info.status.value,
                'free_gb': round(storage_info.free_bytes / (1024**3), 2)
            }
            
            if storage_info.usage_percent > self.cleanup_threshold:
                self.logger.info("Storage usage high, performing archival")
                for subdir_key in ['photos', 'videos', 'documents']:
                    try:
                        archived = await self.archive_old_files(subdir_key)
                        results['files_archived'] += archived
                    except Exception as e:
                        results['errors'].append(f"Archive {subdir_key} failed: {e}")
            
            self.logger.info(f"File system maintenance completed: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"File system maintenance failed: {e}")
            return {'error': str(e)}
    
    async def shutdown(self) -> None:
        """Shutdown the file system manager and cleanup resources."""
        try:
            self.logger.info("Shutting down FileSystemManager")
            
            # Shutdown executor
            self.executor.shutdown(wait=True)
            
            self._initialized = False
            self.logger.info("FileSystemManager shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during FileSystemManager shutdown: {e}")


# Global instance
file_system_manager = FileSystemManager()