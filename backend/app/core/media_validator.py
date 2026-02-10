import os
import hashlib
import asyncio
import logging
import mimetypes
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

try:
    from PIL import Image, ImageFile
    from PIL.ExifTags import TAGS
    PIL_AVAILABLE = True
    # Allow loading of truncated images for validation
    ImageFile.LOAD_TRUNCATED_IMAGES = True
except ImportError:
    PIL_AVAILABLE = False

try:
    import cv2
    import numpy as np
    OPENCV_AVAILABLE = True
except ImportError:
    OPENCV_AVAILABLE = False

try:
    import imagehash
    IMAGEHASH_AVAILABLE = True
except ImportError:
    IMAGEHASH_AVAILABLE = False

from backend.app.core.duplicate_detector import DuplicateDetector, DuplicateDetectionMethod


class ValidationStatus(Enum):
    """Enumeration for validation status."""
    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    CORRUPTED = "corrupted"
    UNSUPPORTED = "unsupported"


@dataclass
class ValidationResult:
    """Data class for validation results."""
    status: ValidationStatus
    file_path: str
    file_size: int
    mime_type: Optional[str] = None
    format_valid: bool = False
    integrity_valid: bool = False
    corruption_detected: bool = False
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    perceptual_hash: Optional[str] = None
    validation_time: Optional[datetime] = None


@dataclass
class MediaMetadata:
    """Data class for media metadata."""
    file_path: str
    file_size: int
    mime_type: Optional[str] = None
    format: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration: Optional[float] = None
    bitrate: Optional[int] = None
    fps: Optional[float] = None
    channels: Optional[int] = None
    sample_rate: Optional[int] = None
    creation_date: Optional[datetime] = None
    exif_data: Optional[Dict[str, Any]] = None
    codec: Optional[str] = None


class MediaValidator:
    """
    Validates downloaded media files for integrity, format correctness,
    and extracts metadata for duplicate detection and quality assessment.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.duplicate_detector = DuplicateDetector()
        
        # Supported file formats and their magic bytes
        self.format_signatures = {
            # Image formats
            'jpeg': [b'\xff\xd8\xff'],
            'png': [b'\x89PNG\r\n\x1a\n'],
            'gif': [b'GIF87a', b'GIF89a'],
            'webp': [b'RIFF', b'WEBP'],
            'bmp': [b'BM'],
            'tiff': [b'II*\x00', b'MM\x00*'],
            
            # Video formats
            'mp4': [b'\x00\x00\x00\x18ftypmp4', b'\x00\x00\x00\x20ftypmp4'],
            'avi': [b'RIFF', b'AVI '],
            'mov': [b'\x00\x00\x00\x14ftyp'],
            'mkv': [b'\x1a\x45\xdf\xa3'],
            'webm': [b'\x1a\x45\xdf\xa3'],
            
            # Audio formats
            'mp3': [b'ID3', b'\xff\xfb', b'\xff\xf3', b'\xff\xf2'],
            'ogg': [b'OggS'],
            'wav': [b'RIFF', b'WAVE'],
            'flac': [b'fLaC'],
            'm4a': [b'\x00\x00\x00\x20ftypM4A'],
            
            # Document formats
            'pdf': [b'%PDF'],
            'zip': [b'PK\x03\x04', b'PK\x05\x06', b'PK\x07\x08'],
            'rar': [b'Rar!\x1a\x07\x00', b'Rar!\x1a\x07\x01\x00'],
            '7z': [b'7z\xbc\xaf\x27\x1c'],
        }
        
        # MIME type mappings
        self.mime_type_map = {
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'webp': 'image/webp',
            'bmp': 'image/bmp',
            'tiff': 'image/tiff',
            'mp4': 'video/mp4',
            'avi': 'video/x-msvideo',
            'mov': 'video/quicktime',
            'mkv': 'video/x-matroska',
            'webm': 'video/webm',
            'mp3': 'audio/mpeg',
            'ogg': 'audio/ogg',
            'wav': 'audio/wav',
            'flac': 'audio/flac',
            'm4a': 'audio/mp4',
            'pdf': 'application/pdf',
            'zip': 'application/zip',
            'rar': 'application/x-rar-compressed',
            '7z': 'application/x-7z-compressed',
        }
        
        # Minimum file sizes for different formats (in bytes)
        self.min_file_sizes = {
            'jpeg': 100,
            'png': 67,  # Minimum PNG header size
            'gif': 26,  # Minimum GIF header size
            'webp': 12,
            'mp4': 32,
            'avi': 56,
            'mp3': 128,
            'ogg': 28,
            'wav': 44,
            'pdf': 100,
        }
    
    async def validate_media_file(
        self,
        file_path: str,
        expected_type: str,
        expected_size: Optional[int] = None
    ) -> ValidationResult:
        """
        Validates a media file for format correctness and integrity.
        
        Args:
            file_path: Path to the media file
            expected_type: Expected media type (photo, video, audio, document)
            expected_size: Expected file size in bytes (optional)
            
        Returns:
            ValidationResult with validation status and details
        """
        start_time = datetime.utcnow()
        
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                return ValidationResult(
                    status=ValidationStatus.INVALID,
                    file_path=file_path,
                    file_size=0,
                    error_message="File does not exist",
                    validation_time=start_time
                )
            
            # Get file size
            file_size = os.path.getsize(file_path)
            
            # Check if file is empty
            if file_size == 0:
                return ValidationResult(
                    status=ValidationStatus.INVALID,
                    file_path=file_path,
                    file_size=file_size,
                    error_message="File is empty",
                    validation_time=start_time
                )
            
            # Validate expected size if provided
            if expected_size is not None and abs(file_size - expected_size) > 1024:  # Allow 1KB tolerance
                self.logger.warning(f"File size mismatch: expected {expected_size}, got {file_size}")
            
            # Detect file format
            detected_format = await self._detect_file_format(file_path)
            if not detected_format:
                return ValidationResult(
                    status=ValidationStatus.UNSUPPORTED,
                    file_path=file_path,
                    file_size=file_size,
                    error_message="Unable to detect file format",
                    validation_time=start_time
                )
            
            # Get MIME type
            mime_type = self.mime_type_map.get(detected_format)
            if not mime_type:
                mime_type, _ = mimetypes.guess_type(file_path)
            
            # Validate format
            format_valid = await self._validate_format(file_path, detected_format)
            
            # Check for corruption
            corruption_detected = await self.detect_corruption(file_path)
            
            # Validate integrity
            integrity_valid = await self._validate_integrity(file_path, detected_format)
            
            # Determine overall status
            if corruption_detected:
                status = ValidationStatus.CORRUPTED
            elif not format_valid or not integrity_valid:
                status = ValidationStatus.INVALID
            else:
                status = ValidationStatus.VALID
            
            # Extract metadata
            metadata = await self.extract_metadata(file_path)
            
            # Compute perceptual hash if supported
            perceptual_hash = None
            if detected_format in ['jpeg', 'png', 'gif', 'webp', 'bmp']:
                perceptual_hash = await self.compute_perceptual_hash(file_path, expected_type)
            
            return ValidationResult(
                status=status,
                file_path=file_path,
                file_size=file_size,
                mime_type=mime_type,
                format_valid=format_valid,
                integrity_valid=integrity_valid,
                corruption_detected=corruption_detected,
                metadata=metadata.__dict__ if metadata else None,
                perceptual_hash=perceptual_hash,
                validation_time=start_time
            )
            
        except Exception as e:
            self.logger.error(f"Error validating file {file_path}: {e}")
            return ValidationResult(
                status=ValidationStatus.INVALID,
                file_path=file_path,
                file_size=os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                error_message=str(e),
                validation_time=start_time
            )
    
    async def compute_perceptual_hash(
        self,
        file_path: str,
        media_type: str
    ) -> Optional[str]:
        """
        Computes perceptual hash for duplicate detection.
        
        Args:
            file_path: Path to the media file
            media_type: Type of media (photo, video, etc.)
            
        Returns:
            Perceptual hash string or None if computation fails
        """
        try:
            # Use the enhanced duplicate detector for perceptual hashing
            hash_result = await self.duplicate_detector.compute_perceptual_hash(
                file_path, media_type
            )
            return hash_result.hash_value if hash_result else None
        except Exception as e:
            self.logger.error(f"Error computing perceptual hash for {file_path}: {e}")
            return None
    
    async def find_similar_media(
        self,
        media_file,
        db_session,
        similarity_threshold=None
    ):
        """
        Finds similar media files using the duplicate detector.
        
        Args:
            media_file: MediaFile object to find duplicates for
            db_session: Database session
            similarity_threshold: Similarity threshold level
            
        Returns:
            List of duplicate matches
        """
        try:
            from backend.app.core.duplicate_detector import SimilarityLevel
            threshold = similarity_threshold or SimilarityLevel.HIGH
            return await self.duplicate_detector.find_duplicates(
                media_file, db_session, threshold
            )
        except Exception as e:
            self.logger.error(f"Error finding similar media: {e}")
            return []
    
    async def compute_perceptual_hash_original(
        self,
        file_path: str,
        media_type: str
    ) -> Optional[str]:
        """
        Original perceptual hash computation method (kept for compatibility).
        
        Args:
            file_path: Path to the media file
            media_type: Type of media (photo, video, etc.)
            
        Returns:
            Perceptual hash string or None if computation fails
        """
        try:
            if media_type == "photo" and PIL_AVAILABLE and IMAGEHASH_AVAILABLE:
                return await self._compute_image_hash(file_path)
            elif media_type == "video" and OPENCV_AVAILABLE:
                return await self._compute_video_hash(file_path)
            else:
                # Fallback to content hash for other types
                return await self._compute_content_hash(file_path)
        except Exception as e:
            self.logger.error(f"Error computing perceptual hash for {file_path}: {e}")
            return None
    
    async def detect_corruption(self, file_path: str) -> bool:
        """
        Detects if a media file is corrupted.
        
        Args:
            file_path: Path to the media file
            
        Returns:
            True if corruption is detected, False otherwise
        """
        try:
            # Detect file format first
            detected_format = await self._detect_file_format(file_path)
            if not detected_format:
                return True  # Unknown format considered corrupted
            
            # Format-specific corruption detection
            if detected_format in ['jpeg', 'png', 'gif', 'webp', 'bmp']:
                return await self._detect_image_corruption(file_path)
            elif detected_format in ['mp4', 'avi', 'mov', 'mkv', 'webm']:
                return await self._detect_video_corruption(file_path)
            elif detected_format in ['mp3', 'ogg', 'wav', 'flac', 'm4a']:
                return await self._detect_audio_corruption(file_path)
            else:
                # Generic corruption detection
                return await self._detect_generic_corruption(file_path)
                
        except Exception as e:
            self.logger.error(f"Error detecting corruption in {file_path}: {e}")
            return True  # Assume corrupted if we can't check
    
    async def extract_metadata(self, file_path: str) -> Optional[MediaMetadata]:
        """
        Extracts metadata from a media file.
        
        Args:
            file_path: Path to the media file
            
        Returns:
            MediaMetadata object or None if extraction fails
        """
        try:
            file_size = os.path.getsize(file_path)
            mime_type, _ = mimetypes.guess_type(file_path)
            detected_format = await self._detect_file_format(file_path)
            
            metadata = MediaMetadata(
                file_path=file_path,
                file_size=file_size,
                mime_type=mime_type,
                format=detected_format
            )
            
            # Extract format-specific metadata
            if detected_format in ['jpeg', 'png', 'gif', 'webp', 'bmp']:
                await self._extract_image_metadata(metadata)
            elif detected_format in ['mp4', 'avi', 'mov', 'mkv', 'webm']:
                await self._extract_video_metadata(metadata)
            elif detected_format in ['mp3', 'ogg', 'wav', 'flac', 'm4a']:
                await self._extract_audio_metadata(metadata)
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error extracting metadata from {file_path}: {e}")
            return None
    
    async def _detect_file_format(self, file_path: str) -> Optional[str]:
        """Detects file format based on magic bytes."""
        try:
            with open(file_path, 'rb') as f:
                header = f.read(32)  # Read first 32 bytes
            
            for format_name, signatures in self.format_signatures.items():
                for signature in signatures:
                    if header.startswith(signature):
                        return format_name
                    # Special case for some formats that have signatures at different positions
                    if format_name == 'mp4' and signature in header:
                        return format_name
                    if format_name == 'webp' and b'RIFF' in header[:4] and b'WEBP' in header[8:12]:
                        return format_name
                    if format_name == 'avi' and b'RIFF' in header[:4] and b'AVI ' in header[8:12]:
                        return format_name
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error detecting file format for {file_path}: {e}")
            return None
    
    async def _validate_format(self, file_path: str, detected_format: str) -> bool:
        """Validates file format structure."""
        try:
            # Check minimum file size
            file_size = os.path.getsize(file_path)
            min_size = self.min_file_sizes.get(detected_format, 0)
            if file_size < min_size:
                return False
            
            # Format-specific validation
            if detected_format in ['jpeg', 'png', 'gif', 'webp', 'bmp']:
                return await self._validate_image_format(file_path, detected_format)
            elif detected_format in ['mp4', 'avi', 'mov', 'mkv', 'webm']:
                return await self._validate_video_format(file_path, detected_format)
            elif detected_format in ['mp3', 'ogg', 'wav', 'flac', 'm4a']:
                return await self._validate_audio_format(file_path, detected_format)
            else:
                # Generic validation - just check if file is readable
                with open(file_path, 'rb') as f:
                    f.read(1024)  # Try to read first 1KB
                return True
                
        except Exception as e:
            self.logger.error(f"Error validating format for {file_path}: {e}")
            return False
    
    async def _validate_integrity(self, file_path: str, detected_format: str) -> bool:
        """Validates file integrity."""
        try:
            # Basic integrity check - ensure file is fully readable
            with open(file_path, 'rb') as f:
                chunk_size = 65536  # 64KB chunks
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating integrity for {file_path}: {e}")
            return False
    
    async def _compute_image_hash(self, file_path: str) -> Optional[str]:
        """Computes perceptual hash for images."""
        if not PIL_AVAILABLE or not IMAGEHASH_AVAILABLE:
            return None
        
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._compute_image_hash_sync, file_path)
        except Exception as e:
            self.logger.error(f"Error computing image hash for {file_path}: {e}")
            return None
    
    def _compute_image_hash_sync(self, file_path: str) -> str:
        """Synchronous image hash computation."""
        with Image.open(file_path) as img:
            # Use average hash for good balance of speed and accuracy
            hash_obj = imagehash.average_hash(img, hash_size=16)
            return str(hash_obj)
    
    async def _compute_video_hash(self, file_path: str) -> Optional[str]:
        """Computes perceptual hash for videos by sampling frames."""
        if not OPENCV_AVAILABLE:
            return None
        
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._compute_video_hash_sync, file_path)
        except Exception as e:
            self.logger.error(f"Error computing video hash for {file_path}: {e}")
            return None
    
    def _compute_video_hash_sync(self, file_path: str) -> Optional[str]:
        """Synchronous video hash computation."""
        try:
            cap = cv2.VideoCapture(file_path)
            if not cap.isOpened():
                return None
            
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            if frame_count == 0:
                return None
            
            # Sample 5 frames evenly distributed throughout the video
            sample_frames = []
            for i in range(5):
                frame_pos = int((i + 1) * frame_count / 6)
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
                ret, frame = cap.read()
                if ret:
                    # Convert to grayscale and resize
                    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                    resized = cv2.resize(gray, (8, 8))
                    sample_frames.append(resized.flatten())
            
            cap.release()
            
            if not sample_frames:
                return None
            
            # Compute average of sampled frames
            avg_frame = np.mean(sample_frames, axis=0)
            
            # Create hash from average frame
            hash_bits = []
            mean_val = np.mean(avg_frame)
            for pixel in avg_frame:
                hash_bits.append('1' if pixel > mean_val else '0')
            
            # Convert to hexadecimal
            hash_str = ''.join(hash_bits)
            hash_int = int(hash_str, 2)
            return format(hash_int, 'x')
            
        except Exception as e:
            self.logger.error(f"Error in video hash computation: {e}")
            return None
    
    async def _compute_content_hash(self, file_path: str) -> str:
        """Computes SHA-256 hash of file content."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._compute_content_hash_sync, file_path)
    
    def _compute_content_hash_sync(self, file_path: str) -> str:
        """Synchronous content hash computation."""
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    async def _detect_image_corruption(self, file_path: str) -> bool:
        """Detects corruption in image files."""
        if not PIL_AVAILABLE:
            return False  # Can't check without PIL
        
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._detect_image_corruption_sync, file_path)
        except Exception:
            return True  # Assume corrupted if we can't check
    
    def _detect_image_corruption_sync(self, file_path: str) -> bool:
        """Synchronous image corruption detection."""
        try:
            with Image.open(file_path) as img:
                # Try to load the image data
                img.load()
                # Verify image has valid dimensions
                if img.size[0] <= 0 or img.size[1] <= 0:
                    return True
                # Try to convert to RGB (this will fail for corrupted images)
                img.convert('RGB')
            return False
        except Exception:
            return True
    
    async def _detect_video_corruption(self, file_path: str) -> bool:
        """Detects corruption in video files."""
        if not OPENCV_AVAILABLE:
            return False  # Can't check without OpenCV
        
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._detect_video_corruption_sync, file_path)
        except Exception:
            return True
    
    def _detect_video_corruption_sync(self, file_path: str) -> bool:
        """Synchronous video corruption detection."""
        try:
            cap = cv2.VideoCapture(file_path)
            if not cap.isOpened():
                return True
            
            # Check if we can read basic properties
            frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
            fps = cap.get(cv2.CAP_PROP_FPS)
            width = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
            height = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
            
            cap.release()
            
            # Validate properties
            if frame_count <= 0 or fps <= 0 or width <= 0 or height <= 0:
                return True
            
            return False
        except Exception:
            return True
    
    async def _detect_audio_corruption(self, file_path: str) -> bool:
        """Detects corruption in audio files."""
        # Basic check - ensure file has reasonable size and structure
        try:
            file_size = os.path.getsize(file_path)
            if file_size < 1000:  # Very small audio files are likely corrupted
                return True
            
            # Check file header integrity
            with open(file_path, 'rb') as f:
                header = f.read(100)
                if len(header) < 50:  # Audio files should have substantial headers
                    return True
            
            return False
        except Exception:
            return True
    
    async def _detect_generic_corruption(self, file_path: str) -> bool:
        """Generic corruption detection for unknown file types."""
        try:
            # Check if file is readable
            with open(file_path, 'rb') as f:
                # Try to read the entire file in chunks
                chunk_size = 65536
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
            return False
        except Exception:
            return True
    
    async def _validate_image_format(self, file_path: str, format_name: str) -> bool:
        """Validates image format structure."""
        if not PIL_AVAILABLE:
            return True  # Can't validate without PIL
        
        try:
            with Image.open(file_path) as img:
                # Check if format matches expected
                if format_name == 'jpeg' and img.format != 'JPEG':
                    return False
                elif format_name == 'png' and img.format != 'PNG':
                    return False
                elif format_name == 'gif' and img.format != 'GIF':
                    return False
                # Add more format checks as needed
            return True
        except Exception:
            return False
    
    async def _validate_video_format(self, file_path: str, format_name: str) -> bool:
        """Validates video format structure."""
        # Basic validation - check if OpenCV can open the file
        if not OPENCV_AVAILABLE:
            return True
        
        try:
            cap = cv2.VideoCapture(file_path)
            is_valid = cap.isOpened()
            cap.release()
            return is_valid
        except Exception:
            return False
    
    async def _validate_audio_format(self, file_path: str, format_name: str) -> bool:
        """Validates audio format structure."""
        # Basic validation - check file headers
        try:
            with open(file_path, 'rb') as f:
                header = f.read(32)
            
            if format_name == 'mp3':
                return header.startswith(b'ID3') or header.startswith(b'\xff\xfb') or header.startswith(b'\xff\xf3')
            elif format_name == 'ogg':
                return header.startswith(b'OggS')
            elif format_name == 'wav':
                return header.startswith(b'RIFF') and b'WAVE' in header
            # Add more format checks as needed
            
            return True
        except Exception:
            return False
    
    async def _extract_image_metadata(self, metadata: MediaMetadata):
        """Extracts metadata from image files."""
        if not PIL_AVAILABLE:
            return
        
        try:
            with Image.open(metadata.file_path) as img:
                metadata.width = img.width
                metadata.height = img.height
                metadata.format = img.format
                
                # Extract EXIF data if available
                if hasattr(img, '_getexif') and img._getexif():
                    exif_data = {}
                    for tag_id, value in img._getexif().items():
                        tag = TAGS.get(tag_id, tag_id)
                        exif_data[tag] = value
                    metadata.exif_data = exif_data
                    
                    # Extract creation date from EXIF
                    if 'DateTime' in exif_data:
                        try:
                            metadata.creation_date = datetime.strptime(
                                exif_data['DateTime'], '%Y:%m:%d %H:%M:%S'
                            )
                        except ValueError:
                            pass
        except Exception as e:
            self.logger.error(f"Error extracting image metadata: {e}")
    
    async def _extract_video_metadata(self, metadata: MediaMetadata):
        """Extracts metadata from video files."""
        if not OPENCV_AVAILABLE:
            return
        
        try:
            cap = cv2.VideoCapture(metadata.file_path)
            if cap.isOpened():
                metadata.width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                metadata.height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                metadata.fps = cap.get(cv2.CAP_PROP_FPS)
                
                frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
                if frame_count > 0 and metadata.fps > 0:
                    metadata.duration = frame_count / metadata.fps
                
                cap.release()
        except Exception as e:
            self.logger.error(f"Error extracting video metadata: {e}")
    
    async def _extract_audio_metadata(self, metadata: MediaMetadata):
        """Extracts metadata from audio files."""
        # Basic audio metadata extraction
        # For more advanced metadata, consider using libraries like mutagen
        try:
            # This is a placeholder - implement with proper audio library
            pass
        except Exception as e:
            self.logger.error(f"Error extracting audio metadata: {e}")