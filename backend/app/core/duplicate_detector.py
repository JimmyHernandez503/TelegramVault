import os
import asyncio
import logging
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum

try:
    from PIL import Image
    import imagehash
    PIL_AVAILABLE = True
    IMAGEHASH_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    IMAGEHASH_AVAILABLE = False

try:
    import cv2
    import numpy as np
    OPENCV_AVAILABLE = True
except ImportError:
    OPENCV_AVAILABLE = False

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from backend.app.models.media import MediaFile


class DuplicateDetectionMethod(Enum):
    """Enumeration for duplicate detection methods."""
    HASH = "hash"
    PERCEPTUAL = "perceptual"
    CONTENT = "content"
    MANUAL = "manual"


class SimilarityLevel(Enum):
    """Enumeration for similarity levels."""
    IDENTICAL = "identical"
    VERY_HIGH = "very_high"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


@dataclass
class DuplicateMatch:
    """Data class for duplicate match results."""
    original_media_id: int
    duplicate_media_id: int
    similarity_score: float
    detection_method: DuplicateDetectionMethod
    similarity_level: SimilarityLevel
    hash_distance: Optional[int] = None
    metadata_similarity: Optional[float] = None


@dataclass
class PerceptualHashResult:
    """Data class for perceptual hash computation results."""
    file_path: str
    hash_value: str
    hash_method: str
    computation_time: float
    error_message: Optional[str] = None


class DuplicateDetector:
    """
    Advanced duplicate detection using multiple algorithms including
    perceptual hashing, content hashing, and metadata comparison.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Similarity thresholds for different hash distances
        self.similarity_thresholds = {
            SimilarityLevel.IDENTICAL: 0,
            SimilarityLevel.VERY_HIGH: 5,
            SimilarityLevel.HIGH: 10,
            SimilarityLevel.MEDIUM: 15,
            SimilarityLevel.LOW: 20
        }
        
        # Hash methods for different media types
        self.image_hash_methods = {
            'average': imagehash.average_hash if IMAGEHASH_AVAILABLE else None,
            'perceptual': imagehash.phash if IMAGEHASH_AVAILABLE else None,
            'difference': imagehash.dhash if IMAGEHASH_AVAILABLE else None,
            'wavelet': imagehash.whash if IMAGEHASH_AVAILABLE else None,
        }
    
    async def find_duplicates(
        self,
        media_file: MediaFile,
        db_session: AsyncSession,
        similarity_threshold: SimilarityLevel = SimilarityLevel.HIGH
    ) -> List[DuplicateMatch]:
        """
        Finds duplicate media files using multiple detection methods.
        
        Args:
            media_file: The media file to check for duplicates
            db_session: Database session
            similarity_threshold: Minimum similarity level to consider as duplicate
            
        Returns:
            List of duplicate matches found
        """
        duplicates = []
        
        try:
            # Method 1: Exact hash match
            if media_file.file_hash:
                hash_duplicates = await self._find_hash_duplicates(
                    media_file, db_session
                )
                duplicates.extend(hash_duplicates)
            
            # Method 2: Perceptual hash match (for images and videos)
            if media_file.file_type in ['photo', 'video', 'gif'] and media_file.file_path:
                perceptual_duplicates = await self._find_perceptual_duplicates(
                    media_file, db_session, similarity_threshold
                )
                duplicates.extend(perceptual_duplicates)
            
            # Method 3: Metadata-based similarity (for all media types)
            metadata_duplicates = await self._find_metadata_duplicates(
                media_file, db_session, similarity_threshold
            )
            duplicates.extend(metadata_duplicates)
            
            # Remove duplicates from results list
            seen_ids = set()
            unique_duplicates = []
            for dup in duplicates:
                if dup.duplicate_media_id not in seen_ids:
                    unique_duplicates.append(dup)
                    seen_ids.add(dup.duplicate_media_id)
            
            return unique_duplicates
            
        except Exception as e:
            self.logger.error(f"Error finding duplicates for media {media_file.id}: {e}")
            return []
    
    async def compute_perceptual_hash(
        self,
        file_path: str,
        media_type: str,
        hash_method: str = 'average'
    ) -> Optional[PerceptualHashResult]:
        """
        Computes perceptual hash for a media file.
        
        Args:
            file_path: Path to the media file
            media_type: Type of media (photo, video, etc.)
            hash_method: Hash method to use
            
        Returns:
            PerceptualHashResult or None if computation fails
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            if media_type == "photo":
                hash_value = await self._compute_image_perceptual_hash(
                    file_path, hash_method
                )
            elif media_type in ["video", "gif"]:
                hash_value = await self._compute_video_perceptual_hash(file_path)
            else:
                # Fallback to content hash for other types
                hash_value = await self._compute_content_hash(file_path)
                hash_method = 'content'
            
            computation_time = asyncio.get_event_loop().time() - start_time
            
            if hash_value:
                return PerceptualHashResult(
                    file_path=file_path,
                    hash_value=hash_value,
                    hash_method=hash_method,
                    computation_time=computation_time
                )
            else:
                return PerceptualHashResult(
                    file_path=file_path,
                    hash_value="",
                    hash_method=hash_method,
                    computation_time=computation_time,
                    error_message="Hash computation failed"
                )
                
        except Exception as e:
            computation_time = asyncio.get_event_loop().time() - start_time
            self.logger.error(f"Error computing perceptual hash for {file_path}: {e}")
            return PerceptualHashResult(
                file_path=file_path,
                hash_value="",
                hash_method=hash_method,
                computation_time=computation_time,
                error_message=str(e)
            )
    
    async def compare_perceptual_hashes(
        self,
        hash1: str,
        hash2: str,
        hash_method: str = 'average'
    ) -> Tuple[int, SimilarityLevel]:
        """
        Compares two perceptual hashes and returns similarity.
        
        Args:
            hash1: First hash string
            hash2: Second hash string
            hash_method: Hash method used
            
        Returns:
            Tuple of (hamming_distance, similarity_level)
        """
        try:
            if not hash1 or not hash2:
                return float('inf'), SimilarityLevel.NONE
            
            if hash_method == 'content':
                # For content hashes, exact match only
                distance = 0 if hash1 == hash2 else float('inf')
            else:
                # For perceptual hashes, compute Hamming distance
                if IMAGEHASH_AVAILABLE:
                    try:
                        h1 = imagehash.hex_to_hash(hash1)
                        h2 = imagehash.hex_to_hash(hash2)
                        distance = h1 - h2
                    except ValueError:
                        # Fallback to string comparison
                        distance = sum(c1 != c2 for c1, c2 in zip(hash1, hash2))
                else:
                    # Simple string comparison fallback
                    distance = sum(c1 != c2 for c1, c2 in zip(hash1, hash2))
            
            # Determine similarity level
            similarity_level = SimilarityLevel.NONE
            for level, threshold in self.similarity_thresholds.items():
                if distance <= threshold:
                    similarity_level = level
                    break
            
            return distance, similarity_level
            
        except Exception as e:
            self.logger.error(f"Error comparing hashes: {e}")
            return float('inf'), SimilarityLevel.NONE
    
    async def detect_near_duplicates(
        self,
        media_files: List[MediaFile],
        similarity_threshold: SimilarityLevel = SimilarityLevel.HIGH
    ) -> List[List[MediaFile]]:
        """
        Detects groups of near-duplicate media files.
        
        Args:
            media_files: List of media files to analyze
            similarity_threshold: Minimum similarity level for grouping
            
        Returns:
            List of groups, where each group contains similar media files
        """
        try:
            # Group files by type for more efficient processing
            photo_files = [f for f in media_files if f.file_type == 'photo']
            video_files = [f for f in media_files if f.file_type in ['video', 'gif']]
            
            duplicate_groups = []
            
            # Process photos
            if photo_files:
                photo_groups = await self._group_similar_images(
                    photo_files, similarity_threshold
                )
                duplicate_groups.extend(photo_groups)
            
            # Process videos
            if video_files:
                video_groups = await self._group_similar_videos(
                    video_files, similarity_threshold
                )
                duplicate_groups.extend(video_groups)
            
            return duplicate_groups
            
        except Exception as e:
            self.logger.error(f"Error detecting near duplicates: {e}")
            return []
    
    async def _find_hash_duplicates(
        self,
        media_file: MediaFile,
        db_session: AsyncSession
    ) -> List[DuplicateMatch]:
        """Finds duplicates based on exact file hash match."""
        if not media_file.file_hash:
            return []
        
        try:
            # Find files with same hash but different ID
            result = await db_session.execute(
                select(MediaFile).where(
                    and_(
                        MediaFile.file_hash == media_file.file_hash,
                        MediaFile.id != media_file.id
                    )
                )
            )
            
            duplicates = []
            for duplicate in result.scalars():
                duplicates.append(DuplicateMatch(
                    original_media_id=duplicate.id,
                    duplicate_media_id=media_file.id,
                    similarity_score=1.0,
                    detection_method=DuplicateDetectionMethod.HASH,
                    similarity_level=SimilarityLevel.IDENTICAL,
                    hash_distance=0
                ))
            
            return duplicates
            
        except Exception as e:
            self.logger.error(f"Error finding hash duplicates: {e}")
            return []
    
    async def _find_perceptual_duplicates(
        self,
        media_file: MediaFile,
        db_session: AsyncSession,
        similarity_threshold: SimilarityLevel
    ) -> List[DuplicateMatch]:
        """Finds duplicates based on perceptual hash similarity."""
        if not media_file.perceptual_hash:
            return []
        
        try:
            # Find files with perceptual hashes of the same type
            result = await db_session.execute(
                select(MediaFile).where(
                    and_(
                        MediaFile.perceptual_hash.isnot(None),
                        MediaFile.file_type == media_file.file_type,
                        MediaFile.id != media_file.id
                    )
                )
            )
            
            duplicates = []
            threshold_value = self.similarity_thresholds[similarity_threshold]
            
            for candidate in result.scalars():
                if not candidate.perceptual_hash:
                    continue
                
                distance, similarity_level = await self.compare_perceptual_hashes(
                    media_file.perceptual_hash,
                    candidate.perceptual_hash
                )
                
                if distance <= threshold_value:
                    similarity_score = max(0.0, 1.0 - (distance / 64.0))  # Normalize to 0-1
                    
                    duplicates.append(DuplicateMatch(
                        original_media_id=candidate.id,
                        duplicate_media_id=media_file.id,
                        similarity_score=similarity_score,
                        detection_method=DuplicateDetectionMethod.PERCEPTUAL,
                        similarity_level=similarity_level,
                        hash_distance=distance
                    ))
            
            return duplicates
            
        except Exception as e:
            self.logger.error(f"Error finding perceptual duplicates: {e}")
            return []
    
    async def _find_metadata_duplicates(
        self,
        media_file: MediaFile,
        db_session: AsyncSession,
        similarity_threshold: SimilarityLevel
    ) -> List[DuplicateMatch]:
        """Finds duplicates based on metadata similarity."""
        try:
            # Find files with similar metadata
            conditions = [MediaFile.id != media_file.id]
            
            # Add conditions based on available metadata
            if media_file.file_size:
                # Allow 5% size difference
                size_tolerance = int(media_file.file_size * 0.05)
                conditions.extend([
                    MediaFile.file_size >= media_file.file_size - size_tolerance,
                    MediaFile.file_size <= media_file.file_size + size_tolerance
                ])
            
            if media_file.mime_type:
                conditions.append(MediaFile.mime_type == media_file.mime_type)
            
            if media_file.width and media_file.height:
                conditions.extend([
                    MediaFile.width == media_file.width,
                    MediaFile.height == media_file.height
                ])
            
            if media_file.duration:
                # Allow 1 second duration difference
                conditions.extend([
                    MediaFile.duration >= media_file.duration - 1,
                    MediaFile.duration <= media_file.duration + 1
                ])
            
            result = await db_session.execute(
                select(MediaFile).where(and_(*conditions))
            )
            
            duplicates = []
            for candidate in result.scalars():
                similarity_score = self._calculate_metadata_similarity(
                    media_file, candidate
                )
                
                # Convert similarity score to similarity level
                if similarity_score >= 0.95:
                    similarity_level = SimilarityLevel.VERY_HIGH
                elif similarity_score >= 0.85:
                    similarity_level = SimilarityLevel.HIGH
                elif similarity_score >= 0.70:
                    similarity_level = SimilarityLevel.MEDIUM
                elif similarity_score >= 0.50:
                    similarity_level = SimilarityLevel.LOW
                else:
                    continue  # Skip low similarity matches
                
                # Check if this meets our threshold
                threshold_scores = {
                    SimilarityLevel.VERY_HIGH: 0.95,
                    SimilarityLevel.HIGH: 0.85,
                    SimilarityLevel.MEDIUM: 0.70,
                    SimilarityLevel.LOW: 0.50
                }
                
                if similarity_score >= threshold_scores.get(similarity_threshold, 0.85):
                    duplicates.append(DuplicateMatch(
                        original_media_id=candidate.id,
                        duplicate_media_id=media_file.id,
                        similarity_score=similarity_score,
                        detection_method=DuplicateDetectionMethod.CONTENT,
                        similarity_level=similarity_level,
                        metadata_similarity=similarity_score
                    ))
            
            return duplicates
            
        except Exception as e:
            self.logger.error(f"Error finding metadata duplicates: {e}")
            return []
    
    def _calculate_metadata_similarity(
        self,
        file1: MediaFile,
        file2: MediaFile
    ) -> float:
        """Calculates similarity score based on metadata comparison."""
        similarity_factors = []
        
        # File size similarity (weight: 0.3)
        if file1.file_size and file2.file_size:
            size_diff = abs(file1.file_size - file2.file_size)
            max_size = max(file1.file_size, file2.file_size)
            size_similarity = 1.0 - (size_diff / max_size) if max_size > 0 else 1.0
            similarity_factors.append((size_similarity, 0.3))
        
        # MIME type match (weight: 0.2)
        if file1.mime_type and file2.mime_type:
            mime_similarity = 1.0 if file1.mime_type == file2.mime_type else 0.0
            similarity_factors.append((mime_similarity, 0.2))
        
        # Dimensions similarity (weight: 0.25)
        if file1.width and file1.height and file2.width and file2.height:
            width_diff = abs(file1.width - file2.width)
            height_diff = abs(file1.height - file2.height)
            max_width = max(file1.width, file2.width)
            max_height = max(file1.height, file2.height)
            
            width_sim = 1.0 - (width_diff / max_width) if max_width > 0 else 1.0
            height_sim = 1.0 - (height_diff / max_height) if max_height > 0 else 1.0
            dim_similarity = (width_sim + height_sim) / 2
            similarity_factors.append((dim_similarity, 0.25))
        
        # Duration similarity (weight: 0.25)
        if file1.duration and file2.duration:
            duration_diff = abs(file1.duration - file2.duration)
            max_duration = max(file1.duration, file2.duration)
            duration_similarity = 1.0 - (duration_diff / max_duration) if max_duration > 0 else 1.0
            similarity_factors.append((duration_similarity, 0.25))
        
        # Calculate weighted average
        if similarity_factors:
            total_weight = sum(weight for _, weight in similarity_factors)
            weighted_sum = sum(score * weight for score, weight in similarity_factors)
            return weighted_sum / total_weight if total_weight > 0 else 0.0
        
        return 0.0
    
    async def _compute_image_perceptual_hash(
        self,
        file_path: str,
        hash_method: str = 'average'
    ) -> Optional[str]:
        """Computes perceptual hash for images."""
        if not PIL_AVAILABLE or not IMAGEHASH_AVAILABLE:
            return None
        
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, self._compute_image_hash_sync, file_path, hash_method
            )
        except Exception as e:
            self.logger.error(f"Error computing image hash: {e}")
            return None
    
    def _compute_image_hash_sync(self, file_path: str, hash_method: str) -> Optional[str]:
        """Synchronous image hash computation."""
        try:
            with Image.open(file_path) as img:
                hash_func = self.image_hash_methods.get(hash_method)
                if not hash_func:
                    hash_func = self.image_hash_methods['average']
                
                hash_obj = hash_func(img, hash_size=16)
                return str(hash_obj)
        except Exception as e:
            self.logger.error(f"Error in sync image hash computation: {e}")
            return None
    
    async def _compute_video_perceptual_hash(self, file_path: str) -> Optional[str]:
        """Computes perceptual hash for videos."""
        if not OPENCV_AVAILABLE:
            return None
        
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, self._compute_video_hash_sync, file_path
            )
        except Exception as e:
            self.logger.error(f"Error computing video hash: {e}")
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
            
            # Sample frames at different positions
            sample_positions = [0.1, 0.3, 0.5, 0.7, 0.9]
            sample_frames = []
            
            for pos in sample_positions:
                frame_pos = int(pos * frame_count)
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
                ret, frame = cap.read()
                if ret:
                    # Convert to grayscale and resize to 8x8
                    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                    resized = cv2.resize(gray, (8, 8))
                    sample_frames.append(resized.flatten())
            
            cap.release()
            
            if not sample_frames:
                return None
            
            # Compute average frame
            avg_frame = np.mean(sample_frames, axis=0)
            
            # Create hash
            hash_bits = []
            mean_val = np.mean(avg_frame)
            for pixel in avg_frame:
                hash_bits.append('1' if pixel > mean_val else '0')
            
            # Convert to hexadecimal
            hash_str = ''.join(hash_bits)
            hash_int = int(hash_str, 2)
            return format(hash_int, '016x')  # 16-character hex string
            
        except Exception as e:
            self.logger.error(f"Error in video hash computation: {e}")
            return None
    
    async def _compute_content_hash(self, file_path: str) -> str:
        """Computes SHA-256 hash of file content."""
        import hashlib
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._compute_content_hash_sync, file_path)
    
    def _compute_content_hash_sync(self, file_path: str) -> str:
        """Synchronous content hash computation."""
        import hashlib
        
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    async def _group_similar_images(
        self,
        image_files: List[MediaFile],
        similarity_threshold: SimilarityLevel
    ) -> List[List[MediaFile]]:
        """Groups similar images together."""
        if not image_files:
            return []
        
        groups = []
        processed = set()
        threshold_value = self.similarity_thresholds[similarity_threshold]
        
        for i, file1 in enumerate(image_files):
            if file1.id in processed or not file1.perceptual_hash:
                continue
            
            group = [file1]
            processed.add(file1.id)
            
            for j, file2 in enumerate(image_files[i+1:], i+1):
                if file2.id in processed or not file2.perceptual_hash:
                    continue
                
                distance, _ = await self.compare_perceptual_hashes(
                    file1.perceptual_hash, file2.perceptual_hash
                )
                
                if distance <= threshold_value:
                    group.append(file2)
                    processed.add(file2.id)
            
            if len(group) > 1:
                groups.append(group)
        
        return groups
    
    async def _group_similar_videos(
        self,
        video_files: List[MediaFile],
        similarity_threshold: SimilarityLevel
    ) -> List[List[MediaFile]]:
        """Groups similar videos together."""
        if not video_files:
            return []
        
        groups = []
        processed = set()
        threshold_value = self.similarity_thresholds[similarity_threshold]
        
        for i, file1 in enumerate(video_files):
            if file1.id in processed:
                continue
            
            group = [file1]
            processed.add(file1.id)
            
            for j, file2 in enumerate(video_files[i+1:], i+1):
                if file2.id in processed:
                    continue
                
                # Compare based on available hashes or metadata
                similarity_score = 0.0
                
                if file1.perceptual_hash and file2.perceptual_hash:
                    distance, _ = await self.compare_perceptual_hashes(
                        file1.perceptual_hash, file2.perceptual_hash
                    )
                    if distance <= threshold_value:
                        similarity_score = max(similarity_score, 1.0 - (distance / 64.0))
                
                # Also check metadata similarity
                metadata_sim = self._calculate_metadata_similarity(file1, file2)
                similarity_score = max(similarity_score, metadata_sim)
                
                if similarity_score >= 0.85:  # High similarity threshold for videos
                    group.append(file2)
                    processed.add(file2.id)
            
            if len(group) > 1:
                groups.append(group)
        
        return groups