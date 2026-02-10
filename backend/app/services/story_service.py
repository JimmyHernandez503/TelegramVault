import os
import asyncio
from datetime import datetime
from typing import Optional
from telethon import TelegramClient
from telethon.tl.functions.stories import GetPeerStoriesRequest
from telethon.tl.types import InputPeerUser, StoryItemSkipped, StoryItemDeleted
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.app.models.telegram_user import TelegramUser
from backend.app.models.history import UserStory
from backend.app.core.config import settings


class StoryService:
    def __init__(self, client: TelegramClient, db: AsyncSession):
        self.client = client
        self.db = db
        self.media_path = settings.MEDIA_PATH
    
    async def download_user_stories(self, user: TelegramUser) -> list[dict]:
        if not user.has_stories:
            return []
        
        try:
            input_peer = InputPeerUser(user.telegram_id, user.access_hash or 0)
            
            result = await self.client(GetPeerStoriesRequest(peer=input_peer))
            
            stories_data = []
            
            if not hasattr(result, 'stories') or not result.stories:
                return []
            
            peer_stories = result.stories
            if not hasattr(peer_stories, 'stories'):
                return []
            
            for story in peer_stories.stories:
                if isinstance(story, (StoryItemSkipped, StoryItemDeleted)):
                    continue
                
                existing = await self.db.execute(
                    select(UserStory).where(
                        UserStory.user_id == user.id,
                        UserStory.story_id == story.id
                    )
                )
                if existing.scalar_one_or_none():
                    continue
                
                story_type = "photo"
                width = None
                height = None
                duration = None
                file_path = None
                
                if hasattr(story, 'media'):
                    media = story.media
                    
                    if hasattr(media, 'video'):
                        story_type = "video"
                        if hasattr(media, 'w'):
                            width = media.w
                        if hasattr(media, 'h'):
                            height = media.h
                        if hasattr(media, 'duration'):
                            duration = int(media.duration)
                    elif hasattr(media, 'photo'):
                        story_type = "photo"
                        if hasattr(media.photo, 'sizes') and media.photo.sizes:
                            largest = max(media.photo.sizes, key=lambda s: getattr(s, 'w', 0) * getattr(s, 'h', 0))
                            width = getattr(largest, 'w', None)
                            height = getattr(largest, 'h', None)
                    
                    story_dir = os.path.join(self.media_path, "stories", str(user.telegram_id))
                    os.makedirs(story_dir, exist_ok=True)
                    
                    ext = "mp4" if story_type == "video" else "jpg"
                    filename = f"{story.id}.{ext}"
                    full_path = os.path.join(story_dir, filename)
                    
                    try:
                        await self.client.download_media(media, file=full_path)
                        file_path = f"media/stories/{user.telegram_id}/{filename}"
                    except Exception as e:
                        print(f"[StoryService] Failed to download story {story.id}: {e}")
                
                posted_at = None
                if hasattr(story, 'date') and story.date:
                    dt = story.date
                    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                        posted_at = dt.replace(tzinfo=None)
                    else:
                        posted_at = dt
                
                expires_at = None
                if hasattr(story, 'expire_date') and story.expire_date:
                    dt = story.expire_date
                    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                        expires_at = dt.replace(tzinfo=None)
                    else:
                        expires_at = dt
                
                caption = None
                if hasattr(story, 'caption'):
                    caption = story.caption
                
                views_count = 0
                if hasattr(story, 'views') and story.views:
                    views_count = getattr(story.views, 'views_count', 0)
                
                is_pinned = getattr(story, 'pinned', False)
                is_public = getattr(story, 'public', True)
                
                user_story = UserStory(
                    user_id=user.id,
                    story_id=story.id,
                    story_type=story_type,
                    file_path=file_path,
                    caption=caption,
                    width=width,
                    height=height,
                    duration=duration,
                    views_count=views_count,
                    posted_at=posted_at,
                    expires_at=expires_at,
                    is_pinned=is_pinned,
                    is_public=is_public
                )
                
                self.db.add(user_story)
                
                stories_data.append({
                    "story_id": story.id,
                    "story_type": story_type,
                    "file_path": file_path,
                    "caption": caption,
                    "views_count": views_count,
                    "posted_at": posted_at.isoformat() if posted_at else None
                })
            
            await self.db.commit()
            print(f"[StoryService] Saved {len(stories_data)} stories for user {user.telegram_id}")
            
            return stories_data
            
        except Exception as e:
            print(f"[StoryService] Error fetching stories for user {user.telegram_id}: {e}")
            try:
                await self.db.rollback()
            except:
                pass
            return []
    
