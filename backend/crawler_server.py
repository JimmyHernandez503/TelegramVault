import asyncio
import atexit
import os
from pathlib import Path
from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from sqlalchemy import select, func, distinct, or_, exists
from sqlalchemy.orm import selectinload
from contextlib import asynccontextmanager
import uvicorn

from backend.app.db.database import async_session_maker
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.media import MediaFile
from backend.app.models.history import UserProfilePhoto, UserStory
from backend.app.core.config import get_settings

settings = get_settings()
DATA_DIR = Path(settings.MEDIA_PATH)
USERS_PER_PAGE = 100

DATA_DIR.mkdir(parents=True, exist_ok=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

app = FastAPI(
    title="TelegramVault Crawler API",
    description="Static API for facial recognition crawlers",
    version="1.0.0",
    lifespan=lifespan
)

app.mount("/media", StaticFiles(directory=str(DATA_DIR)), name="media")


def clean_media_path(file_path: str) -> str:
    if file_path and file_path.startswith("media/"):
        return file_path[6:]
    return file_path


def file_exists(file_path: str) -> bool:
    """Check if a media file exists in the mounted volume"""
    if not file_path:
        return False
    
    clean_path = clean_media_path(file_path)
    full_path = DATA_DIR / clean_path
    return full_path.exists()


async def get_filtered_users(page: int = 1):
    offset = (page - 1) * USERS_PER_PAGE
    
    async with async_session_maker() as db:
        has_profile_photo = (
            select(UserProfilePhoto.user_id)
            .where(UserProfilePhoto.user_id == TelegramUser.id)
            .where(UserProfilePhoto.file_path.isnot(None))
            .exists()
        )
        
        has_story = (
            select(UserStory.user_id)
            .where(UserStory.user_id == TelegramUser.id)
            .where(UserStory.file_path.isnot(None))
            .exists()
        )
        
        has_media_message = (
            select(TelegramMessage.sender_id)
            .join(MediaFile, MediaFile.message_id == TelegramMessage.id)
            .where(TelegramMessage.sender_id == TelegramUser.id)
            .where(MediaFile.file_type.in_(["photo", "video", "gif", "video_note"]))
            .where(MediaFile.file_path.isnot(None))
            .exists()
        )
        
        filter_condition = or_(has_profile_photo, has_story, has_media_message)
        
        count_query = select(func.count(TelegramUser.id)).where(filter_condition)
        total_result = await db.execute(count_query)
        total_users = total_result.scalar() or 0
        total_pages = (total_users + USERS_PER_PAGE - 1) // USERS_PER_PAGE
        
        photo_count_subq = (
            select(func.count(UserProfilePhoto.id))
            .where(UserProfilePhoto.user_id == TelegramUser.id)
            .where(UserProfilePhoto.file_path.isnot(None))
            .correlate(TelegramUser)
            .scalar_subquery()
        )
        
        story_count_subq = (
            select(func.count(UserStory.id))
            .where(UserStory.user_id == TelegramUser.id)
            .where(UserStory.file_path.isnot(None))
            .correlate(TelegramUser)
            .scalar_subquery()
        )
        
        query = (
            select(TelegramUser)
            .options(
                selectinload(TelegramUser.profile_photos),
                selectinload(TelegramUser.stories)
            )
            .where(filter_condition)
            .order_by((photo_count_subq + story_count_subq).desc(), TelegramUser.id)
            .offset(offset)
            .limit(USERS_PER_PAGE)
        )
        
        result = await db.execute(query)
        users = result.scalars().all()
        
        user_ids = [u.id for u in users]
        if user_ids:
            media_query = (
                select(
                    TelegramMessage.sender_id,
                    MediaFile.id,
                    MediaFile.file_path,
                    MediaFile.file_type
                )
                .join(MediaFile, MediaFile.message_id == TelegramMessage.id)
                .where(TelegramMessage.sender_id.in_(user_ids))
                .where(MediaFile.file_type.in_(["photo", "video", "gif", "video_note"]))
                .where(MediaFile.file_path.isnot(None))
            )
            media_result = await db.execute(media_query)
            media_rows = media_result.fetchall()
            
            user_media = {}
            for row in media_rows:
                sender_id = row[0]
                if sender_id not in user_media:
                    user_media[sender_id] = []
                user_media[sender_id].append({
                    "id": row[1],
                    "file_path": row[2],
                    "file_type": row[3]
                })
        else:
            user_media = {}
        
        users_data = []
        for user in users:
            profile_photos = [
                {
                    "id": p.id,
                    "file_path": p.file_path,
                    "is_current": p.is_current,
                    "is_video": p.is_video,
                    "captured_at": p.captured_at.isoformat() if p.captured_at else None
                }
                for p in user.profile_photos
                if p.file_path and file_exists(p.file_path)
            ]
            
            stories = [
                {
                    "id": s.id,
                    "story_id": s.story_id,
                    "story_type": s.story_type,
                    "file_path": s.file_path,
                    "caption": s.caption,
                    "posted_at": s.posted_at.isoformat() if s.posted_at else None
                }
                for s in user.stories
                if s.file_path and file_exists(s.file_path)
            ]
            
            sent_media = [m for m in user_media.get(user.id, []) if file_exists(m['file_path'])]
            
            users_data.append({
                "id": user.id,
                "telegram_id": user.telegram_id,
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "phone": user.phone,
                "profile_photos": profile_photos,
                "stories": stories,
                "sent_media": sent_media,
                "stats": {
                    "profile_photos_count": len(profile_photos),
                    "stories_count": len(stories),
                    "sent_media_count": len(sent_media)
                }
            })
        
        return {
            "page": page,
            "total_pages": total_pages,
            "total_users": total_users,
            "users_per_page": USERS_PER_PAGE,
            "users": users_data
        }


@app.get("/", response_class=HTMLResponse)
async def index(page: int = Query(1, ge=1)):
    data = await get_filtered_users(page)
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TelegramVault Crawler - User List</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a0a; color: #fff; padding: 20px; }}
        .header {{ margin-bottom: 30px; padding-bottom: 20px; border-bottom: 1px solid #333; }}
        .header h1 {{ color: #3b82f6; font-size: 24px; }}
        .header p {{ color: #888; margin-top: 5px; }}
        .stats {{ display: flex; gap: 20px; margin-top: 15px; flex-wrap: wrap; }}
        .stat {{ background: #1a1a1a; padding: 10px 20px; border-radius: 8px; }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: #3b82f6; }}
        .stat-label {{ font-size: 12px; color: #888; }}
        .pagination {{ display: flex; gap: 10px; margin: 20px 0; flex-wrap: wrap; }}
        .pagination a {{ padding: 8px 16px; background: #1a1a1a; border-radius: 6px; color: #fff; text-decoration: none; }}
        .pagination a:hover {{ background: #333; }}
        .pagination a.active {{ background: #3b82f6; }}
        .user-list {{ display: flex; flex-direction: column; gap: 8px; }}
        .user-row {{ display: flex; align-items: center; gap: 15px; padding: 15px 20px; background: #1a1a1a; border-radius: 10px; border: 1px solid #333; text-decoration: none; color: #fff; transition: all 0.2s; }}
        .user-row:hover {{ background: #252525; border-color: #3b82f6; transform: translateX(5px); }}
        .user-avatar {{ width: 50px; height: 50px; border-radius: 50%; object-fit: cover; background: #333; flex-shrink: 0; }}
        .user-info {{ flex: 1; min-width: 0; }}
        .user-name {{ font-size: 16px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .user-username {{ color: #888; font-size: 14px; }}
        .user-id {{ color: #666; font-size: 12px; font-family: monospace; }}
        .user-stats {{ display: flex; gap: 15px; flex-shrink: 0; }}
        .user-stat {{ text-align: center; min-width: 60px; }}
        .user-stat-value {{ font-size: 18px; font-weight: bold; color: #3b82f6; }}
        .user-stat-label {{ font-size: 10px; color: #666; text-transform: uppercase; }}
        .arrow {{ color: #666; font-size: 20px; }}
        .json-link {{ display: inline-block; margin-top: 10px; color: #3b82f6; text-decoration: none; font-size: 14px; }}
        .json-link:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>TelegramVault Crawler</h1>
        <p>Users with media content - sorted by media count</p>
        <div class="stats">
            <div class="stat">
                <div class="stat-value">{data['total_users']}</div>
                <div class="stat-label">Users with Media</div>
            </div>
            <div class="stat">
                <div class="stat-value">{data['page']} / {data['total_pages']}</div>
                <div class="stat-label">Current Page</div>
            </div>
        </div>
        <a href="/api/users?page={page}" class="json-link">View as JSON</a>
    </div>
    
    <div class="pagination">"""
    
    start_page = max(1, data['page'] - 5)
    end_page = min(data['total_pages'], data['page'] + 5)
    
    if data['page'] > 1:
        html += f'<a href="/?page=1">First</a>'
        html += f'<a href="/?page={data["page"]-1}">Prev</a>'
    
    for p in range(start_page, end_page + 1):
        active = "active" if p == data['page'] else ""
        html += f'<a href="/?page={p}" class="{active}">{p}</a>'
    
    if data['page'] < data['total_pages']:
        html += f'<a href="/?page={data["page"]+1}">Next</a>'
        html += f'<a href="/?page={data["total_pages"]}">Last</a>'
    
    html += """
    </div>
    
    <div class="user-list">"""
    
    for user in data['users']:
        display_name = user['first_name'] or ''
        if user['last_name']:
            display_name += f" {user['last_name']}"
        if not display_name.strip():
            display_name = user['username'] or f"User {user['telegram_id']}"
        
        avatar_url = ""
        if user['profile_photos']:
            current_photos = [p for p in user['profile_photos'] if p['is_current']]
            if current_photos:
                avatar_url = f"/media/{clean_media_path(current_photos[0]['file_path'])}"
            elif user['profile_photos']:
                avatar_url = f"/media/{clean_media_path(user['profile_photos'][0]['file_path'])}"
        
        stats = user['stats']
        
        html += f"""
        <a href="/user/{user['telegram_id']}" class="user-row">
            {'<img src="' + avatar_url + '" class="user-avatar" alt="Avatar">' if avatar_url else '<div class="user-avatar"></div>'}
            <div class="user-info">
                <div class="user-name">{display_name}</div>
                <div class="user-username">@{user['username'] or 'no_username'}</div>
                <div class="user-id">ID: {user['telegram_id']}</div>
            </div>
            <div class="user-stats">
                <div class="user-stat">
                    <div class="user-stat-value">{stats['profile_photos_count']}</div>
                    <div class="user-stat-label">Photos</div>
                </div>
                <div class="user-stat">
                    <div class="user-stat-value">{stats['stories_count']}</div>
                    <div class="user-stat-label">Stories</div>
                </div>
                <div class="user-stat">
                    <div class="user-stat-value">{stats['sent_media_count']}</div>
                    <div class="user-stat-label">Media</div>
                </div>
            </div>
            <span class="arrow">›</span>
        </a>"""
    
    html += """
    </div>
    
    <div class="pagination" style="margin-top: 30px;">"""
    
    if data['page'] > 1:
        html += f'<a href="/?page=1">First</a>'
        html += f'<a href="/?page={data["page"]-1}">Prev</a>'
    
    for p in range(start_page, end_page + 1):
        active = "active" if p == data['page'] else ""
        html += f'<a href="/?page={p}" class="{active}">{p}</a>'
    
    if data['page'] < data['total_pages']:
        html += f'<a href="/?page={data["page"]+1}">Next</a>'
        html += f'<a href="/?page={data["total_pages"]}">Last</a>'
    
    html += """
    </div>
</body>
</html>"""
    
    return HTMLResponse(content=html)


@app.get("/user/{telegram_id}", response_class=HTMLResponse)
async def user_profile_page(telegram_id: int):
    async with async_session_maker() as db:
        result = await db.execute(
            select(TelegramUser)
            .where(TelegramUser.telegram_id == telegram_id)
            .options(
                selectinload(TelegramUser.profile_photos),
                selectinload(TelegramUser.stories)
            )
        )
        user = result.scalar_one_or_none()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        media_query = (
            select(
                MediaFile.id,
                MediaFile.file_path,
                MediaFile.file_type,
                TelegramMessage.date
            )
            .join(TelegramMessage, MediaFile.message_id == TelegramMessage.id)
            .where(TelegramMessage.sender_id == user.id)
            .where(MediaFile.file_type.in_(["photo", "video", "gif", "video_note"]))
            .where(MediaFile.file_path.isnot(None))
            .order_by(TelegramMessage.date.desc())
        )
        media_result = await db.execute(media_query)
        sent_media = [
            {"id": row[0], "file_path": row[1], "file_type": row[2]}
            for row in media_result.fetchall()
            if file_exists(row[1])
        ]
        
        profile_photos = [p for p in user.profile_photos if p.file_path and file_exists(p.file_path)]
        stories = [s for s in user.stories if s.file_path and file_exists(s.file_path)]
        
        display_name = user.first_name or ''
        if user.last_name:
            display_name += f" {user.last_name}"
        if not display_name.strip():
            display_name = user.username or f"User {user.telegram_id}"
        
        title = f"{user.telegram_id} | {display_name}"
        if user.username:
            title += f" | @{user.username}"
        
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a0a; color: #fff; padding: 20px; }}
        .back-link {{ display: inline-flex; align-items: center; gap: 8px; color: #3b82f6; text-decoration: none; margin-bottom: 20px; font-size: 14px; }}
        .back-link:hover {{ text-decoration: underline; }}
        .profile-header {{ display: flex; align-items: center; gap: 20px; padding: 25px; background: #1a1a1a; border-radius: 12px; margin-bottom: 25px; }}
        .profile-avatar {{ width: 100px; height: 100px; border-radius: 50%; object-fit: cover; background: #333; }}
        .profile-info h1 {{ font-size: 24px; margin-bottom: 5px; }}
        .profile-info .username {{ color: #3b82f6; font-size: 18px; }}
        .profile-info .telegram-id {{ color: #888; font-size: 14px; font-family: monospace; margin-top: 5px; }}
        .profile-info .bio {{ color: #aaa; margin-top: 10px; font-size: 14px; }}
        .profile-stats {{ display: flex; gap: 20px; margin-top: 15px; }}
        .profile-stat {{ background: #252525; padding: 8px 15px; border-radius: 6px; }}
        .profile-stat-value {{ font-weight: bold; color: #3b82f6; }}
        .section {{ margin-bottom: 30px; }}
        .section h2 {{ font-size: 18px; color: #888; margin-bottom: 15px; padding-bottom: 10px; border-bottom: 1px solid #333; }}
        .media-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 12px; }}
        .media-item {{ position: relative; aspect-ratio: 1; overflow: hidden; border-radius: 10px; background: #222; }}
        .media-item img, .media-item video {{ width: 100%; height: 100%; object-fit: cover; cursor: pointer; }}
        .media-item a {{ display: block; width: 100%; height: 100%; }}
        .media-type {{ position: absolute; top: 8px; right: 8px; background: rgba(0,0,0,0.8); padding: 3px 8px; border-radius: 4px; font-size: 11px; color: #fff; }}
        .current-badge {{ position: absolute; top: 8px; left: 8px; background: #22c55e; padding: 3px 8px; border-radius: 4px; font-size: 10px; color: #fff; font-weight: bold; }}
        .empty {{ color: #666; font-style: italic; padding: 20px; background: #1a1a1a; border-radius: 8px; }}
        .json-link {{ display: inline-block; margin-left: 15px; color: #666; text-decoration: none; font-size: 14px; }}
        .json-link:hover {{ color: #3b82f6; }}
    </style>
</head>
<body>
    <a href="/" class="back-link">← Back to User List</a>
    
    <div class="profile-header">"""
        
        avatar_url = ""
        if profile_photos:
            current = [p for p in profile_photos if p.is_current]
            if current:
                avatar_url = f"/media/{clean_media_path(current[0].file_path)}"
            else:
                avatar_url = f"/media/{clean_media_path(profile_photos[0].file_path)}"
        
        html += f"""
        {'<img src="' + avatar_url + '" class="profile-avatar" alt="Avatar">' if avatar_url else '<div class="profile-avatar"></div>'}
        <div class="profile-info">
            <h1>{display_name}</h1>
            <div class="username">@{user.username or 'no_username'}</div>
            <div class="telegram-id">Telegram ID: {user.telegram_id}</div>
            {'<div class="bio">' + (user.bio or '') + '</div>' if user.bio else ''}
            <div class="profile-stats">
                <div class="profile-stat"><span class="profile-stat-value">{len(profile_photos)}</span> Photos</div>
                <div class="profile-stat"><span class="profile-stat-value">{len(stories)}</span> Stories</div>
                <div class="profile-stat"><span class="profile-stat-value">{len(sent_media)}</span> Media</div>
            </div>
        </div>
    </div>
    
    <a href="/api/user/{user.telegram_id}" class="json-link">View as JSON</a>"""
        
        if profile_photos:
            html += f"""
    <div class="section">
        <h2>Profile Photos ({len(profile_photos)})</h2>
        <div class="media-grid">"""
            for photo in profile_photos:
                media_url = f"/media/{clean_media_path(photo.file_path)}"
                media_type = "video" if photo.is_video else "photo"
                html += f"""
            <div class="media-item">
                <a href="{media_url}" target="_blank">
                    <img src="{media_url}" loading="lazy" alt="Profile Photo">
                </a>
                {'<span class="current-badge">CURRENT</span>' if photo.is_current else ''}
                <span class="media-type">{media_type}</span>
            </div>"""
            html += """
        </div>
    </div>"""
        
        if stories:
            html += f"""
    <div class="section">
        <h2>Stories ({len(stories)})</h2>
        <div class="media-grid">"""
            for story in stories:
                media_url = f"/media/{clean_media_path(story.file_path)}"
                is_video = story.story_type == 'video'
                html += f"""
            <div class="media-item">
                <a href="{media_url}" target="_blank">
                    {'<video src="' + media_url + '" muted></video>' if is_video else '<img src="' + media_url + '" loading="lazy" alt="Story">'}
                </a>
                <span class="media-type">{story.story_type}</span>
            </div>"""
            html += """
        </div>
    </div>"""
        
        if sent_media:
            html += f"""
    <div class="section">
        <h2>Sent Media ({len(sent_media)})</h2>
        <div class="media-grid">"""
            for media in sent_media:
                media_url = f"/media/{clean_media_path(media['file_path'])}"
                is_video = media['file_type'] in ['video', 'gif', 'video_note']
                html += f"""
            <div class="media-item">
                <a href="{media_url}" target="_blank">
                    {'<video src="' + media_url + '" muted></video>' if is_video else '<img src="' + media_url + '" loading="lazy" alt="Media">'}
                </a>
                <span class="media-type">{media['file_type']}</span>
            </div>"""
            html += """
        </div>
    </div>"""
        
        if not profile_photos and not stories and not sent_media:
            html += '<p class="empty">No media available for this user</p>'
        
        html += """
</body>
</html>"""
        
        return HTMLResponse(content=html)


@app.get("/api/users")
async def get_users_api(page: int = Query(1, ge=1)):
    data = await get_filtered_users(page)
    return JSONResponse(content=data)


@app.get("/api/user/{telegram_id}")
async def get_user_api(telegram_id: int):
    async with async_session_maker() as db:
        result = await db.execute(
            select(TelegramUser)
            .where(TelegramUser.telegram_id == telegram_id)
            .options(
                selectinload(TelegramUser.profile_photos),
                selectinload(TelegramUser.stories)
            )
        )
        user = result.scalar_one_or_none()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        media_query = (
            select(
                MediaFile.id,
                MediaFile.file_path,
                MediaFile.file_type
            )
            .join(TelegramMessage, MediaFile.message_id == TelegramMessage.id)
            .where(TelegramMessage.sender_id == user.id)
            .where(MediaFile.file_type.in_(["photo", "video", "gif", "video_note"]))
            .where(MediaFile.file_path.isnot(None))
        )
        media_result = await db.execute(media_query)
        sent_media = [
            {"id": row[0], "file_path": row[1], "file_type": row[2]}
            for row in media_result.fetchall()
            if file_exists(row[1])
        ]
        
        profile_photos = [
            {
                "id": p.id,
                "file_path": p.file_path,
                "is_current": p.is_current,
                "is_video": p.is_video,
                "captured_at": p.captured_at.isoformat() if p.captured_at else None
            }
            for p in user.profile_photos
            if p.file_path and file_exists(p.file_path)
        ]
        
        stories = [
            {
                "id": s.id,
                "story_id": s.story_id,
                "story_type": s.story_type,
                "file_path": s.file_path,
                "caption": s.caption,
                "posted_at": s.posted_at.isoformat() if s.posted_at else None
            }
            for s in user.stories
            if s.file_path and file_exists(s.file_path)
        ]
        
        return {
            "id": user.id,
            "telegram_id": user.telegram_id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "phone": user.phone,
            "bio": user.bio,
            "profile_photos": profile_photos,
            "stories": stories,
            "sent_media": sent_media,
            "stats": {
                "profile_photos_count": len(profile_photos),
                "stories_count": len(stories),
                "sent_media_count": len(sent_media)
            }
        }


@app.get("/api/stats")
async def get_stats():
    async with async_session_maker() as db:
        has_profile_photo = (
            select(UserProfilePhoto.user_id)
            .where(UserProfilePhoto.user_id == TelegramUser.id)
            .where(UserProfilePhoto.file_path.isnot(None))
            .exists()
        )
        
        has_story = (
            select(UserStory.user_id)
            .where(UserStory.user_id == TelegramUser.id)
            .where(UserStory.file_path.isnot(None))
            .exists()
        )
        
        has_media_message = (
            select(TelegramMessage.sender_id)
            .join(MediaFile, MediaFile.message_id == TelegramMessage.id)
            .where(TelegramMessage.sender_id == TelegramUser.id)
            .where(MediaFile.file_type.in_(["photo", "video", "gif", "video_note"]))
            .where(MediaFile.file_path.isnot(None))
            .exists()
        )
        
        filter_condition = or_(has_profile_photo, has_story, has_media_message)
        
        users_with_media = await db.execute(
            select(func.count(TelegramUser.id)).where(filter_condition)
        )
        
        total_users = await db.execute(select(func.count(TelegramUser.id)))
        total_photos = await db.execute(
            select(func.count(UserProfilePhoto.id))
            .where(UserProfilePhoto.file_path.isnot(None))
        )
        total_stories = await db.execute(
            select(func.count(UserStory.id))
            .where(UserStory.file_path.isnot(None))
        )
        total_media = await db.execute(
            select(func.count(MediaFile.id))
            .where(MediaFile.file_path.isnot(None))
            .where(MediaFile.file_type.in_(["photo", "video", "gif", "video_note"]))
        )
        
        return {
            "users_with_media": users_with_media.scalar() or 0,
            "total_users": total_users.scalar() or 0,
            "total_profile_photos": total_photos.scalar() or 0,
            "total_stories": total_stories.scalar() or 0,
            "total_sent_media": total_media.scalar() or 0
        }


if __name__ == "__main__":
    uvicorn.run(
        "backend.crawler_server:app",
        host="0.0.0.0",
        port=8080,
        reload=False
    )
