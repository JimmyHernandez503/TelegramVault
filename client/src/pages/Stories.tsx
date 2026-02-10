import { useState, useEffect } from 'react';
import { Camera, User, Clock, Eye, Play, Download, ChevronLeft, ChevronRight, Star, Bookmark, RefreshCw, X } from 'lucide-react';
import { api } from '@/api/client';

interface StoryUser {
  id: number;
  telegram_id: number;
  username: string | null;
  first_name: string | null;
  last_name: string | null;
  photo_path: string | null;
  is_watchlist: boolean;
  is_favorite: boolean;
  story_count: number;
  last_seen: string | null;
}

interface Story {
  id: number;
  story_id: number;
  story_type: string;
  file_path: string | null;
  caption: string | null;
  width: number | null;
  height: number | null;
  duration: number | null;
  views_count: number;
  posted_at: string | null;
  expires_at: string | null;
  is_pinned: boolean;
  created_at: string | null;
}

interface StoryStats {
  users_with_stories: number;
  total_stories_downloaded: number;
  watchlist_with_stories: number;
}

export default function StoriesPage() {
  const [users, setUsers] = useState<StoryUser[]>([]);
  const [selectedUser, setSelectedUser] = useState<StoryUser | null>(null);
  const [stories, setStories] = useState<Story[]>([]);
  const [currentStoryIndex, setCurrentStoryIndex] = useState(0);
  const [stats, setStats] = useState<StoryStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingStories, setLoadingStories] = useState(false);
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [downloading, setDownloading] = useState(false);
  const [viewerOpen, setViewerOpen] = useState(false);

  useEffect(() => {
    loadStats();
    loadUsers();
  }, [page, watchlistOnly]);

  const loadStats = async () => {
    try {
      const res = await api.get<StoryStats>('/stories/stats');
      setStats(res);
    } catch (error) {
      console.error('Error loading stats:', error);
    }
  };

  const loadUsers = async () => {
    setLoading(true);
    try {
      const res = await api.get<{users: StoryUser[], total: number, pages: number}>(
        `/stories/users?page=${page}&limit=50&watchlist_only=${watchlistOnly}`
      );
      setUsers(res.users);
      setTotalPages(res.pages);
    } catch (error) {
      console.error('Error loading users:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadUserStories = async (user: StoryUser) => {
    setSelectedUser(user);
    setLoadingStories(true);
    try {
      const res = await api.get<{user: any, stories: Story[]}>(`/stories/user/${user.id}`);
      setStories(res.stories);
      setCurrentStoryIndex(0);
    } catch (error) {
      console.error('Error loading stories:', error);
    } finally {
      setLoadingStories(false);
    }
  };

  const downloadNow = async () => {
    setDownloading(true);
    try {
      await api.post('/stories/download-now', {});
      setTimeout(() => {
        loadStats();
        loadUsers();
      }, 2000);
    } catch (error) {
      console.error('Error downloading:', error);
    } finally {
      setDownloading(false);
    }
  };

  const openViewer = (index: number) => {
    setCurrentStoryIndex(index);
    setViewerOpen(true);
  };

  const nextStory = () => {
    if (currentStoryIndex < stories.length - 1) {
      setCurrentStoryIndex(currentStoryIndex + 1);
    }
  };

  const prevStory = () => {
    if (currentStoryIndex > 0) {
      setCurrentStoryIndex(currentStoryIndex - 1);
    }
  };

  const formatDate = (date: string | null) => {
    if (!date) return 'Unknown';
    return new Date(date).toLocaleString();
  };

  const getDisplayName = (user: StoryUser) => {
    if (user.first_name || user.last_name) {
      return `${user.first_name || ''} ${user.last_name || ''}`.trim();
    }
    return user.username || `User ${user.telegram_id}`;
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Camera className="w-7 h-7 text-pink-500" />
            Stories
          </h1>
          <p className="text-gray-400 mt-1">Ver y descargar historias de usuarios</p>
        </div>
        <button
          onClick={downloadNow}
          disabled={downloading}
          className="flex items-center gap-2 bg-pink-600 hover:bg-pink-700 px-4 py-2 rounded-lg disabled:opacity-50"
        >
          {downloading ? (
            <RefreshCw className="w-4 h-4 animate-spin" />
          ) : (
            <Download className="w-4 h-4" />
          )}
          Descargar Ahora
        </button>
      </div>

      {stats && (
        <div className="grid grid-cols-3 gap-4">
          <div className="bg-card border border-border rounded-lg p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-pink-500/20 rounded-lg">
                <User className="w-5 h-5 text-pink-500" />
              </div>
              <div>
                <p className="text-2xl font-bold">{stats.users_with_stories}</p>
                <p className="text-sm text-gray-400">Usuarios con Stories</p>
              </div>
            </div>
          </div>
          <div className="bg-card border border-border rounded-lg p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-blue-500/20 rounded-lg">
                <Camera className="w-5 h-5 text-blue-500" />
              </div>
              <div>
                <p className="text-2xl font-bold">{stats.total_stories_downloaded}</p>
                <p className="text-sm text-gray-400">Stories Descargadas</p>
              </div>
            </div>
          </div>
          <div className="bg-card border border-border rounded-lg p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-yellow-500/20 rounded-lg">
                <Star className="w-5 h-5 text-yellow-500" />
              </div>
              <div>
                <p className="text-2xl font-bold">{stats.watchlist_with_stories}</p>
                <p className="text-sm text-gray-400">Watchlist con Stories</p>
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="flex gap-6">
        <div className="w-80 flex-shrink-0">
          <div className="bg-card border border-border rounded-lg">
            <div className="p-4 border-b border-border">
              <h2 className="font-semibold">Usuarios con Stories</h2>
              <label className="flex items-center gap-2 mt-2 text-sm">
                <input
                  type="checkbox"
                  checked={watchlistOnly}
                  onChange={(e) => {
                    setWatchlistOnly(e.target.checked);
                    setPage(1);
                  }}
                  className="w-4 h-4 rounded bg-gray-800 border-gray-700"
                />
                Solo Watchlist
              </label>
            </div>
            
            <div className="max-h-[600px] overflow-y-auto">
              {loading ? (
                <div className="p-4 text-center text-gray-400">Cargando...</div>
              ) : users.length === 0 ? (
                <div className="p-4 text-center text-gray-400">
                  No hay usuarios con stories
                </div>
              ) : (
                users.map((user) => (
                  <div
                    key={user.id}
                    onClick={() => loadUserStories(user)}
                    className={`flex items-center gap-3 p-3 cursor-pointer hover:bg-gray-800/50 border-b border-gray-800/50 ${
                      selectedUser?.id === user.id ? 'bg-pink-500/10 border-l-2 border-l-pink-500' : ''
                    }`}
                  >
                    <div className="relative">
                      {user.photo_path ? (
                        <img
                          src={`/${user.photo_path}`}
                          alt=""
                          className="w-12 h-12 rounded-full object-cover ring-2 ring-pink-500"
                        />
                      ) : (
                        <div className="w-12 h-12 rounded-full bg-gray-700 flex items-center justify-center ring-2 ring-pink-500">
                          <User className="w-6 h-6 text-gray-400" />
                        </div>
                      )}
                      {user.story_count > 0 && (
                        <span className="absolute -bottom-1 -right-1 bg-pink-500 text-xs px-1.5 rounded-full">
                          {user.story_count}
                        </span>
                      )}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-1">
                        <p className="font-medium truncate">{getDisplayName(user)}</p>
                        {user.is_favorite && <Star className="w-3 h-3 text-yellow-500 flex-shrink-0" />}
                        {user.is_watchlist && <Bookmark className="w-3 h-3 text-blue-500 flex-shrink-0" />}
                      </div>
                      {user.username && (
                        <p className="text-sm text-gray-400 truncate">@{user.username}</p>
                      )}
                    </div>
                  </div>
                ))
              )}
            </div>

            {totalPages > 1 && (
              <div className="p-3 border-t border-border flex items-center justify-between">
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="p-1 hover:bg-gray-800 rounded disabled:opacity-50"
                >
                  <ChevronLeft className="w-5 h-5" />
                </button>
                <span className="text-sm text-gray-400">{page} / {totalPages}</span>
                <button
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={page === totalPages}
                  className="p-1 hover:bg-gray-800 rounded disabled:opacity-50"
                >
                  <ChevronRight className="w-5 h-5" />
                </button>
              </div>
            )}
          </div>
        </div>

        <div className="flex-1">
          {selectedUser ? (
            <div className="bg-card border border-border rounded-lg p-6">
              <div className="flex items-center gap-4 mb-6">
                {selectedUser.photo_path ? (
                  <img
                    src={`/${selectedUser.photo_path}`}
                    alt=""
                    className="w-16 h-16 rounded-full object-cover"
                  />
                ) : (
                  <div className="w-16 h-16 rounded-full bg-gray-700 flex items-center justify-center">
                    <User className="w-8 h-8 text-gray-400" />
                  </div>
                )}
                <div>
                  <h2 className="text-xl font-bold">{getDisplayName(selectedUser)}</h2>
                  {selectedUser.username && (
                    <p className="text-gray-400">@{selectedUser.username}</p>
                  )}
                  <p className="text-sm text-gray-500">{stories.length} stories descargadas</p>
                </div>
              </div>

              {loadingStories ? (
                <div className="text-center py-12 text-gray-400">Cargando stories...</div>
              ) : stories.length === 0 ? (
                <div className="text-center py-12 text-gray-400">
                  No hay stories descargadas para este usuario
                </div>
              ) : (
                <div className="grid grid-cols-4 gap-3">
                  {stories.map((story, index) => (
                    <div
                      key={story.id}
                      onClick={() => openViewer(index)}
                      className="relative aspect-[9/16] bg-gray-800 rounded-lg overflow-hidden cursor-pointer hover:ring-2 ring-pink-500 transition-all"
                    >
                      {story.file_path ? (
                        story.story_type === 'video' ? (
                          <div className="w-full h-full flex items-center justify-center bg-gray-900">
                            <Play className="w-10 h-10 text-white/80" />
                          </div>
                        ) : (
                          <img
                            src={`/${story.file_path}`}
                            alt=""
                            className="w-full h-full object-cover"
                          />
                        )
                      ) : (
                        <div className="w-full h-full flex items-center justify-center">
                          <Camera className="w-8 h-8 text-gray-600" />
                        </div>
                      )}
                      
                      <div className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black/80 to-transparent p-2">
                        <div className="flex items-center gap-1 text-xs text-white/80">
                          <Eye className="w-3 h-3" />
                          {story.views_count}
                        </div>
                        {story.posted_at && (
                          <p className="text-xs text-white/60 mt-0.5">
                            {new Date(story.posted_at).toLocaleDateString()}
                          </p>
                        )}
                      </div>
                      
                      {story.is_pinned && (
                        <div className="absolute top-2 right-2 bg-yellow-500 text-black text-xs px-1.5 py-0.5 rounded">
                          Pinned
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ) : (
            <div className="bg-card border border-border rounded-lg p-12 text-center">
              <Camera className="w-16 h-16 text-gray-600 mx-auto mb-4" />
              <h3 className="text-lg font-medium text-gray-400">Selecciona un usuario</h3>
              <p className="text-gray-500 mt-1">Elige un usuario de la lista para ver sus stories</p>
            </div>
          )}
        </div>
      </div>

      {viewerOpen && stories[currentStoryIndex] && (
        <div className="fixed inset-0 bg-black/95 z-50 flex items-center justify-center">
          <button
            onClick={() => setViewerOpen(false)}
            className="absolute top-4 right-4 p-2 hover:bg-gray-800 rounded-full"
          >
            <X className="w-6 h-6" />
          </button>

          <button
            onClick={prevStory}
            disabled={currentStoryIndex === 0}
            className="absolute left-4 p-3 hover:bg-gray-800 rounded-full disabled:opacity-30"
          >
            <ChevronLeft className="w-8 h-8" />
          </button>

          <div className="max-w-lg w-full mx-auto">
            {selectedUser && (
              <div className="flex items-center gap-3 mb-4 px-4">
                {selectedUser.photo_path ? (
                  <img src={`/${selectedUser.photo_path}`} alt="" className="w-10 h-10 rounded-full" />
                ) : (
                  <div className="w-10 h-10 rounded-full bg-gray-700 flex items-center justify-center">
                    <User className="w-5 h-5" />
                  </div>
                )}
                <div>
                  <p className="font-medium">{getDisplayName(selectedUser)}</p>
                  <p className="text-sm text-gray-400">
                    {formatDate(stories[currentStoryIndex].posted_at)}
                  </p>
                </div>
              </div>
            )}

            <div className="aspect-[9/16] bg-gray-900 rounded-lg overflow-hidden">
              {stories[currentStoryIndex].story_type === 'video' ? (
                <video
                  src={`/${stories[currentStoryIndex].file_path}`}
                  className="w-full h-full object-contain"
                  controls
                  autoPlay
                />
              ) : (
                <img
                  src={`/${stories[currentStoryIndex].file_path}`}
                  alt=""
                  className="w-full h-full object-contain"
                />
              )}
            </div>

            {stories[currentStoryIndex].caption && (
              <p className="mt-4 px-4 text-gray-300">{stories[currentStoryIndex].caption}</p>
            )}

            <div className="flex items-center justify-center gap-4 mt-4 text-sm text-gray-400">
              <span className="flex items-center gap-1">
                <Eye className="w-4 h-4" />
                {stories[currentStoryIndex].views_count} vistas
              </span>
              <span>{currentStoryIndex + 1} / {stories.length}</span>
            </div>
          </div>

          <button
            onClick={nextStory}
            disabled={currentStoryIndex === stories.length - 1}
            className="absolute right-4 p-3 hover:bg-gray-800 rounded-full disabled:opacity-30"
          >
            <ChevronRight className="w-8 h-8" />
          </button>
        </div>
      )}
    </div>
  );
}
