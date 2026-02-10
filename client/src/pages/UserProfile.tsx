import { useState, useEffect } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { api } from '@/api/client';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import {
  ArrowLeft,
  Loader2,
  User,
  Star,
  Eye,
  MessageSquare,
  Users,
  Image,
  Calendar,
  Clock,
  Shield,
  Bot,
  BadgeCheck,
  AlertTriangle,
  Phone,
  AtSign,
  Hash,
  FolderOpen,
  Radio,
  Crown,
  History,
  X,
  Download,
  ImageOff,
  RefreshCw,
  PlayCircle
} from 'lucide-react';

interface ProfilePhoto {
  id: number;
  file_path: string;
  is_current: boolean;
  is_video?: boolean;
  captured_at?: string | null;
  created_at: string | null;
}

interface Membership {
  group_id: number;
  group_title: string;
  group_username: string | null;
  is_channel: boolean;
  is_admin: boolean;
  admin_title: string | null;
  joined_at: string | null;
  is_active: boolean;
  leave_reason: string | null;
}

interface MediaFile {
  id: number;
  file_type: string;
  file_path: string | null;
  file_name: string | null;
  file_size: number | null;
  width: number | null;
  height: number | null;
  duration: number | null;
  group_id: number | null;
  group_title: string | null;
  created_at: string | null;
}

interface HistoryItem {
  field: string;
  old_value: string | null;
  new_value: string | null;
  changed_at: string | null;
}

interface StoryItem {
  id: number;
  story_id: number;
  story_type: string;
  file_path: string | null;
  caption: string | null;
  width: number | null;
  height: number | null;
  duration: number | null;
  views_count: number;
  is_pinned: boolean;
  posted_at: string | null;
  expires_at: string | null;
}

interface UserDetail {
  id: number;
  telegram_id: number;
  username: string | null;
  first_name: string | null;
  last_name: string | null;
  phone: string | null;
  bio: string | null;
  is_premium: boolean;
  is_verified: boolean;
  is_bot: boolean;
  is_scam: boolean;
  is_fake: boolean;
  is_restricted: boolean;
  is_deleted: boolean;
  is_watchlist: boolean;
  is_favorite: boolean;
  last_seen: string | null;
  current_photo_path: string | null;
  has_stories: boolean;
  messages_count: number;
  groups_count: number;
  media_count: number;
  attachments_count: number;
  created_at: string | null;
  updated_at: string | null;
  memberships: Membership[];
  media_files: MediaFile[];
  profile_photos: ProfilePhoto[];
  stories: StoryItem[];
  history: HistoryItem[];
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return 'N/A';
  return new Date(dateStr).toLocaleString('es-ES');
}

function formatBytes(bytes: number | null): string {
  if (!bytes) return '';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export default function UserProfilePage() {
  const { userId } = useParams<{ userId: string }>();
  const navigate = useNavigate();
  const [user, setUser] = useState<UserDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<'info' | 'groups' | 'media' | 'stories' | 'history' | 'messages'>('info');
  const [previewPhoto, setPreviewPhoto] = useState<string | null>(null);
  const [downloadingStories, setDownloadingStories] = useState(false);
  const [messages, setMessages] = useState<any[]>([]);
  const [messageGroups, setMessageGroups] = useState<any[]>([]);
  const [selectedGroupId, setSelectedGroupId] = useState<number | null>(null);
  const [loadingMessages, setLoadingMessages] = useState(false);

  useEffect(() => {
    if (userId) {
      loadUser();
    }
  }, [userId]);

  const loadUser = async () => {
    setLoading(true);
    try {
      const data = await api.get<UserDetail>(`/users/${userId}/detail`);
      setUser(data);
    } catch (error) {
      console.error('Failed to load user:', error);
    } finally {
      setLoading(false);
    }
  };

  const toggleWatchlist = async () => {
    if (!user) return;
    try {
      await api.post(`/users/${user.id}/watchlist`, {});
      setUser({ ...user, is_watchlist: !user.is_watchlist });
    } catch (error) {
      console.error('Failed to toggle watchlist:', error);
    }
  };

  const toggleFavorite = async () => {
    if (!user) return;
    try {
      await api.post(`/users/${user.id}/favorite`, {});
      setUser({ ...user, is_favorite: !user.is_favorite });
    } catch (error) {
      console.error('Failed to toggle favorite:', error);
    }
  };

  const downloadStories = async () => {
    if (!user || downloadingStories) return;
    setDownloadingStories(true);
    try {
      await api.post(`/users/${user.id}/download-stories`, {});
      await loadUser();
    } catch (error) {
      console.error('Failed to download stories:', error);
    } finally {
      setDownloadingStories(false);
    }
  };

  const [syncingPhotos, setSyncingPhotos] = useState(false);
  
  const syncProfilePhotos = async () => {
    if (!user || syncingPhotos) return;
    setSyncingPhotos(true);
    try {
      const result = await api.post<{success: boolean, photos_downloaded: number, total_photos: number}>(`/users/${user.id}/sync-photos`, {});
      if (result.success) {
        await loadUser();
      }
    } catch (error) {
      console.error('Failed to sync photos:', error);
    } finally {
      setSyncingPhotos(false);
    }
  };

  const loadMessages = async (groupId: number | null = null) => {
    if (!user) return;
    setLoadingMessages(true);
    setMessages([]);
    try {
      const params = new URLSearchParams();
      if (groupId) params.append('group_id', groupId.toString());
      params.append('limit', '200');
      const data = await api.get<{messages: any[], groups: any[], total_messages: number, filtered_count: number}>(`/users/${user.id}/messages?${params}`);
      setMessages(data.messages);
      setMessageGroups(data.groups);
    } catch (error) {
      console.error('Failed to load messages:', error);
    } finally {
      setLoadingMessages(false);
    }
  };

  useEffect(() => {
    if (activeTab === 'messages' && user && messages.length === 0) {
      loadMessages();
    }
  }, [activeTab, user]);

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  if (!user) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-4">
        <User className="h-16 w-16 text-muted-foreground" />
        <p className="text-muted-foreground">Usuario no encontrado</p>
        <Button onClick={() => navigate('/users')}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Volver a Usuarios
        </Button>
      </div>
    );
  }

  const displayName = user.first_name || user.username || 'Usuario';
  const fullName = [user.first_name, user.last_name].filter(Boolean).join(' ') || 'Sin nombre';

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="icon" onClick={() => navigate('/users')}>
          <ArrowLeft className="h-5 w-5" />
        </Button>
        <div>
          <h1 className="text-3xl font-bold">Perfil de Usuario</h1>
          <p className="text-muted-foreground">Informacion detallada del usuario</p>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-3">
        <div className="lg:col-span-1 space-y-6">
          <Card>
            <CardContent className="pt-6">
              <div className="text-center">
                <div 
                  className={`w-32 h-32 rounded-full mx-auto mb-4 overflow-hidden cursor-pointer ${
                    user.has_stories ? 'ring-4 ring-pink-500 ring-offset-4 ring-offset-background' : ''
                  }`}
                  onClick={() => user.current_photo_path && setPreviewPhoto(`/${user.current_photo_path}`)}
                >
                  {user.current_photo_path ? (
                    <img
                      src={`/${user.current_photo_path}`}
                      alt={displayName}
                      className="w-full h-full object-cover"
                      onError={(e) => {
                        (e.target as HTMLImageElement).style.display = 'none';
                      }}
                    />
                  ) : (
                    <div className="w-full h-full bg-secondary flex items-center justify-center">
                      <User className="w-16 h-16 text-muted-foreground" />
                    </div>
                  )}
                </div>
                
                <h2 className="text-2xl font-bold">{fullName}</h2>
                {user.username && (
                  <p className="text-primary">@{user.username}</p>
                )}
                
                <div className="flex flex-wrap justify-center gap-2 mt-3">
                  {user.is_premium && (
                    <span className="text-xs bg-purple-500/20 text-purple-400 px-2 py-1 rounded flex items-center gap-1">
                      <Crown className="w-3 h-3" /> Premium
                    </span>
                  )}
                  {user.is_verified && (
                    <span className="text-xs bg-blue-500/20 text-blue-400 px-2 py-1 rounded flex items-center gap-1">
                      <BadgeCheck className="w-3 h-3" /> Verificado
                    </span>
                  )}
                  {user.is_bot && (
                    <span className="text-xs bg-orange-500/20 text-orange-400 px-2 py-1 rounded flex items-center gap-1">
                      <Bot className="w-3 h-3" /> Bot
                    </span>
                  )}
                  {user.is_scam && (
                    <span className="text-xs bg-red-500/20 text-red-400 px-2 py-1 rounded flex items-center gap-1">
                      <AlertTriangle className="w-3 h-3" /> Scam
                    </span>
                  )}
                  {user.has_stories && (
                    <span className="text-xs bg-pink-500/20 text-pink-400 px-2 py-1 rounded">Stories</span>
                  )}
                </div>
                
                <div className="flex justify-center gap-2 mt-4">
                  <Button
                    variant={user.is_watchlist ? 'default' : 'outline'}
                    size="sm"
                    onClick={toggleWatchlist}
                  >
                    <Eye className="mr-2 h-4 w-4" />
                    {user.is_watchlist ? 'En Watchlist' : 'Watchlist'}
                  </Button>
                  <Button
                    variant={user.is_favorite ? 'default' : 'outline'}
                    size="sm"
                    onClick={toggleFavorite}
                  >
                    <Star className={`mr-2 h-4 w-4 ${user.is_favorite ? 'fill-current' : ''}`} />
                    {user.is_favorite ? 'Favorito' : 'Favorito'}
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Estadisticas</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="flex items-center gap-2 text-sm text-muted-foreground">
                  <MessageSquare className="w-4 h-4" /> Mensajes
                </span>
                <span className="font-bold">{user.messages_count.toLocaleString()}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Users className="w-4 h-4" /> Grupos
                </span>
                <span className="font-bold">{user.groups_count}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Image className="w-4 h-4" /> Media
                </span>
                <span className="font-bold">{user.media_count}</span>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm flex items-center gap-2">
                <History className="w-4 h-4" />
                Fotos de Perfil ({user.profile_photos.length})
              </CardTitle>
              <Button
                variant="ghost"
                size="sm"
                onClick={syncProfilePhotos}
                disabled={syncingPhotos}
                title="Sincronizar todas las fotos de perfil historicas"
              >
                {syncingPhotos ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <RefreshCw className="h-4 w-4" />
                )}
              </Button>
            </CardHeader>
            <CardContent>
              {user.profile_photos.length > 0 ? (
                <div className="grid grid-cols-3 gap-2">
                  {user.profile_photos.map((photo, idx) => (
                    <div
                      key={photo.id}
                      className={`relative aspect-square rounded-lg overflow-hidden cursor-pointer border-2 transition-all hover:scale-105 ${
                        photo.is_current ? 'border-primary ring-2 ring-primary/50' : 'border-transparent hover:border-muted-foreground'
                      }`}
                      onClick={() => setPreviewPhoto(`/${photo.file_path}`)}
                    >
                      {photo.is_video ? (
                        <video
                          src={`/${photo.file_path}`}
                          className="w-full h-full object-cover"
                          muted
                          loop
                          autoPlay
                          playsInline
                        />
                      ) : (
                        <img
                          src={`/${photo.file_path}`}
                          alt={`Profile ${idx + 1}`}
                          className="w-full h-full object-cover"
                        />
                      )}
                      {photo.is_current && (
                        <div className="absolute top-1 right-1 bg-primary rounded-full p-0.5">
                          <BadgeCheck className="w-3 h-3 text-white" />
                        </div>
                      )}
                      {photo.is_video && (
                        <div className="absolute top-1 left-1 bg-black/60 rounded-full p-0.5">
                          <PlayCircle className="w-3 h-3 text-white" />
                        </div>
                      )}
                      {(photo.captured_at || photo.created_at) && (
                        <div className="absolute bottom-0 left-0 right-0 bg-black/60 p-1">
                          <p className="text-[10px] text-white text-center truncate">
                            {new Date(photo.captured_at || photo.created_at).toLocaleDateString('es-ES', { month: 'short', day: 'numeric' })}
                          </p>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center py-4">
                  <p className="text-sm text-muted-foreground mb-2">No hay fotos descargadas</p>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={syncProfilePhotos}
                    disabled={syncingPhotos}
                  >
                    {syncingPhotos ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                      <RefreshCw className="mr-2 h-4 w-4" />
                    )}
                    Descargar Historial de Fotos
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        <div className="lg:col-span-2 space-y-6">
          <div className="flex gap-2 flex-wrap">
            {(['info', 'groups', 'media', 'messages', 'stories', 'history'] as const).map((tab) => (
              <Button
                key={tab}
                variant={activeTab === tab ? 'default' : 'outline'}
                size="sm"
                onClick={() => setActiveTab(tab)}
              >
                {tab === 'info' && <User className="mr-2 h-4 w-4" />}
                {tab === 'groups' && <FolderOpen className="mr-2 h-4 w-4" />}
                {tab === 'media' && <Image className="mr-2 h-4 w-4" />}
                {tab === 'messages' && <MessageSquare className="mr-2 h-4 w-4" />}
                {tab === 'stories' && <Radio className="mr-2 h-4 w-4" />}
                {tab === 'history' && <History className="mr-2 h-4 w-4" />}
                {tab === 'info' && 'Informacion'}
                {tab === 'groups' && `Grupos (${user.memberships.length})`}
                {tab === 'media' && `Media (${user.media_files.length})`}
                {tab === 'messages' && `Mensajes (${user.messages_count})`}
                {tab === 'stories' && `Stories (${user.stories?.length || 0})`}
                {tab === 'history' && `Historial (${user.history.length})`}
              </Button>
            ))}
          </div>

          {activeTab === 'info' && (
            <Card>
              <CardHeader>
                <CardTitle>Informacion del Perfil</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="p-4 bg-secondary/50 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">Telegram ID</p>
                    <p className="font-mono font-bold flex items-center gap-2">
                      <Hash className="w-4 h-4" /> {user.telegram_id}
                    </p>
                  </div>
                  <div className="p-4 bg-secondary/50 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">Username</p>
                    <p className="font-bold flex items-center gap-2">
                      <AtSign className="w-4 h-4" /> {user.username || 'Sin username'}
                    </p>
                  </div>
                  <div className="p-4 bg-secondary/50 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">Nombre</p>
                    <p className="font-bold">{user.first_name || 'N/A'}</p>
                  </div>
                  <div className="p-4 bg-secondary/50 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">Apellido</p>
                    <p className="font-bold">{user.last_name || 'N/A'}</p>
                  </div>
                  {user.phone && (
                    <div className="p-4 bg-secondary/50 rounded-lg">
                      <p className="text-xs text-muted-foreground mb-1">Telefono</p>
                      <p className="font-bold flex items-center gap-2">
                        <Phone className="w-4 h-4" /> {user.phone}
                      </p>
                    </div>
                  )}
                  <div className="p-4 bg-secondary/50 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">Ultima vez visto</p>
                    <p className="font-bold flex items-center gap-2">
                      <Clock className="w-4 h-4" /> {formatDate(user.last_seen)}
                    </p>
                  </div>
                  <div className="p-4 bg-secondary/50 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">Primera captura</p>
                    <p className="font-bold flex items-center gap-2">
                      <Calendar className="w-4 h-4" /> {formatDate(user.created_at)}
                    </p>
                  </div>
                  <div className="p-4 bg-secondary/50 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">Ultima actualizacion</p>
                    <p className="font-bold flex items-center gap-2">
                      <Calendar className="w-4 h-4" /> {formatDate(user.updated_at)}
                    </p>
                  </div>
                </div>

                {user.bio && (
                  <div className="p-4 bg-secondary/50 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">Bio</p>
                    <p className="whitespace-pre-wrap">{user.bio}</p>
                  </div>
                )}

                <div className="p-4 bg-secondary/50 rounded-lg">
                  <p className="text-xs text-muted-foreground mb-2">Flags</p>
                  <div className="flex flex-wrap gap-2">
                    {user.is_premium && <span className="text-xs bg-purple-500/30 px-2 py-1 rounded">Premium</span>}
                    {user.is_verified && <span className="text-xs bg-blue-500/30 px-2 py-1 rounded">Verificado</span>}
                    {user.is_bot && <span className="text-xs bg-orange-500/30 px-2 py-1 rounded">Bot</span>}
                    {user.is_scam && <span className="text-xs bg-red-500/30 px-2 py-1 rounded">Scam</span>}
                    {user.is_fake && <span className="text-xs bg-red-500/30 px-2 py-1 rounded">Fake</span>}
                    {user.is_restricted && <span className="text-xs bg-yellow-500/30 px-2 py-1 rounded">Restringido</span>}
                    {user.is_deleted && <span className="text-xs bg-gray-500/30 px-2 py-1 rounded">Eliminado</span>}
                    {!user.is_premium && !user.is_verified && !user.is_bot && !user.is_scam && !user.is_fake && !user.is_restricted && !user.is_deleted && (
                      <span className="text-xs text-muted-foreground">Sin flags especiales</span>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {activeTab === 'groups' && (
            <Card>
              <CardHeader>
                <CardTitle>Grupos ({user.memberships.length})</CardTitle>
              </CardHeader>
              <CardContent>
                {user.memberships.length === 0 ? (
                  <p className="text-center text-muted-foreground py-8">No hay grupos registrados</p>
                ) : (
                  <div className="space-y-3">
                    {user.memberships.map((m) => (
                      <Link
                        key={m.group_id}
                        to={`/groups/${m.group_id}`}
                        className="flex items-center justify-between p-4 bg-secondary/50 rounded-lg hover:bg-secondary transition-colors"
                      >
                        <div className="flex items-center gap-3">
                          <div className="w-10 h-10 rounded-lg bg-primary/20 flex items-center justify-center">
                            {m.is_channel ? (
                              <Radio className="w-5 h-5 text-primary" />
                            ) : (
                              <Users className="w-5 h-5 text-primary" />
                            )}
                          </div>
                          <div>
                            <p className="font-medium">{m.group_title}</p>
                            <div className="flex items-center gap-2 text-xs text-muted-foreground">
                              {m.group_username && <span>@{m.group_username}</span>}
                              {m.is_channel && <span className="bg-blue-500/20 text-blue-400 px-1 rounded">Canal</span>}
                              {m.is_admin && (
                                <span className="bg-yellow-500/20 text-yellow-400 px-1 rounded flex items-center gap-1">
                                  <Shield className="w-3 h-3" /> {m.admin_title || 'Admin'}
                                </span>
                              )}
                            </div>
                          </div>
                        </div>
                        <div className="text-right text-xs text-muted-foreground">
                          {m.joined_at && <p>Desde: {new Date(m.joined_at).toLocaleDateString()}</p>}
                          {!m.is_active && (
                            <span className="text-red-400">{m.leave_reason || 'Inactivo'}</span>
                          )}
                        </div>
                      </Link>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {activeTab === 'media' && (
            <Card>
              <CardHeader>
                <CardTitle>Archivos Multimedia ({user.media_files.length})</CardTitle>
              </CardHeader>
              <CardContent>
                {user.media_files.length === 0 ? (
                  <p className="text-center text-muted-foreground py-8">No hay archivos multimedia</p>
                ) : (
                  <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
                    {user.media_files.map((m) => (
                      <div
                        key={m.id}
                        className="aspect-square bg-secondary/50 rounded-lg overflow-hidden cursor-pointer hover:ring-2 hover:ring-primary transition-all"
                        onClick={() => m.file_path && (m.file_type === 'photo' || m.file_type === 'sticker') && setPreviewPhoto(`/${m.file_path}`)}
                      >
                        {m.file_path ? (
                          (m.file_type === 'photo' || m.file_type === 'sticker') ? (
                            <img
                              src={`/${m.file_path}`}
                              alt={m.file_name || 'Media'}
                              className="w-full h-full object-cover"
                              onError={(e) => {
                                (e.target as HTMLImageElement).style.display = 'none';
                                (e.target as HTMLImageElement).parentElement!.innerHTML = '<div class="w-full h-full flex flex-col items-center justify-center p-2"><svg class="w-8 h-8 text-muted-foreground mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"></path></svg><p class="text-xs text-muted-foreground">Error cargando</p></div>';
                              }}
                            />
                          ) : m.file_type === 'video' ? (
                            <video
                              src={`/${m.file_path}`}
                              className="w-full h-full object-cover"
                            />
                          ) : (
                            <div className="w-full h-full flex flex-col items-center justify-center p-2">
                              <Image className="w-8 h-8 text-muted-foreground mb-2" />
                              <p className="text-xs text-muted-foreground text-center capitalize">{m.file_type}</p>
                              {m.file_size && <p className="text-xs text-muted-foreground">{formatBytes(m.file_size)}</p>}
                            </div>
                          )
                        ) : (
                          <div className="w-full h-full flex flex-col items-center justify-center p-2 bg-secondary/80">
                            <ImageOff className="w-8 h-8 text-muted-foreground mb-2" />
                            <p className="text-xs text-muted-foreground text-center capitalize">{m.file_type}</p>
                            <p className="text-xs text-yellow-500 mt-1">No descargado</p>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {activeTab === 'stories' && (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between">
                <CardTitle>Stories ({user.stories?.length || 0})</CardTitle>
                {user.has_stories && (
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={downloadStories}
                    disabled={downloadingStories}
                  >
                    {downloadingStories ? (
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    ) : (
                      <Download className="w-4 h-4 mr-2" />
                    )}
                    Descargar Stories
                  </Button>
                )}
              </CardHeader>
              <CardContent>
                {!user.stories || user.stories.length === 0 ? (
                  <div className="text-center py-8">
                    <Radio className="w-12 h-12 text-muted-foreground mx-auto mb-2" />
                    <p className="text-muted-foreground mb-4">
                      {user.has_stories ? 'Stories disponibles pero no descargadas aun' : 'Este usuario no tiene stories'}
                    </p>
                    {user.has_stories && (
                      <Button
                        onClick={downloadStories}
                        disabled={downloadingStories}
                      >
                        {downloadingStories ? (
                          <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                        ) : (
                          <Download className="w-4 h-4 mr-2" />
                        )}
                        Descargar Stories
                      </Button>
                    )}
                  </div>
                ) : (
                  <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
                    {user.stories.map((s) => (
                      <div
                        key={s.id}
                        className="relative aspect-[9/16] bg-secondary/50 rounded-lg overflow-hidden cursor-pointer hover:ring-2 hover:ring-primary transition-all"
                        onClick={() => s.file_path && s.story_type === 'photo' && setPreviewPhoto(`/${s.file_path}`)}
                      >
                        {s.file_path ? (
                          s.story_type === 'video' ? (
                            <video
                              src={`/${s.file_path}`}
                              className="w-full h-full object-cover"
                              controls
                            />
                          ) : (
                            <img
                              src={`/${s.file_path}`}
                              alt="Story"
                              className="w-full h-full object-cover"
                            />
                          )
                        ) : (
                          <div className="w-full h-full flex items-center justify-center">
                            <Radio className="w-8 h-8 text-muted-foreground" />
                          </div>
                        )}
                        <div className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black/80 to-transparent p-2">
                          <div className="flex items-center gap-2 text-xs text-white">
                            <Eye className="w-3 h-3" />
                            <span>{s.views_count}</span>
                          </div>
                          {s.posted_at && (
                            <p className="text-xs text-white/70 mt-1">
                              {new Date(s.posted_at).toLocaleDateString('es-ES')}
                            </p>
                          )}
                        </div>
                        {s.is_pinned && (
                          <div className="absolute top-2 right-2 bg-primary/80 rounded-full p-1">
                            <Star className="w-3 h-3 text-white" />
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {activeTab === 'messages' && (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between">
                <CardTitle>Mensajes del Usuario</CardTitle>
                {messageGroups.length > 0 && (
                  <select
                    className="bg-secondary text-sm rounded px-3 py-1 border-none"
                    value={selectedGroupId || ''}
                    onChange={(e) => {
                      const val = e.target.value ? parseInt(e.target.value) : null;
                      setSelectedGroupId(val);
                      loadMessages(val);
                    }}
                  >
                    <option value="">Todos los grupos</option>
                    {messageGroups.map((g) => (
                      <option key={g.id} value={g.id}>
                        {g.title} ({g.message_count})
                      </option>
                    ))}
                  </select>
                )}
              </CardHeader>
              <CardContent>
                {loadingMessages ? (
                  <div className="flex justify-center py-8">
                    <Loader2 className="w-8 h-8 animate-spin text-primary" />
                  </div>
                ) : messages.length === 0 ? (
                  <p className="text-center text-muted-foreground py-8">No hay mensajes registrados</p>
                ) : (
                  <div className="space-y-3 max-h-[600px] overflow-y-auto">
                    {messages.map((m) => (
                      <div key={m.id} className="p-4 bg-secondary/50 rounded-lg">
                        <div className="flex items-center justify-between mb-2">
                          <Link 
                            to={`/groups/${m.group_id}`}
                            className="text-xs text-primary hover:underline"
                          >
                            {m.group_title || 'Grupo desconocido'}
                          </Link>
                          <span className="text-xs text-muted-foreground">
                            {m.date && new Date(m.date).toLocaleString('es-ES')}
                          </span>
                        </div>
                        <p className="text-sm whitespace-pre-wrap break-words">
                          {m.text || <span className="text-muted-foreground italic">[{m.message_type}]</span>}
                        </p>
                        {(m.views || m.forwards) && (
                          <div className="flex gap-4 mt-2 text-xs text-muted-foreground">
                            {m.views && <span><Eye className="w-3 h-3 inline mr-1" />{m.views.toLocaleString()}</span>}
                            {m.forwards && <span>Forwards: {m.forwards}</span>}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {activeTab === 'history' && (
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle>Historial de Identidad</CardTitle>
                </CardHeader>
                <CardContent>
                  {user.history.length === 0 ? (
                    <p className="text-center text-muted-foreground py-8">No hay historial de cambios</p>
                  ) : (
                    <div className="grid gap-6 md:grid-cols-2">
                      <div>
                        <h4 className="text-sm font-semibold mb-3 flex items-center gap-2">
                          <AtSign className="w-4 h-4" /> Usernames
                        </h4>
                        <div className="space-y-2">
                          {user.history
                            .filter(h => h.field === 'username')
                            .map((h, i) => (
                              <div key={i} className="p-3 bg-secondary/50 rounded-lg">
                                <p className="font-medium text-primary">@{h.new_value || 'sin username'}</p>
                                <p className="text-xs text-muted-foreground">
                                  Desde: {h.changed_at ? new Date(h.changed_at).toLocaleDateString('es-ES', { 
                                    year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit'
                                  }) : 'N/A'}
                                </p>
                                {h.old_value && (
                                  <p className="text-xs text-muted-foreground mt-1">Anterior: @{h.old_value}</p>
                                )}
                              </div>
                            ))}
                          {user.history.filter(h => h.field === 'username').length === 0 && (
                            <p className="text-sm text-muted-foreground">Sin cambios de username registrados</p>
                          )}
                        </div>
                      </div>
                      
                      <div>
                        <h4 className="text-sm font-semibold mb-3 flex items-center gap-2">
                          <User className="w-4 h-4" /> Nombres
                        </h4>
                        <div className="space-y-2">
                          {user.history
                            .filter(h => h.field === 'first_name' || h.field === 'last_name')
                            .map((h, i) => (
                              <div key={i} className="p-3 bg-secondary/50 rounded-lg">
                                <p className="font-medium">{h.new_value || 'vacio'}</p>
                                <p className="text-xs text-muted-foreground">
                                  {h.field === 'first_name' ? 'Nombre' : 'Apellido'} - Desde: {h.changed_at ? new Date(h.changed_at).toLocaleDateString('es-ES', { 
                                    year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit'
                                  }) : 'N/A'}
                                </p>
                                {h.old_value && (
                                  <p className="text-xs text-muted-foreground mt-1">Anterior: {h.old_value}</p>
                                )}
                              </div>
                            ))}
                          {user.history.filter(h => h.field === 'first_name' || h.field === 'last_name').length === 0 && (
                            <p className="text-sm text-muted-foreground">Sin cambios de nombre registrados</p>
                          )}
                        </div>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
              
              <Card>
                <CardHeader>
                  <CardTitle>Todos los Cambios ({user.history.length})</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3 max-h-[400px] overflow-y-auto">
                    {user.history.map((h, i) => (
                      <div key={i} className="p-4 bg-secondary/50 rounded-lg">
                        <div className="flex items-center justify-between mb-2">
                          <span className="text-sm font-medium capitalize">{h.field.replace('_', ' ')}</span>
                          <span className="text-xs text-muted-foreground">{formatDate(h.changed_at)}</span>
                        </div>
                        <div className="grid grid-cols-2 gap-4 text-sm">
                          <div>
                            <p className="text-xs text-muted-foreground">Anterior</p>
                            <p className="text-red-400 line-through">{h.old_value || 'N/A'}</p>
                          </div>
                          <div>
                            <p className="text-xs text-muted-foreground">Nuevo</p>
                            <p className="text-green-400">{h.new_value || 'N/A'}</p>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </div>
          )}
        </div>
      </div>

      {previewPhoto && (
        <div
          className="fixed inset-0 bg-black/80 z-50 flex items-center justify-center p-8"
          onClick={() => setPreviewPhoto(null)}
        >
          <Button
            variant="ghost"
            size="icon"
            className="absolute top-4 right-4 text-white"
            onClick={() => setPreviewPhoto(null)}
          >
            <X className="h-6 w-6" />
          </Button>
          <img
            src={previewPhoto}
            alt="Preview"
            className="max-w-full max-h-full object-contain"
            onClick={(e) => e.stopPropagation()}
          />
        </div>
      )}
    </div>
  );
}
