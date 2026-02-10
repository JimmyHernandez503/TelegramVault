import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '@/api/client';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Eye,
  EyeOff,
  Star,
  StarOff,
  Search,
  Users,
  MessageSquare,
  Crown,
  Bot,
  AlertTriangle,
  Loader2,
  Filter,
  X,
  ExternalLink
} from 'lucide-react';

interface WatchlistUser {
  id: number;
  telegram_id: number;
  username: string | null;
  first_name: string | null;
  last_name: string | null;
  bio: string | null;
  is_premium: boolean;
  is_bot: boolean;
  is_scam: boolean;
  is_verified: boolean;
  messages_count: number;
  groups_count: number;
  is_watchlist: boolean;
  is_favorite: boolean;
  last_seen: string | null;
  current_photo_path: string | null;
}

export default function WatchlistPage() {
  const navigate = useNavigate();
  const [users, setUsers] = useState<WatchlistUser[]>([]);
  const [allUsers, setAllUsers] = useState<WatchlistUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [viewMode, setViewMode] = useState<'watchlist' | 'all'>('watchlist');
  const [selectedUser, setSelectedUser] = useState<WatchlistUser | null>(null);

  useEffect(() => {
    loadUsers();
  }, [viewMode]);

  const loadUsers = async () => {
    setLoading(true);
    try {
      if (viewMode === 'watchlist') {
        const data = await api.getUsers({ watchlist_only: true });
        setUsers(data || []);
      } else {
        const data = await api.getUsers({ search: search || undefined });
        setAllUsers(data || []);
      }
    } catch (error) {
      console.error('Failed to load users:', error);
    } finally {
      setLoading(false);
    }
  };

  const searchUsers = async () => {
    if (!search.trim()) return;
    setLoading(true);
    try {
      const data = await api.getUsers({ search });
      setAllUsers(data || []);
    } catch (error) {
      console.error('Search failed:', error);
    } finally {
      setLoading(false);
    }
  };

  const toggleWatchlist = async (userId: number) => {
    try {
      await api.post(`/users/${userId}/watchlist`);
      loadUsers();
      if (selectedUser?.id === userId) {
        setSelectedUser({ ...selectedUser, is_watchlist: !selectedUser.is_watchlist });
      }
    } catch (error) {
      console.error('Failed to toggle watchlist:', error);
    }
  };

  const toggleFavorite = async (userId: number) => {
    try {
      await api.post(`/users/${userId}/favorite`);
      loadUsers();
      if (selectedUser?.id === userId) {
        setSelectedUser({ ...selectedUser, is_favorite: !selectedUser.is_favorite });
      }
    } catch (error) {
      console.error('Failed to toggle favorite:', error);
    }
  };

  const displayUsers = viewMode === 'watchlist' ? users : allUsers;

  const getDisplayName = (user: WatchlistUser) => {
    if (user.first_name || user.last_name) {
      return `${user.first_name || ''} ${user.last_name || ''}`.trim();
    }
    return user.username || `User ${user.telegram_id}`;
  };

  if (loading && displayUsers.length === 0) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold flex items-center gap-3">
            <Eye className="h-8 w-8 text-primary" />
            Watchlist
          </h1>
          <p className="text-muted-foreground mt-1">Usuarios de interes bajo vigilancia especial</p>
        </div>
        <div className="flex gap-2">
          <Button
            variant={viewMode === 'watchlist' ? 'default' : 'outline'}
            onClick={() => setViewMode('watchlist')}
          >
            <Eye className="w-4 h-4 mr-2" />
            En Watchlist
          </Button>
          <Button
            variant={viewMode === 'all' ? 'default' : 'outline'}
            onClick={() => setViewMode('all')}
          >
            <Users className="w-4 h-4 mr-2" />
            Buscar Usuarios
          </Button>
        </div>
      </div>

      {viewMode === 'all' && (
        <div className="flex gap-2">
          <Input
            placeholder="Buscar por nombre o username..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && searchUsers()}
            className="max-w-md"
          />
          <Button onClick={searchUsers}>
            <Search className="w-4 h-4 mr-2" />
            Buscar
          </Button>
        </div>
      )}

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        <Card className="bg-gradient-to-br from-primary/20 to-primary/5">
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">En Watchlist</p>
                <p className="text-3xl font-bold">{users.length}</p>
              </div>
              <Eye className="h-10 w-10 text-primary opacity-80" />
            </div>
          </CardContent>
        </Card>
        
        <Card className="bg-gradient-to-br from-yellow-500/20 to-yellow-500/5">
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Favoritos</p>
                <p className="text-3xl font-bold">{users.filter(u => u.is_favorite).length}</p>
              </div>
              <Star className="h-10 w-10 text-yellow-400 opacity-80" />
            </div>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-red-500/20 to-red-500/5">
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Marcados Scam</p>
                <p className="text-3xl font-bold">{users.filter(u => u.is_scam).length}</p>
              </div>
              <AlertTriangle className="h-10 w-10 text-red-400 opacity-80" />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <Card>
            <CardHeader>
              <CardTitle>
                {viewMode === 'watchlist' ? 'Usuarios en Watchlist' : 'Resultados de Busqueda'}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {displayUsers.length === 0 ? (
                <div className="text-center py-12 text-muted-foreground">
                  <Users className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  {viewMode === 'watchlist' ? (
                    <>
                      <p>No hay usuarios en la watchlist</p>
                      <p className="text-sm mt-1">Busca usuarios y agregalos a la watchlist</p>
                    </>
                  ) : (
                    <p>Busca usuarios por nombre o username</p>
                  )}
                </div>
              ) : (
                <div className="space-y-3">
                  {displayUsers.map((user) => (
                    <div
                      key={user.id}
                      className={`flex items-center gap-4 p-4 rounded-lg cursor-pointer transition-all ${
                        selectedUser?.id === user.id
                          ? 'bg-primary/20 ring-2 ring-primary'
                          : 'bg-secondary/30 hover:bg-secondary/50'
                      }`}
                      onClick={() => setSelectedUser(user)}
                    >
                      <div className="w-12 h-12 rounded-full bg-secondary flex items-center justify-center overflow-hidden">
                        {user.current_photo_path ? (
                          <img src={`/${user.current_photo_path}`} alt="" className="w-full h-full object-cover" />
                        ) : (
                          <Users className="w-6 h-6 text-muted-foreground" />
                        )}
                      </div>
                      
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="font-medium truncate">{getDisplayName(user)}</span>
                          {user.is_premium && <Crown className="w-4 h-4 text-yellow-400" />}
                          {user.is_bot && <Bot className="w-4 h-4 text-gray-400" />}
                          {user.is_scam && <AlertTriangle className="w-4 h-4 text-red-400" />}
                        </div>
                        <div className="text-sm text-muted-foreground">
                          {user.username ? `@${user.username}` : `ID: ${user.telegram_id}`}
                        </div>
                      </div>

                      <div className="flex items-center gap-4 text-sm text-muted-foreground">
                        <div className="flex items-center gap-1">
                          <MessageSquare className="w-4 h-4" />
                          {user.messages_count}
                        </div>
                        <div className="flex items-center gap-1">
                          <Users className="w-4 h-4" />
                          {user.groups_count}
                        </div>
                      </div>

                      <div className="flex items-center gap-2">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={(e) => { e.stopPropagation(); toggleFavorite(user.id); }}
                        >
                          {user.is_favorite ? (
                            <Star className="w-5 h-5 text-yellow-400 fill-yellow-400" />
                          ) : (
                            <StarOff className="w-5 h-5 text-muted-foreground" />
                          )}
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={(e) => { e.stopPropagation(); toggleWatchlist(user.id); }}
                        >
                          {user.is_watchlist ? (
                            <Eye className="w-5 h-5 text-primary" />
                          ) : (
                            <EyeOff className="w-5 h-5 text-muted-foreground" />
                          )}
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        <div>
          <Card className="sticky top-6">
            <CardHeader>
              <CardTitle>Detalle de Usuario</CardTitle>
            </CardHeader>
            <CardContent>
              {selectedUser ? (
                <div className="space-y-4">
                  <div className="text-center">
                    <div className="w-20 h-20 rounded-full bg-secondary mx-auto flex items-center justify-center overflow-hidden mb-3">
                      {selectedUser.current_photo_path ? (
                        <img src={`/${selectedUser.current_photo_path}`} alt="" className="w-full h-full object-cover" />
                      ) : (
                        <Users className="w-10 h-10 text-muted-foreground" />
                      )}
                    </div>
                    <h3 className="font-bold text-lg">{getDisplayName(selectedUser)}</h3>
                    {selectedUser.username && (
                      <p className="text-primary">@{selectedUser.username}</p>
                    )}
                  </div>

                  {selectedUser.bio && (
                    <div className="p-3 bg-secondary/30 rounded-lg">
                      <p className="text-sm text-muted-foreground mb-1">Bio</p>
                      <p className="text-sm">{selectedUser.bio}</p>
                    </div>
                  )}

                  <div className="grid grid-cols-2 gap-3">
                    <div className="p-3 bg-secondary/30 rounded-lg text-center">
                      <p className="text-2xl font-bold">{selectedUser.messages_count}</p>
                      <p className="text-xs text-muted-foreground">Mensajes</p>
                    </div>
                    <div className="p-3 bg-secondary/30 rounded-lg text-center">
                      <p className="text-2xl font-bold">{selectedUser.groups_count}</p>
                      <p className="text-xs text-muted-foreground">Grupos</p>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground">Telegram ID</span>
                      <span className="font-mono">{selectedUser.telegram_id}</span>
                    </div>
                    {selectedUser.last_seen && (
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">Ultima vez</span>
                        <span>{new Date(selectedUser.last_seen).toLocaleString()}</span>
                      </div>
                    )}
                  </div>

                  <div className="flex flex-wrap gap-2">
                    {selectedUser.is_premium && (
                      <span className="flex items-center gap-1 text-xs bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded">
                        <Crown className="w-3 h-3" /> Premium
                      </span>
                    )}
                    {selectedUser.is_verified && (
                      <span className="text-xs bg-blue-500/20 text-blue-400 px-2 py-1 rounded">Verificado</span>
                    )}
                    {selectedUser.is_bot && (
                      <span className="flex items-center gap-1 text-xs bg-gray-500/20 text-gray-400 px-2 py-1 rounded">
                        <Bot className="w-3 h-3" /> Bot
                      </span>
                    )}
                    {selectedUser.is_scam && (
                      <span className="flex items-center gap-1 text-xs bg-red-500/20 text-red-400 px-2 py-1 rounded">
                        <AlertTriangle className="w-3 h-3" /> Scam
                      </span>
                    )}
                  </div>

                  <Button
                    className="w-full mb-3"
                    onClick={() => navigate(`/users/${selectedUser.id}`)}
                  >
                    <ExternalLink className="w-4 h-4 mr-2" /> Ver Perfil Completo
                  </Button>

                  <div className="flex gap-2">
                    <Button
                      className="flex-1"
                      variant={selectedUser.is_watchlist ? 'default' : 'outline'}
                      onClick={() => toggleWatchlist(selectedUser.id)}
                    >
                      {selectedUser.is_watchlist ? (
                        <>
                          <Eye className="w-4 h-4 mr-2" /> En Watchlist
                        </>
                      ) : (
                        <>
                          <EyeOff className="w-4 h-4 mr-2" /> Agregar
                        </>
                      )}
                    </Button>
                    <Button
                      variant="outline"
                      onClick={() => toggleFavorite(selectedUser.id)}
                    >
                      {selectedUser.is_favorite ? (
                        <Star className="w-4 h-4 text-yellow-400 fill-yellow-400" />
                      ) : (
                        <StarOff className="w-4 h-4" />
                      )}
                    </Button>
                  </div>
                </div>
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <Users className="w-12 h-12 mx-auto mb-3 opacity-50" />
                  <p>Selecciona un usuario para ver detalles</p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
