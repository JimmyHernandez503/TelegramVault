import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '@/api/client';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Loader2, Star, Eye, Search, User, Circle } from 'lucide-react';

interface TelegramEntity {
  id: number;
  telegram_id: number;
  type: 'user' | 'channel' | 'group' | 'supergroup' | 'megagroup';
  username: string | null;
  first_name: string | null;
  last_name: string | null;
  title: string | null;
  bio: string | null;
  is_premium: boolean;
  is_verified: boolean;
  is_bot: boolean;
  is_watchlist: boolean;
  is_favorite: boolean;
  messages_count: number;
  groups_count: number;
  media_count: number;
  current_photo_path: string | null;
  has_stories: boolean;
  member_count?: number;
}

export function UsersPage() {
  const navigate = useNavigate();
  const [entities, setEntities] = useState<TelegramEntity[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const [favoritesOnly, setFavoritesOnly] = useState(false);
  const [includeChannels, setIncludeChannels] = useState(true);

  const fetchEntities = async () => {
    setLoading(true);
    try {
      // Try the combined endpoint first, fallback to users only
      let data: TelegramEntity[];
      try {
        data = await api.get<TelegramEntity[]>(`/users/combined?search=${search || ''}&watchlist_only=${watchlistOnly}&favorites_only=${favoritesOnly}&include_channels=${includeChannels}`);
      } catch (combinedError) {
        console.warn('Combined endpoint failed, falling back to users only:', combinedError);
        // Fallback to original users endpoint
        const usersData = await api.getUsers({
          search: search || undefined,
          watchlist_only: watchlistOnly,
          favorites_only: favoritesOnly,
        });
        // Convert to TelegramEntity format
        data = usersData.map((user: any) => ({
          ...user,
          type: 'user' as const,
          title: null,
          member_count: undefined,
        }));
      }
      setEntities(data);
    } catch (err) {
      console.error('Failed to fetch entities:', err);
      setEntities([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchEntities();
  }, [watchlistOnly, favoritesOnly, includeChannels]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    fetchEntities();
  };

  const getEntityName = (entity: TelegramEntity) => {
    if (entity.type === 'user') {
      return entity.first_name || entity.username || 'Usuario';
    }
    return entity.title || entity.username || 'Canal/Grupo';
  };

  const getEntityType = (entity: TelegramEntity) => {
    switch (entity.type) {
      case 'channel':
        return 'Canal';
      case 'group':
      case 'supergroup':
      case 'megagroup':
        return 'Grupo';
      default:
        return 'Usuario';
    }
  };

  const getEntityTypeColor = (entity: TelegramEntity) => {
    switch (entity.type) {
      case 'channel':
        return 'bg-blue-500/20 text-blue-400';
      case 'group':
      case 'supergroup':
      case 'megagroup':
        return 'bg-green-500/20 text-green-400';
      default:
        return 'bg-gray-500/20 text-gray-400';
    }
  };

  if (loading && entities.length === 0) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Usuarios y Canales</h1>
        <p className="text-muted-foreground">Perfiles de usuarios y canales de Telegram recolectados</p>
      </div>

      <div className="flex flex-wrap gap-4">
        <form onSubmit={handleSearch} className="flex gap-2">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Buscar usuario o canal..."
              className="pl-9 w-64"
            />
          </div>
          <Button type="submit" variant="secondary">Buscar</Button>
        </form>
        <div className="flex gap-2">
          <Button
            variant={includeChannels ? 'default' : 'outline'}
            onClick={() => setIncludeChannels(!includeChannels)}
          >
            Incluir Canales
          </Button>
          <Button
            variant={watchlistOnly ? 'default' : 'outline'}
            onClick={() => setWatchlistOnly(!watchlistOnly)}
          >
            <Eye className="mr-2 h-4 w-4" />
            Watchlist
          </Button>
          <Button
            variant={favoritesOnly ? 'default' : 'outline'}
            onClick={() => setFavoritesOnly(!favoritesOnly)}
          >
            <Star className="mr-2 h-4 w-4" />
            Favoritos
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
        {entities.map((entity) => (
          <Card 
            key={`${entity.type}-${entity.id}`}
            className="hover:border-primary/50 transition-colors cursor-pointer"
            onClick={() => {
              if (entity.type === 'user') {
                navigate(`/users/${entity.id}`);
              } else {
                navigate(`/groups/${entity.id}`);
              }
            }}
          >
            <CardContent className="pt-6">
              <div className="flex items-start gap-4">
                <div className="relative">
                  {entity.current_photo_path ? (
                    <img
                      src={`/${entity.current_photo_path}`}
                      alt={getEntityName(entity)}
                      className={`h-12 w-12 rounded-full object-cover ${entity.has_stories ? 'ring-2 ring-pink-500 ring-offset-2 ring-offset-background' : ''}`}
                      onError={(e) => {
                        (e.target as HTMLImageElement).style.display = 'none';
                        (e.target as HTMLImageElement).nextElementSibling?.classList.remove('hidden');
                      }}
                    />
                  ) : null}
                  <div className={`flex h-12 w-12 items-center justify-center rounded-full bg-secondary ${entity.current_photo_path ? 'hidden' : ''} ${entity.has_stories ? 'ring-2 ring-pink-500 ring-offset-2 ring-offset-background' : ''}`}>
                    <User className="h-6 w-6 text-muted-foreground" />
                  </div>
                  {entity.has_stories && (
                    <Circle className="absolute -top-0.5 -right-0.5 h-3 w-3 fill-pink-500 text-pink-500" />
                  )}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <h3 className="font-medium truncate">
                      {getEntityName(entity)}
                      {entity.type === 'user' && entity.last_name ? ` ${entity.last_name}` : ''}
                    </h3>
                    {entity.is_watchlist && <Eye className="h-4 w-4 text-blue-500" />}
                    {entity.is_favorite && <Star className="h-4 w-4 text-yellow-500" />}
                  </div>
                  {entity.username && (
                    <p className="text-sm text-muted-foreground">@{entity.username}</p>
                  )}
                  {entity.bio && (
                    <p className="mt-1 text-xs text-muted-foreground line-clamp-2">{entity.bio}</p>
                  )}
                  <div className="mt-2 flex gap-4 text-xs text-muted-foreground">
                    <span>{entity.messages_count} msgs</span>
                    {entity.type === 'user' && <span>{entity.groups_count} grupos</span>}
                    {entity.member_count !== undefined && <span>{entity.member_count} miembros</span>}
                    {entity.media_count > 0 && <span>{entity.media_count} media</span>}
                  </div>
                  <div className="mt-2 flex flex-wrap gap-1">
                    <span className={`text-xs px-2 py-0.5 rounded ${getEntityTypeColor(entity)}`}>
                      {getEntityType(entity)}
                    </span>
                    {entity.is_premium && (
                      <span className="text-xs bg-purple-500/20 text-purple-400 px-2 py-0.5 rounded">Premium</span>
                    )}
                    {entity.is_verified && (
                      <span className="text-xs bg-blue-500/20 text-blue-400 px-2 py-0.5 rounded">Verificado</span>
                    )}
                    {entity.is_bot && (
                      <span className="text-xs bg-orange-500/20 text-orange-400 px-2 py-0.5 rounded">Bot</span>
                    )}
                    {entity.has_stories && (
                      <span className="text-xs bg-pink-500/20 text-pink-400 px-2 py-0.5 rounded">Stories</span>
                    )}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {entities.length === 0 && (
        <Card>
          <CardContent className="py-10 text-center">
            <p className="text-muted-foreground">No se encontraron usuarios o canales</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
