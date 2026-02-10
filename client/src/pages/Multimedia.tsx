import { useState, useEffect } from 'react';
import { api } from '@/api/client';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import {
  Image,
  Video,
  Music,
  FileText,
  Mic,
  Loader2,
  BarChart3,
  Grid3X3,
  HardDrive,
  Eye,
  Clock,
  Filter,
  X,
  Play,
  Download,
  RefreshCw,
  AlertCircle,
  CheckCircle,
  Settings
} from 'lucide-react';

interface MediaStats {
  total: number;
  photos: number;
  videos: number;
  gifs: number;
  audio: number;
  documents: number;
  voice: number;
  stickers: number;
  video_notes: number;
  total_size_bytes: number;
  ocr_completed: number;
  ocr_pending: number;
}

interface MediaItem {
  id: number;
  file_type: string;
  file_path: string | null;
  file_name: string | null;
  file_size: number | null;
  mime_type: string | null;
  width: number | null;
  height: number | null;
  duration: number | null;
  ocr_status: string;
  ocr_text: string | null;
  group_id: number | null;
  group_name: string | null;
  created_at: string | null;
}

interface GroupOption {
  id: number;
  name: string;
  media_count: number;
}

interface RetryStatus {
  running: boolean;
  settings: {
    enabled: boolean;
    interval_minutes: number;
    batch_size: number;
    max_retries: number;
    parallel_downloads: number;
  };
  stats: {
    total_retried: number;
    successful: number;
    failed: number;
    last_run: string | null;
    pending_count: number;
  };
}

interface PendingMediaItem {
  id: number;
  file_type: string;
  error: string | null;
  created_at: string | null;
  group_id: number | null;
  group_name: string | null;
}

const fileTypeConfig: Record<string, { icon: typeof Image; color: string; label: string }> = {
  photo: { icon: Image, color: 'text-green-400 bg-green-500/20', label: 'Fotos' },
  video: { icon: Video, color: 'text-blue-400 bg-blue-500/20', label: 'Videos' },
  gif: { icon: Play, color: 'text-purple-400 bg-purple-500/20', label: 'GIFs' },
  audio: { icon: Music, color: 'text-yellow-400 bg-yellow-500/20', label: 'Audio' },
  voice: { icon: Mic, color: 'text-pink-400 bg-pink-500/20', label: 'Voz' },
  document: { icon: FileText, color: 'text-orange-400 bg-orange-500/20', label: 'Documentos' },
  sticker: { icon: Image, color: 'text-cyan-400 bg-cyan-500/20', label: 'Stickers' },
  video_note: { icon: Video, color: 'text-indigo-400 bg-indigo-500/20', label: 'Notas de Video' },
};

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDuration(seconds: number): string {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}:${secs.toString().padStart(2, '0')}`;
}

export default function Multimedia() {
  const [activeTab, setActiveTab] = useState<'stats' | 'gallery' | 'retry'>('stats');
  const [stats, setStats] = useState<MediaStats | null>(null);
  const [media, setMedia] = useState<MediaItem[]>([]);
  const [groups, setGroups] = useState<GroupOption[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedGroup, setSelectedGroup] = useState<number | null>(null);
  const [selectedType, setSelectedType] = useState<string | null>(null);
  const [previewItem, setPreviewItem] = useState<MediaItem | null>(null);
  const [retryStatus, setRetryStatus] = useState<RetryStatus | null>(null);
  const [pendingMedia, setPendingMedia] = useState<PendingMediaItem[]>([]);
  const [retryLoading, setRetryLoading] = useState(false);

  useEffect(() => {
    loadGroups();
    loadStats();
    loadRetryStatus();
  }, []);

  useEffect(() => {
    loadStats();
    if (activeTab === 'gallery') {
      loadMedia();
    }
    if (activeTab === 'retry') {
      loadRetryStatus();
      loadPendingMedia();
    }
  }, [selectedGroup, selectedType, activeTab]);

  const loadGroups = async () => {
    try {
      const data = await api.get<GroupOption[]>('/media/groups');
      setGroups(data || []);
    } catch (error) {
      console.error('Failed to load groups:', error);
    }
  };

  const loadStats = async () => {
    try {
      const params = selectedGroup ? `?group_id=${selectedGroup}` : '';
      const data = await api.get<MediaStats>(`/media/stats${params}`);
      setStats(data || null);
    } catch (error) {
      console.error('Failed to load stats:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadMedia = async () => {
    try {
      let params = new URLSearchParams();
      if (selectedGroup) params.append('group_id', selectedGroup.toString());
      if (selectedType) params.append('file_type', selectedType);
      params.append('limit', '100');
      
      const data = await api.get<MediaItem[]>(`/media/?${params.toString()}`);
      setMedia(data || []);
    } catch (error) {
      console.error('Failed to load media:', error);
    }
  };

  const loadRetryStatus = async () => {
    try {
      const data = await api.get<RetryStatus>('/media/retry/status');
      setRetryStatus(data || null);
    } catch (error) {
      console.error('Failed to load retry status:', error);
    }
  };

  const loadPendingMedia = async () => {
    try {
      const data = await api.get<{ count: number; items: PendingMediaItem[] }>('/media/retry/pending?limit=100');
      setPendingMedia(data?.items || []);
    } catch (error) {
      console.error('Failed to load pending media:', error);
    }
  };

  const handleRetryNow = async () => {
    setRetryLoading(true);
    try {
      await api.post('/media/retry/now', {});
      await loadRetryStatus();
      await loadPendingMedia();
    } catch (error) {
      console.error('Failed to retry media:', error);
    } finally {
      setRetryLoading(false);
    }
  };

  const handleToggleRetryService = async () => {
    try {
      if (retryStatus?.running) {
        await api.post('/media/retry/stop', {});
      } else {
        await api.post('/media/retry/start', {});
      }
      await loadRetryStatus();
    } catch (error) {
      console.error('Failed to toggle retry service:', error);
    }
  };

  const handleUpdateSettings = async (key: string, value: number | boolean) => {
    try {
      await api.post('/media/retry/settings', { [key]: value });
      await loadRetryStatus();
    } catch (error) {
      console.error('Failed to update settings:', error);
    }
  };

  const getMediaUrl = (item: MediaItem): string | null => {
    if (!item.file_path) return null;
    // file_path already includes "media/" prefix, so we just add leading slash
    return `/${item.file_path}`;
  };

  const statCards = stats ? [
    { label: 'Fotos', value: stats.photos, icon: Image, color: 'text-green-400' },
    { label: 'Videos', value: stats.videos, icon: Video, color: 'text-blue-400' },
    { label: 'GIFs', value: stats.gifs, icon: Play, color: 'text-purple-400' },
    { label: 'Audio', value: stats.audio, icon: Music, color: 'text-yellow-400' },
    { label: 'Voz', value: stats.voice, icon: Mic, color: 'text-pink-400' },
    { label: 'Documentos', value: stats.documents, icon: FileText, color: 'text-orange-400' },
    { label: 'Stickers', value: stats.stickers, icon: Image, color: 'text-cyan-400' },
    { label: 'Video Notes', value: stats.video_notes, icon: Video, color: 'text-indigo-400' },
  ] : [];

  if (loading) {
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
            <Image className="h-8 w-8 text-primary" />
            Multimedia
          </h1>
          <p className="text-muted-foreground mt-1">Galeria de archivos multimedia recolectados</p>
        </div>
        <div className="flex gap-2">
          <Button
            variant={activeTab === 'stats' ? 'default' : 'outline'}
            onClick={() => setActiveTab('stats')}
          >
            <BarChart3 className="w-4 h-4 mr-2" />
            Estadisticas
          </Button>
          <Button
            variant={activeTab === 'gallery' ? 'default' : 'outline'}
            onClick={() => setActiveTab('gallery')}
          >
            <Grid3X3 className="w-4 h-4 mr-2" />
            Galeria
          </Button>
          <Button
            variant={activeTab === 'retry' ? 'default' : 'outline'}
            onClick={() => setActiveTab('retry')}
          >
            <RefreshCw className="w-4 h-4 mr-2" />
            Reintento
            {retryStatus?.stats?.pending_count ? (
              <span className="ml-2 bg-red-500 text-white text-xs px-2 py-0.5 rounded-full">
                {retryStatus.stats.pending_count}
              </span>
            ) : null}
          </Button>
        </div>
      </div>

      <div className="flex gap-4 items-center flex-wrap">
        <div className="flex items-center gap-2">
          <Filter className="w-4 h-4 text-muted-foreground" />
          <select
            className="bg-secondary border border-border rounded-lg px-3 py-2 text-sm"
            value={selectedGroup || ''}
            onChange={(e) => setSelectedGroup(e.target.value ? parseInt(e.target.value) : null)}
          >
            <option value="">Todos los grupos</option>
            {groups.map((g) => (
              <option key={g.id} value={g.id}>
                {g.name} ({g.media_count})
              </option>
            ))}
          </select>
        </div>
        
        <div className="flex items-center gap-2">
          <select
            className="bg-secondary border border-border rounded-lg px-3 py-2 text-sm"
            value={selectedType || ''}
            onChange={(e) => setSelectedType(e.target.value || null)}
          >
            <option value="">Todos los tipos</option>
            <option value="photo">Fotos</option>
            <option value="video">Videos</option>
            <option value="gif">GIFs</option>
            <option value="audio">Audio</option>
            <option value="voice">Voz</option>
            <option value="document">Documentos</option>
            <option value="sticker">Stickers</option>
            <option value="video_note">Notas de video</option>
          </select>
        </div>

        {(selectedGroup || selectedType) && (
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              setSelectedGroup(null);
              setSelectedType(null);
            }}
          >
            <X className="w-4 h-4 mr-1" />
            Limpiar filtros
          </Button>
        )}
      </div>

      {activeTab === 'stats' && stats && (
        <>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            <Card className="bg-gradient-to-br from-primary/20 to-primary/5">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-muted-foreground">Total Archivos</p>
                    <p className="text-3xl font-bold">{stats.total.toLocaleString()}</p>
                  </div>
                  <HardDrive className="h-10 w-10 text-primary opacity-80" />
                </div>
              </CardContent>
            </Card>
            
            <Card className="bg-gradient-to-br from-blue-500/20 to-blue-500/5">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-muted-foreground">Tamano Total</p>
                    <p className="text-3xl font-bold">{formatBytes(stats.total_size_bytes)}</p>
                  </div>
                  <Download className="h-10 w-10 text-blue-400 opacity-80" />
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gradient-to-br from-green-500/20 to-green-500/5">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-muted-foreground">OCR Completado</p>
                    <p className="text-3xl font-bold">{stats.ocr_completed.toLocaleString()}</p>
                  </div>
                  <Eye className="h-10 w-10 text-green-400 opacity-80" />
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gradient-to-br from-yellow-500/20 to-yellow-500/5">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-muted-foreground">OCR Pendiente</p>
                    <p className="text-3xl font-bold">{stats.ocr_pending.toLocaleString()}</p>
                  </div>
                  <Clock className="h-10 w-10 text-yellow-400 opacity-80" />
                </div>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Tipos de Archivo</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {statCards.map((stat) => (
                  <div
                    key={stat.label}
                    className="flex items-center gap-3 p-4 bg-secondary/50 rounded-lg cursor-pointer hover:bg-secondary/80 transition-colors"
                    onClick={() => {
                      const typeMap: Record<string, string> = {
                        'Fotos': 'photo',
                        'Videos': 'video',
                        'GIFs': 'gif',
                        'Audio': 'audio',
                        'Voz': 'voice',
                        'Documentos': 'document',
                        'Stickers': 'sticker',
                        'Video Notes': 'video_note',
                      };
                      setSelectedType(typeMap[stat.label] || null);
                      setActiveTab('gallery');
                    }}
                  >
                    <stat.icon className={`h-8 w-8 ${stat.color}`} />
                    <div>
                      <p className="text-2xl font-bold">{stat.value.toLocaleString()}</p>
                      <p className="text-sm text-muted-foreground">{stat.label}</p>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </>
      )}

      {activeTab === 'gallery' && (
        <div className="space-y-4">
          {media.length === 0 ? (
            <Card>
              <CardContent className="py-12 text-center">
                <Image className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
                <p className="text-muted-foreground">No hay archivos multimedia</p>
                <p className="text-sm text-muted-foreground mt-1">
                  Los archivos se recolectan automaticamente cuando se procesan mensajes
                </p>
              </CardContent>
            </Card>
          ) : (
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
              {media.map((item) => {
                const config = fileTypeConfig[item.file_type] || fileTypeConfig.document;
                const IconComponent = config.icon;
                
                return (
                  <Card
                    key={item.id}
                    className="overflow-hidden cursor-pointer hover:ring-2 hover:ring-primary transition-all group"
                    onClick={() => setPreviewItem(item)}
                  >
                    <div className="aspect-square bg-secondary/50 flex items-center justify-center relative overflow-hidden">
                      {item.file_path && (item.file_type === 'photo' || item.file_type === 'sticker') ? (
                        <img
                          src={getMediaUrl(item) || ''}
                          alt={item.file_name || 'Media'}
                          className="w-full h-full object-cover"
                          onError={(e) => {
                            (e.target as HTMLImageElement).style.display = 'none';
                          }}
                        />
                      ) : item.file_path && (item.file_type === 'gif' || item.file_type === 'video' || item.file_type === 'video_note') ? (
                        <video
                          src={getMediaUrl(item) || ''}
                          className="w-full h-full object-cover"
                          muted
                          loop
                          playsInline
                          autoPlay={item.file_type === 'gif'}
                          onMouseEnter={(e) => (e.target as HTMLVideoElement).play()}
                          onMouseLeave={(e) => { if (item.file_type !== 'gif') (e.target as HTMLVideoElement).pause(); }}
                          onError={(e) => {
                            (e.target as HTMLVideoElement).style.display = 'none';
                          }}
                        />
                      ) : (
                        <IconComponent className={`h-12 w-12 ${config.color.split(' ')[0]}`} />
                      )}
                      
                      {item.duration && (
                        <div className="absolute bottom-2 right-2 bg-black/70 text-white text-xs px-2 py-1 rounded">
                          {formatDuration(item.duration)}
                        </div>
                      )}
                      
                      <div className="absolute inset-0 bg-black/50 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center">
                        <Eye className="h-8 w-8 text-white" />
                      </div>
                    </div>
                    <CardContent className="p-3">
                      <p className="text-xs truncate text-muted-foreground">
                        {item.file_name || `${item.file_type}_${item.id}`}
                      </p>
                      <div className="flex items-center justify-between mt-1">
                        <span className={`text-xs px-2 py-0.5 rounded ${config.color}`}>
                          {config.label}
                        </span>
                        {item.file_size && (
                          <span className="text-xs text-muted-foreground">
                            {formatBytes(item.file_size)}
                          </span>
                        )}
                      </div>
                      {item.group_name && (
                        <p className="text-xs text-muted-foreground mt-1 truncate">
                          {item.group_name}
                        </p>
                      )}
                    </CardContent>
                  </Card>
                );
              })}
            </div>
          )}
        </div>
      )}

      {activeTab === 'retry' && retryStatus && (
        <div className="space-y-6">
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            <Card className="bg-gradient-to-br from-red-500/20 to-red-500/5">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-muted-foreground">Media Pendiente</p>
                    <p className="text-3xl font-bold">{retryStatus.stats.pending_count}</p>
                  </div>
                  <AlertCircle className="h-10 w-10 text-red-400 opacity-80" />
                </div>
              </CardContent>
            </Card>
            
            <Card className="bg-gradient-to-br from-green-500/20 to-green-500/5">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-muted-foreground">Exitosos</p>
                    <p className="text-3xl font-bold">{retryStatus.stats.successful}</p>
                  </div>
                  <CheckCircle className="h-10 w-10 text-green-400 opacity-80" />
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gradient-to-br from-yellow-500/20 to-yellow-500/5">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-muted-foreground">Fallidos</p>
                    <p className="text-3xl font-bold">{retryStatus.stats.failed}</p>
                  </div>
                  <X className="h-10 w-10 text-yellow-400 opacity-80" />
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gradient-to-br from-blue-500/20 to-blue-500/5">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-muted-foreground">Total Reintentos</p>
                    <p className="text-3xl font-bold">{retryStatus.stats.total_retried}</p>
                  </div>
                  <RefreshCw className="h-10 w-10 text-blue-400 opacity-80" />
                </div>
              </CardContent>
            </Card>
          </div>

          <div className="grid gap-6 md:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Settings className="h-5 w-5" />
                  Control del Servicio
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-medium">Estado</p>
                    <p className="text-sm text-muted-foreground">
                      {retryStatus.running ? 'Servicio activo' : 'Servicio detenido'}
                    </p>
                  </div>
                  <Button
                    variant={retryStatus.running ? 'destructive' : 'default'}
                    onClick={handleToggleRetryService}
                  >
                    {retryStatus.running ? 'Detener' : 'Iniciar'}
                  </Button>
                </div>

                <div className="border-t pt-4">
                  <Button
                    className="w-full"
                    onClick={handleRetryNow}
                    disabled={retryLoading || retryStatus.stats.pending_count === 0}
                  >
                    {retryLoading ? (
                      <>
                        <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                        Reintentando...
                      </>
                    ) : (
                      <>
                        <RefreshCw className="w-4 h-4 mr-2" />
                        Reintentar Ahora ({retryStatus.stats.pending_count} pendientes)
                      </>
                    )}
                  </Button>
                </div>

                {retryStatus.stats.last_run && (
                  <p className="text-xs text-muted-foreground text-center">
                    Ultimo reintento: {new Date(retryStatus.stats.last_run).toLocaleString()}
                  </p>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Settings className="h-5 w-5" />
                  Configuracion
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-medium">Reintento Automatico</p>
                    <p className="text-sm text-muted-foreground">Reintentar cada intervalo</p>
                  </div>
                  <Button
                    variant={retryStatus.settings.enabled ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => handleUpdateSettings('enabled', !retryStatus.settings.enabled)}
                  >
                    {retryStatus.settings.enabled ? 'Activado' : 'Desactivado'}
                  </Button>
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-medium">Intervalo (minutos)</label>
                  <select
                    className="w-full bg-secondary border border-border rounded-lg px-3 py-2"
                    value={retryStatus.settings.interval_minutes}
                    onChange={(e) => handleUpdateSettings('interval_minutes', parseInt(e.target.value))}
                  >
                    <option value={15}>15 minutos</option>
                    <option value={30}>30 minutos</option>
                    <option value={60}>1 hora</option>
                    <option value={120}>2 horas</option>
                    <option value={360}>6 horas</option>
                  </select>
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-medium">Lote por reintento</label>
                  <select
                    className="w-full bg-secondary border border-border rounded-lg px-3 py-2"
                    value={retryStatus.settings.batch_size}
                    onChange={(e) => handleUpdateSettings('batch_size', parseInt(e.target.value))}
                  >
                    <option value={25}>25 archivos</option>
                    <option value={50}>50 archivos</option>
                    <option value={100}>100 archivos</option>
                    <option value={200}>200 archivos</option>
                  </select>
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-medium">Descargas paralelas</label>
                  <select
                    className="w-full bg-secondary border border-border rounded-lg px-3 py-2"
                    value={retryStatus.settings.parallel_downloads}
                    onChange={(e) => handleUpdateSettings('parallel_downloads', parseInt(e.target.value))}
                  >
                    <option value={1}>1</option>
                    <option value={2}>2</option>
                    <option value={3}>3</option>
                    <option value={5}>5</option>
                    <option value={10}>10</option>
                  </select>
                </div>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Media Pendiente de Descarga</CardTitle>
            </CardHeader>
            <CardContent>
              {pendingMedia.length === 0 ? (
                <div className="text-center py-8">
                  <CheckCircle className="h-12 w-12 mx-auto text-green-400 mb-4" />
                  <p className="text-muted-foreground">No hay media pendiente</p>
                  <p className="text-sm text-muted-foreground mt-1">
                    Toda la media ha sido descargada correctamente
                  </p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left py-2 px-4">ID</th>
                        <th className="text-left py-2 px-4">Tipo</th>
                        <th className="text-left py-2 px-4">Grupo</th>
                        <th className="text-left py-2 px-4">Error</th>
                        <th className="text-left py-2 px-4">Fecha</th>
                      </tr>
                    </thead>
                    <tbody>
                      {pendingMedia.slice(0, 50).map((item) => {
                        const config = fileTypeConfig[item.file_type] || fileTypeConfig.document;
                        return (
                          <tr key={item.id} className="border-b hover:bg-secondary/50">
                            <td className="py-2 px-4">{item.id}</td>
                            <td className="py-2 px-4">
                              <span className={`px-2 py-1 rounded text-xs ${config.color}`}>
                                {config.label}
                              </span>
                            </td>
                            <td className="py-2 px-4 text-muted-foreground">
                              {item.group_name || `Grupo ${item.group_id}`}
                            </td>
                            <td className="py-2 px-4">
                              {item.error ? (
                                <span className="text-red-400 text-xs truncate max-w-[200px] block">
                                  {item.error}
                                </span>
                              ) : (
                                <span className="text-yellow-400 text-xs">Sin descargar</span>
                              )}
                            </td>
                            <td className="py-2 px-4 text-muted-foreground text-xs">
                              {item.created_at ? new Date(item.created_at).toLocaleDateString() : '-'}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                  {pendingMedia.length > 50 && (
                    <p className="text-center text-sm text-muted-foreground py-4">
                      Mostrando 50 de {pendingMedia.length} archivos pendientes
                    </p>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {previewItem && (
        <div
          className="fixed inset-0 bg-black/80 z-50 flex items-center justify-center p-8"
          onClick={() => setPreviewItem(null)}
        >
          <div
            className="bg-card rounded-lg max-w-4xl w-full max-h-[90vh] overflow-auto"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="p-6">
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="text-lg font-bold">{previewItem.file_name || 'Sin nombre'}</h3>
                  <p className="text-sm text-muted-foreground">{previewItem.group_name || 'Grupo desconocido'}</p>
                </div>
                <Button variant="ghost" size="sm" onClick={() => setPreviewItem(null)}>
                  <X className="h-4 w-4" />
                </Button>
              </div>
              
              <div className="aspect-video bg-secondary rounded-lg flex items-center justify-center mb-4">
                {previewItem.file_path && (previewItem.file_type === 'photo' || previewItem.file_type === 'sticker') ? (
                  <img
                    src={getMediaUrl(previewItem) || ''}
                    alt={previewItem.file_name || 'Media'}
                    className="max-w-full max-h-full object-contain"
                  />
                ) : previewItem.file_path && (previewItem.file_type === 'video' || previewItem.file_type === 'gif' || previewItem.file_type === 'video_note') ? (
                  <video
                    src={getMediaUrl(previewItem) || ''}
                    controls
                    autoPlay={previewItem.file_type === 'gif'}
                    loop={previewItem.file_type === 'gif'}
                    className="max-w-full max-h-full"
                  />
                ) : previewItem.file_path && (previewItem.file_type === 'audio' || previewItem.file_type === 'voice') ? (
                  <audio
                    src={getMediaUrl(previewItem) || ''}
                    controls
                    className="w-full"
                  />
                ) : (
                  <div className="text-center">
                    <FileText className="h-16 w-16 text-muted-foreground mx-auto mb-2" />
                    <p className="text-muted-foreground">Vista previa no disponible</p>
                    {previewItem.file_path && (
                      <a 
                        href={getMediaUrl(previewItem) || ''} 
                        download 
                        className="text-primary hover:underline mt-2 inline-block"
                      >
                        Descargar archivo
                      </a>
                    )}
                  </div>
                )}
              </div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div className="bg-secondary/50 p-3 rounded-lg">
                  <p className="text-muted-foreground">Tipo</p>
                  <p className="font-medium capitalize">{previewItem.file_type}</p>
                </div>
                {previewItem.file_size && (
                  <div className="bg-secondary/50 p-3 rounded-lg">
                    <p className="text-muted-foreground">Tamano</p>
                    <p className="font-medium">{formatBytes(previewItem.file_size)}</p>
                  </div>
                )}
                {previewItem.width && previewItem.height && (
                  <div className="bg-secondary/50 p-3 rounded-lg">
                    <p className="text-muted-foreground">Dimensiones</p>
                    <p className="font-medium">{previewItem.width} x {previewItem.height}</p>
                  </div>
                )}
                {previewItem.duration && (
                  <div className="bg-secondary/50 p-3 rounded-lg">
                    <p className="text-muted-foreground">Duracion</p>
                    <p className="font-medium">{formatDuration(previewItem.duration)}</p>
                  </div>
                )}
              </div>

              {previewItem.ocr_text && (
                <div className="mt-4 p-4 bg-secondary/50 rounded-lg">
                  <p className="text-sm text-muted-foreground mb-2">Texto OCR detectado:</p>
                  <p className="text-sm whitespace-pre-wrap">{previewItem.ocr_text}</p>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
