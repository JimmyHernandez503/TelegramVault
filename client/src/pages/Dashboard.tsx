import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '@/api/client';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { RealTimeMessages } from '@/components/RealTimeMessages';
import {
  MessageSquare,
  Users,
  Image,
  AlertTriangle,
  Activity,
  Link2,
  Key,
  FolderOpen,
  Loader2,
  Shield,
  Wifi,
  Mail,
  Phone,
  Bitcoin,
  Globe,
  AtSign,
  TrendingUp,
  Zap,
  Gauge,
  Download,
  UserPlus,
  Clock
} from 'lucide-react';

interface Stats {
  total_messages: number;
  total_users: number;
  total_groups: number;
  total_media: number;
  total_detections: number;
  active_accounts: number;
  total_accounts: number;
  pending_invites: number;
  backfills_in_progress: number;
  ocr_pending: number;
}

interface DetectionStats {
  email: number;
  phone: number;
  crypto: number;
  url: number;
  invite_link: number;
  telegram_link: number;
  telegram_username: number;
  total: number;
}

interface LiveStats {
  uptime_seconds: number;
  messages: { per_second: number; per_minute: number; last_minute: number; last_hour: number };
  media: { per_second: number; per_minute: number; last_minute: number; last_hour: number; queued: number };
  members: { per_second: number; per_minute: number; last_minute: number; last_hour: number };
  detections: { per_second: number; per_minute: number; last_minute: number; last_hour: number };
  users: { per_second: number; per_minute: number; last_minute: number; last_hour: number };
  stories: { per_minute: number; last_hour: number };
  backfill: { per_second: number; per_minute: number; last_minute: number };
}

export function DashboardPage() {
  const navigate = useNavigate();
  const [stats, setStats] = useState<Stats | null>(null);
  const [detectionStats, setDetectionStats] = useState<DetectionStats | null>(null);
  const [liveStats, setLiveStats] = useState<LiveStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [statsData, detectionsData] = await Promise.all([
          api.getDashboardStats(),
          api.get<DetectionStats>('/detections/stats').catch(() => null)
        ]);
        setStats(statsData);
        if (detectionsData) setDetectionStats(detectionsData);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    const fetchLiveStats = async () => {
      try {
        const data = await api.getLiveStats();
        setLiveStats(data);
      } catch (e) {}
    };

    fetchData();
    fetchLiveStats();
    const interval = setInterval(fetchData, 30000);
    const liveInterval = setInterval(fetchLiveStats, 5000);
    return () => {
      clearInterval(interval);
      clearInterval(liveInterval);
    };
  }, []);

  const formatUptime = (seconds: number) => {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    return h > 0 ? `${h}h ${m}m` : `${m}m`;
  };

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-full items-center justify-center">
        <p className="text-destructive">{error}</p>
      </div>
    );
  }

  const mainStats = [
    { title: 'Mensajes Capturados', value: stats?.total_messages || 0, icon: MessageSquare, color: 'bg-blue-500/20 text-blue-400', trend: '+12%' },
    { title: 'Usuarios Rastreados', value: stats?.total_users || 0, icon: Users, color: 'bg-green-500/20 text-green-400', trend: '+8%' },
    { title: 'Grupos Monitoreados', value: stats?.total_groups || 0, icon: FolderOpen, color: 'bg-purple-500/20 text-purple-400' },
    { title: 'Archivos Multimedia', value: stats?.total_media || 0, icon: Image, color: 'bg-orange-500/20 text-orange-400' },
  ];

  const detectionCards = [
    { title: 'Emails', value: detectionStats?.email || 0, icon: Mail, color: 'text-blue-400', filter: 'email' },
    { title: 'Telefonos', value: detectionStats?.phone || 0, icon: Phone, color: 'text-green-400', filter: 'phone' },
    { title: 'Crypto Wallets', value: detectionStats?.crypto || 0, icon: Bitcoin, color: 'text-yellow-400', filter: 'crypto' },
    { title: 'URLs', value: detectionStats?.url || 0, icon: Globe, color: 'text-cyan-400', filter: 'url' },
    { title: 'Invites', value: detectionStats?.invite_link || 0, icon: Link2, color: 'text-pink-400', filter: 'invite_link' },
    { title: '@Usernames', value: detectionStats?.telegram_username || 0, icon: AtSign, color: 'text-purple-400', filter: 'telegram_username' },
  ];

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold flex items-center gap-3">
            <Shield className="h-8 w-8 text-primary" />
            Centro de Inteligencia
          </h1>
          <p className="text-muted-foreground mt-1">Monitoreo y vigilancia en tiempo real de Telegram</p>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 px-4 py-2 bg-green-500/20 rounded-lg">
            <Wifi className="h-4 w-4 text-green-400" />
            <span className="text-sm text-green-400">{stats?.active_accounts || 0} cuentas activas</span>
          </div>
          <div className="flex items-center gap-2 px-4 py-2 bg-yellow-500/20 rounded-lg">
            <Activity className="h-4 w-4 text-yellow-400" />
            <span className="text-sm text-yellow-400">{stats?.backfills_in_progress || 0} backfills</span>
          </div>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {mainStats.map((stat) => (
          <Card key={stat.title} className="relative overflow-hidden">
            <div className={`absolute top-0 right-0 w-32 h-32 ${stat.color.split(' ')[0]} rounded-full blur-3xl opacity-30 -mr-10 -mt-10`}></div>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                {stat.title}
              </CardTitle>
              <div className={`p-2 rounded-lg ${stat.color}`}>
                <stat.icon className="h-5 w-5" />
              </div>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">{typeof stat.value === 'number' ? stat.value.toLocaleString() : stat.value}</div>
              {stat.trend && (
                <div className="flex items-center gap-1 mt-1 text-green-400 text-sm">
                  <TrendingUp className="h-3 w-3" />
                  <span>{stat.trend} esta semana</span>
                </div>
              )}
            </CardContent>
          </Card>
        ))}
      </div>

      <Card className="mb-6">
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2">
            <Gauge className="h-5 w-5 text-cyan-400" />
            Rendimiento en Tiempo Real
            {liveStats && (
              <span className="ml-auto text-sm font-normal text-muted-foreground flex items-center gap-1">
                <Clock className="h-3 w-3" />
                Uptime: {formatUptime(liveStats.uptime_seconds)}
              </span>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
            <div className="p-4 bg-gradient-to-br from-blue-500/20 to-blue-600/10 rounded-lg border border-blue-500/30">
              <div className="flex items-center gap-2 mb-2">
                <MessageSquare className="h-4 w-4 text-blue-400" />
                <span className="text-xs text-muted-foreground">Mensajes</span>
              </div>
              <div className="text-2xl font-bold text-blue-400">
                {(liveStats?.messages?.per_minute ?? 0).toFixed(1)}
                <span className="text-xs font-normal text-muted-foreground">/min</span>
              </div>
              <div className="text-xs text-muted-foreground mt-1">
                {liveStats?.messages?.last_hour ?? 0} ultima hora
              </div>
            </div>

            <div className="p-4 bg-gradient-to-br from-orange-500/20 to-orange-600/10 rounded-lg border border-orange-500/30">
              <div className="flex items-center gap-2 mb-2">
                <Download className="h-4 w-4 text-orange-400" />
                <span className="text-xs text-muted-foreground">Media</span>
              </div>
              <div className="text-2xl font-bold text-orange-400">
                {(liveStats?.media?.per_minute ?? 0).toFixed(1)}
                <span className="text-xs font-normal text-muted-foreground">/min</span>
              </div>
              <div className="text-xs text-muted-foreground mt-1">
                {liveStats?.media?.queued ?? 0} en cola
              </div>
            </div>

            <div className="p-4 bg-gradient-to-br from-green-500/20 to-green-600/10 rounded-lg border border-green-500/30">
              <div className="flex items-center gap-2 mb-2">
                <UserPlus className="h-4 w-4 text-green-400" />
                <span className="text-xs text-muted-foreground">Miembros</span>
              </div>
              <div className="text-2xl font-bold text-green-400">
                {(liveStats?.members?.per_minute ?? 0).toFixed(1)}
                <span className="text-xs font-normal text-muted-foreground">/min</span>
              </div>
              <div className="text-xs text-muted-foreground mt-1">
                {liveStats?.members?.last_hour ?? 0} ultima hora
              </div>
            </div>

            <div className="p-4 bg-gradient-to-br from-red-500/20 to-red-600/10 rounded-lg border border-red-500/30">
              <div className="flex items-center gap-2 mb-2">
                <AlertTriangle className="h-4 w-4 text-red-400" />
                <span className="text-xs text-muted-foreground">Detecciones</span>
              </div>
              <div className="text-2xl font-bold text-red-400">
                {(liveStats?.detections?.per_minute ?? 0).toFixed(1)}
                <span className="text-xs font-normal text-muted-foreground">/min</span>
              </div>
              <div className="text-xs text-muted-foreground mt-1">
                {liveStats?.detections?.last_hour ?? 0} ultima hora
              </div>
            </div>

            <div className="p-4 bg-gradient-to-br from-purple-500/20 to-purple-600/10 rounded-lg border border-purple-500/30">
              <div className="flex items-center gap-2 mb-2">
                <Users className="h-4 w-4 text-purple-400" />
                <span className="text-xs text-muted-foreground">Usuarios</span>
              </div>
              <div className="text-2xl font-bold text-purple-400">
                {(liveStats?.users?.per_minute ?? 0).toFixed(1)}
                <span className="text-xs font-normal text-muted-foreground">/min</span>
              </div>
              <div className="text-xs text-muted-foreground mt-1">
                {liveStats?.users?.last_hour ?? 0} enriquecidos
              </div>
            </div>

            <div className="p-4 bg-gradient-to-br from-yellow-500/20 to-yellow-600/10 rounded-lg border border-yellow-500/30">
              <div className="flex items-center gap-2 mb-2">
                <Activity className="h-4 w-4 text-yellow-400" />
                <span className="text-xs text-muted-foreground">Backfill</span>
              </div>
              <div className="text-2xl font-bold text-yellow-400">
                {(liveStats?.backfill?.per_minute ?? 0).toFixed(1)}
                <span className="text-xs font-normal text-muted-foreground">/min</span>
              </div>
              <div className="text-xs text-muted-foreground mt-1">
                {liveStats?.backfill?.last_minute ?? 0} ultimo min
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-6 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader className="flex flex-row items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-red-400" />
            <CardTitle>Detecciones de Inteligencia</CardTitle>
            <span className="ml-auto text-2xl font-bold text-red-400">{detectionStats?.total || 0}</span>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
              {detectionCards.map((det) => (
                <div 
                  key={det.title} 
                  className="flex items-center gap-3 p-4 bg-gray-800/50 rounded-lg cursor-pointer hover:bg-gray-700/50 transition-colors"
                  onClick={() => navigate(`/detections?filter=${det.filter}`)}
                >
                  <det.icon className={`h-8 w-8 ${det.color}`} />
                  <div>
                    <p className="text-2xl font-bold">{det.value.toLocaleString()}</p>
                    <p className="text-sm text-muted-foreground">{det.title}</p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Zap className="h-5 w-5 text-yellow-400" />
              Estado del Sistema
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between p-3 bg-gray-800/50 rounded-lg">
              <div className="flex items-center gap-2">
                <Key className="h-4 w-4 text-cyan-400" />
                <span className="text-sm">Cuentas Telegram</span>
              </div>
              <span className="font-bold">{stats?.active_accounts}/{stats?.total_accounts}</span>
            </div>
            <div className="flex items-center justify-between p-3 bg-gray-800/50 rounded-lg">
              <div className="flex items-center gap-2">
                <Link2 className="h-4 w-4 text-pink-400" />
                <span className="text-sm">Invites Pendientes</span>
              </div>
              <span className="font-bold">{stats?.pending_invites || 0}</span>
            </div>
            <div className="flex items-center justify-between p-3 bg-gray-800/50 rounded-lg">
              <div className="flex items-center gap-2">
                <Gauge className="h-4 w-4 text-blue-400" />
                <span className="text-sm">Backfills Activos</span>
              </div>
              <span className="font-bold">{stats?.backfills_in_progress || 0}</span>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="h-96">
          <RealTimeMessages maxMessages={30} />
        </div>
        <div className="h-96">
          <RealTimeMessages maxMessages={30} showDetectionsOnly />
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <a href="/accounts" className="block p-6 bg-gradient-to-br from-blue-500/20 to-blue-600/10 rounded-xl border border-blue-500/30 hover:border-blue-400/50 transition-all group">
          <Key className="h-8 w-8 text-blue-400 mb-3 group-hover:scale-110 transition-transform" />
          <h3 className="font-semibold mb-1">Gestionar Cuentas</h3>
          <p className="text-sm text-muted-foreground">Agregar y configurar cuentas de Telegram</p>
        </a>
        <a href="/groups" className="block p-6 bg-gradient-to-br from-purple-500/20 to-purple-600/10 rounded-xl border border-purple-500/30 hover:border-purple-400/50 transition-all group">
          <FolderOpen className="h-8 w-8 text-purple-400 mb-3 group-hover:scale-110 transition-transform" />
          <h3 className="font-semibold mb-1">Grupos Monitoreados</h3>
          <p className="text-sm text-muted-foreground">Ver y gestionar grupos bajo vigilancia</p>
        </a>
        <a href="/detections" className="block p-6 bg-gradient-to-br from-red-500/20 to-red-600/10 rounded-xl border border-red-500/30 hover:border-red-400/50 transition-all group">
          <AlertTriangle className="h-8 w-8 text-red-400 mb-3 group-hover:scale-110 transition-transform" />
          <h3 className="font-semibold mb-1">Ver Detecciones</h3>
          <p className="text-sm text-muted-foreground">Analizar patrones detectados</p>
        </a>
      </div>
    </div>
  );
}
