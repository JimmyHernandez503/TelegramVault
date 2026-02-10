import { useState, useEffect } from 'react';
import { api } from '@/api/client';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Link } from 'react-router-dom';
import {
  Radio,
  Play,
  Pause,
  AlertCircle,
  Loader2,
  RefreshCw,
  Users,
  MessageSquare,
  Image,
  Download,
  Activity,
  X,
  User
} from 'lucide-react';

interface AssignedAccount {
  id: number;
  phone: string;
  username: string | null;
  status: string;
}

interface MonitoredGroup {
  id: number;
  telegram_id: number;
  title: string;
  username: string | null;
  group_type: string;
  status: string;
  member_count: number;
  messages_count: number;
  photo_path: string | null;
  backfill_enabled: boolean;
  download_media: boolean;
  ocr_enabled: boolean;
  assigned_account_id: number | null;
  assigned_account: AssignedAccount | null;
}

interface TelegramAccount {
  id: number;
  phone: string;
  username: string | null;
  status: string;
}

interface MonitoringStatus {
  total: number;
  active: number;
  paused: number;
  backfilling: number;
  error: number;
  groups: MonitoredGroup[];
}

export default function MonitoringPage() {
  const [status, setStatus] = useState<MonitoringStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<string | null>(null);
  const [runningBackfill, setRunningBackfill] = useState<number | null>(null);
  const [runningScrape, setRunningScrape] = useState<number | null>(null);
  const [showMembersModal, setShowMembersModal] = useState<number | null>(null);
  const [members, setMembers] = useState<any[]>([]);
  const [membersLoading, setMembersLoading] = useState(false);
  const [accounts, setAccounts] = useState<TelegramAccount[]>([]);

  useEffect(() => {
    loadStatus();
    loadAccounts();
    const interval = setInterval(loadStatus, 10000);
    return () => clearInterval(interval);
  }, []);

  const loadAccounts = async () => {
    try {
      const data = await api.get<TelegramAccount[]>('/accounts/');
      setAccounts(data);
    } catch (error) {
      console.error('Failed to load accounts:', error);
    }
  };

  const loadStatus = async () => {
    try {
      const data = await api.get<MonitoringStatus>('/groups/monitoring/status');
      setStatus(data);
    } catch (error) {
      console.error('Failed to load monitoring status:', error);
    } finally {
      setLoading(false);
    }
  };

  const toggleMonitoring = async (groupId: number) => {
    try {
      await api.post(`/groups/${groupId}/toggle-monitoring`);
      loadStatus();
    } catch (error) {
      console.error('Failed to toggle monitoring:', error);
    }
  };

  const runBackfill = async (groupId: number) => {
    setRunningBackfill(groupId);
    try {
      await api.post(`/groups/${groupId}/start-backfill`);
      loadStatus();
    } catch (error) {
      console.error('Failed to start backfill:', error);
    } finally {
      setRunningBackfill(null);
    }
  };

  const scrapeMembers = async (groupId: number) => {
    setRunningScrape(groupId);
    try {
      const result = await api.post<{ members_scraped: number }>(`/groups/${groupId}/scrape-members`);
      alert(`Miembros escaneados: ${result.members_scraped}`);
      loadStatus();
    } catch (error) {
      console.error('Failed to scrape members:', error);
    } finally {
      setRunningScrape(null);
    }
  };

  const loadMembers = async (groupId: number) => {
    setShowMembersModal(groupId);
    setMembersLoading(true);
    try {
      const result = await api.get<{ members: any[], total: number }>(`/groups/${groupId}/members?limit=100`);
      setMembers(result.members);
    } catch (error) {
      console.error('Failed to load members:', error);
    } finally {
      setMembersLoading(false);
    }
  };

  const assignAccount = async (groupId: number, accountId: number | null) => {
    try {
      await api.post(`/groups/${groupId}/assign-account?account_id=${accountId || 0}`);
      loadStatus();
    } catch (error) {
      console.error('Failed to assign account:', error);
    }
  };

  const getStatusIcon = (s: string) => {
    switch (s) {
      case 'active': return <Play className="w-4 h-4 text-green-400" />;
      case 'paused': return <Pause className="w-4 h-4 text-yellow-400" />;
      case 'backfilling': return <Loader2 className="w-4 h-4 text-blue-400 animate-spin" />;
      case 'error': return <AlertCircle className="w-4 h-4 text-red-400" />;
      default: return <Radio className="w-4 h-4" />;
    }
  };

  const getStatusColor = (s: string) => {
    switch (s) {
      case 'active': return 'bg-green-500/20 text-green-400';
      case 'paused': return 'bg-yellow-500/20 text-yellow-400';
      case 'backfilling': return 'bg-blue-500/20 text-blue-400';
      case 'error': return 'bg-red-500/20 text-red-400';
      default: return 'bg-gray-500/20 text-gray-400';
    }
  };

  const filteredGroups = status?.groups.filter(g => !filter || g.status === filter) || [];

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
            <Activity className="h-8 w-8 text-primary" />
            Panel de Monitoreo
          </h1>
          <p className="text-muted-foreground mt-1">Estado de monitoreo activo de grupos y canales</p>
        </div>
        <Button variant="outline" onClick={loadStatus}>
          <RefreshCw className="w-4 h-4 mr-2" />
          Actualizar
        </Button>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-5">
        <Card 
          className={`cursor-pointer transition-all ${filter === null ? 'ring-2 ring-primary' : 'hover:bg-secondary/50'}`}
          onClick={() => setFilter(null)}
        >
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Total</p>
                <p className="text-3xl font-bold">{status?.total || 0}</p>
              </div>
              <Radio className="h-10 w-10 text-primary opacity-80" />
            </div>
          </CardContent>
        </Card>
        
        <Card 
          className={`cursor-pointer transition-all ${filter === 'active' ? 'ring-2 ring-green-400' : 'hover:bg-secondary/50'}`}
          onClick={() => setFilter(filter === 'active' ? null : 'active')}
        >
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Activos</p>
                <p className="text-3xl font-bold text-green-400">{status?.active || 0}</p>
              </div>
              <Play className="h-10 w-10 text-green-400 opacity-80" />
            </div>
          </CardContent>
        </Card>

        <Card 
          className={`cursor-pointer transition-all ${filter === 'paused' ? 'ring-2 ring-yellow-400' : 'hover:bg-secondary/50'}`}
          onClick={() => setFilter(filter === 'paused' ? null : 'paused')}
        >
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Pausados</p>
                <p className="text-3xl font-bold text-yellow-400">{status?.paused || 0}</p>
              </div>
              <Pause className="h-10 w-10 text-yellow-400 opacity-80" />
            </div>
          </CardContent>
        </Card>

        <Card 
          className={`cursor-pointer transition-all ${filter === 'backfilling' ? 'ring-2 ring-blue-400' : 'hover:bg-secondary/50'}`}
          onClick={() => setFilter(filter === 'backfilling' ? null : 'backfilling')}
        >
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Backfill</p>
                <p className="text-3xl font-bold text-blue-400">{status?.backfilling || 0}</p>
              </div>
              <Download className="h-10 w-10 text-blue-400 opacity-80" />
            </div>
          </CardContent>
        </Card>

        <Card 
          className={`cursor-pointer transition-all ${filter === 'error' ? 'ring-2 ring-red-400' : 'hover:bg-secondary/50'}`}
          onClick={() => setFilter(filter === 'error' ? null : 'error')}
        >
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Error</p>
                <p className="text-3xl font-bold text-red-400">{status?.error || 0}</p>
              </div>
              <AlertCircle className="h-10 w-10 text-red-400 opacity-80" />
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Grupos y Canales Monitoreados</CardTitle>
        </CardHeader>
        <CardContent>
          {filteredGroups.length === 0 ? (
            <div className="text-center py-12 text-muted-foreground">
              <Radio className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p>No hay grupos en monitoreo</p>
              <p className="text-sm mt-1">Agrega una cuenta de Telegram y selecciona grupos para monitorear</p>
            </div>
          ) : (
            <div className="space-y-3">
              {filteredGroups.map((group) => (
                <div
                  key={group.id}
                  className="flex items-center gap-4 p-4 rounded-lg bg-secondary/30 hover:bg-secondary/50 transition-all"
                >
                  <div className="w-12 h-12 rounded-lg bg-secondary flex items-center justify-center overflow-hidden">
                    {group.photo_path ? (
                      <img src={`/media/${group.photo_path}`} alt="" className="w-full h-full object-cover" />
                    ) : (
                      <Users className="w-6 h-6 text-muted-foreground" />
                    )}
                  </div>

                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="font-medium truncate">{group.title}</span>
                      <span className={`text-xs px-2 py-0.5 rounded ${getStatusColor(group.status)}`}>
                        {group.status}
                      </span>
                      <span className="text-xs text-muted-foreground bg-secondary px-2 py-0.5 rounded">
                        {group.group_type}
                      </span>
                    </div>
                    <div className="text-sm text-muted-foreground">
                      {group.username ? `@${group.username}` : `ID: ${group.telegram_id}`}
                    </div>
                  </div>

                  <div className="flex items-center gap-6 text-sm text-muted-foreground">
                    <div className="flex items-center gap-2">
                      <Users className="w-4 h-4" />
                      <span>{group.member_count}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <MessageSquare className="w-4 h-4" />
                      <span>{group.messages_count}</span>
                    </div>
                  </div>

                  <div className="flex items-center gap-2 text-muted-foreground">
                    {group.download_media && <span title="Descarga media"><Image className="w-4 h-4" /></span>}
                    {group.backfill_enabled && <span title="Backfill habilitado"><Download className="w-4 h-4" /></span>}
                  </div>

                  <div className="min-w-36">
                    <select
                      value={group.assigned_account_id || ''}
                      onChange={(e) => assignAccount(group.id, e.target.value ? parseInt(e.target.value) : null)}
                      className="w-full text-xs rounded-md border border-input bg-background px-2 py-1.5"
                    >
                      <option value="">Sin cuenta</option>
                      {accounts.map((acc) => (
                        <option key={acc.id} value={acc.id}>
                          {acc.phone} {acc.username ? `(@${acc.username})` : ''}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="flex items-center gap-1">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => runBackfill(group.id)}
                      disabled={runningBackfill === group.id || !group.assigned_account_id}
                      title="Backfill historial"
                    >
                      {runningBackfill === group.id ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                      ) : (
                        <Download className="w-4 h-4" />
                      )}
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => scrapeMembers(group.id)}
                      disabled={runningScrape === group.id || !group.assigned_account_id}
                      title="Escanear miembros"
                    >
                      {runningScrape === group.id ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                      ) : (
                        <Users className="w-4 h-4" />
                      )}
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => loadMembers(group.id)}
                      title="Ver miembros"
                    >
                      <User className="w-4 h-4" />
                    </Button>
                    <Button
                      variant={group.status === 'active' ? 'default' : 'outline'}
                      size="sm"
                      onClick={() => toggleMonitoring(group.id)}
                    >
                      {group.status === 'active' ? (
                        <>
                          <Pause className="w-4 h-4 mr-1" />
                          Pausar
                        </>
                      ) : (
                        <>
                          <Play className="w-4 h-4 mr-1" />
                          Activar
                        </>
                      )}
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {showMembersModal !== null && (
        <div className="fixed inset-0 bg-black/80 flex items-center justify-center z-50" onClick={() => setShowMembersModal(null)}>
          <div className="bg-background border rounded-lg w-full max-w-2xl max-h-[80vh] overflow-hidden" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between p-4 border-b">
              <h2 className="text-lg font-semibold flex items-center gap-2">
                <Users className="w-5 h-5" />
                Miembros del Grupo
              </h2>
              <Button variant="ghost" size="sm" onClick={() => setShowMembersModal(null)}>
                <X className="w-4 h-4" />
              </Button>
            </div>
            <div className="p-4 overflow-auto max-h-[calc(80vh-80px)]">
              {membersLoading ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="w-8 h-8 animate-spin text-primary" />
                </div>
              ) : members.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  <Users className="w-12 h-12 mx-auto mb-4 opacity-50" />
                  <p>No hay miembros registrados</p>
                  <p className="text-sm">Usa el boton de escanear miembros para obtenerlos</p>
                </div>
              ) : (
                <div className="space-y-2">
                  {members.map((member) => (
                    <Link
                      key={member.id}
                      to={`/users/${member.id}`}
                      className="flex items-center gap-3 p-3 rounded-lg bg-secondary/30 hover:bg-secondary/50 transition-all cursor-pointer"
                      onClick={() => setShowMembersModal(null)}
                    >
                      <div className="w-10 h-10 rounded-full bg-primary/20 flex items-center justify-center overflow-hidden flex-shrink-0">
                        {member.photo_path ? (
                          <img 
                            src={`/${member.photo_path}`} 
                            alt="" 
                            className="w-full h-full object-cover"
                            onError={(e) => {
                              (e.target as HTMLImageElement).style.display = 'none';
                            }}
                          />
                        ) : (
                          <User className="w-5 h-5 text-primary" />
                        )}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="font-medium truncate">
                            {member.first_name || ''} {member.last_name || ''}
                          </span>
                          {member.is_premium && (
                            <span className="text-xs bg-yellow-500/20 text-yellow-400 px-1.5 py-0.5 rounded">Premium</span>
                          )}
                          {member.is_bot && (
                            <span className="text-xs bg-blue-500/20 text-blue-400 px-1.5 py-0.5 rounded">Bot</span>
                          )}
                          {member.is_watchlist && (
                            <span className="text-xs bg-red-500/20 text-red-400 px-1.5 py-0.5 rounded">Watchlist</span>
                          )}
                        </div>
                        <div className="text-sm text-muted-foreground">
                          {member.username ? `@${member.username}` : `ID: ${member.telegram_id}`}
                        </div>
                      </div>
                      <div className="text-sm text-muted-foreground flex items-center gap-1">
                        <MessageSquare className="w-4 h-4" />
                        {member.messages_count}
                      </div>
                    </Link>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
