import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '@/api/client';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Loader2, Users, MessageSquare, CheckCircle, XCircle, Pause, ChevronRight, UserSearch, Download } from 'lucide-react';

interface Group {
  id: number;
  telegram_id: number;
  title: string;
  username: string | null;
  group_type: string;
  status: string;
  member_count: number;
  messages_count: number;
  is_public: boolean;
  assigned_account_id: number | null;
}

export function GroupsPage() {
  const navigate = useNavigate();
  const [groups, setGroups] = useState<Group[]>([]);
  const [loading, setLoading] = useState(true);
  const [scrapingGroup, setScrapingGroup] = useState<number | null>(null);
  const [scrapingAll, setScrapingAll] = useState(false);

  useEffect(() => {
    fetchGroups();
  }, []);

  const fetchGroups = async () => {
    try {
      const data = await api.getGroups();
      setGroups(data);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleScrapeMembers = async (e: React.MouseEvent, group: Group) => {
    e.stopPropagation();
    if (!group.assigned_account_id) {
      alert('Este grupo no tiene cuenta asignada');
      return;
    }
    
    setScrapingGroup(group.id);
    try {
      await api.scrapeGroupMembers(group.assigned_account_id, group.id);
      setTimeout(() => {
        fetchGroups();
        setScrapingGroup(null);
      }, 3000);
    } catch (err: any) {
      console.error(err);
      alert(err.message || 'Error al scrapear miembros');
      setScrapingGroup(null);
    }
  };

  const handleScrapeAllMembers = async () => {
    const accountIds = [...new Set(groups.filter(g => g.assigned_account_id).map(g => g.assigned_account_id!))];
    if (accountIds.length === 0) {
      alert('No hay grupos con cuentas asignadas');
      return;
    }
    
    setScrapingAll(true);
    try {
      for (const accountId of accountIds) {
        await api.scrapeAllMembers(accountId);
      }
      setTimeout(() => {
        fetchGroups();
        setScrapingAll(false);
      }, 5000);
    } catch (err: any) {
      console.error(err);
      alert(err.message || 'Error al scrapear miembros');
      setScrapingAll(false);
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'active':
        return <CheckCircle className="h-5 w-5 text-green-500" />;
      case 'paused':
        return <Pause className="h-5 w-5 text-yellow-500" />;
      case 'backfilling':
        return <Loader2 className="h-5 w-5 text-blue-500 animate-spin" />;
      default:
        return <XCircle className="h-5 w-5 text-red-500" />;
    }
  };

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Grupos Monitoreados</h1>
          <p className="text-muted-foreground">Lista de grupos y canales bajo vigilancia</p>
        </div>
        <Button
          onClick={handleScrapeAllMembers}
          disabled={scrapingAll}
          className="gap-2"
        >
          {scrapingAll ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Download className="h-4 w-4" />
          )}
          {scrapingAll ? 'Scrapeando...' : 'Scrapear Todos los Miembros'}
        </Button>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {groups.map((group) => (
          <Card 
            key={group.id} 
            className="hover:border-primary/50 transition-colors cursor-pointer group/card"
            onClick={() => navigate(`/groups/${group.id}`)}
          >
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-lg truncate flex-1">{group.title}</CardTitle>
              <div className="flex items-center gap-2">
                {getStatusIcon(group.status)}
                <ChevronRight className="h-4 w-4 text-muted-foreground opacity-0 group-hover/card:opacity-100 transition-opacity" />
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-2 text-sm">
                {group.username && (
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Username</span>
                    <span>@{group.username}</span>
                  </div>
                )}
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Tipo</span>
                  <span className="capitalize">{group.group_type}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Miembros</span>
                  <span className="flex items-center gap-1">
                    <Users className="h-4 w-4" />
                    {group.member_count.toLocaleString()}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Mensajes</span>
                  <span className="flex items-center gap-1">
                    <MessageSquare className="h-4 w-4" />
                    {group.messages_count.toLocaleString()}
                  </span>
                </div>
                
                <div className="pt-2">
                  <Button
                    size="sm"
                    variant="outline"
                    className="w-full gap-2"
                    disabled={scrapingGroup === group.id || !group.assigned_account_id}
                    onClick={(e) => handleScrapeMembers(e, group)}
                  >
                    {scrapingGroup === group.id ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <UserSearch className="h-4 w-4" />
                    )}
                    {scrapingGroup === group.id ? 'Scrapeando...' : 'Scrapear Miembros'}
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {groups.length === 0 && (
        <Card>
          <CardContent className="py-10 text-center">
            <p className="text-muted-foreground">No hay grupos monitoreados</p>
            <p className="text-sm text-muted-foreground mt-2">
              Agrega cuentas de Telegram y links de invitacion para comenzar
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
