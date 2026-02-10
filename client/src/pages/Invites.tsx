import { useEffect, useState } from 'react';
import { api } from '@/api/client';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Plus, Loader2, CheckCircle, XCircle, Clock, AlertCircle, Users, Radio, MessageSquare, Eye, RefreshCw, Trash2, Zap, UserPlus } from 'lucide-react';

interface Invite {
  id: number;
  link: string;
  invite_hash: string | null;
  status: string;
  retry_count: number;
  last_error: string | null;
  preview_title: string | null;
  preview_about: string | null;
  preview_member_count: number | null;
  preview_photo_path: string | null;
  preview_is_channel: boolean | null;
  source_group_title: string | null;
  source_user_name: string | null;
  created_at: string;
}

interface Account {
  id: number;
  phone: string;
  is_connected: boolean;
}

export function InvitesPage() {
  const [invites, setInvites] = useState<Invite[]>([]);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [loading, setLoading] = useState(true);
  const [newLink, setNewLink] = useState('');
  const [selectedAccount, setSelectedAccount] = useState<number | null>(null);
  const [fetchingPreview, setFetchingPreview] = useState<number | null>(null);
  const [joiningInvite, setJoiningInvite] = useState<number | null>(null);
  const [fetchingAllPreviews, setFetchingAllPreviews] = useState(false);

  const fetchInvites = async () => {
    try {
      const data = await api.getInvites();
      setInvites(data);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const fetchAccounts = async () => {
    try {
      const data = await api.get<Account[]>('/accounts/');
      const connected = data?.filter(a => a.is_connected) || [];
      setAccounts(connected);
      if (connected.length > 0 && !selectedAccount) {
        setSelectedAccount(connected[0].id);
      }
    } catch (err) {
      console.error(err);
    }
  };

  const fetchPreview = async (inviteId: number, link: string) => {
    if (!selectedAccount) {
      alert('Selecciona una cuenta primero');
      return;
    }
    setFetchingPreview(inviteId);
    try {
      await api.post(`/invites/${inviteId}/fetch-preview?account_id=${selectedAccount}`, {});
      fetchInvites();
    } catch (err) {
      console.error('Failed to fetch preview:', err);
    } finally {
      setFetchingPreview(null);
    }
  };

  const deleteInvite = async (inviteId: number) => {
    if (!confirm('Eliminar este link de invitacion?')) return;
    try {
      await api.delete(`/invites/${inviteId}`);
      fetchInvites();
    } catch (err) {
      console.error('Failed to delete invite:', err);
    }
  };

  const joinNow = async (inviteId: number) => {
    setJoiningInvite(inviteId);
    try {
      const result = await api.post<{status: string, error?: string}>(`/invites/${inviteId}/join-now`, {});
      if (result.status === 'joined') {
        alert('Se unio exitosamente al grupo/canal!');
      } else if (result.error) {
        alert(`Error: ${result.error}`);
      }
      fetchInvites();
    } catch (err: any) {
      console.error('Failed to join:', err);
      alert('Error al intentar unirse');
    } finally {
      setJoiningInvite(null);
    }
  };

  const fetchAllPreviews = async () => {
    setFetchingAllPreviews(true);
    try {
      const result = await api.post<{fetched?: number, failed?: number, message?: string}>('/invites/fetch-all-previews', {});
      alert(`Previews obtenidos: ${result.fetched || 0}, Fallidos: ${result.failed || 0}`);
      fetchInvites();
    } catch (err) {
      console.error('Failed to fetch previews:', err);
    } finally {
      setFetchingAllPreviews(false);
    }
  };

  useEffect(() => {
    fetchInvites();
    fetchAccounts();
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newLink.trim()) return;
    try {
      await api.createInvite(newLink.trim());
      setNewLink('');
      fetchInvites();
    } catch (err: any) {
      alert(err.message);
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'joined':
        return <CheckCircle className="h-5 w-5 text-green-500" />;
      case 'pending':
        return <Clock className="h-5 w-5 text-yellow-500" />;
      case 'processing':
        return <Loader2 className="h-5 w-5 text-blue-500 animate-spin" />;
      case 'failed':
        return <XCircle className="h-5 w-5 text-red-500" />;
      case 'expired':
        return <AlertCircle className="h-5 w-5 text-orange-500" />;
      case 'invalid':
        return <XCircle className="h-5 w-5 text-red-500" />;
      case 'already_joined':
        return <CheckCircle className="h-5 w-5 text-blue-500" />;
      case 'request_pending':
        return <UserPlus className="h-5 w-5 text-purple-500" />;
      case 'private':
        return <AlertCircle className="h-5 w-5 text-red-500" />;
      default:
        return <Clock className="h-5 w-5 text-muted-foreground" />;
    }
  };
  
  const getStatusLabel = (status: string) => {
    switch (status) {
      case 'joined': return 'Unido';
      case 'pending': return 'Pendiente';
      case 'processing': return 'Procesando';
      case 'failed': return 'Fallido';
      case 'expired': return 'Expirado';
      case 'invalid': return 'Invalido';
      case 'already_joined': return 'Ya miembro';
      case 'request_pending': return 'Esperando aprobacion';
      case 'private': return 'Privado';
      default: return status;
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
      <div>
        <h1 className="text-3xl font-bold">Links de Invitacion</h1>
        <p className="text-muted-foreground">Cola de invitaciones a grupos privados</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Agregar Link</CardTitle>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="flex gap-2">
            <Input
              value={newLink}
              onChange={(e) => setNewLink(e.target.value)}
              placeholder="https://t.me/+ABC123... o t.me/joinchat/..."
              className="flex-1"
            />
            <Button type="submit">
              <Plus className="mr-2 h-4 w-4" />
              Agregar
            </Button>
          </form>
          
          <div className="mt-3 flex items-center justify-between">
            {accounts.length > 0 && (
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">Cuenta para preview:</span>
                <select
                  className="bg-secondary border border-border rounded px-2 py-1 text-sm"
                  value={selectedAccount || ''}
                  onChange={(e) => setSelectedAccount(Number(e.target.value))}
                >
                  {accounts.map(acc => (
                    <option key={acc.id} value={acc.id}>{acc.phone}</option>
                  ))}
                </select>
              </div>
            )}
            <Button 
              variant="outline" 
              size="sm"
              onClick={fetchAllPreviews}
              disabled={fetchingAllPreviews}
            >
              {fetchingAllPreviews ? (
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
              ) : (
                <Eye className="h-4 w-4 mr-2" />
              )}
              Obtener Previews
            </Button>
          </div>
        </CardContent>
      </Card>

      <div className="space-y-3">
        {invites.map((invite) => (
          <Card key={invite.id} className="overflow-hidden">
            <CardContent className="p-4">
              <div className="flex gap-4">
                {invite.preview_photo_path ? (
                  <img
                    src={`/${invite.preview_photo_path}`}
                    alt={invite.preview_title || 'Preview'}
                    className="w-16 h-16 rounded-lg object-cover flex-shrink-0"
                    onError={(e) => {
                      (e.target as HTMLImageElement).style.display = 'none';
                    }}
                  />
                ) : (
                  <div className="w-16 h-16 rounded-lg bg-secondary flex items-center justify-center flex-shrink-0">
                    {invite.preview_is_channel ? (
                      <Radio className="w-6 h-6 text-muted-foreground" />
                    ) : (
                      <Users className="w-6 h-6 text-muted-foreground" />
                    )}
                  </div>
                )}
                
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    {getStatusIcon(invite.status)}
                    <h3 className="font-medium truncate">
                      {invite.preview_title || invite.link}
                    </h3>
                    {invite.preview_is_channel && (
                      <span className="text-xs bg-blue-500/20 text-blue-400 px-1.5 py-0.5 rounded">Canal</span>
                    )}
                  </div>
                  
                  {invite.preview_about && (
                    <p className="text-xs text-muted-foreground mt-1 line-clamp-2">{invite.preview_about}</p>
                  )}
                  
                  <div className="flex flex-wrap gap-x-3 gap-y-1 mt-2 text-xs text-muted-foreground">
                    {invite.preview_member_count && (
                      <span className="flex items-center gap-1">
                        <Users className="w-3 h-3" />
                        {invite.preview_member_count.toLocaleString()} miembros
                      </span>
                    )}
                    <span className={`${
                      invite.status === 'joined' ? 'text-green-400' : 
                      invite.status === 'pending' ? 'text-yellow-400' : 
                      invite.status === 'already_joined' ? 'text-blue-400' :
                      invite.status === 'request_pending' ? 'text-purple-400' :
                      invite.status === 'failed' || invite.status === 'expired' || invite.status === 'invalid' || invite.status === 'private' ? 'text-red-400' : ''
                    }`}>
                      {getStatusLabel(invite.status)}
                    </span>
                    {invite.retry_count > 0 && <span>Reintentos: {invite.retry_count}</span>}
                  </div>
                  
                  {(invite.source_group_title || invite.source_user_name) && (
                    <div className="flex gap-2 mt-2 text-xs">
                      {invite.source_group_title && (
                        <span className="text-green-400/70">
                          <MessageSquare className="w-3 h-3 inline mr-1" />
                          {invite.source_group_title}
                        </span>
                      )}
                      {invite.source_user_name && (
                        <span className="text-blue-400/70">De: {invite.source_user_name}</span>
                      )}
                    </div>
                  )}
                  
                  {invite.last_error && (
                    <p className="text-xs text-destructive mt-1 truncate">{invite.last_error}</p>
                  )}
                  
                  {!invite.preview_title && (
                    <p className="font-mono text-xs text-muted-foreground mt-1 truncate">{invite.link}</p>
                  )}
                </div>
                
                <div className="flex flex-col items-end gap-2 flex-shrink-0">
                  <div className="text-xs text-muted-foreground">
                    {new Date(invite.created_at).toLocaleDateString('es-ES')}
                  </div>
                  <div className="flex gap-1">
                    {(invite.status === 'pending' || invite.status === 'failed') && (
                      <Button
                        variant="default"
                        size="sm"
                        onClick={() => joinNow(invite.id)}
                        disabled={joiningInvite === invite.id}
                        className="h-7 px-2 bg-green-600 hover:bg-green-700"
                        title="Unirse ahora"
                      >
                        {joiningInvite === invite.id ? (
                          <Loader2 className="h-3 w-3 animate-spin" />
                        ) : (
                          <>
                            <UserPlus className="h-3 w-3 mr-1" />
                            <span className="text-xs">Unirse</span>
                          </>
                        )}
                      </Button>
                    )}
                    {!invite.preview_title && selectedAccount && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => fetchPreview(invite.id, invite.link)}
                        disabled={fetchingPreview === invite.id}
                        className="h-7 px-2"
                      >
                        {fetchingPreview === invite.id ? (
                          <Loader2 className="h-3 w-3 animate-spin" />
                        ) : (
                          <Eye className="h-3 w-3" />
                        )}
                      </Button>
                    )}
                    {invite.preview_title && selectedAccount && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => fetchPreview(invite.id, invite.link)}
                        disabled={fetchingPreview === invite.id}
                        className="h-7 px-2"
                        title="Actualizar preview"
                      >
                        {fetchingPreview === invite.id ? (
                          <Loader2 className="h-3 w-3 animate-spin" />
                        ) : (
                          <RefreshCw className="h-3 w-3" />
                        )}
                      </Button>
                    )}
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => deleteInvite(invite.id)}
                      className="h-7 px-2 text-destructive hover:text-destructive"
                    >
                      <Trash2 className="h-3 w-3" />
                    </Button>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {invites.length === 0 && (
        <Card>
          <CardContent className="py-10 text-center">
            <p className="text-muted-foreground">No hay links de invitacion</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
