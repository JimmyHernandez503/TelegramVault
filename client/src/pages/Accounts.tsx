import { useEffect, useState } from 'react';
import { api } from '@/api/client';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { 
  Plus, 
  Trash2, 
  Loader2, 
  CheckCircle, 
  XCircle, 
  AlertCircle, 
  Phone, 
  Send, 
  List, 
  Users, 
  Radio,
  MessageSquare,
  Check,
  X
} from 'lucide-react';

interface AccountGroup {
  id: number;
  title: string;
  username: string | null;
  group_type: string;
  status: string;
  is_monitoring: boolean;
}

interface Account {
  id: number;
  phone: string;
  api_id: number;
  telegram_id: number | null;
  username: string | null;
  first_name: string | null;
  last_name: string | null;
  status: string;
  is_active: boolean;
  messages_collected: number;
  errors_count: number;
  proxy_type: string | null;
  proxy_host: string | null;
  last_activity: string | null;
  created_at: string;
  groups?: AccountGroup[];
}

interface Dialog {
  id: number;
  name: string;
  type: string;
  unread_count: number;
  message_count: number;
  username: string | null;
  member_count: number;
  is_megagroup: boolean;
  is_broadcast: boolean;
  photo_path: string | null;
}

interface ManagedDialog extends Dialog {
  is_monitored: boolean;
  group_id: number | null;
  backfill_enabled: boolean;
  download_media: boolean;
  ocr_enabled: boolean;
  status: string | null;
  owned_by_other_account: boolean;
  assigned_account_id: number | null;
}

type WizardStep = 'form' | 'code' | 'password' | 'dialogs' | 'done';

export function AccountsPage() {
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [loading, setLoading] = useState(true);
  const [showWizard, setShowWizard] = useState(false);
  const [wizardStep, setWizardStep] = useState<WizardStep>('form');
  const [selectedAccountId, setSelectedAccountId] = useState<number | null>(null);
  const [dialogs, setDialogs] = useState<Dialog[]>([]);
  const [selectedDialogs, setSelectedDialogs] = useState<Set<number>>(new Set());
  const [autoBackfill, setAutoBackfill] = useState(true);
  const [wizardLoading, setWizardLoading] = useState(false);
  const [verificationCode, setVerificationCode] = useState('');
  const [password2FA, setPassword2FA] = useState('');
  const [showManageGroups, setShowManageGroups] = useState(false);
  const [managedDialogs, setManagedDialogs] = useState<ManagedDialog[]>([]);
  const [managedLoading, setManagedLoading] = useState(false);
  
  const [formData, setFormData] = useState({
    phone: '',
    proxy_type: '',
    proxy_host: '',
    proxy_port: '',
    proxy_username: '',
    proxy_password: '',
  });

  const fetchAccounts = async () => {
    try {
      const data = await api.get<Account[]>('/accounts/with-groups');
      setAccounts(data);
    } catch (err) {
      console.error('Failed to fetch accounts with groups, falling back to basic:', err);
      try {
        const fallback = await api.getAccounts();
        setAccounts(fallback);
      } catch (e) {
        console.error(e);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAccounts();
  }, []);

  const startWizard = () => {
    setShowWizard(true);
    setWizardStep('form');
    setSelectedAccountId(null);
    setDialogs([]);
    setSelectedDialogs(new Set());
    setVerificationCode('');
    setPassword2FA('');
  };

  const closeWizard = () => {
    setShowWizard(false);
    setWizardStep('form');
    setSelectedAccountId(null);
    fetchAccounts();
  };

  const handleCreateAccount = async (e: React.FormEvent) => {
    e.preventDefault();
    setWizardLoading(true);
    try {
      const account = await api.createAccount({
        phone: formData.phone,
        proxy_type: formData.proxy_type || null,
        proxy_host: formData.proxy_host || null,
        proxy_port: formData.proxy_port ? parseInt(formData.proxy_port) : null,
        proxy_username: formData.proxy_username || null,
        proxy_password: formData.proxy_password || null,
      }) as Account;
      
      setSelectedAccountId(account.id);
      
      const connectResult = await api.post<{ success: boolean; status: string; error?: string }>(`/telegram/${account.id}/connect`);
      
      if (connectResult.success) {
        if (connectResult.status === 'code_required') {
          setWizardStep('code');
        } else if (connectResult.status === 'connected') {
          await loadDialogs(account.id);
          setWizardStep('dialogs');
        }
      } else {
        alert(connectResult.error || 'Error al conectar');
      }
    } catch (err: any) {
      alert(err.message);
    } finally {
      setWizardLoading(false);
    }
  };

  const handleVerifyCode = async () => {
    if (!selectedAccountId || !verificationCode) return;
    setWizardLoading(true);
    try {
      const result = await api.post<{ success: boolean; status: string; error?: string }>(
        `/telegram/${selectedAccountId}/verify?code=${verificationCode}${password2FA ? `&password=${password2FA}` : ''}`
      );
      
      if (result.success) {
        if (result.status === 'password_required') {
          setWizardStep('password');
        } else if (result.status === 'connected') {
          await loadDialogs(selectedAccountId);
          setWizardStep('dialogs');
        }
      } else {
        alert(result.error || 'Codigo invalido');
      }
    } catch (err: any) {
      alert(err.message);
    } finally {
      setWizardLoading(false);
    }
  };

  const handleVerify2FA = async () => {
    if (!selectedAccountId || !password2FA) return;
    setWizardLoading(true);
    try {
      const result = await api.post<{ success: boolean; status: string; error?: string }>(
        `/telegram/${selectedAccountId}/verify?code=${verificationCode}&password=${password2FA}`
      );
      
      if (result.success && result.status === 'connected') {
        await loadDialogs(selectedAccountId);
        setWizardStep('dialogs');
      } else {
        alert(result.error || 'Contraseña incorrecta');
      }
    } catch (err: any) {
      alert(err.message);
    } finally {
      setWizardLoading(false);
    }
  };

  const loadDialogs = async (accountId: number) => {
    try {
      const result = await api.get<{ dialogs: Dialog[] }>(`/telegram/${accountId}/dialogs`);
      const groupsAndChannels = result.dialogs.filter(d => d.type !== 'user');
      setDialogs(groupsAndChannels);
    } catch (err) {
      console.error('Error loading dialogs:', err);
    }
  };

  const toggleDialogSelection = (dialogId: number) => {
    const newSelected = new Set(selectedDialogs);
    if (newSelected.has(dialogId)) {
      newSelected.delete(dialogId);
    } else {
      newSelected.add(dialogId);
    }
    setSelectedDialogs(newSelected);
  };

  const selectAllDialogs = () => {
    if (selectedDialogs.size === dialogs.length) {
      setSelectedDialogs(new Set());
    } else {
      setSelectedDialogs(new Set(dialogs.map(d => d.id)));
    }
  };

  const handleAddDialogs = async () => {
    if (!selectedAccountId || selectedDialogs.size === 0) return;
    setWizardLoading(true);
    try {
      const result = await api.post<{ added: any[]; skipped: any[] }>(
        `/telegram/${selectedAccountId}/add-dialogs`,
        { dialog_ids: Array.from(selectedDialogs), auto_backfill: autoBackfill }
      );
      
      alert(`Agregados: ${result.added.length} grupos/canales\nOmitidos: ${result.skipped.length}`);
      setWizardStep('done');
    } catch (err: any) {
      alert(err.message);
    } finally {
      setWizardLoading(false);
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm('Estas seguro de eliminar esta cuenta?')) return;
    try {
      await api.deleteAccount(id);
      fetchAccounts();
    } catch (err: any) {
      alert(err.message);
    }
  };

  const connectExistingAccount = async (accountId: number) => {
    setSelectedAccountId(accountId);
    setShowWizard(true);
    setWizardLoading(true);
    try {
      const result = await api.post<{ success: boolean; status: string; error?: string }>(`/telegram/${accountId}/connect`);
      
      if (result.success) {
        if (result.status === 'code_required') {
          setWizardStep('code');
        } else if (result.status === 'connected') {
          await loadDialogs(accountId);
          setWizardStep('dialogs');
        }
      } else {
        alert(result.error || 'Error al conectar');
        setShowWizard(false);
      }
    } catch (err: any) {
      alert(err.message);
      setShowWizard(false);
    } finally {
      setWizardLoading(false);
    }
  };

  const openManageGroups = async (accountId: number) => {
    setSelectedAccountId(accountId);
    setShowManageGroups(true);
    setManagedLoading(true);
    setSelectedDialogs(new Set());
    try {
      const result = await api.get<{ dialogs: ManagedDialog[]; needs_auth: boolean; error?: string }>(
        `/telegram/${accountId}/managed-dialogs`
      );
      
      if (result.needs_auth) {
        alert('La cuenta necesita reconectarse. Use el boton Conectar.');
        setShowManageGroups(false);
        return;
      }
      
      setManagedDialogs(result.dialogs);
      const monitored = new Set(result.dialogs.filter(d => d.is_monitored).map(d => d.id));
      setSelectedDialogs(monitored);
    } catch (err: any) {
      alert(err.message);
      setShowManageGroups(false);
    } finally {
      setManagedLoading(false);
    }
  };

  const closeManageGroups = () => {
    setShowManageGroups(false);
    setManagedDialogs([]);
    setSelectedAccountId(null);
  };

  const handleSaveMonitoring = async () => {
    if (!selectedAccountId) return;
    setManagedLoading(true);
    try {
      const toAdd = Array.from(selectedDialogs).filter(id => {
        const d = managedDialogs.find(md => md.id === id);
        return d && !d.is_monitored;
      });
      
      if (toAdd.length > 0) {
        await api.post(`/telegram/${selectedAccountId}/add-dialogs`, {
          dialog_ids: toAdd,
          auto_backfill: autoBackfill
        });
      }
      
      for (const dialog of managedDialogs) {
        if (dialog.group_id) {
          const shouldMonitor = selectedDialogs.has(dialog.id);
          if (dialog.is_monitored !== shouldMonitor) {
            await api.patch(`/groups/${dialog.group_id}`, {
              status: shouldMonitor ? 'active' : 'paused'
            });
          }
        }
      }
      
      alert('Monitoreo actualizado correctamente');
      closeManageGroups();
      fetchAccounts();
    } catch (err: any) {
      alert(err.message);
    } finally {
      setManagedLoading(false);
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'connected':
      case 'active':
        return <CheckCircle className="h-5 w-5 text-green-500" />;
      case 'flood_wait':
        return <AlertCircle className="h-5 w-5 text-yellow-500" />;
      case 'banned':
        return <XCircle className="h-5 w-5 text-red-500" />;
      default:
        return <XCircle className="h-5 w-5 text-muted-foreground" />;
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
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Cuentas de Telegram</h1>
          <p className="text-muted-foreground">Gestiona las cuentas para recoleccion de datos</p>
        </div>
        <Button onClick={startWizard}>
          <Plus className="mr-2 h-4 w-4" />
          Agregar Cuenta
        </Button>
      </div>

      {showWizard && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <Card className="w-full max-w-2xl max-h-[90vh] overflow-auto">
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle>
                  {wizardStep === 'form' && 'Nueva Cuenta de Telegram'}
                  {wizardStep === 'code' && 'Verificacion de Codigo'}
                  {wizardStep === 'password' && 'Autenticacion 2FA'}
                  {wizardStep === 'dialogs' && 'Seleccionar Grupos y Canales'}
                  {wizardStep === 'done' && 'Configuracion Completada'}
                </CardTitle>
                <p className="text-sm text-muted-foreground mt-1">
                  {wizardStep === 'form' && 'Ingresa los datos de tu cuenta'}
                  {wizardStep === 'code' && 'Ingresa el codigo enviado a tu telefono'}
                  {wizardStep === 'password' && 'Ingresa tu contraseña de 2FA'}
                  {wizardStep === 'dialogs' && 'Selecciona los grupos y canales a monitorear'}
                  {wizardStep === 'done' && 'La cuenta ha sido configurada exitosamente'}
                </p>
              </div>
              <Button variant="ghost" size="sm" onClick={closeWizard}>
                <X className="h-4 w-4" />
              </Button>
            </CardHeader>
            <CardContent>
              {wizardStep === 'form' && (
                <form onSubmit={handleCreateAccount} className="space-y-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Telefono</label>
                    <Input
                      value={formData.phone}
                      onChange={(e) => setFormData({ ...formData, phone: e.target.value })}
                      placeholder="+1234567890"
                      required
                    />
                  </div>
                  <div className="border-t pt-4 mt-4">
                    <p className="text-sm font-medium mb-3">Configuracion de Proxy (Opcional)</p>
                    <div className="grid gap-4 md:grid-cols-3">
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Tipo Proxy</label>
                        <select
                          value={formData.proxy_type}
                          onChange={(e) => setFormData({ ...formData, proxy_type: e.target.value })}
                          className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                        >
                          <option value="">Sin proxy</option>
                          <option value="socks5">SOCKS5</option>
                          <option value="http">HTTP</option>
                        </select>
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Host Proxy</label>
                        <Input
                          value={formData.proxy_host}
                          onChange={(e) => setFormData({ ...formData, proxy_host: e.target.value })}
                          placeholder="proxy.example.com"
                        />
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Puerto</label>
                        <Input
                          type="number"
                          value={formData.proxy_port}
                          onChange={(e) => setFormData({ ...formData, proxy_port: e.target.value })}
                          placeholder="8080"
                        />
                      </div>
                    </div>
                    {formData.proxy_type && (
                      <div className="grid gap-4 md:grid-cols-2 mt-3">
                        <div className="space-y-2">
                          <label className="text-sm font-medium">Usuario Proxy</label>
                          <Input
                            value={formData.proxy_username}
                            onChange={(e) => setFormData({ ...formData, proxy_username: e.target.value })}
                            placeholder="Usuario (opcional)"
                          />
                        </div>
                        <div className="space-y-2">
                          <label className="text-sm font-medium">Password Proxy</label>
                          <Input
                            type="password"
                            value={formData.proxy_password}
                            onChange={(e) => setFormData({ ...formData, proxy_password: e.target.value })}
                            placeholder="Password (opcional)"
                          />
                        </div>
                      </div>
                    )}
                  </div>
                  <div className="flex gap-2 pt-4">
                    <Button type="submit" disabled={wizardLoading}>
                      {wizardLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Phone className="h-4 w-4 mr-2" />}
                      Conectar
                    </Button>
                    <Button type="button" variant="outline" onClick={closeWizard}>
                      Cancelar
                    </Button>
                  </div>
                </form>
              )}

              {wizardStep === 'code' && (
                <div className="space-y-4">
                  <div className="text-center py-4">
                    <Send className="h-12 w-12 mx-auto text-primary mb-4" />
                    <p>Se ha enviado un codigo de verificacion a tu telefono</p>
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Codigo de Verificacion</label>
                    <Input
                      value={verificationCode}
                      onChange={(e) => setVerificationCode(e.target.value)}
                      placeholder="12345"
                      className="text-center text-2xl tracking-widest"
                      maxLength={6}
                    />
                  </div>
                  <div className="flex gap-2 pt-4">
                    <Button onClick={handleVerifyCode} disabled={wizardLoading || !verificationCode}>
                      {wizardLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Check className="h-4 w-4 mr-2" />}
                      Verificar
                    </Button>
                    <Button variant="outline" onClick={closeWizard}>
                      Cancelar
                    </Button>
                  </div>
                </div>
              )}

              {wizardStep === 'password' && (
                <div className="space-y-4">
                  <div className="text-center py-4">
                    <AlertCircle className="h-12 w-12 mx-auto text-yellow-500 mb-4" />
                    <p>Esta cuenta tiene autenticacion de dos factores</p>
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Contraseña 2FA</label>
                    <Input
                      type="password"
                      value={password2FA}
                      onChange={(e) => setPassword2FA(e.target.value)}
                      placeholder="Tu contraseña de 2FA"
                    />
                  </div>
                  <div className="flex gap-2 pt-4">
                    <Button onClick={handleVerify2FA} disabled={wizardLoading || !password2FA}>
                      {wizardLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Check className="h-4 w-4 mr-2" />}
                      Continuar
                    </Button>
                    <Button variant="outline" onClick={closeWizard}>
                      Cancelar
                    </Button>
                  </div>
                </div>
              )}

              {wizardStep === 'dialogs' && (
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Button variant="outline" size="sm" onClick={selectAllDialogs}>
                        {selectedDialogs.size === dialogs.length ? 'Deseleccionar todo' : 'Seleccionar todo'}
                      </Button>
                      <span className="text-sm text-muted-foreground">
                        {selectedDialogs.size} de {dialogs.length} seleccionados
                      </span>
                    </div>
                    <label className="flex items-center gap-2 text-sm">
                      <input 
                        type="checkbox" 
                        checked={autoBackfill} 
                        onChange={(e) => setAutoBackfill(e.target.checked)}
                        className="rounded"
                      />
                      Backfill automatico
                    </label>
                  </div>

                  <div className="max-h-96 overflow-auto space-y-2 border rounded-lg p-2">
                    {dialogs.length === 0 ? (
                      <div className="text-center py-8 text-muted-foreground">
                        <List className="h-12 w-12 mx-auto mb-4 opacity-50" />
                        <p>No se encontraron grupos o canales</p>
                      </div>
                    ) : (
                      dialogs.map((dialog) => (
                        <div
                          key={dialog.id}
                          className={`flex items-center gap-3 p-3 rounded-lg cursor-pointer transition-all ${
                            selectedDialogs.has(dialog.id)
                              ? 'bg-primary/20 ring-2 ring-primary'
                              : 'bg-secondary/30 hover:bg-secondary/50'
                          }`}
                          onClick={() => toggleDialogSelection(dialog.id)}
                        >
                          <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center overflow-hidden">
                            {dialog.photo_path ? (
                              <img src={`/media/${dialog.photo_path}`} alt="" className="w-full h-full object-cover" />
                            ) : (
                              dialog.type === 'channel' ? (
                                <Radio className="w-5 h-5 text-muted-foreground" />
                              ) : (
                                <Users className="w-5 h-5 text-muted-foreground" />
                              )
                            )}
                          </div>
                          
                          <div className="flex-1 min-w-0">
                            <div className="font-medium truncate">{dialog.name}</div>
                            <div className="text-xs text-muted-foreground flex items-center gap-2">
                              {dialog.username && <span>@{dialog.username}</span>}
                              <span className="bg-secondary px-1.5 py-0.5 rounded">
                                {dialog.type === 'channel' ? 'Canal' : dialog.is_megagroup ? 'Supergrupo' : 'Grupo'}
                              </span>
                            </div>
                          </div>

                          <div className="flex items-center gap-4 text-sm text-muted-foreground">
                            <div className="flex items-center gap-1">
                              <Users className="w-4 h-4" />
                              {dialog.member_count}
                            </div>
                            <div className="flex items-center gap-1">
                              <MessageSquare className="w-4 h-4" />
                              {dialog.message_count}
                            </div>
                          </div>

                          <div className={`w-6 h-6 rounded-full border-2 flex items-center justify-center ${
                            selectedDialogs.has(dialog.id) ? 'bg-primary border-primary' : 'border-muted-foreground'
                          }`}>
                            {selectedDialogs.has(dialog.id) && <Check className="w-4 h-4 text-primary-foreground" />}
                          </div>
                        </div>
                      ))
                    )}
                  </div>

                  <div className="flex gap-2 pt-4">
                    <Button onClick={handleAddDialogs} disabled={wizardLoading || selectedDialogs.size === 0}>
                      {wizardLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Check className="h-4 w-4 mr-2" />}
                      Agregar {selectedDialogs.size} Seleccionados
                    </Button>
                    <Button variant="outline" onClick={closeWizard}>
                      Omitir
                    </Button>
                  </div>
                </div>
              )}

              {wizardStep === 'done' && (
                <div className="text-center py-8">
                  <CheckCircle className="h-16 w-16 mx-auto text-green-500 mb-4" />
                  <h3 className="text-xl font-bold mb-2">Cuenta Configurada</h3>
                  <p className="text-muted-foreground mb-6">
                    La cuenta ha sido agregada y los grupos seleccionados estan siendo monitoreados.
                    {autoBackfill && ' El backfill de mensajes se ha iniciado automaticamente.'}
                  </p>
                  <Button onClick={closeWizard}>
                    Cerrar
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {accounts.map((account) => (
          <Card key={account.id}>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <div>
                <CardTitle className="text-lg">{account.phone}</CardTitle>
                {account.username && (
                  <p className="text-sm text-muted-foreground">@{account.username}</p>
                )}
                {account.first_name && (
                  <p className="text-xs text-muted-foreground">
                    {account.first_name} {account.last_name || ''}
                  </p>
                )}
              </div>
              {getStatusIcon(account.status)}
            </CardHeader>
            <CardContent>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Estado</span>
                  <span className="capitalize">{account.status}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Mensajes</span>
                  <span>{account.messages_collected.toLocaleString()}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Errores</span>
                  <span>{account.errors_count}</span>
                </div>
                {account.proxy_host && (
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Proxy</span>
                    <span>{account.proxy_type}: {account.proxy_host}</span>
                  </div>
                )}
                {account.telegram_id && (
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Telegram ID</span>
                    <span className="font-mono text-xs">{account.telegram_id}</span>
                  </div>
                )}
                {account.groups && account.groups.length > 0 && (
                  <div className="pt-2 mt-2 border-t border-border">
                    <span className="text-muted-foreground flex items-center gap-1">
                      <Radio className="w-3 h-3" />
                      Monitoreando ({account.groups.length}):
                    </span>
                    <div className="mt-1 space-y-1">
                      {account.groups.slice(0, 3).map((g) => (
                        <div key={g.id} className="text-xs flex items-center gap-1">
                          <span className={`w-2 h-2 rounded-full ${g.is_monitoring ? 'bg-green-400' : 'bg-gray-400'}`}></span>
                          <span className="truncate">{g.title}</span>
                        </div>
                      ))}
                      {account.groups.length > 3 && (
                        <div className="text-xs text-muted-foreground">
                          +{account.groups.length - 3} mas...
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
              <div className="mt-4 flex gap-2">
                <Button variant="outline" size="sm" onClick={() => openManageGroups(account.id)}>
                  <List className="h-4 w-4 mr-1" />
                  Gestionar Grupos
                </Button>
                {account.status !== 'active' && account.status !== 'connected' && (
                  <Button variant="outline" size="sm" onClick={() => connectExistingAccount(account.id)}>
                    <Phone className="h-4 w-4 mr-1" />
                    Conectar
                  </Button>
                )}
                <Button variant="destructive" size="sm" onClick={() => handleDelete(account.id)}>
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {accounts.length === 0 && !showWizard && (
        <Card>
          <CardContent className="py-10 text-center">
            <Phone className="h-12 w-12 mx-auto mb-4 text-muted-foreground opacity-50" />
            <p className="text-muted-foreground">No hay cuentas configuradas</p>
            <Button className="mt-4" onClick={startWizard}>
              Agregar primera cuenta
            </Button>
          </CardContent>
        </Card>
      )}

      {showManageGroups && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <Card className="w-full max-w-3xl max-h-[90vh] overflow-auto">
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle>Gestionar Grupos y Canales</CardTitle>
                <p className="text-sm text-muted-foreground mt-1">
                  Selecciona los grupos que deseas monitorear
                </p>
              </div>
              <Button variant="ghost" size="sm" onClick={closeManageGroups}>
                <X className="h-4 w-4" />
              </Button>
            </CardHeader>
            <CardContent>
              {managedLoading ? (
                <div className="flex items-center justify-center py-12">
                  <Loader2 className="h-8 w-8 animate-spin text-primary" />
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        const selectable = managedDialogs.filter(d => !d.owned_by_other_account);
                        if (selectedDialogs.size === selectable.length) {
                          setSelectedDialogs(new Set());
                        } else {
                          setSelectedDialogs(new Set(selectable.map(d => d.id)));
                        }
                      }}
                    >
                      {selectedDialogs.size === managedDialogs.filter(d => !d.owned_by_other_account).length ? 'Deseleccionar todo' : 'Seleccionar todo'}
                    </Button>
                    <span className="text-sm text-muted-foreground">
                      {selectedDialogs.size} de {managedDialogs.length} seleccionados
                    </span>
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={autoBackfill}
                        onChange={(e) => setAutoBackfill(e.target.checked)}
                        className="rounded"
                      />
                      Backfill automatico
                    </label>
                  </div>

                  <div className="space-y-2 max-h-[50vh] overflow-y-auto">
                    {managedDialogs.map((dialog) => (
                      <div
                        key={dialog.id}
                        onClick={() => !dialog.owned_by_other_account && toggleDialogSelection(dialog.id)}
                        className={`flex items-center gap-4 p-3 rounded-lg transition-all ${
                          dialog.owned_by_other_account
                            ? 'bg-yellow-500/10 border border-yellow-500/30 opacity-60 cursor-not-allowed'
                            : selectedDialogs.has(dialog.id)
                              ? 'bg-primary/20 border border-primary cursor-pointer'
                              : 'bg-secondary/30 hover:bg-secondary/50 border border-transparent cursor-pointer'
                        }`}
                      >
                        <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center overflow-hidden">
                          {dialog.photo_path ? (
                            <img src={`/media/${dialog.photo_path}`} alt="" className="w-full h-full object-cover" />
                          ) : (
                            <Users className="w-5 h-5 text-muted-foreground" />
                          )}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="font-medium truncate">{dialog.name}</span>
                            {dialog.is_monitored && (
                              <span className="text-xs px-2 py-0.5 rounded bg-green-500/20 text-green-400">
                                Activo
                              </span>
                            )}
                            {dialog.owned_by_other_account && (
                              <span className="text-xs px-2 py-0.5 rounded bg-yellow-500/20 text-yellow-400">
                                Otra cuenta
                              </span>
                            )}
                            <span className="text-xs px-2 py-0.5 rounded bg-secondary text-muted-foreground">
                              {dialog.type}
                            </span>
                          </div>
                          {dialog.username && (
                            <span className="text-xs text-muted-foreground">@{dialog.username}</span>
                          )}
                        </div>
                        <div className="flex items-center gap-4 text-sm text-muted-foreground">
                          <div className="flex items-center gap-1">
                            <Users className="w-4 h-4" />
                            <span>{dialog.member_count}</span>
                          </div>
                          <div className="flex items-center gap-1">
                            <MessageSquare className="w-4 h-4" />
                            <span>{dialog.message_count}</span>
                          </div>
                        </div>
                        {!dialog.owned_by_other_account && (
                          <div className={`w-5 h-5 rounded border flex items-center justify-center ${
                            selectedDialogs.has(dialog.id)
                              ? 'bg-primary border-primary'
                              : 'border-muted-foreground'
                          }`}>
                            {selectedDialogs.has(dialog.id) && <Check className="w-4 h-4 text-primary-foreground" />}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>

                  <div className="flex gap-2 pt-4 border-t">
                    <Button onClick={handleSaveMonitoring} disabled={managedLoading}>
                      {managedLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Check className="h-4 w-4 mr-2" />}
                      Guardar Cambios
                    </Button>
                    <Button variant="outline" onClick={closeManageGroups}>
                      Cancelar
                    </Button>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
