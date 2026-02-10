import { useState, useEffect } from 'react';
import { Save, Key, Scan, Download, Image, Clock, Settings as SettingsIcon, Search, Plus, Trash2, RefreshCw, Users, Play, Camera, Link, Zap, Globe } from 'lucide-react';
import { api } from '@/api/client';

interface ConfigItem {
  value: string;
  type: string;
  label: string;
  category: string;
}

interface Category {
  name: string;
  icon: string;
}

const iconMap: Record<string, any> = {
  key: Key,
  scan: Scan,
  download: Download,
  image: Image,
  clock: Clock,
  settings: SettingsIcon,
  search: Search,
  users: Users,
};

interface ScrapeGroup {
  id: number;
  title: string;
  member_count: number;
  is_monitoring: boolean;
  last_member_scrape_at: string | null;
  group_type: string;
}

interface AutoJoinConfig {
  enabled: boolean;
  mode: 'rotation' | 'specific';
  delay_minutes: number;
  enabled_accounts: number[];
  auto_backfill: boolean;
  auto_scrape_members: boolean;
  auto_monitor: boolean;
  auto_stories: boolean;
  max_joins_per_day: number;
}

interface Account {
  id: number;
  phone: string;
  status: string;
}

export default function SettingsPage() {
  const [configs, setConfigs] = useState<Record<string, ConfigItem>>({});
  const [categories, setCategories] = useState<Record<string, Category>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{type: 'success' | 'error', text: string} | null>(null);
  const [domains, setDomains] = useState<any[]>([]);
  const [newDomain, setNewDomain] = useState('');
  const [scrapeInterval, setScrapeInterval] = useState(24);
  const [scrapeGroups, setScrapeGroups] = useState<ScrapeGroup[]>([]);
  const [selectedGroups, setSelectedGroups] = useState<number[]>([]);
  const [scrapingAll, setScrapingAll] = useState(false);
  const [scrapingSelected, setScrapingSelected] = useState(false);
  const [storyInterval, setStoryInterval] = useState(1);
  const [storyBatchSize, setStoryBatchSize] = useState(100);
  const [storyParallelWorkers, setStoryParallelWorkers] = useState(5);
  const [downloadingStories, setDownloadingStories] = useState(false);
  const [autoJoinConfig, setAutoJoinConfig] = useState<AutoJoinConfig>({
    enabled: false,
    mode: 'rotation',
    delay_minutes: 5,
    enabled_accounts: [],
    auto_backfill: true,
    auto_scrape_members: true,
    auto_monitor: true,
    auto_stories: true,
    max_joins_per_day: 20
  });
  const [autoJoinStats, setAutoJoinStats] = useState<any>({});
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [savingAutoJoin, setSavingAutoJoin] = useState(false);
  const [photoScanInterval, setPhotoScanInterval] = useState(24);
  const [photoScanBatchSize, setPhotoScanBatchSize] = useState(50);
  const [photoScanWorkers, setPhotoScanWorkers] = useState(3);
  const [photoScanEnabled, setPhotoScanEnabled] = useState(true);
  const [scanningPhotos, setScanningPhotos] = useState(false);
  const [photoScanStatus, setPhotoScanStatus] = useState<any>({});
  const [crawlerEnabled, setCrawlerEnabled] = useState(false);
  const [crawlerPort, setCrawlerPort] = useState(8080);
  const [crawlerStatus, setCrawlerStatus] = useState<any>({});

  useEffect(() => {
    loadSettings();
    loadDomains();
    loadMemberScrapeSettings();
    loadStorySettings();
    loadAutoJoinConfig();
    loadAccounts();
    loadPhotoScanSettings();
    loadCrawlerSettings();
  }, []);

  const loadSettings = async () => {
    try {
      const response = await api.getSettings();
      setConfigs(response.configs);
      setCategories(response.categories);
    } catch (error) {
      console.error('Error loading settings:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadDomains = async () => {
    try {
      const response = await api.getDomainWatchlist();
      setDomains(response.domains);
    } catch (error) {
      console.error('Error loading domains:', error);
    }
  };

  const loadMemberScrapeSettings = async () => {
    try {
      const [settingsRes, groupsRes] = await Promise.all([
        api.get<{interval_hours: number}>('/member-scrape/settings'),
        api.get<ScrapeGroup[]>('/member-scrape/groups')
      ]);
      setScrapeInterval(settingsRes.interval_hours);
      setScrapeGroups(groupsRes);
    } catch (error) {
      console.error('Error loading member scrape settings:', error);
    }
  };

  const saveScrapeInterval = async () => {
    try {
      await api.put('/member-scrape/settings', { interval_hours: scrapeInterval });
      setMessage({ type: 'success', text: `Intervalo de scraping establecido a ${scrapeInterval} horas` });
      setTimeout(() => setMessage(null), 3000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al guardar intervalo' });
    }
  };

  const scrapeAllNow = async () => {
    setScrapingAll(true);
    try {
      await api.post('/member-scrape/scrape-all', {});
      setMessage({ type: 'success', text: 'Scraping de todos los grupos iniciado' });
      setTimeout(() => setMessage(null), 3000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al iniciar scraping' });
    } finally {
      setScrapingAll(false);
    }
  };

  const scrapeSelectedNow = async () => {
    if (selectedGroups.length === 0) return;
    setScrapingSelected(true);
    try {
      await api.post('/member-scrape/scrape-now', { group_ids: selectedGroups });
      setMessage({ type: 'success', text: `Scraping iniciado para ${selectedGroups.length} grupos` });
      setSelectedGroups([]);
      setTimeout(() => setMessage(null), 3000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al iniciar scraping' });
    } finally {
      setScrapingSelected(false);
    }
  };

  const toggleGroupSelection = (id: number) => {
    setSelectedGroups(prev => 
      prev.includes(id) ? prev.filter(g => g !== id) : [...prev, id]
    );
  };

  const selectAllGroups = () => {
    if (selectedGroups.length === scrapeGroups.length) {
      setSelectedGroups([]);
    } else {
      setSelectedGroups(scrapeGroups.map(g => g.id));
    }
  };

  const loadStorySettings = async () => {
    try {
      const res = await api.get<{interval_hours: number, batch_size: number, parallel_workers: number}>('/stories/settings');
      setStoryInterval(res.interval_hours);
      setStoryBatchSize(res.batch_size || 100);
      setStoryParallelWorkers(res.parallel_workers || 5);
    } catch (error) {
      console.error('Error loading story settings:', error);
    }
  };

  const saveStorySettings = async () => {
    try {
      await api.put('/stories/settings', { 
        interval_hours: storyInterval, 
        batch_size: storyBatchSize,
        parallel_workers: storyParallelWorkers
      });
      setMessage({ type: 'success', text: `TURBO config guardada: ${storyBatchSize} batch, ${storyParallelWorkers} workers` });
      setTimeout(() => setMessage(null), 3000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al guardar configuracion de stories' });
    }
  };

  const downloadStoriesNow = async () => {
    setDownloadingStories(true);
    try {
      await api.post('/stories/download-now', {});
      setMessage({ type: 'success', text: 'Descarga de stories iniciada' });
      setTimeout(() => setMessage(null), 3000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al iniciar descarga de stories' });
    } finally {
      setDownloadingStories(false);
    }
  };

  const loadPhotoScanSettings = async () => {
    try {
      const [settingsRes, statusRes] = await Promise.all([
        api.get<{interval_hours: number, batch_size: number, parallel_workers: number, enabled: boolean}>('/profile-photos/settings'),
        api.get<any>('/profile-photos/status')
      ]);
      setPhotoScanInterval(settingsRes.interval_hours);
      setPhotoScanBatchSize(settingsRes.batch_size);
      setPhotoScanWorkers(settingsRes.parallel_workers);
      setPhotoScanEnabled(settingsRes.enabled);
      setPhotoScanStatus(statusRes);
    } catch (error) {
      console.error('Error loading photo scan settings:', error);
    }
  };

  const savePhotoScanSettings = async () => {
    try {
      await api.put('/profile-photos/settings', {
        interval_hours: photoScanInterval,
        batch_size: photoScanBatchSize,
        parallel_workers: photoScanWorkers,
        enabled: photoScanEnabled
      });
      setMessage({ type: 'success', text: 'Configuracion de escaneo de fotos guardada' });
      setTimeout(() => setMessage(null), 3000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al guardar configuracion' });
    }
  };

  const scanPhotosNow = async () => {
    setScanningPhotos(true);
    try {
      await api.post('/profile-photos/scan-now', {});
      setMessage({ type: 'success', text: 'Escaneo de fotos de perfil iniciado' });
      setTimeout(() => setMessage(null), 3000);
      setTimeout(() => loadPhotoScanSettings(), 2000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al iniciar escaneo' });
    } finally {
      setScanningPhotos(false);
    }
  };

  const loadCrawlerSettings = async () => {
    try {
      const [settingsRes, statusRes] = await Promise.all([
        api.get<{enabled: boolean, port: number}>('/crawler/settings'),
        api.get<any>('/crawler/status')
      ]);
      setCrawlerEnabled(settingsRes.enabled);
      setCrawlerPort(settingsRes.port);
      setCrawlerStatus(statusRes);
    } catch (error) {
      console.error('Error loading crawler settings:', error);
    }
  };

  const saveCrawlerSettings = async () => {
    try {
      await api.put('/crawler/settings', {
        enabled: crawlerEnabled,
        port: crawlerPort
      });
      setMessage({ type: 'success', text: 'Configuracion del servidor de crawler guardada. Reinicia el servidor para aplicar cambios.' });
      setTimeout(() => setMessage(null), 5000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al guardar configuracion' });
    }
  };

  const loadAutoJoinConfig = async () => {
    try {
      const res = await api.get<any>('/invites/autojoin/config');
      if (res.config) {
        setAutoJoinConfig(res.config);
      }
      setAutoJoinStats(res);
    } catch (error) {
      console.error('Error loading autojoin config:', error);
    }
  };

  const loadAccounts = async () => {
    try {
      const res = await api.getAccounts();
      setAccounts(res.filter((a: Account) => a.status === 'active'));
    } catch (error) {
      console.error('Error loading accounts:', error);
    }
  };

  const saveAutoJoinConfig = async () => {
    setSavingAutoJoin(true);
    try {
      await api.put('/invites/autojoin/config', autoJoinConfig);
      setMessage({ type: 'success', text: 'Configuracion de AutoJoin guardada' });
      setTimeout(() => setMessage(null), 3000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al guardar configuracion' });
    } finally {
      setSavingAutoJoin(false);
    }
  };

  const toggleAutoJoinAccount = (accountId: number) => {
    setAutoJoinConfig(prev => {
      const current = prev.enabled_accounts || [];
      if (current.includes(accountId)) {
        return { ...prev, enabled_accounts: current.filter(id => id !== accountId) };
      } else {
        return { ...prev, enabled_accounts: [...current, accountId] };
      }
    });
  };

  const handleChange = (key: string, value: string) => {
    setConfigs(prev => ({
      ...prev,
      [key]: { ...prev[key], value }
    }));
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const configsToSave: Record<string, any> = {};
      Object.entries(configs).forEach(([key, item]) => {
        if (item.type === 'bool') {
          configsToSave[key] = item.value === 'true';
        } else if (item.type === 'int') {
          configsToSave[key] = parseInt(item.value) || 0;
        } else {
          configsToSave[key] = item.value;
        }
      });

      await api.updateSettings(configsToSave);
      setMessage({ type: 'success', text: 'Configuracion guardada correctamente' });
      setTimeout(() => setMessage(null), 3000);
    } catch (error) {
      setMessage({ type: 'error', text: 'Error al guardar configuracion' });
    } finally {
      setSaving(false);
    }
  };

  const addDomain = async () => {
    if (!newDomain.trim()) return;
    try {
      await api.addDomainToWatchlist(newDomain);
      setNewDomain('');
      loadDomains();
    } catch (error) {
      console.error('Error adding domain:', error);
    }
  };

  const removeDomain = async (id: number) => {
    try {
      await api.removeDomainFromWatchlist(id);
      loadDomains();
    } catch (error) {
      console.error('Error removing domain:', error);
    }
  };

  const renderInput = (key: string, item: ConfigItem) => {
    if (item.type === 'bool') {
      return (
        <label className="relative inline-flex items-center cursor-pointer">
          <input
            type="checkbox"
            checked={item.value === 'true'}
            onChange={(e) => handleChange(key, e.target.checked ? 'true' : 'false')}
            className="sr-only peer"
          />
          <div className="w-11 h-6 bg-gray-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary"></div>
        </label>
      );
    }

    if (key === 'rate_limit_mode') {
      return (
        <select
          value={item.value}
          onChange={(e) => handleChange(key, e.target.value)}
          className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-48"
        >
          <option value="aggressive">Agresivo</option>
          <option value="balanced">Balanceado</option>
          <option value="conservative">Conservador</option>
        </select>
      );
    }

    return (
      <input
        type={item.type === 'int' ? 'number' : 'text'}
        value={item.value}
        onChange={(e) => handleChange(key, e.target.value)}
        className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-64"
        placeholder={item.label}
      />
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="w-8 h-8 animate-spin text-primary" />
      </div>
    );
  }

  const groupedConfigs = Object.entries(configs).reduce((acc, [key, item]) => {
    const category = item.category || 'general';
    if (!acc[category]) acc[category] = [];
    acc[category].push({ key, ...item });
    return acc;
  }, {} as Record<string, any[]>);

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Configuracion</h1>
          <p className="text-gray-400 text-sm mt-1">Ajustes globales del sistema TelegramVault</p>
        </div>
        <button
          onClick={handleSave}
          disabled={saving}
          className="flex items-center gap-2 bg-primary hover:bg-primary/90 text-white px-4 py-2 rounded-lg disabled:opacity-50"
        >
          <Save className="w-4 h-4" />
          {saving ? 'Guardando...' : 'Guardar Cambios'}
        </button>
      </div>

      {message && (
        <div className={`p-4 rounded-lg ${message.type === 'success' ? 'bg-green-900/50 text-green-400' : 'bg-red-900/50 text-red-400'}`}>
          {message.text}
        </div>
      )}

      <div className="grid gap-6">
        {Object.entries(categories).map(([categoryKey, category]) => {
          const items = groupedConfigs[categoryKey] || [];
          if (items.length === 0) return null;

          const IconComponent = iconMap[category.icon] || SettingsIcon;

          return (
            <div key={categoryKey} className="bg-card border border-border rounded-lg p-6">
              <div className="flex items-center gap-3 mb-4">
                <div className="p-2 bg-primary/20 rounded-lg">
                  <IconComponent className="w-5 h-5 text-primary" />
                </div>
                <h2 className="text-lg font-semibold">{category.name}</h2>
              </div>

              <div className="space-y-4">
                {items.map((item: any) => (
                  <div key={item.key} className="flex items-center justify-between py-2 border-b border-gray-800 last:border-0">
                    <div>
                      <label className="text-sm font-medium">{item.label}</label>
                      <p className="text-xs text-gray-500">{item.key}</p>
                    </div>
                    {renderInput(item.key, item)}
                  </div>
                ))}
              </div>
            </div>
          );
        })}

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center gap-3 mb-4">
            <div className="p-2 bg-primary/20 rounded-lg">
              <Users className="w-5 h-5 text-primary" />
            </div>
            <h2 className="text-lg font-semibold">Scraping de Miembros</h2>
          </div>

          <p className="text-sm text-gray-400 mb-4">
            Configuracion del scraping automatico de miembros de grupos
          </p>

          <div className="space-y-4">
            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Intervalo de Scraping Automatico</label>
                <p className="text-xs text-gray-500">Cada cuantas horas scrapear miembros (0 = deshabilitado)</p>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="0"
                  max="168"
                  value={scrapeInterval}
                  onChange={(e) => setScrapeInterval(parseInt(e.target.value) || 0)}
                  className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-20"
                />
                <span className="text-sm text-gray-400">horas</span>
                <button
                  onClick={saveScrapeInterval}
                  className="bg-primary hover:bg-primary/90 px-3 py-2 rounded-lg text-sm"
                >
                  Guardar
                </button>
              </div>
            </div>

            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Scrapear Todos Ahora</label>
                <p className="text-xs text-gray-500">Ejecutar scraping de todos los grupos monitoreados</p>
              </div>
              <button
                onClick={scrapeAllNow}
                disabled={scrapingAll}
                className="flex items-center gap-2 bg-green-600 hover:bg-green-700 px-4 py-2 rounded-lg disabled:opacity-50"
              >
                <Play className="w-4 h-4" />
                {scrapingAll ? 'Iniciando...' : 'Scrapear Todos'}
              </button>
            </div>

            <div className="py-2">
              <div className="flex items-center justify-between mb-3">
                <div>
                  <label className="text-sm font-medium">Seleccionar Grupos para Scrapear</label>
                  <p className="text-xs text-gray-500">Elige grupos especificos para scrapear ahora</p>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={selectAllGroups}
                    className="text-sm text-primary hover:underline"
                  >
                    {selectedGroups.length === scrapeGroups.length ? 'Deseleccionar todos' : 'Seleccionar todos'}
                  </button>
                  <button
                    onClick={scrapeSelectedNow}
                    disabled={scrapingSelected || selectedGroups.length === 0}
                    className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 px-3 py-1.5 rounded-lg text-sm disabled:opacity-50"
                  >
                    <Play className="w-3 h-3" />
                    {scrapingSelected ? 'Iniciando...' : `Scrapear (${selectedGroups.length})`}
                  </button>
                </div>
              </div>

              <div className="max-h-60 overflow-y-auto space-y-1">
                {scrapeGroups.map((group) => (
                  <label
                    key={group.id}
                    className={`flex items-center justify-between p-2 rounded cursor-pointer hover:bg-gray-800/50 ${
                      selectedGroups.includes(group.id) ? 'bg-primary/20 border border-primary/50' : 'bg-gray-800/30'
                    }`}
                  >
                    <div className="flex items-center gap-3">
                      <input
                        type="checkbox"
                        checked={selectedGroups.includes(group.id)}
                        onChange={() => toggleGroupSelection(group.id)}
                        className="w-4 h-4 rounded border-gray-700 bg-gray-800 text-primary focus:ring-primary"
                      />
                      <div>
                        <span className="text-sm font-medium">{group.title}</span>
                        <span className="text-xs text-gray-500 ml-2">({group.member_count} miembros)</span>
                      </div>
                    </div>
                    <div className="text-xs text-gray-500">
                      {group.last_member_scrape_at 
                        ? `Ultimo: ${new Date(group.last_member_scrape_at).toLocaleDateString()}`
                        : 'Nunca scrapeado'
                      }
                    </div>
                  </label>
                ))}
                {scrapeGroups.length === 0 && (
                  <p className="text-gray-500 text-sm text-center py-4">No hay grupos disponibles para scrapear</p>
                )}
              </div>
              <div className="mt-3 p-3 bg-amber-500/10 border border-amber-500/30 rounded-lg">
                <p className="text-xs text-amber-400">
                  <strong>Nota:</strong> Solo se muestran grupos y supergrupos. Los canales de Telegram no permiten listar miembros por limitaciones de la API.
                </p>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center gap-3 mb-4">
            <div className="p-2 bg-pink-500/20 rounded-lg">
              <Camera className="w-5 h-5 text-pink-500" />
            </div>
            <h2 className="text-lg font-semibold">Monitoreo de Stories</h2>
          </div>

          <p className="text-sm text-gray-400 mb-4">
            Configuracion del monitoreo y descarga automatica de stories
          </p>

          <div className="space-y-4">
            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Intervalo de Escaneo Masivo</label>
                <p className="text-xs text-gray-500">Cada cuantas horas escanear TODOS los usuarios (0 = deshabilitado)</p>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="0"
                  max="24"
                  step="0.5"
                  value={storyInterval}
                  onChange={(e) => setStoryInterval(parseFloat(e.target.value) || 0)}
                  className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-20"
                />
                <span className="text-sm text-gray-400">horas</span>
              </div>
            </div>

            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Tamano de Batch</label>
                <p className="text-xs text-gray-500">Usuarios por lote (recomendado: 100-200)</p>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="50"
                  max="500"
                  step="50"
                  value={storyBatchSize}
                  onChange={(e) => setStoryBatchSize(parseInt(e.target.value) || 100)}
                  className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-20"
                />
                <span className="text-sm text-gray-400">usuarios</span>
              </div>
            </div>

            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Workers Paralelos (TURBO)</label>
                <p className="text-xs text-gray-500">Cuantos usuarios procesar simultaneamente (mas = mas rapido)</p>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="1"
                  max="20"
                  step="1"
                  value={storyParallelWorkers}
                  onChange={(e) => setStoryParallelWorkers(parseInt(e.target.value) || 5)}
                  className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-20"
                />
                <span className="text-sm text-gray-400">workers</span>
                <button
                  onClick={saveStorySettings}
                  className="bg-primary hover:bg-primary/90 px-3 py-2 rounded-lg text-sm"
                >
                  Guardar
                </button>
              </div>
            </div>

            <div className="flex items-center justify-between py-2">
              <div>
                <label className="text-sm font-medium">Escanear Stories Ahora</label>
                <p className="text-xs text-gray-500">Ejecutar escaneo masivo de stories de TODOS los usuarios ahora</p>
              </div>
              <button
                onClick={downloadStoriesNow}
                disabled={downloadingStories}
                className="flex items-center gap-2 bg-pink-600 hover:bg-pink-700 px-4 py-2 rounded-lg disabled:opacity-50"
              >
                <Play className="w-4 h-4" />
                {downloadingStories ? 'Escaneando...' : 'Escanear Ahora'}
              </button>
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-purple-500/20 rounded-lg">
                <Camera className="w-5 h-5 text-purple-500" />
              </div>
              <div>
                <h2 className="text-lg font-semibold">Escaneo de Fotos de Perfil</h2>
                <p className="text-xs text-gray-500">
                  {photoScanStatus.users_scanned || 0} usuarios escaneados | {photoScanStatus.photos_downloaded || 0} fotos descargadas
                </p>
              </div>
            </div>
            <label className="relative inline-flex items-center cursor-pointer">
              <input
                type="checkbox"
                checked={photoScanEnabled}
                onChange={(e) => setPhotoScanEnabled(e.target.checked)}
                className="sr-only peer"
              />
              <div className="w-11 h-6 bg-gray-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-purple-500"></div>
            </label>
          </div>

          <p className="text-sm text-gray-400 mb-4">
            Escaneo automatico periodico de fotos de perfil de todos los usuarios. Descarga el historial completo de fotos incluyendo videos animados.
          </p>

          <div className="space-y-4">
            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Intervalo de Escaneo</label>
                <p className="text-xs text-gray-500">Cada cuantas horas escanear todos los usuarios</p>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="1"
                  max="168"
                  value={photoScanInterval}
                  onChange={(e) => setPhotoScanInterval(parseInt(e.target.value) || 24)}
                  className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-20"
                />
                <span className="text-sm text-gray-400">horas</span>
              </div>
            </div>

            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Usuarios por Lote</label>
                <p className="text-xs text-gray-500">Cuantos usuarios procesar por lote</p>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="10"
                  max="200"
                  value={photoScanBatchSize}
                  onChange={(e) => setPhotoScanBatchSize(parseInt(e.target.value) || 50)}
                  className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-20"
                />
                <span className="text-sm text-gray-400">usuarios</span>
              </div>
            </div>

            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Workers Paralelos</label>
                <p className="text-xs text-gray-500">Descargas simultaneas (mas = mas rapido, pero mas riesgo de ban)</p>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="1"
                  max="10"
                  value={photoScanWorkers}
                  onChange={(e) => setPhotoScanWorkers(parseInt(e.target.value) || 3)}
                  className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-20"
                />
                <span className="text-sm text-gray-400">workers</span>
                <button
                  onClick={savePhotoScanSettings}
                  className="bg-primary hover:bg-primary/90 px-3 py-2 rounded-lg text-sm"
                >
                  Guardar
                </button>
              </div>
            </div>

            <div className="flex items-center justify-between py-2">
              <div>
                <label className="text-sm font-medium">Escanear Fotos Ahora</label>
                <p className="text-xs text-gray-500">Iniciar escaneo manual de fotos de todos los usuarios</p>
              </div>
              <button
                onClick={scanPhotosNow}
                disabled={scanningPhotos || photoScanStatus.is_scanning}
                className="flex items-center gap-2 bg-purple-600 hover:bg-purple-700 px-4 py-2 rounded-lg disabled:opacity-50"
              >
                <RefreshCw className={`w-4 h-4 ${photoScanStatus.is_scanning ? 'animate-spin' : ''}`} />
                {photoScanStatus.is_scanning ? 'Escaneando...' : scanningPhotos ? 'Iniciando...' : 'Escanear Ahora'}
              </button>
            </div>

            {photoScanStatus.last_run && (
              <div className="text-xs text-gray-500 text-right">
                Ultimo escaneo: {new Date(photoScanStatus.last_run).toLocaleString()}
              </div>
            )}
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-cyan-500/20 rounded-lg">
                <Globe className="w-5 h-5 text-cyan-500" />
              </div>
              <div>
                <h2 className="text-lg font-semibold">Servidor para Crawler</h2>
                <p className="text-xs text-gray-500">
                  {crawlerStatus.running ? 'Activo' : 'Inactivo'} | Puerto {crawlerPort}
                </p>
              </div>
            </div>
            <label className="relative inline-flex items-center cursor-pointer">
              <input
                type="checkbox"
                checked={crawlerEnabled}
                onChange={(e) => setCrawlerEnabled(e.target.checked)}
                className="sr-only peer"
              />
              <div className="w-11 h-6 bg-gray-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-cyan-500"></div>
            </label>
          </div>

          <p className="text-sm text-gray-400 mb-4">
            Servidor estatico para servir usuarios con fotos/media/stories a crawlers de reconocimiento facial. Sin autenticacion, paginado a 100 usuarios por pagina.
          </p>

          <div className="space-y-4">
            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Puerto del Servidor</label>
                <p className="text-xs text-gray-500">Puerto donde correra el servidor del crawler</p>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="1024"
                  max="65535"
                  value={crawlerPort}
                  onChange={(e) => setCrawlerPort(parseInt(e.target.value) || 8001)}
                  className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-24"
                />
                <button
                  onClick={saveCrawlerSettings}
                  className="bg-primary hover:bg-primary/90 px-3 py-2 rounded-lg text-sm"
                >
                  Guardar
                </button>
              </div>
            </div>

            <div className="p-4 bg-gray-800/50 rounded-lg">
              <h4 className="text-sm font-medium mb-2">Endpoints Disponibles:</h4>
              <ul className="text-xs text-gray-400 space-y-1">
                <li><code className="bg-gray-900 px-1 rounded">GET /</code> - Index HTML paginado con usuarios</li>
                <li><code className="bg-gray-900 px-1 rounded">GET /api/users?page=1</code> - API JSON de usuarios</li>
                <li><code className="bg-gray-900 px-1 rounded">GET /api/user/:id</code> - Detalle de usuario</li>
                <li><code className="bg-gray-900 px-1 rounded">GET /api/stats</code> - Estadisticas del crawler</li>
                <li><code className="bg-gray-900 px-1 rounded">GET /media/...</code> - Archivos de media</li>
              </ul>
            </div>

            <div className="text-xs text-gray-500">
              Nota: Activa el workflow "Crawler Server" para iniciar el servidor.
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-green-500/20 rounded-lg">
                <Link className="w-5 h-5 text-green-500" />
              </div>
              <div>
                <h2 className="text-lg font-semibold">AutoJoin de Links</h2>
                <p className="text-xs text-gray-500">
                  {autoJoinStats.total_joined || 0} unidos | {autoJoinStats.pending_count || 0} pendientes
                </p>
              </div>
            </div>
            <label className="relative inline-flex items-center cursor-pointer">
              <input
                type="checkbox"
                checked={autoJoinConfig.enabled}
                onChange={(e) => setAutoJoinConfig(prev => ({ ...prev, enabled: e.target.checked }))}
                className="sr-only peer"
              />
              <div className="w-11 h-6 bg-gray-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-green-500"></div>
            </label>
          </div>

          <p className="text-sm text-gray-400 mb-4">
            Unirse automaticamente a grupos/canales detectados desde links de invitacion
          </p>

          <div className="space-y-4">
            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Modo de Cuentas</label>
                <p className="text-xs text-gray-500">Como distribuir los joins entre cuentas</p>
              </div>
              <select
                value={autoJoinConfig.mode}
                onChange={(e) => setAutoJoinConfig(prev => ({ ...prev, mode: e.target.value as 'rotation' | 'specific' }))}
                className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm"
              >
                <option value="rotation">Rotacion (balancear)</option>
                <option value="specific">Cuentas especificas</option>
              </select>
            </div>

            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Delay entre Joins</label>
                <p className="text-xs text-gray-500">Minutos de espera entre cada union</p>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="1"
                  max="60"
                  value={autoJoinConfig.delay_minutes}
                  onChange={(e) => setAutoJoinConfig(prev => ({ ...prev, delay_minutes: parseInt(e.target.value) || 5 }))}
                  className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-20"
                />
                <span className="text-sm text-gray-400">min</span>
              </div>
            </div>

            <div className="flex items-center justify-between py-2 border-b border-gray-800">
              <div>
                <label className="text-sm font-medium">Max Joins por Dia</label>
                <p className="text-xs text-gray-500">Limite diario para evitar bans</p>
              </div>
              <input
                type="number"
                min="1"
                max="100"
                value={autoJoinConfig.max_joins_per_day}
                onChange={(e) => setAutoJoinConfig(prev => ({ ...prev, max_joins_per_day: parseInt(e.target.value) || 20 }))}
                className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm w-20"
              />
            </div>

            {accounts.length > 0 && (
              <div className="py-2 border-b border-gray-800">
                <label className="text-sm font-medium block mb-2">Cuentas Habilitadas</label>
                <p className="text-xs text-gray-500 mb-3">Selecciona que cuentas pueden usarse para AutoJoin</p>
                <div className="flex flex-wrap gap-2">
                  {accounts.map((account) => (
                    <label
                      key={account.id}
                      className={`flex items-center gap-2 px-3 py-2 rounded-lg cursor-pointer border ${
                        autoJoinConfig.enabled_accounts?.includes(account.id)
                          ? 'bg-green-500/20 border-green-500/50'
                          : 'bg-gray-800 border-gray-700'
                      }`}
                    >
                      <input
                        type="checkbox"
                        checked={autoJoinConfig.enabled_accounts?.includes(account.id) || false}
                        onChange={() => toggleAutoJoinAccount(account.id)}
                        className="w-4 h-4 rounded border-gray-700 bg-gray-800 text-green-500"
                      />
                      <span className="text-sm">{account.phone}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}

            <div className="py-2 border-b border-gray-800">
              <label className="text-sm font-medium block mb-3">Acciones Post-Join</label>
              <div className="grid grid-cols-2 gap-3">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={autoJoinConfig.auto_monitor}
                    onChange={(e) => setAutoJoinConfig(prev => ({ ...prev, auto_monitor: e.target.checked }))}
                    className="w-4 h-4 rounded"
                  />
                  <span className="text-sm">Monitoreo en tiempo real</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={autoJoinConfig.auto_backfill}
                    onChange={(e) => setAutoJoinConfig(prev => ({ ...prev, auto_backfill: e.target.checked }))}
                    className="w-4 h-4 rounded"
                  />
                  <span className="text-sm">Backfill de mensajes</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={autoJoinConfig.auto_scrape_members}
                    onChange={(e) => setAutoJoinConfig(prev => ({ ...prev, auto_scrape_members: e.target.checked }))}
                    className="w-4 h-4 rounded"
                  />
                  <span className="text-sm">Scraping de miembros</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={autoJoinConfig.auto_stories}
                    onChange={(e) => setAutoJoinConfig(prev => ({ ...prev, auto_stories: e.target.checked }))}
                    className="w-4 h-4 rounded"
                  />
                  <span className="text-sm">Descargar stories</span>
                </label>
              </div>
            </div>

            <div className="flex items-center justify-end pt-2">
              <button
                onClick={saveAutoJoinConfig}
                disabled={savingAutoJoin}
                className="flex items-center gap-2 bg-green-600 hover:bg-green-700 px-4 py-2 rounded-lg disabled:opacity-50"
              >
                <Zap className="w-4 h-4" />
                {savingAutoJoin ? 'Guardando...' : 'Guardar AutoJoin'}
              </button>
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center gap-3 mb-4">
            <div className="p-2 bg-primary/20 rounded-lg">
              <Search className="w-5 h-5 text-primary" />
            </div>
            <h2 className="text-lg font-semibold">Watchlist de Dominios</h2>
          </div>

          <p className="text-sm text-gray-400 mb-4">
            Dominios y URLs de interes para monitorear en mensajes
          </p>

          <div className="flex gap-2 mb-4">
            <input
              type="text"
              value={newDomain}
              onChange={(e) => setNewDomain(e.target.value)}
              placeholder="ejemplo.com"
              className="flex-1 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm"
              onKeyDown={(e) => e.key === 'Enter' && addDomain()}
            />
            <button
              onClick={addDomain}
              className="flex items-center gap-2 bg-primary hover:bg-primary/90 px-4 py-2 rounded-lg"
            >
              <Plus className="w-4 h-4" />
              Agregar
            </button>
          </div>

          <div className="space-y-2">
            {domains.map((domain) => (
              <div key={domain.id} className="flex items-center justify-between bg-gray-800/50 rounded-lg px-4 py-2">
                <div>
                  <span className="font-medium">{domain.domain}</span>
                  {domain.description && (
                    <span className="text-gray-500 text-sm ml-2">- {domain.description}</span>
                  )}
                </div>
                <div className="flex items-center gap-4">
                  <span className="text-sm text-gray-400">{domain.mention_count} menciones</span>
                  <button
                    onClick={() => removeDomain(domain.id)}
                    className="text-red-400 hover:text-red-300"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              </div>
            ))}
            {domains.length === 0 && (
              <p className="text-gray-500 text-sm text-center py-4">No hay dominios en la watchlist</p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
