import { useState, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { api } from '@/api/client'
import { Mail, Phone, Bitcoin, Link, Users, AlertCircle, Settings, ChevronDown, ChevronUp, User, Globe, MessageSquare, Calendar, Hash, X } from 'lucide-react'

interface DetectionStats {
  email: number
  phone: number
  crypto: number
  url: number
  invite_link: number
  total: number
  unique_counts?: Record<string, number>
}

interface GroupedDetection {
  value: string
  normalized_value: string
  detection_type: string
  occurrence_count: number
  first_seen: string | null
  last_seen: string | null
  sample_sender: {
    id: number
    telegram_id: number
    name: string | null
    username: string | null
    photo: string | null
  } | null
  sample_group: string | null
}

interface DetectionOccurrence {
  id: number
  matched_text: string
  context_before: string | null
  context_after: string | null
  message_text: string | null
  sender: {
    id: number
    telegram_id: number
    name: string | null
    username: string | null
    photo: string | null
  } | null
  group: {
    id: number
    title: string
    photo: string | null
  } | null
  message_date: string | null
  telegram_message_id: number | null
  created_at: string | null
}

interface DomainStat {
  domain: string
  count: number
}

interface Detector {
  id: number
  name: string
  description: string | null
  pattern: string
  category: string
  priority: number
  is_builtin: boolean
  is_active: boolean
}

export default function Detections() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [stats, setStats] = useState<DetectionStats | null>(null)
  const [groupedDetections, setGroupedDetections] = useState<GroupedDetection[]>([])
  const [detectors, setDetectors] = useState<Detector[]>([])
  const [domainStats, setDomainStats] = useState<DomainStat[]>([])
  const [activeTab, setActiveTab] = useState<'stats' | 'list' | 'detectors'>('stats')
  const [filter, setFilter] = useState<string | null>(null)
  const [domainFilter, setDomainFilter] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [expandedItem, setExpandedItem] = useState<string | null>(null)
  const [occurrencesCache, setOccurrencesCache] = useState<Record<string, DetectionOccurrence[]>>({})
  const [loadingOccurrences, setLoadingOccurrences] = useState<string | null>(null)

  useEffect(() => {
    const urlFilter = searchParams.get('filter')
    if (urlFilter) {
      setFilter(urlFilter)
      setActiveTab('list')
    }
    loadData()
  }, [searchParams])

  useEffect(() => {
    if (activeTab === 'list') {
      loadGroupedDetections()
    }
    if (activeTab === 'stats') {
      loadDomainStats()
    }
  }, [filter, domainFilter, activeTab])

  const loadData = async () => {
    try {
      const [statsRes, detectorsRes] = await Promise.all([
        api.get<DetectionStats>('/detections/stats'),
        api.get<Detector[]>('/detections/detectors')
      ])
      setStats(statsRes || null)
      setDetectors(detectorsRes || [])
    } catch (error) {
      console.error('Failed to load detection data:', error)
    } finally {
      setLoading(false)
    }
  }

  const loadGroupedDetections = async () => {
    try {
      const queryParams = new URLSearchParams()
      if (filter) queryParams.append('detection_type', filter)
      if (domainFilter) queryParams.append('domain', domainFilter)
      const params = queryParams.toString() ? `?${queryParams.toString()}` : ''
      const res = await api.get<GroupedDetection[]>(`/detections/grouped${params}`)
      setGroupedDetections(res || [])
    } catch (error) {
      console.error('Failed to load grouped detections:', error)
    }
  }

  const handleDomainClick = (domain: string) => {
    setDomainFilter(domain)
    setFilter('url')
    setActiveTab('list')
  }

  const clearDomainFilter = () => {
    setDomainFilter(null)
  }

  const loadDomainStats = async () => {
    try {
      const res = await api.get<DomainStat[]>('/detections/url-domains')
      setDomainStats(res || [])
    } catch (error) {
      console.error('Failed to load domain stats:', error)
    }
  }

  const loadOccurrences = async (normalizedValue: string, detectionType: string, cacheKey: string) => {
    if (occurrencesCache[cacheKey]) return
    
    setLoadingOccurrences(cacheKey)
    try {
      const params = detectionType ? `?detection_type=${detectionType}` : ''
      const res = await api.get<DetectionOccurrence[]>(`/detections/occurrences/${encodeURIComponent(normalizedValue)}${params}`)
      setOccurrencesCache(prev => ({ ...prev, [cacheKey]: res || [] }))
    } catch (error) {
      console.error('Failed to load occurrences:', error)
    } finally {
      setLoadingOccurrences(null)
    }
  }

  const toggleExpand = async (item: GroupedDetection) => {
    const key = `${item.normalized_value}_${item.detection_type}`
    if (expandedItem === key) {
      setExpandedItem(null)
    } else {
      setExpandedItem(key)
      await loadOccurrences(item.normalized_value, item.detection_type, key)
    }
  }

  const seedDefaults = async () => {
    try {
      await api.post('/detections/seed-defaults')
      loadData()
    } catch (error) {
      console.error('Failed to seed defaults:', error)
    }
  }

  const getIcon = (type: string) => {
    switch (type) {
      case 'email': return <Mail className="w-5 h-5" />
      case 'phone': return <Phone className="w-5 h-5" />
      case 'crypto': return <Bitcoin className="w-5 h-5" />
      case 'url': return <Link className="w-5 h-5" />
      case 'invite_link': return <Users className="w-5 h-5" />
      default: return <AlertCircle className="w-5 h-5" />
    }
  }

  const getTypeColor = (type: string) => {
    switch (type) {
      case 'email': return 'text-blue-400'
      case 'phone': return 'text-green-400'
      case 'crypto': return 'text-yellow-400'
      case 'url': return 'text-purple-400'
      case 'invite_link': return 'text-pink-400'
      default: return 'text-gray-400'
    }
  }

  if (loading) {
    return <div className="flex items-center justify-center h-64">Cargando...</div>
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold">Detecciones</h1>
          <p className="text-muted-foreground">Emails, telefonos, direcciones crypto y enlaces detectados</p>
        </div>
        <div className="flex gap-2">
          <Button 
            variant={activeTab === 'stats' ? 'default' : 'outline'}
            onClick={() => setActiveTab('stats')}
          >
            Estadisticas
          </Button>
          <Button 
            variant={activeTab === 'list' ? 'default' : 'outline'}
            onClick={() => setActiveTab('list')}
          >
            Lista
          </Button>
          <Button 
            variant={activeTab === 'detectors' ? 'default' : 'outline'}
            onClick={() => setActiveTab('detectors')}
          >
            Detectores
          </Button>
        </div>
      </div>

      {activeTab === 'stats' && stats && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
            <Card className="cursor-pointer hover:bg-secondary/50" onClick={() => { setFilter(null); setActiveTab('list') }}>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Total</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{stats.total}</div>
                {stats.unique_counts && (
                  <div className="text-xs text-muted-foreground mt-1">
                    {Object.values(stats.unique_counts).reduce((a, b) => a + b, 0)} unicos
                  </div>
                )}
              </CardContent>
            </Card>
            
            <Card className="cursor-pointer hover:bg-secondary/50" onClick={() => { setFilter('email'); setActiveTab('list') }}>
              <CardHeader className="pb-2">
                <div className="flex items-center gap-2">
                  <Mail className="w-4 h-4 text-blue-400" />
                  <CardTitle className="text-sm font-medium text-muted-foreground">Emails</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-blue-400">{stats.email}</div>
                {stats.unique_counts?.email !== undefined && (
                  <div className="text-xs text-muted-foreground mt-1">{stats.unique_counts.email} unicos</div>
                )}
              </CardContent>
            </Card>

            <Card className="cursor-pointer hover:bg-secondary/50" onClick={() => { setFilter('phone'); setActiveTab('list') }}>
              <CardHeader className="pb-2">
                <div className="flex items-center gap-2">
                  <Phone className="w-4 h-4 text-green-400" />
                  <CardTitle className="text-sm font-medium text-muted-foreground">Telefonos</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-green-400">{stats.phone}</div>
                {stats.unique_counts?.phone !== undefined && (
                  <div className="text-xs text-muted-foreground mt-1">{stats.unique_counts.phone} unicos</div>
                )}
              </CardContent>
            </Card>

            <Card className="cursor-pointer hover:bg-secondary/50" onClick={() => { setFilter('crypto'); setActiveTab('list') }}>
              <CardHeader className="pb-2">
                <div className="flex items-center gap-2">
                  <Bitcoin className="w-4 h-4 text-yellow-400" />
                  <CardTitle className="text-sm font-medium text-muted-foreground">Crypto</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-yellow-400">{stats.crypto}</div>
                {stats.unique_counts?.crypto !== undefined && (
                  <div className="text-xs text-muted-foreground mt-1">{stats.unique_counts.crypto} unicos</div>
                )}
              </CardContent>
            </Card>

            <Card className="cursor-pointer hover:bg-secondary/50" onClick={() => { setFilter('url'); setActiveTab('list') }}>
              <CardHeader className="pb-2">
                <div className="flex items-center gap-2">
                  <Link className="w-4 h-4 text-purple-400" />
                  <CardTitle className="text-sm font-medium text-muted-foreground">URLs</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-purple-400">{stats.url}</div>
                {stats.unique_counts?.url !== undefined && (
                  <div className="text-xs text-muted-foreground mt-1">{stats.unique_counts.url} unicos</div>
                )}
              </CardContent>
            </Card>

            <Card className="cursor-pointer hover:bg-secondary/50" onClick={() => { setFilter('invite_link'); setActiveTab('list') }}>
              <CardHeader className="pb-2">
                <div className="flex items-center gap-2">
                  <Users className="w-4 h-4 text-pink-400" />
                  <CardTitle className="text-sm font-medium text-muted-foreground">Invites</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-pink-400">{stats.invite_link}</div>
                {stats.unique_counts?.invite_link !== undefined && (
                  <div className="text-xs text-muted-foreground mt-1">{stats.unique_counts.invite_link} unicos</div>
                )}
              </CardContent>
            </Card>
          </div>

          {domainStats.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Globe className="w-5 h-5" />
                  Dominios mas frecuentes
                  <span className="text-xs text-muted-foreground font-normal">(clic para filtrar)</span>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-2">
                  {domainStats.slice(0, 12).map((d) => (
                    <div 
                      key={d.domain} 
                      className="bg-secondary/30 rounded-lg p-3 text-center cursor-pointer hover:bg-secondary/60 transition-colors"
                      onClick={() => handleDomainClick(d.domain)}
                    >
                      <div className="text-sm font-medium truncate">{d.domain}</div>
                      <div className="text-2xl font-bold text-purple-400">{d.count}</div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}

      {activeTab === 'list' && (
        <Card>
          <CardHeader>
            <div className="flex justify-between items-center">
              <div>
                <CardTitle>
                  {filter ? `Detecciones: ${filter}` : 'Todas las detecciones'}
                  <span className="text-sm font-normal text-muted-foreground ml-2">
                    ({groupedDetections.length} valores unicos)
                  </span>
                </CardTitle>
                {domainFilter && (
                  <div className="text-sm text-purple-400 mt-1 flex items-center gap-2">
                    <Globe className="w-4 h-4" />
                    Filtrado por dominio: {domainFilter}
                    <Button variant="ghost" size="sm" className="h-5 px-2" onClick={clearDomainFilter}>
                      <X className="w-3 h-3" />
                    </Button>
                  </div>
                )}
              </div>
              {(filter || domainFilter) && (
                <Button variant="ghost" size="sm" onClick={() => { setFilter(null); setDomainFilter(null); }}>
                  Limpiar filtros
                </Button>
              )}
            </div>
          </CardHeader>
          <CardContent>
            {groupedDetections.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                No hay detecciones registradas. Las detecciones se generan automaticamente cuando se procesan mensajes.
              </div>
            ) : (
              <div className="space-y-2">
                {groupedDetections.map((d) => {
                  const key = `${d.normalized_value}_${d.detection_type}`
                  const isExpanded = expandedItem === key
                  
                  return (
                    <div key={key} className="rounded-lg bg-secondary/30 overflow-hidden">
                      <div 
                        className="flex items-center gap-3 p-3 cursor-pointer hover:bg-secondary/50"
                        onClick={() => toggleExpand(d)}
                      >
                        <div className={`${getTypeColor(d.detection_type)}`}>
                          {getIcon(d.detection_type)}
                        </div>
                        
                        {d.sample_sender?.photo && (
                          <img 
                            src={d.sample_sender.photo} 
                            alt="" 
                            className="w-8 h-8 rounded-full object-cover"
                          />
                        )}
                        
                        <div className="flex-1 min-w-0">
                          <div className="font-mono text-sm break-all font-medium">{d.value}</div>
                          <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
                            <span className="bg-secondary px-1.5 py-0.5 rounded">{d.detection_type}</span>
                            {d.occurrence_count > 1 && (
                              <span className="bg-primary/20 text-primary px-1.5 py-0.5 rounded font-medium">
                                {d.occurrence_count} ocurrencias
                              </span>
                            )}
                            {d.sample_sender?.name && (
                              <span className="text-blue-400">
                                Ultimo: {d.sample_sender.name}
                              </span>
                            )}
                            {d.sample_group && (
                              <span className="text-green-400">En: {d.sample_group}</span>
                            )}
                          </div>
                        </div>
                        
                        <div className="flex items-center gap-2">
                          {d.last_seen && (
                            <span className="text-xs text-muted-foreground">
                              {new Date(d.last_seen).toLocaleDateString('es-ES')}
                            </span>
                          )}
                          {isExpanded ? (
                            <ChevronUp className="w-5 h-5 text-muted-foreground" />
                          ) : (
                            <ChevronDown className="w-5 h-5 text-muted-foreground" />
                          )}
                        </div>
                      </div>
                      
                      {isExpanded && (
                        <div className="border-t border-border p-3 bg-background/50">
                          {loadingOccurrences === key ? (
                            <div className="text-center py-4 text-muted-foreground">Cargando...</div>
                          ) : (
                            <div className="space-y-3">
                              <div className="text-xs text-muted-foreground mb-2">
                                {(occurrencesCache[key] || []).length} ocurrencias encontradas
                              </div>
                              {(occurrencesCache[key] || []).map((occ) => (
                                <div key={occ.id} className="flex gap-3 p-2 rounded bg-secondary/30">
                                  {occ.sender?.photo ? (
                                    <img 
                                      src={occ.sender.photo} 
                                      alt="" 
                                      className="w-10 h-10 rounded-full object-cover flex-shrink-0"
                                    />
                                  ) : (
                                    <div className="w-10 h-10 rounded-full bg-secondary flex items-center justify-center flex-shrink-0">
                                      <User className="w-5 h-5 text-muted-foreground" />
                                    </div>
                                  )}
                                  
                                  <div className="flex-1 min-w-0">
                                    <div className="flex items-center gap-2 flex-wrap">
                                      {occ.sender && (
                                        <span className="font-medium text-sm">
                                          {occ.sender.name || occ.sender.username || `ID: ${occ.sender.telegram_id}`}
                                        </span>
                                      )}
                                      {occ.sender?.username && (
                                        <span className="text-xs text-blue-400">@{occ.sender.username}</span>
                                      )}
                                      {occ.sender?.telegram_id && (
                                        <span className="text-xs text-muted-foreground">
                                          <Hash className="w-3 h-3 inline" />{occ.sender.telegram_id}
                                        </span>
                                      )}
                                    </div>
                                    
                                    <div className="flex items-center gap-2 mt-1 text-xs text-muted-foreground">
                                      {occ.group && (
                                        <span className="text-green-400 flex items-center gap-1">
                                          <MessageSquare className="w-3 h-3" />
                                          {occ.group.title}
                                        </span>
                                      )}
                                      {occ.message_date && (
                                        <span className="flex items-center gap-1">
                                          <Calendar className="w-3 h-3" />
                                          {new Date(occ.message_date).toLocaleString('es-ES')}
                                        </span>
                                      )}
                                    </div>
                                    
                                    {occ.message_text && (
                                      <div className="mt-2 text-xs text-muted-foreground/80 line-clamp-2 bg-background/50 p-2 rounded">
                                        {occ.message_text}
                                      </div>
                                    )}
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {activeTab === 'detectors' && (
        <Card>
          <CardHeader>
            <div className="flex justify-between items-center">
              <CardTitle className="flex items-center gap-2">
                <Settings className="w-5 h-5" />
                Detectores Regex
              </CardTitle>
              <Button onClick={seedDefaults}>Cargar detectores por defecto</Button>
            </div>
          </CardHeader>
          <CardContent>
            {detectors.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                No hay detectores configurados. Haz clic en "Cargar detectores por defecto" para inicializar.
              </div>
            ) : (
              <div className="space-y-3">
                {detectors.map((d) => (
                  <div key={d.id} className="p-4 rounded-lg bg-secondary/30">
                    <div className="flex justify-between items-start">
                      <div>
                        <div className="font-medium flex items-center gap-2">
                          {d.name}
                          {d.is_builtin && (
                            <span className="text-xs bg-primary/20 text-primary px-2 py-0.5 rounded">builtin</span>
                          )}
                        </div>
                        <div className="text-sm text-muted-foreground">{d.description}</div>
                      </div>
                      <div className={`text-xs px-2 py-1 rounded ${d.is_active ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}`}>
                        {d.is_active ? 'Activo' : 'Inactivo'}
                      </div>
                    </div>
                    <div className="mt-2 font-mono text-xs bg-background/50 p-2 rounded overflow-x-auto">
                      {d.pattern}
                    </div>
                    <div className="mt-2 text-xs text-muted-foreground">
                      Categoria: {d.category} | Prioridad: {d.priority}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
