import { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api/client';

interface SearchResult {
  type: 'message' | 'user' | 'detection';
  id: number;
  highlight?: string;
  relevance: number;
  text?: string;
  date?: string;
  group_id?: number;
  group_title?: string;
  sender_id?: number;
  sender_name?: string;
  sender_username?: string;
  telegram_id?: number;
  username?: string;
  first_name?: string;
  last_name?: string;
  phone?: string;
  bio?: string;
  photo_path?: string;
  messages_count?: number;
  is_watchlist?: boolean;
  is_favorite?: boolean;
  detection_type?: string;
  matched_text?: string;
  context_before?: string;
  context_after?: string;
  detector_name?: string;
  created_at?: string;
  message_id?: number;
  user_id?: number;
}

interface SearchResponse {
  results: SearchResult[];
  total: number;
  query: string;
  filters: {
    group_id?: number;
    user_id?: number;
    source_types?: string[];
    date_from?: string;
    date_to?: string;
  };
}

interface Group {
  id: number;
  title: string;
}

export default function SearchPage() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [groups, setGroups] = useState<Group[]>([]);
  
  const [filterGroup, setFilterGroup] = useState<number | ''>('');
  const [filterTypes, setFilterTypes] = useState<string[]>(['messages', 'users', 'detections']);
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');

  useEffect(() => {
    api.get<Group[]>('/groups/').then(res => setGroups(res.data || [])).catch(() => setGroups([]));
  }, []);

  const doSearch = useCallback(async () => {
    if (!query || query.length < 2) {
      setResults([]);
      setTotal(0);
      return;
    }

    setLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('q', query);
      if (filterGroup) params.set('group_id', String(filterGroup));
      if (filterTypes.length > 0 && filterTypes.length < 3) {
        params.set('types', filterTypes.join(','));
      }
      if (dateFrom) params.set('date_from', dateFrom);
      if (dateTo) params.set('date_to', dateTo);
      params.set('limit', '100');

      const res = await api.get<SearchResponse>(`/search/?${params.toString()}`);
      setResults(res.results || []);
      setTotal(res.total || 0);
    } catch (err) {
      console.error('Search error:', err);
    } finally {
      setLoading(false);
    }
  }, [query, filterGroup, filterTypes, dateFrom, dateTo]);

  useEffect(() => {
    const timeout = setTimeout(() => {
      if (query.length >= 2) {
        doSearch();
      }
    }, 300);
    return () => clearTimeout(timeout);
  }, [query, doSearch]);

  const toggleType = (type: string) => {
    setFilterTypes(prev => 
      prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
    );
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'message': return '💬';
      case 'user': return '👤';
      case 'detection': return '🔍';
      default: return '📄';
    }
  };

  const getTypeColor = (type: string) => {
    switch (type) {
      case 'message': return 'bg-blue-500/20 text-blue-400 border-blue-500/30';
      case 'user': return 'bg-green-500/20 text-green-400 border-green-500/30';
      case 'detection': return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30';
      default: return 'bg-gray-500/20 text-gray-400 border-gray-500/30';
    }
  };

  const renderResult = (result: SearchResult) => {
    switch (result.type) {
      case 'message':
        return (
          <div key={`msg-${result.id}`} className="bg-slate-800 rounded-lg p-4 hover:bg-slate-700/50 transition-colors">
            <div className="flex items-start gap-3">
              <div className="text-2xl">{getTypeIcon(result.type)}</div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-2">
                  <span className={`px-2 py-0.5 rounded text-xs border ${getTypeColor(result.type)}`}>
                    Mensaje
                  </span>
                  {result.group_title && (
                    <Link 
                      to={`/groups/${result.group_id}`} 
                      className="text-sm text-cyan-400 hover:underline"
                    >
                      {result.group_title}
                    </Link>
                  )}
                  {result.date && (
                    <span className="text-xs text-slate-500">
                      {new Date(result.date).toLocaleString()}
                    </span>
                  )}
                </div>
                <div 
                  className="text-slate-300 text-sm"
                  dangerouslySetInnerHTML={{ __html: result.highlight || result.text || '' }}
                />
                {result.sender_name && (
                  <div className="mt-2 text-xs text-slate-500">
                    De: {result.sender_name} {result.sender_username && `(@${result.sender_username})`}
                  </div>
                )}
              </div>
            </div>
          </div>
        );

      case 'user':
        return (
          <Link 
            key={`user-${result.id}`} 
            to={`/users/${result.id}`}
            className="bg-slate-800 rounded-lg p-4 hover:bg-slate-700/50 transition-colors block"
          >
            <div className="flex items-start gap-3">
              {result.photo_path ? (
                <img 
                  src={`/${result.photo_path}`} 
                  alt="" 
                  className="w-12 h-12 rounded-full object-cover"
                />
              ) : (
                <div className="w-12 h-12 rounded-full bg-slate-600 flex items-center justify-center text-xl">
                  {getTypeIcon(result.type)}
                </div>
              )}
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <span className={`px-2 py-0.5 rounded text-xs border ${getTypeColor(result.type)}`}>
                    Usuario
                  </span>
                  {result.is_watchlist && <span className="text-xs bg-red-500/20 text-red-400 px-1.5 py-0.5 rounded">Watchlist</span>}
                  {result.is_favorite && <span className="text-xs bg-yellow-500/20 text-yellow-400 px-1.5 py-0.5 rounded">Favorito</span>}
                </div>
                <div className="font-medium text-white">
                  {result.first_name} {result.last_name}
                </div>
                {result.username && (
                  <div className="text-cyan-400 text-sm">@{result.username}</div>
                )}
                {result.phone && (
                  <div className="text-slate-400 text-sm">{result.phone}</div>
                )}
                <div 
                  className="text-slate-500 text-xs mt-1"
                  dangerouslySetInnerHTML={{ __html: result.highlight || result.bio || '' }}
                />
                <div className="text-xs text-slate-500 mt-1">
                  {result.messages_count || 0} mensajes
                </div>
              </div>
            </div>
          </Link>
        );

      case 'detection':
        return (
          <div key={`det-${result.id}`} className="bg-slate-800 rounded-lg p-4 hover:bg-slate-700/50 transition-colors">
            <div className="flex items-start gap-3">
              <div className="text-2xl">{getTypeIcon(result.type)}</div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-2 flex-wrap">
                  <span className={`px-2 py-0.5 rounded text-xs border ${getTypeColor(result.type)}`}>
                    {result.detection_type || 'Deteccion'}
                  </span>
                  {result.detector_name && (
                    <span className="text-xs text-slate-400">{result.detector_name}</span>
                  )}
                  {result.group_title && (
                    <Link 
                      to={`/groups/${result.group_id}`} 
                      className="text-sm text-cyan-400 hover:underline"
                    >
                      {result.group_title}
                    </Link>
                  )}
                  {result.created_at && (
                    <span className="text-xs text-slate-500">
                      {new Date(result.created_at).toLocaleString()}
                    </span>
                  )}
                </div>
                <div className="font-mono text-sm text-yellow-400 bg-slate-900 p-2 rounded">
                  {result.matched_text}
                </div>
                <div 
                  className="text-slate-400 text-xs mt-2"
                  dangerouslySetInnerHTML={{ __html: result.highlight || '' }}
                />
              </div>
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="p-6">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-white mb-2">Busqueda Global</h1>
        <p className="text-slate-400">Busca en mensajes, usuarios y detecciones</p>
      </div>

      <div className="bg-slate-800 rounded-lg p-4 mb-6">
        <div className="flex flex-col gap-4">
          <div className="relative">
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Escribe para buscar (minimo 2 caracteres)..."
              className="w-full bg-slate-900 text-white px-4 py-3 pl-12 rounded-lg focus:ring-2 focus:ring-cyan-500 focus:outline-none text-lg"
            />
            <svg className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            {loading && (
              <div className="absolute right-4 top-1/2 -translate-y-1/2">
                <div className="animate-spin w-5 h-5 border-2 border-cyan-500 border-t-transparent rounded-full" />
              </div>
            )}
          </div>

          <div className="flex flex-wrap gap-4 items-center">
            <div className="flex gap-2">
              <button
                onClick={() => toggleType('messages')}
                className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                  filterTypes.includes('messages') 
                    ? 'bg-blue-500 text-white' 
                    : 'bg-slate-700 text-slate-400 hover:bg-slate-600'
                }`}
              >
                💬 Mensajes
              </button>
              <button
                onClick={() => toggleType('users')}
                className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                  filterTypes.includes('users') 
                    ? 'bg-green-500 text-white' 
                    : 'bg-slate-700 text-slate-400 hover:bg-slate-600'
                }`}
              >
                👤 Usuarios
              </button>
              <button
                onClick={() => toggleType('detections')}
                className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                  filterTypes.includes('detections') 
                    ? 'bg-yellow-500 text-white' 
                    : 'bg-slate-700 text-slate-400 hover:bg-slate-600'
                }`}
              >
                🔍 Detecciones
              </button>
            </div>

            <select
              value={filterGroup}
              onChange={(e) => setFilterGroup(e.target.value ? Number(e.target.value) : '')}
              className="bg-slate-700 text-white px-3 py-1.5 rounded-lg text-sm"
            >
              <option value="">Todos los grupos</option>
              {groups.map(g => (
                <option key={g.id} value={g.id}>{g.title}</option>
              ))}
            </select>

            <input
              type="date"
              value={dateFrom}
              onChange={(e) => setDateFrom(e.target.value)}
              className="bg-slate-700 text-white px-3 py-1.5 rounded-lg text-sm"
              placeholder="Desde"
            />
            <input
              type="date"
              value={dateTo}
              onChange={(e) => setDateTo(e.target.value)}
              className="bg-slate-700 text-white px-3 py-1.5 rounded-lg text-sm"
              placeholder="Hasta"
            />

            <button
              onClick={doSearch}
              disabled={query.length < 2}
              className="bg-cyan-500 hover:bg-cyan-600 disabled:bg-slate-600 disabled:cursor-not-allowed text-white px-4 py-1.5 rounded-lg text-sm transition-colors"
            >
              Buscar
            </button>
          </div>
        </div>
      </div>

      {total > 0 && (
        <div className="mb-4 text-slate-400">
          {total} resultado{total !== 1 ? 's' : ''} encontrado{total !== 1 ? 's' : ''}
        </div>
      )}

      <div className="space-y-3">
        {results.map(result => renderResult(result))}
      </div>

      {query.length >= 2 && !loading && results.length === 0 && (
        <div className="text-center py-12 text-slate-400">
          <div className="text-4xl mb-4">🔍</div>
          <p>No se encontraron resultados para "{query}"</p>
        </div>
      )}

      {query.length < 2 && (
        <div className="text-center py-12 text-slate-500">
          <div className="text-4xl mb-4">🔎</div>
          <p>Escribe al menos 2 caracteres para buscar</p>
        </div>
      )}
    </div>
  );
}
