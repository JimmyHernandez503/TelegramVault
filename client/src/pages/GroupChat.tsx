import { useEffect, useState, useRef, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { api } from '@/api/client';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { 
  Loader2, ArrowLeft, Users, Image as ImageIcon, 
  File, Video, Music, Download, Eye, Forward, 
  Heart, RefreshCw, Play, Pause, UserPlus
} from 'lucide-react';

interface MediaItem {
  type: string;
  path: string | null;
  file_name: string | null;
}

interface Message {
  id: number;
  message_id: number;
  text: string;
  date: string;
  sender_id: number;
  sender_name: string;
  sender_username: string | null;
  sender_photo: string | null;
  media_type: string | null;
  media_path: string | null;
  media_items: MediaItem[];
  views: number;
  forwards: number;
  reactions: Record<string, number>;
  reply_to_msg_id: number | null;
  grouped_id: number | null;
}

interface Group {
  id: number;
  telegram_id: number;
  title: string;
  username: string | null;
  group_type: string;
  status: string;
  member_count: number;
  messages_count: number;
  assigned_account_id: number | null;
  photo_path: string | null;
  is_monitoring: boolean;
  backfill_in_progress: boolean;
  backfill_done: boolean;
}

export function GroupChatPage() {
  const { groupId } = useParams<{ groupId: string }>();
  const navigate = useNavigate();
  
  const [group, setGroup] = useState<Group | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [isBackfilling, setIsBackfilling] = useState(false);
  const [liveMonitoring, setLiveMonitoring] = useState(false);
  const [togglingMonitor, setTogglingMonitor] = useState(false);
  const [scrapingMembers, setScrapingMembers] = useState(false);
  
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const loadGroup = useCallback(async () => {
    if (!groupId) return;
    try {
      const groups = await api.getGroups();
      const found = groups.find((g: Group) => g.id === parseInt(groupId));
      if (found) {
        setGroup(found);
        setLiveMonitoring(found.is_monitoring || false);
        setIsBackfilling(found.backfill_in_progress || false);
      }
    } catch (err) {
      console.error('Error loading group:', err);
    }
  }, [groupId]);

  const loadMessages = useCallback(async (offsetId: number = 0) => {
    if (!groupId) return;
    
    try {
      if (offsetId === 0) setLoading(true);
      else setLoadingMore(true);
      
      const response = await api.get(
        `/groups/${groupId}/messages?limit=50&offset_id=${offsetId}`
      ) as { messages: Message[] };
      
      const newMessages = response.messages || [];
      
      if (offsetId === 0) {
        setMessages(newMessages);
      } else {
        setMessages(prev => [...prev, ...newMessages]);
      }
      
      setHasMore(newMessages.length >= 50);
    } catch (err) {
      console.error('Error loading messages:', err);
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  }, [groupId]);

  const startBackfill = async () => {
    if (!group?.assigned_account_id || !group?.telegram_id) return;
    
    try {
      setIsBackfilling(true);
      await api.post(`/telegram/${group.assigned_account_id}/backfill/${group.telegram_id}?limit=1000`);
      setTimeout(() => {
        loadMessages();
        setIsBackfilling(false);
      }, 5000);
    } catch (err) {
      console.error('Error starting backfill:', err);
      setIsBackfilling(false);
    }
  };

  const scrapeMembers = async () => {
    if (!group?.assigned_account_id || !group?.telegram_id) return;
    
    try {
      setScrapingMembers(true);
      await api.post(`/telegram/${group.assigned_account_id}/save-participants/${group.telegram_id}`);
      await loadGroup();
    } catch (err) {
      console.error('Error scraping members:', err);
    } finally {
      setScrapingMembers(false);
    }
  };

  const loadMoreMessages = () => {
    if (messages.length > 0 && hasMore && !loadingMore) {
      const lastMessage = messages[messages.length - 1];
      loadMessages(lastMessage.message_id);
    }
  };

  useEffect(() => {
    loadGroup();
  }, [loadGroup]);

  useEffect(() => {
    if (groupId) {
      loadMessages();
    }
  }, [groupId, loadMessages]);

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleString('es-ES', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const getMediaIcon = (mediaType: string | null) => {
    switch (mediaType) {
      case 'photo': return <ImageIcon className="w-4 h-4" />;
      case 'video': return <Video className="w-4 h-4" />;
      case 'audio': return <Music className="w-4 h-4" />;
      case 'document': return <File className="w-4 h-4" />;
      default: return null;
    }
  };

  const renderReactions = (reactions: Record<string, number>) => {
    if (!reactions || Object.keys(reactions).length === 0) return null;
    
    return (
      <div className="flex flex-wrap gap-1 mt-2">
        {Object.entries(reactions).map(([emoji, count]) => (
          <span key={emoji} className="text-xs bg-secondary/50 px-2 py-0.5 rounded-full flex items-center gap-1">
            {emoji} {count}
          </span>
        ))}
      </div>
    );
  };

  if (loading && !group) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      <div className="flex items-center gap-4 p-4 border-b bg-card">
        <Button variant="ghost" size="icon" onClick={() => navigate('/groups')}>
          <ArrowLeft className="h-5 w-5" />
        </Button>
        
        <div className="w-10 h-10 rounded-full bg-secondary flex items-center justify-center overflow-hidden">
          {group?.photo_path ? (
            <img src={`/media/${group.photo_path}`} alt="" className="w-full h-full object-cover" />
          ) : (
            <Users className="w-5 h-5 text-muted-foreground" />
          )}
        </div>
        
        <div className="flex-1">
          <h2 className="font-semibold">{group?.title}</h2>
          <div className="flex items-center gap-4 text-sm text-muted-foreground">
            {group?.username && <span>@{group.username}</span>}
            <span className="flex items-center gap-1">
              <Users className="w-3 h-3" />
              {group?.member_count?.toLocaleString()} miembros
            </span>
            <span>{messages.length} mensajes cargados</span>
          </div>
        </div>
        
        <div className="flex items-center gap-2">
          <Button 
            variant="outline" 
            size="sm"
            onClick={startBackfill}
            disabled={isBackfilling}
          >
            {isBackfilling ? (
              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
            ) : (
              <Download className="w-4 h-4 mr-2" />
            )}
            Backfill
          </Button>
          
          <Button
            variant={liveMonitoring ? "default" : "outline"}
            size="sm"
            onClick={async () => {
              if (!group?.assigned_account_id || !group?.telegram_id) return;
              setTogglingMonitor(true);
              try {
                if (liveMonitoring) {
                  await api.post(`/telegram/${group.assigned_account_id}/monitor/${group.telegram_id}/stop`);
                  setLiveMonitoring(false);
                } else {
                  await api.post(`/telegram/${group.assigned_account_id}/monitor/${group.telegram_id}/start`);
                  setLiveMonitoring(true);
                }
              } catch (err) {
                console.error('Error toggling monitor:', err);
              } finally {
                setTogglingMonitor(false);
              }
            }}
            disabled={togglingMonitor}
          >
            {togglingMonitor ? (
              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
            ) : liveMonitoring ? (
              <Pause className="w-4 h-4 mr-2" />
            ) : (
              <Play className="w-4 h-4 mr-2" />
            )}
            Live
          </Button>
          
          <Button 
            variant="outline" 
            size="sm"
            onClick={scrapeMembers}
            disabled={scrapingMembers}
            title="Descargar lista de miembros"
          >
            {scrapingMembers ? (
              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
            ) : (
              <UserPlus className="w-4 h-4 mr-2" />
            )}
            Miembros
          </Button>
          
          <Button variant="ghost" size="icon" onClick={() => loadMessages()}>
            <RefreshCw className="w-4 h-4" />
          </Button>
        </div>
      </div>

      <div 
        ref={containerRef}
        className="flex-1 overflow-y-auto p-4 space-y-3"
      >
        {loading ? (
          <div className="flex justify-center py-10">
            <Loader2 className="h-6 w-6 animate-spin text-primary" />
          </div>
        ) : messages.length === 0 ? (
          <div className="text-center py-20 text-muted-foreground">
            <p>No hay mensajes</p>
            <p className="text-sm mt-2">Haz clic en Backfill para cargar el historial</p>
          </div>
        ) : (
          <>
            {[...messages].reverse().map((msg) => (
              <div key={msg.id || msg.message_id} className="group">
                <Card className="p-3 max-w-[85%] hover:bg-secondary/30 transition-colors">
                  <div className="flex items-start gap-3">
                    <button 
                      onClick={() => msg.sender_id && navigate(`/users/${msg.sender_id}`)}
                      className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center shrink-0 overflow-hidden hover:ring-2 hover:ring-primary transition-all cursor-pointer"
                      disabled={!msg.sender_id}
                    >
                      {msg.sender_photo ? (
                        <img 
                          src={`/${msg.sender_photo}`} 
                          alt="" 
                          className="w-full h-full object-cover"
                        />
                      ) : (
                        <span className="text-xs font-medium">
                          {msg.sender_name?.charAt(0) || '?'}
                        </span>
                      )}
                    </button>
                    
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <button 
                          onClick={() => msg.sender_id && navigate(`/users/${msg.sender_id}`)}
                          className="font-medium text-sm text-primary hover:underline cursor-pointer"
                          disabled={!msg.sender_id}
                        >
                          {msg.sender_name || 'Unknown'}
                        </button>
                        {msg.sender_username && (
                          <button 
                            onClick={() => msg.sender_id && navigate(`/users/${msg.sender_id}`)}
                            className="text-xs text-muted-foreground hover:text-primary cursor-pointer"
                            disabled={!msg.sender_id}
                          >
                            @{msg.sender_username}
                          </button>
                        )}
                        <span className="text-xs text-muted-foreground ml-auto">
                          {formatDate(msg.date)}
                        </span>
                      </div>
                      
                      {msg.reply_to_msg_id && (
                        <div className="text-xs text-muted-foreground bg-secondary/30 px-2 py-1 rounded mb-2 border-l-2 border-primary">
                          Respuesta a mensaje #{msg.reply_to_msg_id}
                        </div>
                      )}
                      
                      {(msg.media_items?.length > 0 || msg.media_type) && (
                        <div className="mb-2">
                          {msg.media_items?.length > 1 ? (
                            <div className={`grid gap-1 ${msg.media_items.length === 2 ? 'grid-cols-2' : msg.media_items.length === 3 ? 'grid-cols-3' : 'grid-cols-2'}`}>
                              {msg.media_items.map((media, idx) => (
                                <div key={idx} className="relative">
                                  {media.path ? (
                                    media.type === 'photo' ? (
                                      <img 
                                        src={`/media/${media.path}`} 
                                        alt={`Media ${idx + 1}`}
                                        className="w-full h-32 object-cover rounded cursor-pointer hover:opacity-90 transition-opacity"
                                        onClick={() => window.open(`/media/${media.path}`, '_blank')}
                                      />
                                    ) : media.type === 'video' ? (
                                      <video 
                                        src={`/media/${media.path}`}
                                        controls
                                        preload="metadata"
                                        className="w-full h-32 object-cover rounded"
                                      />
                                    ) : (
                                      <div className="flex items-center gap-2 bg-secondary/50 px-2 py-1 rounded h-32">
                                        {getMediaIcon(media.type)}
                                        <span className="text-xs">{media.file_name || media.type}</span>
                                      </div>
                                    )
                                  ) : (
                                    <div className="w-full h-32 bg-yellow-900/30 rounded flex items-center justify-center">
                                      {getMediaIcon(media.type)}
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          ) : msg.media_path ? (
                            msg.media_type === 'photo' ? (
                              <img 
                                src={`/media/${msg.media_path}`} 
                                alt="Media" 
                                className="max-w-full max-h-64 rounded-lg object-cover cursor-pointer hover:opacity-90 transition-opacity"
                                onClick={() => window.open(`/media/${msg.media_path}`, '_blank')}
                              />
                            ) : msg.media_type === 'video' ? (
                              <video 
                                src={`/media/${msg.media_path}`}
                                controls
                                preload="metadata"
                                className="max-w-full max-h-64 rounded-lg"
                              >
                                Tu navegador no soporta video
                              </video>
                            ) : (
                              <div className="flex items-center gap-2 bg-secondary/50 px-3 py-2 rounded-lg">
                                {getMediaIcon(msg.media_type)}
                                <span className="text-sm capitalize">{msg.media_type}</span>
                                <a 
                                  href={`/media/${msg.media_path}`} 
                                  target="_blank" 
                                  rel="noopener noreferrer"
                                  className="ml-auto text-primary hover:underline text-sm"
                                >
                                  Descargar
                                </a>
                              </div>
                            )
                          ) : msg.media_type ? (
                            <div className="flex items-center gap-2 bg-yellow-900/30 border border-yellow-700/50 px-3 py-2 rounded-lg">
                              {getMediaIcon(msg.media_type)}
                              <span className="text-sm text-yellow-500">
                                {msg.media_type === 'photo' ? 'Foto' : msg.media_type} no descargado
                              </span>
                            </div>
                          ) : null}
                        </div>
                      )}
                      
                      {msg.text && (
                        <p className="text-sm whitespace-pre-wrap break-words">{msg.text}</p>
                      )}
                      
                      {renderReactions(msg.reactions)}
                      
                      {(msg.views > 0 || msg.forwards > 0) && (
                        <div className="flex items-center gap-4 mt-2 text-xs text-muted-foreground">
                          {msg.views > 0 && (
                            <span className="flex items-center gap-1">
                              <Eye className="w-3 h-3" />
                              {msg.views.toLocaleString()}
                            </span>
                          )}
                          {msg.forwards > 0 && (
                            <span className="flex items-center gap-1">
                              <Forward className="w-3 h-3" />
                              {msg.forwards.toLocaleString()}
                            </span>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                </Card>
              </div>
            ))}
            
            {hasMore && (
              <div className="flex justify-center py-4">
                <Button 
                  variant="outline" 
                  onClick={loadMoreMessages}
                  disabled={loadingMore}
                >
                  {loadingMore ? (
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  ) : null}
                  Cargar mas mensajes
                </Button>
              </div>
            )}
          </>
        )}
        
        <div ref={messagesEndRef} />
      </div>
    </div>
  );
}
