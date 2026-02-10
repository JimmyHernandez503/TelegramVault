import { useState, useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import { useWebSocket } from '@/hooks/useWebSocket';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { 
  Radio, 
  Wifi, 
  WifiOff, 
  MessageSquare, 
  AlertTriangle,
  User,
  Users,
  Image as ImageIcon,
  FileText,
  Video,
  Music,
  Paperclip
} from 'lucide-react';

interface RealtimeMessage {
  id: number;
  text: string;
  sender_name: string;
  sender_id: number;
  sender_photo: string | null;
  group_name: string;
  group_id: number;
  has_media: boolean;
  media_type: string | null;
  timestamp: string;
  detections: string[];
}

interface RealTimeMessagesProps {
  maxMessages?: number;
  showDetectionsOnly?: boolean;
}

export function RealTimeMessages({ maxMessages = 50, showDetectionsOnly = false }: RealTimeMessagesProps) {
  const [messages, setMessages] = useState<RealtimeMessage[]>([]);
  const [detections, setDetections] = useState<any[]>([]);
  const [isPaused, setIsPaused] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  
  const { isConnected, lastMessage, error, reconnect } = useWebSocket({
    channel: showDetectionsOnly ? 'detections' : 'messages',
    autoReconnect: true
  });

  useEffect(() => {
    if (lastMessage && !isPaused) {
      if (lastMessage.event === 'new_message') {
        setMessages(prev => {
          const newMessages = [lastMessage.data, ...prev];
          return newMessages.slice(0, maxMessages);
        });
      } else if (lastMessage.event === 'new_detection') {
        setDetections(prev => {
          const newDetections = [lastMessage.data, ...prev];
          return newDetections.slice(0, maxMessages);
        });
      }
    }
  }, [lastMessage, isPaused, maxMessages]);

  useEffect(() => {
    if (!isPaused) {
      messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages, detections, isPaused]);

  const getMediaIcon = (mediaType: string | null) => {
    switch (mediaType) {
      case 'photo': return <ImageIcon className="w-4 h-4 text-green-400" />;
      case 'video': return <Video className="w-4 h-4 text-purple-400" />;
      case 'audio': return <Music className="w-4 h-4 text-orange-400" />;
      case 'document': return <FileText className="w-4 h-4 text-blue-400" />;
      default: return <Paperclip className="w-4 h-4 text-gray-400" />;
    }
  };

  const formatTime = (timestamp: string) => {
    try {
      return new Date(timestamp).toLocaleTimeString();
    } catch {
      return timestamp;
    }
  };

  return (
    <Card className="h-full flex flex-col">
      <CardHeader className="pb-2 flex flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Radio className={`w-5 h-5 ${isConnected ? 'text-green-400 animate-pulse' : 'text-gray-400'}`} />
          {showDetectionsOnly ? 'Detecciones en Tiempo Real' : 'Mensajes en Tiempo Real'}
        </CardTitle>
        <div className="flex items-center gap-2">
          {isConnected ? (
            <span className="flex items-center gap-1 text-xs text-green-400">
              <Wifi className="w-3 h-3" /> Conectado
            </span>
          ) : (
            <span className="flex items-center gap-1 text-xs text-red-400">
              <WifiOff className="w-3 h-3" /> Desconectado
            </span>
          )}
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setIsPaused(!isPaused)}
          >
            {isPaused ? 'Reanudar' : 'Pausar'}
          </Button>
          {!isConnected && (
            <Button variant="outline" size="sm" onClick={reconnect}>
              Reconectar
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent className="flex-1 overflow-auto">
        {error && (
          <div className="text-center py-4 text-red-400">
            <AlertTriangle className="w-8 h-8 mx-auto mb-2" />
            <p>{error}</p>
          </div>
        )}

        {!showDetectionsOnly && messages.length === 0 && (
          <div className="text-center py-8 text-muted-foreground">
            <MessageSquare className="w-12 h-12 mx-auto mb-4 opacity-50" />
            <p>Esperando mensajes en tiempo real...</p>
            <p className="text-sm">Los mensajes nuevos apareceran automaticamente</p>
          </div>
        )}

        {showDetectionsOnly && detections.length === 0 && (
          <div className="text-center py-8 text-muted-foreground">
            <AlertTriangle className="w-12 h-12 mx-auto mb-4 opacity-50" />
            <p>Esperando detecciones...</p>
            <p className="text-sm">Las detecciones nuevas apareceran automaticamente</p>
          </div>
        )}

        {!showDetectionsOnly && (
          <div className="space-y-2">
            {messages.map((msg, index) => (
              <div
                key={`${msg.id}-${index}`}
                className={`p-3 rounded-lg bg-secondary/30 hover:bg-secondary/50 transition-all ${
                  msg.detections?.length > 0 ? 'ring-1 ring-yellow-500/50' : ''
                }`}
              >
                <div className="flex items-start gap-3">
                  <div className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center flex-shrink-0 overflow-hidden">
                    {msg.sender_photo ? (
                      <img 
                        src={`/${msg.sender_photo}`} 
                        alt={msg.sender_name} 
                        className="w-full h-full object-cover"
                        onError={(e) => {
                          (e.target as HTMLImageElement).style.display = 'none';
                          (e.target as HTMLImageElement).parentElement!.innerHTML = '<svg class="w-4 h-4 text-primary" fill="currentColor" viewBox="0 0 24 24"><path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z"/></svg>';
                        }}
                      />
                    ) : (
                      <User className="w-4 h-4 text-primary" />
                    )}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 text-sm">
                      {msg.sender_id ? (
                        <Link 
                          to={`/users/${msg.sender_id}`} 
                          className="font-medium truncate text-cyan-400 hover:underline cursor-pointer"
                        >
                          {msg.sender_name}
                        </Link>
                      ) : (
                        <span className="font-medium truncate">{msg.sender_name}</span>
                      )}
                      <span className="text-muted-foreground">en</span>
                      <Link 
                        to={`/groups?id=${msg.group_id}`}
                        className="text-primary truncate hover:underline cursor-pointer"
                      >
                        {msg.group_name}
                      </Link>
                      <span className="text-xs text-muted-foreground ml-auto">{formatTime(msg.timestamp)}</span>
                    </div>
                    <p className="text-sm mt-1 break-words">{msg.text || '[Sin texto]'}</p>
                    {msg.has_media && (
                      <div className="flex items-center gap-1 mt-1 text-xs text-muted-foreground">
                        {getMediaIcon(msg.media_type)}
                        <span>{msg.media_type || 'archivo'}</span>
                      </div>
                    )}
                    {msg.detections?.length > 0 && (
                      <div className="flex flex-wrap gap-1 mt-2">
                        {msg.detections.map((det, i) => (
                          <span key={i} className="text-xs bg-yellow-500/20 text-yellow-400 px-2 py-0.5 rounded">
                            {det}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            ))}
            <div ref={messagesEndRef} />
          </div>
        )}

        {showDetectionsOnly && (
          <div className="space-y-2">
            {detections.map((det, index) => (
              <div
                key={index}
                className="p-3 rounded-lg bg-yellow-500/10 border border-yellow-500/20"
              >
                <div className="flex items-center gap-2 text-sm">
                  <AlertTriangle className="w-4 h-4 text-yellow-400" />
                  <span className="font-medium text-yellow-400">{det.pattern_name || det.type}</span>
                  <span className="text-xs text-muted-foreground ml-auto">{formatTime(det.timestamp)}</span>
                </div>
                <p className="text-sm mt-1 font-mono bg-secondary/50 rounded px-2 py-1">
                  {det.matched_text}
                </p>
                <div className="text-xs text-muted-foreground mt-1">
                  De: {det.sender_name} en {det.group_name}
                </div>
              </div>
            ))}
            <div ref={messagesEndRef} />
          </div>
        )}
      </CardContent>
    </Card>
  );
}
