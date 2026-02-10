import { useState, useEffect, useCallback, useRef } from 'react'

interface WSMessage {
  event: string
  data: any
  timestamp: string
}

interface UseWebSocketOptions {
  channel?: string
  autoReconnect?: boolean
  reconnectDelay?: number
}

export function useWebSocket(options: UseWebSocketOptions = {}) {
  const { 
    channel = 'general', 
    autoReconnect = true, 
    reconnectDelay = 3000 
  } = options
  
  const [isConnected, setIsConnected] = useState(false)
  const [lastMessage, setLastMessage] = useState<WSMessage | null>(null)
  const [error, setError] = useState<string | null>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null)

  const connect = useCallback(() => {
    const token = localStorage.getItem('token')
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.host
    const url = `${protocol}//${host}/ws?channel=${channel}${token ? `&token=${token}` : ''}`
    
    try {
      const ws = new WebSocket(url)
      wsRef.current = ws
      
      ws.onopen = () => {
        setIsConnected(true)
        setError(null)
        console.log('WebSocket connected')
      }
      
      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data) as WSMessage
          setLastMessage(message)
        } catch (e) {
          console.error('Failed to parse WebSocket message:', e)
        }
      }
      
      ws.onerror = (event) => {
        setError('WebSocket error')
        console.error('WebSocket error:', event)
      }
      
      ws.onclose = () => {
        setIsConnected(false)
        wsRef.current = null
        
        if (autoReconnect) {
          reconnectTimeoutRef.current = setTimeout(() => {
            connect()
          }, reconnectDelay)
        }
      }
    } catch (e) {
      setError('Failed to connect')
      console.error('WebSocket connection error:', e)
    }
  }, [channel, autoReconnect, reconnectDelay])

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current)
    }
    if (wsRef.current) {
      wsRef.current.close()
      wsRef.current = null
    }
    setIsConnected(false)
  }, [])

  const send = useCallback((event: string, data: any = {}) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ event, data }))
    }
  }, [])

  const subscribe = useCallback((newChannel: string) => {
    send('subscribe', { channel: newChannel })
  }, [send])

  useEffect(() => {
    connect()
    return () => {
      disconnect()
    }
  }, [connect, disconnect])

  return {
    isConnected,
    lastMessage,
    error,
    send,
    subscribe,
    disconnect,
    reconnect: connect
  }
}
