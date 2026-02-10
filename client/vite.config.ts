import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// Read backend URL from environment variable with fallback to default
const backendUrl = process.env.VITE_BACKEND_URL || 'http://127.0.0.1:8000'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
      '@assets': path.resolve(__dirname, '../attached_assets'),
    },
  },
  server: {
    host: '0.0.0.0',
    port: 5000,
    strictPort: true,
    allowedHosts: true,
    proxy: {
      '/api': {
        target: backendUrl,
        changeOrigin: true,
      },
      '/ws': {
        target: backendUrl.replace('http://', 'ws://').replace('https://', 'wss://'),
        ws: true,
        changeOrigin: true,
      },
      '/media': {
        target: backendUrl,
        changeOrigin: true,
      },
    },
  },
})
