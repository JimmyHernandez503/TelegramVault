import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '@/api/client';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Database, Activity, Users, MessageSquare, HardDrive, TrendingUp, Send } from 'lucide-react';

export function LoginPage() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState({
    users: 0,
    messages: 0,
    storage: 0,
    uptime: 0
  });
  const navigate = useNavigate();

  useEffect(() => {
    // Fetch real system statistics
    const fetchStats = async () => {
      try {
        const response = await fetch('/api/v1/stats/public/system');
        if (response.ok) {
          const data = await response.json();
          setStats({
            users: data.users,
            messages: data.messages,
            storage: data.storage_gb,
            uptime: data.uptime
          });
        }
      } catch (err) {
        console.error('Failed to fetch stats:', err);
        // Keep showing 0 values if fetch fails
      }
    };

    // Fetch immediately
    fetchStats();
    
    // Refresh every 30 seconds
    const interval = setInterval(fetchStats, 30000);

    return () => clearInterval(interval);
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      await api.login(username, password);
      navigate('/');
    } catch (err: any) {
      setError(err.message || 'Error al iniciar sesion');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="relative flex min-h-screen overflow-hidden bg-gradient-to-br from-slate-950 via-blue-950 to-slate-950">
      {/* Animated background grid */}
      <div className="absolute inset-0 bg-[linear-gradient(to_right,#4f4f4f12_1px,transparent_1px),linear-gradient(to_bottom,#4f4f4f12_1px,transparent_1px)] bg-[size:4rem_4rem] [mask-image:radial-gradient(ellipse_80%_50%_at_50%_0%,#000_70%,transparent_110%)]" />
      
      {/* Animated particles */}
      <div className="absolute inset-0 overflow-hidden">
        {[...Array(20)].map((_, i) => (
          <div
            key={i}
            className="absolute h-1 w-1 rounded-full bg-blue-400/30 animate-pulse"
            style={{
              left: `${Math.random() * 100}%`,
              top: `${Math.random() * 100}%`,
              animationDelay: `${Math.random() * 3}s`,
              animationDuration: `${2 + Math.random() * 3}s`
            }}
          />
        ))}
      </div>

      {/* Stats sidebar */}
      <div className="hidden lg:flex lg:w-1/3 relative z-10 flex-col justify-center p-12 space-y-6">
        <div className="space-y-2 mb-8 animate-fade-in">
          <div className="flex items-center gap-3 mb-4">
            <div className="relative">
              <Send className="h-12 w-12 text-blue-400" />
              <div className="absolute inset-0 blur-xl bg-blue-400/30 animate-pulse" />
            </div>
            <div>
              <h1 className="text-4xl font-bold text-white">TelegramVault</h1>
              <p className="text-blue-300/70 text-sm">Intelligence System by OceanoSV Team</p>
            </div>
          </div>
          <p className="text-slate-300/80 text-lg">
            Sistema avanzado de análisis y gestión de datos de Telegram
          </p>
        </div>

        <div className="space-y-4">
          <StatCard
            icon={<Users className="h-5 w-5" />}
            label="Usuarios Activos"
            value={stats.users.toLocaleString()}
            trend="+12%"
            delay="0s"
          />
          <StatCard
            icon={<MessageSquare className="h-5 w-5" />}
            label="Mensajes Procesados"
            value={stats.messages.toLocaleString()}
            trend="+8%"
            delay="0.1s"
          />
          <StatCard
            icon={<HardDrive className="h-5 w-5" />}
            label="Almacenamiento"
            value={`${stats.storage.toFixed(1)} GB`}
            trend="+5%"
            delay="0.2s"
          />
          <StatCard
            icon={<Activity className="h-5 w-5" />}
            label="Uptime del Sistema"
            value={`${stats.uptime.toFixed(1)}%`}
            trend="Estable"
            delay="0.3s"
          />
        </div>

        <div className="mt-8 p-4 rounded-lg bg-purple-500/10 border border-purple-500/20 backdrop-blur-sm animate-fade-in" style={{ animationDelay: '0.4s' }}>
          <div className="flex items-center gap-2 text-purple-300 mb-2">
            <Activity className="h-4 w-4" />
            <span className="text-sm font-medium">Vigilancia en Tiempo Real</span>
          </div>
          <p className="text-xs text-slate-400">
            Monitoreo masivo y análisis continuo de actividad en Telegram
          </p>
        </div>
      </div>

      {/* Login form */}
      <div className="flex-1 flex items-center justify-center p-4 relative z-10">
        <Card className="w-full max-w-md bg-slate-900/50 backdrop-blur-xl border-slate-700/50 shadow-2xl animate-slide-up">
          <CardHeader className="text-center space-y-4">
            <div className="mx-auto mb-2 relative">
              <div className="flex h-20 w-20 items-center justify-center rounded-full bg-gradient-to-br from-blue-500 to-cyan-500 shadow-lg shadow-blue-500/50 animate-float">
                <Send className="h-10 w-10 text-white" />
              </div>
              <div className="absolute inset-0 rounded-full bg-blue-500/20 blur-2xl animate-pulse" />
            </div>
            <div className="space-y-2">
              <CardTitle className="text-3xl font-bold bg-gradient-to-r from-blue-400 to-cyan-400 bg-clip-text text-transparent">
                TelegramVault
              </CardTitle>
              <CardDescription className="text-slate-400">
                Iniciar sesión en el sistema
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleSubmit} className="space-y-5">
              {error && (
                <div className="rounded-lg bg-red-500/10 border border-red-500/20 p-3 text-sm text-red-400 animate-shake">
                  {error}
                </div>
              )}
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-300">Usuario</label>
                <Input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="admin"
                  required
                  className="bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-500 focus:border-blue-500 focus:ring-blue-500/20 transition-all"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-300">Contraseña</label>
                <Input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  required
                  className="bg-slate-800/50 border-slate-700 text-white placeholder:text-slate-500 focus:border-blue-500 focus:ring-blue-500/20 transition-all"
                />
              </div>
              <Button 
                type="submit" 
                className="w-full bg-gradient-to-r from-blue-600 to-cyan-600 hover:from-blue-700 hover:to-cyan-700 text-white font-medium shadow-lg shadow-blue-500/30 transition-all hover:shadow-blue-500/50 hover:scale-[1.02] disabled:opacity-50 disabled:cursor-not-allowed" 
                disabled={loading}
              >
                {loading ? (
                  <span className="flex items-center gap-2">
                    <div className="h-4 w-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    Verificando...
                  </span>
                ) : (
                  'Iniciar sesión'
                )}
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>

      <style>{`
        @keyframes fade-in {
          from { opacity: 0; transform: translateY(10px); }
          to { opacity: 1; transform: translateY(0); }
        }
        @keyframes slide-up {
          from { opacity: 0; transform: translateY(30px); }
          to { opacity: 1; transform: translateY(0); }
        }
        @keyframes float {
          0%, 100% { transform: translateY(0px); }
          50% { transform: translateY(-10px); }
        }
        @keyframes shake {
          0%, 100% { transform: translateX(0); }
          25% { transform: translateX(-5px); }
          75% { transform: translateX(5px); }
        }
        .animate-fade-in {
          animation: fade-in 0.6s ease-out forwards;
          opacity: 0;
        }
        .animate-slide-up {
          animation: slide-up 0.8s ease-out;
        }
        .animate-float {
          animation: float 3s ease-in-out infinite;
        }
        .animate-shake {
          animation: shake 0.4s ease-in-out;
        }
      `}</style>
    </div>
  );
}

function StatCard({ icon, label, value, trend, delay }: { 
  icon: React.ReactNode; 
  label: string; 
  value: string; 
  trend: string;
  delay: string;
}) {
  return (
    <div 
      className="group p-4 rounded-lg bg-slate-800/30 backdrop-blur-sm border border-slate-700/50 hover:border-blue-500/50 transition-all hover:bg-slate-800/50 animate-fade-in"
      style={{ animationDelay: delay }}
    >
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-blue-500/10 text-blue-400 group-hover:bg-blue-500/20 transition-colors">
            {icon}
          </div>
          <span className="text-sm text-slate-400">{label}</span>
        </div>
        <div className="flex items-center gap-1 text-xs text-green-400">
          <TrendingUp className="h-3 w-3" />
          {trend}
        </div>
      </div>
      <div className="text-2xl font-bold text-white ml-11">{value}</div>
    </div>
  );
}
