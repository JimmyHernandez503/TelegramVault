import { Link, useLocation } from 'react-router-dom';
import { cn } from '@/lib/utils';
import {
  LayoutDashboard,
  Users,
  MessageSquare,
  Image,
  Settings,
  Key,
  Link2,
  AlertTriangle,
  Eye,
  Database,
  Radio,
  Camera,
  Search
} from 'lucide-react';

const navigation = [
  { name: 'Dashboard', href: '/', icon: LayoutDashboard },
  { name: 'Busqueda', href: '/search', icon: Search },
  { name: 'Cuentas', href: '/accounts', icon: Key },
  { name: 'Grupos', href: '/groups', icon: MessageSquare },
  { name: 'Monitoreo', href: '/monitoring', icon: Radio },
  { name: 'Usuarios', href: '/users', icon: Users },
  { name: 'Galeria', href: '/gallery', icon: Image },
  { name: 'Detecciones', href: '/detections', icon: AlertTriangle },
  { name: 'Invitaciones', href: '/invites', icon: Link2 },
  { name: 'Watchlist', href: '/watchlist', icon: Eye },
  { name: 'Stories', href: '/stories', icon: Camera },
  { name: 'Configuracion', href: '/settings', icon: Settings },
];

export function Sidebar() {
  const location = useLocation();

  return (
    <div className="flex h-full w-64 flex-col bg-card border-r border-border">
      <div className="flex h-16 items-center gap-2 px-6 border-b border-border">
        <Database className="h-8 w-8 text-primary" />
        <span className="text-xl font-bold text-foreground">TelegramVault</span>
      </div>
      <nav className="flex-1 space-y-1 px-3 py-4">
        {navigation.map((item) => {
          const isActive = location.pathname === item.href;
          return (
            <Link
              key={item.name}
              to={item.href}
              className={cn(
                'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                isActive
                  ? 'bg-primary text-primary-foreground'
                  : 'text-muted-foreground hover:bg-secondary hover:text-foreground'
              )}
            >
              <item.icon className="h-5 w-5" />
              {item.name}
            </Link>
          );
        })}
      </nav>
    </div>
  );
}
