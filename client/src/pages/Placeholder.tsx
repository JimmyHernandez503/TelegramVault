import { Card, CardContent } from '@/components/ui/card';
import { Construction } from 'lucide-react';

interface PlaceholderPageProps {
  title: string;
  description: string;
}

export function PlaceholderPage({ title, description }: PlaceholderPageProps) {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">{title}</h1>
        <p className="text-muted-foreground">{description}</p>
      </div>

      <Card>
        <CardContent className="py-20 text-center">
          <Construction className="mx-auto h-16 w-16 text-muted-foreground mb-4" />
          <p className="text-xl font-medium text-muted-foreground">En construccion</p>
          <p className="text-sm text-muted-foreground mt-2">
            Esta seccion estara disponible proximamente
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
