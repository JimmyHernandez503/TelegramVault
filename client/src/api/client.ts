const API_BASE = '/api/v1';

class ApiClient {
  private token: string | null = null;

  constructor() {
    this.token = localStorage.getItem('token');
  }

  setToken(token: string) {
    this.token = token;
    localStorage.setItem('token', token);
  }

  clearToken() {
    this.token = null;
    localStorage.removeItem('token');
  }

  private async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...options.headers as Record<string, string>,
    };

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    const response = await fetch(`${API_BASE}${endpoint}`, {
      ...options,
      headers,
    });

    if (response.status === 401) {
      this.clearToken();
      window.location.href = '/login';
      throw new Error('Unauthorized');
    }

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Request failed' }));
      throw new Error(error.detail || 'Request failed');
    }

    return response.json();
  }

  async login(username: string, password: string) {
    const data = await this.request<{ access_token: string }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    this.setToken(data.access_token);
    return data;
  }

  async register(username: string, password: string) {
    const data = await this.request<{ id: number; username: string }>('/auth/register', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    return data;
  }

  async getMe() {
    return this.request<{ id: number; username: string }>('/auth/me');
  }

  async getDashboardStats() {
    return this.request<{
      total_messages: number;
      total_users: number;
      total_groups: number;
      total_media: number;
      total_detections: number;
      active_accounts: number;
      total_accounts: number;
      pending_invites: number;
      backfills_in_progress: number;
      ocr_pending: number;
    }>('/stats/dashboard');
  }

  async getLiveStats() {
    return this.request<{
      uptime_seconds: number;
      messages: { per_second: number; per_minute: number; last_minute: number; last_hour: number };
      media: { per_second: number; per_minute: number; last_minute: number; last_hour: number; queued: number };
      members: { per_second: number; per_minute: number; last_minute: number; last_hour: number };
      detections: { per_second: number; per_minute: number; last_minute: number; last_hour: number };
      users: { per_second: number; per_minute: number; last_minute: number; last_hour: number };
      stories: { per_minute: number; last_hour: number };
      backfill: { per_second: number; per_minute: number; last_minute: number };
    }>('/stats/live');
  }

  async getAccounts() {
    return this.request<any[]>('/accounts/');
  }

  async createAccount(data: any) {
    return this.request('/accounts/', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  async deleteAccount(id: number) {
    return this.request(`/accounts/${id}`, { method: 'DELETE' });
  }

  async getGroups() {
    return this.request<any[]>('/groups/');
  }

  async getUsers(params?: { watchlist_only?: boolean; favorites_only?: boolean; search?: string }) {
    const searchParams = new URLSearchParams();
    if (params?.watchlist_only) searchParams.set('watchlist_only', 'true');
    if (params?.favorites_only) searchParams.set('favorites_only', 'true');
    if (params?.search) searchParams.set('search', params.search);
    const query = searchParams.toString();
    return this.request<any[]>(`/users/${query ? `?${query}` : ''}`);
  }

  async getInvites() {
    return this.request<any[]>('/invites/');
  }

  async createInvite(link: string) {
    return this.request('/invites/', {
      method: 'POST',
      body: JSON.stringify({ link }),
    });
  }

  isAuthenticated() {
    return !!this.token;
  }

  async get<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint);
  }

  async post<T>(endpoint: string, data?: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: 'POST',
      body: data ? JSON.stringify(data) : undefined,
    });
  }

  async put<T>(endpoint: string, data?: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: 'PUT',
      body: data ? JSON.stringify(data) : undefined,
    });
  }

  async delete<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint, { method: 'DELETE' });
  }

  async patch<T>(endpoint: string, data?: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: 'PATCH',
      body: data ? JSON.stringify(data) : undefined,
    });
  }

  async getSettings() {
    return this.get<{ configs: any; categories: any }>('/settings/');
  }

  async updateSettings(configs: Record<string, any>) {
    return this.put('/settings/', { configs });
  }

  async getDomainWatchlist() {
    return this.get<{ domains: any[] }>('/settings/watchlist/domains');
  }

  async addDomainToWatchlist(domain: string, description?: string) {
    return this.post('/settings/watchlist/domains', { domain, description });
  }

  async removeDomainFromWatchlist(id: number) {
    return this.delete(`/settings/watchlist/domains/${id}`);
  }

  async scrapeGroupMembers(accountId: number, groupId: number) {
    return this.post<{ task_id: string }>(`/telegram/${accountId}/scrape-members/${groupId}`);
  }

  async scrapeAllMembers(accountId: number) {
    return this.post<{ task_id: string }>(`/telegram/${accountId}/scrape-all-members`);
  }

  async getGroup(id: number) {
    return this.get<any>(`/groups/${id}`);
  }
}

export const api = new ApiClient();
