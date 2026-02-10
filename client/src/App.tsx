import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Layout } from './components/Layout';
import { LoginPage } from './pages/Login';
import { DashboardPage } from './pages/Dashboard';
import { AccountsPage } from './pages/Accounts';
import { GroupsPage } from './pages/Groups';
import { GroupChatPage } from './pages/GroupChat';
import { UsersPage } from './pages/Users';
import UserProfilePage from './pages/UserProfile';
import { InvitesPage } from './pages/Invites';
import { PlaceholderPage } from './pages/Placeholder';
import DetectionsPage from './pages/Detections';
import SettingsPage from './pages/Settings';
import MultimediaPage from './pages/Multimedia';
import WatchlistPage from './pages/Watchlist';
import MonitoringPage from './pages/Monitoring';
import StoriesPage from './pages/Stories';
import SearchPage from './pages/Search';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/" element={<Layout />}>
          <Route index element={<DashboardPage />} />
          <Route path="accounts" element={<AccountsPage />} />
          <Route path="groups" element={<GroupsPage />} />
          <Route path="groups/:groupId" element={<GroupChatPage />} />
          <Route path="monitoring" element={<MonitoringPage />} />
          <Route path="users" element={<UsersPage />} />
          <Route path="users/:userId" element={<UserProfilePage />} />
          <Route path="invites" element={<InvitesPage />} />
          <Route path="gallery" element={<MultimediaPage />} />
          <Route path="detections" element={<DetectionsPage />} />
          <Route path="watchlist" element={<WatchlistPage />} />
          <Route path="stories" element={<StoriesPage />} />
          <Route path="search" element={<SearchPage />} />
          <Route path="settings" element={<SettingsPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;
