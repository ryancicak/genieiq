/**
 * API Client for GenieIQ
 */

const API_BASE = '/api';

async function fetchAPI(endpoint, options = {}) {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers
    }
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Request failed' }));
    throw new Error(error.error || 'API request failed');
  }

  return response.json();
}

// Auth
export async function getAuthStatus() {
  return fetchAPI('/auth/status');
}

export async function getCurrentUser() {
  return fetchAPI('/auth/me');
}

export async function getHealth() {
  return fetchAPI('/health');
}

// Spaces
export async function getSpaces({ page = 1, pageSize = 12, q = '', sort = 'scanned_desc', scannedByMe = false } = {}) {
  const params = new URLSearchParams();
  params.set('page', String(page));
  params.set('page_size', String(pageSize));
  if (q) params.set('q', q);
  if (sort) params.set('sort', sort);
  if (scannedByMe) params.set('scanned_by_me', 'true'); // legacy
  return fetchAPI(`/spaces?${params.toString()}`);
}

export async function getSpace(id, refresh = false) {
  return fetchAPI(`/spaces/${id}${refresh ? '?refresh=true' : ''}`);
}

export async function getAllSpacesPage({ pageToken = null, pageSize = 50, scannedByMe = false } = {}) {
  const params = new URLSearchParams();
  params.set('page_size', String(pageSize));
  if (pageToken) params.set('page_token', pageToken);
  if (scannedByMe) params.set('scanned_by_me', 'true'); // legacy
  return fetchAPI(`/spaces/all?${params.toString()}`);
}

export async function getAllSpacesPageNew({ pageToken = null, pageSize = 50, days = 7 } = {}) {
  const params = new URLSearchParams();
  params.set('page_size', String(pageSize));
  params.set('new_only', 'true');
  params.set('new_days', String(days));
  if (pageToken) params.set('page_token', pageToken);
  return fetchAPI(`/spaces/all?${params.toString()}`);
}

export async function getSpacesStarred({ page = 1, pageSize = 12, q = '', sort = 'scanned_desc' } = {}) {
  const params = new URLSearchParams();
  params.set('page', String(page));
  params.set('page_size', String(pageSize));
  params.set('starred_only', 'true');
  if (q) params.set('q', q);
  if (sort) params.set('sort', sort);
  return fetchAPI(`/spaces?${params.toString()}`);
}

export async function getAllSpacesPageStarred({ pageToken = null, pageSize = 50 } = {}) {
  const params = new URLSearchParams();
  params.set('page_size', String(pageSize));
  params.set('starred_only', 'true');
  if (pageToken) params.set('page_token', pageToken);
  return fetchAPI(`/spaces/all?${params.toString()}`);
}

export async function getNewSpacesFeed({ days = 7, limit = 10 } = {}) {
  const params = new URLSearchParams();
  params.set('days', String(days));
  params.set('limit', String(limit));
  return fetchAPI(`/spaces/new?${params.toString()}`);
}

export async function scanSpace(id) {
  return fetchAPI(`/spaces/${id}/scan`, { method: 'POST' });
}

export async function getSpaceHistory(id, { days = 30, limit = 90 } = {}) {
  const params = new URLSearchParams();
  if (days != null) params.set('days', String(days));
  if (limit != null) params.set('limit', String(limit));
  const qs = params.toString();
  return fetchAPI(`/spaces/${id}/history${qs ? `?${qs}` : ''}`);
}

export async function setSpaceStar(id, starred) {
  return fetchAPI(`/spaces/${id}/star`, {
    method: 'PUT',
    body: JSON.stringify({ starred: Boolean(starred) })
  });
}

export async function generateFixNotebook(id) {
  return fetchAPI(`/spaces/${id}/fix-notebook`, { method: 'POST' });
}

// Admin
export async function getAdminDashboard() {
  return fetchAPI('/admin/dashboard');
}

export async function getLeaderboard() {
  return fetchAPI('/admin/leaderboard');
}

export async function scanAllSpaces() {
  return fetchAPI('/admin/scan-all', { method: 'POST' });
}

export async function startScanAllJob({ concurrency = 2, delayMs = 250, limit = null } = {}) {
  return fetchAPI('/admin/scan-all-job', {
    method: 'POST',
    body: JSON.stringify({ concurrency, delayMs, limit })
  });
}

export async function getScanAllJob(jobId) {
  return fetchAPI(`/admin/scan-all-job/${jobId}`);
}

export async function getAlerts() {
  return fetchAPI('/admin/alerts');
}

export default {
  getAuthStatus,
  getCurrentUser,
  getHealth,
  getSpaces,
  getAllSpacesPage,
  getAllSpacesPageNew,
  getSpacesStarred,
  getAllSpacesPageStarred,
  getSpace,
  scanSpace,
  getSpaceHistory,
  setSpaceStar,
  generateFixNotebook,
  getNewSpacesFeed,
  getAdminDashboard,
  getLeaderboard,
  scanAllSpaces,
  startScanAllJob,
  getScanAllJob,
  getAlerts
};

