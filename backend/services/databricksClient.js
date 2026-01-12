/**
 * Databricks API Client
 * 
 * Uses OAuth Client Credentials flow with the app's service principal.
 */

// Cache tokens per OAuth scope (different APIs require different scopes).
// Map<scope, { token: string, expiry: number }>
const tokenCache = new Map();

function decodeJwtClaims(token) {
  try {
    const parts = token.split('.');
    if (parts.length < 2) return null;
    const payload = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const padded = payload + '='.repeat((4 - (payload.length % 4)) % 4);
    const json = Buffer.from(padded, 'base64').toString('utf8');
    return JSON.parse(json);
  } catch {
    return null;
  }
}

/**
 * Get OAuth token using client credentials flow
 */
async function getServicePrincipalToken(scope = 'all-apis') {
  const cached = tokenCache.get(scope);
  if (cached?.token && Date.now() < cached.expiry - 60000) {
    return cached.token;
  }

  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;
  const host = process.env.DATABRICKS_HOST;

  if (!clientId || !clientSecret || !host) {
    console.warn('Missing client credentials:', { 
      clientId: !!clientId, 
      clientSecret: !!clientSecret, 
      host: !!host 
    });
    return null;
  }

  // Clean up host
  const baseUrl = host.startsWith('http') ? host : `https://${host}`;
  const tokenUrl = `${baseUrl}/oidc/v1/token`;

  console.log(`🔑 Requesting OAuth token for service principal (scope: ${scope})...`);

  try {
    const response = await fetch(tokenUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: clientId,
        client_secret: clientSecret,
        scope
      })
    });

    if (!response.ok) {
      const error = await response.text();
      console.error('❌ OAuth token request failed:', response.status, error);
      return null;
    }

    const data = await response.json();
    const token = data.access_token;
    const expiry = Date.now() + (data.expires_in * 1000);
    tokenCache.set(scope, { token, expiry });
    
    const claims = token ? decodeJwtClaims(token) : null;
    console.log('✅ Got OAuth token (expires in', data.expires_in, 'seconds)');
    if (claims) {
      console.log('🔎 OAuth token claims (redacted):', {
        client_id: claims.client_id,
        scope: claims.scope,
        aud: claims.aud,
        iss: claims.iss
      });
    } else {
      console.log('🔎 OAuth token is not a JWT (cannot decode claims)');
    }
    return token;
  } catch (error) {
    console.error('❌ OAuth token error:', error.message);
    return null;
  }
}

class DatabricksClient {
  constructor(config = {}) {
    this.host = config.host || process.env.DATABRICKS_HOST;
    this.userToken = config.token; // Token from user's request (if any)
    
    // Clean up host URL
    if (this.host && !this.host.startsWith('http')) {
      this.host = `https://${this.host}`;
    }
    if (this.host) {
      this.host = this.host.replace(/\/$/, '');
    }
  }

  getEnvTokenIfAllowed() {
    // In Databricks Apps, the platform may set DATABRICKS_TOKEN, but it's not a general-purpose
    // workspace REST API token. To avoid "Invalid Token" failures, we only honor DATABRICKS_TOKEN
    // in local/dev, or when explicitly enabled.
    const allow =
      process.env.DATABRICKS_USE_ENV_TOKEN === 'true' ||
      process.env.NODE_ENV !== 'production';

    if (!allow) return null;
    return process.env.DATABRICKS_TOKEN || null;
  }

  async getTokenForEndpoint(endpoint) {
    const isGenie = typeof endpoint === 'string' && endpoint.startsWith('/genie/');
    const isSql = typeof endpoint === 'string' && endpoint.startsWith('/sql/');
    const isWorkspace = typeof endpoint === 'string' && endpoint.startsWith('/workspace/');
    const isGenieExport = typeof endpoint === 'string' && endpoint.startsWith('/genie/spaces/export');
    const isGenieGet = typeof endpoint === 'string' && endpoint.startsWith('/genie/spaces/get');
    const isGenieSpaceRead =
      typeof endpoint === 'string' &&
      endpoint.startsWith('/genie/spaces/') &&
      !endpoint.startsWith('/genie/spaces/export');

    // Genie endpoints currently require broader scopes than Databricks Apps user tokens provide.
    // Prefer service principal OAuth (or an explicit env token override) for Genie calls.
    if (isGenie) {
      // Special-case: Genie "export" is a user-driven feature and may only be accessible
      // via the Databricks Apps proxy user token. Try that first when present.
      if (isGenieExport && this.userToken) return this.userToken;
      // Special-case: Genie "get" (used by the UI for richer space settings) is often permissioned
      // on the user and may not be accessible to the app service principal.
      if (isGenieGet && this.userToken) return this.userToken;
      // Prefer the user token for read calls on a specific space (captures user-visible settings like Sample questions),
      // with a retry fallback inside `fetch` if the token is invalid-scoped.
      if (isGenieSpaceRead && this.userToken) return this.userToken;
      const envToken = this.getEnvTokenIfAllowed();
      if (envToken) return envToken;
      return await getServicePrincipalToken('all-apis');
    }

    // SQL endpoints often require the dedicated `sql` scope.
    // Use a service principal `sql`-scoped token to avoid "Invalid scope" failures.
    if (isSql) {
      const envToken = this.getEnvTokenIfAllowed();
      if (envToken) return envToken;
      const sqlToken = await getServicePrincipalToken('sql');
      if (sqlToken) return sqlToken;
    }

    // Workspace endpoints: prefer service principal OAuth.
    // Databricks Apps user tokens often lack `workspace` scope, which causes "Invalid scope"
    // for /workspace/* APIs.
    if (isWorkspace) {
      const envToken = this.getEnvTokenIfAllowed();
      if (envToken) return envToken;
      return await getServicePrincipalToken('all-apis');
    }

    // Non-Genie endpoints: prefer user token for per-user access control.
    if (this.userToken) return this.userToken;
    const envToken = this.getEnvTokenIfAllowed();
    if (envToken) return envToken;
    return await getServicePrincipalToken('all-apis');
  }

  async fetch(endpoint, options = {}) {
    if (!this.host) {
      throw new Error('DATABRICKS_HOST not configured');
    }

    const token = await this.getTokenForEndpoint(endpoint);
    if (!token) {
      throw new Error('No authentication token available');
    }

    const url = `${this.host}/api/2.0${endpoint}`;
    
    const headers = {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
      ...options.headers
    };

    console.log(`📡 Databricks API: ${options.method || 'GET'} ${endpoint}`);

    try {
      const response = await fetch(url, {
        ...options,
        headers
      });

      if (!response.ok) {
        const errorText = await response.text();

        // Retry: Genie endpoints sometimes fail with user tokens due to missing scopes.
        // If we used the user token and got a scope-related 403, retry once with the service principal token.
        const isGenie = typeof endpoint === 'string' && endpoint.startsWith('/genie/');
        const isScope403 =
          response.status === 403 &&
          /invalid scope|insufficient_scope|scope/i.test(errorText || '');
        const usedUserToken = Boolean(this.userToken) && token === this.userToken;

        if (isGenie && usedUserToken && isScope403) {
          try {
            const spToken = await getServicePrincipalToken('all-apis');
            if (spToken) {
              const retryHeaders = {
                ...headers,
                Authorization: `Bearer ${spToken}`
              };
              const retryResp = await fetch(url, { ...options, headers: retryHeaders });
              if (retryResp.ok) {
                const ct = retryResp.headers.get('content-type') || '';
                if (ct.includes('application/json')) return retryResp.json();
                const text = await retryResp.text();
                if (!text) return {};
                try {
                  return JSON.parse(text);
                } catch {
                  return { raw: text };
                }
              }
            }
          } catch {
            // fall through to original error
          }
        }

        console.error(`❌ Databricks API error: ${response.status}`, (errorText || '').substring(0, 200));
        throw new Error(`Databricks API error: ${response.status} - ${(errorText || '').substring(0, 100)}`);
      }

      // Some Databricks endpoints return empty bodies on 200.
      const contentType = response.headers.get('content-type') || '';
      if (contentType.includes('application/json')) {
        return response.json();
      }
      const text = await response.text();
      if (!text) return {};
      try {
        return JSON.parse(text);
      } catch {
        return { raw: text };
      }
    } catch (error) {
      console.error(`❌ Fetch failed:`, error.message);
      throw error;
    }
  }

  // Genie Spaces API
  async listGenieSpacesPage({ pageToken = null, pageSize = 200 } = {}) {
    const qs = [];
    qs.push(`page_size=${encodeURIComponent(String(pageSize || 200))}`);
    if (pageToken) qs.push(`page_token=${encodeURIComponent(String(pageToken))}`);
    const endpoint = `/genie/spaces${qs.length ? `?${qs.join('&')}` : ''}`;
    return await this.fetch(endpoint);
  }

  async listGenieSpaces() {
    // This endpoint is paginated in some workspaces (returns `next_page_token`).
    // We transparently page through results so callers always get the full set.
    const allSpaces = [];
    let nextToken = null;
    let safetyPages = 0;

    do {
      const resp = await this.listGenieSpacesPage({ pageToken: nextToken, pageSize: 200 });
      const spaces = resp?.spaces || resp?.rooms || [];
      if (Array.isArray(spaces)) allSpaces.push(...spaces);

      nextToken = resp?.next_page_token || resp?.nextPageToken || null;
      safetyPages += 1;

      // Safety break in case the API misbehaves and keeps returning a token.
      if (safetyPages > 50) {
        console.warn('⚠️ listGenieSpaces pagination exceeded safety limit; returning partial results');
        break;
      }
    } while (nextToken);

    return { spaces: allSpaces };
  }

  async getGenieSpace(spaceId) {
    return await this.fetch(`/genie/spaces/${spaceId}`);
  }

  async getGenieSpaceWithSerialized(spaceId) {
    // `serialized_space` contains the authoritative config (tables, instructions, etc.)
    return await this.fetch(`/genie/spaces/${spaceId}?include_serialized_space=true`);
  }

  async getGenieSpaceDetails(spaceId) {
    // Some workspaces expose richer “get” endpoint payloads (often used by the UI).
    const encoded = encodeURIComponent(spaceId);
    return await this.fetch(`/genie/spaces/get?space_id=${encoded}`);
  }

  async exportGenieSpace(spaceId) {
    // NOTE: This endpoint name is inferred from the Genie UI "Export" feature.
    // If the API shape changes, we still keep this isolated to avoid breaking the rest of the client.
    const encoded = encodeURIComponent(spaceId);
    return await this.fetch(`/genie/spaces/export?space_id=${encoded}`);
  }

  async listWarehouses() {
    return this.fetch('/sql/warehouses');
  }

  async getWarehouse(warehouseId) {
    return this.fetch(`/sql/warehouses/${warehouseId}`);
  }

  async getTable(fullName) {
    const encoded = encodeURIComponent(fullName);
    return this.fetch(`/unity-catalog/tables/${encoded}`);
  }

  async listTables(catalogName, schemaName) {
    return this.fetch(`/unity-catalog/tables?catalog_name=${catalogName}&schema_name=${schemaName}`);
  }

  async getCurrentUser() {
    return this.fetch('/preview/scim/v2/Me');
  }

  // Workspace API (for generated notebooks, etc.)
  async workspaceMkdirs(path) {
    return this.fetch('/workspace/mkdirs', {
      method: 'POST',
      body: JSON.stringify({ path })
    });
  }

  async workspaceImport({ path, contentBase64, format = 'JUPYTER', overwrite = true }) {
    return this.fetch('/workspace/import', {
      method: 'POST',
      body: JSON.stringify({
        path,
        format,
        content: contentBase64,
        overwrite
      })
    });
  }
}

function getDatabricksClient(config = {}) {
  return new DatabricksClient(config);
}

module.exports = { getDatabricksClient, DatabricksClient };
