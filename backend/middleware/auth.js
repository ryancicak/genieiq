/**
 * Authentication Middleware
 * 
 * In Databricks Apps, the proxy passes user identity and OAuth token via headers.
 */

const { getDatabricksClient } = require('../services/databricksClient.js');

async function authMiddleware(req, res, next) {
  try {
    // Extract user info from Databricks Apps proxy headers
    const userEmail = req.headers['x-forwarded-email'] || 
                      req.headers['x-user-email'] ||
                      req.headers['x-databricks-user-email'];
    const userId = req.headers['x-forwarded-user'] || 
                   req.headers['x-user-id'] ||
                   req.headers['x-databricks-user-id'];
    
    // Extract OAuth token from Databricks Apps proxy headers.
    // Prefer user token for per-user access control, but allow falling back to
    // service principal OAuth (handled inside DatabricksClient) when missing.
    const authz =
      req.headers['x-forwarded-authorization'] ||
      req.headers['authorization'] ||
      null;
    const bearerFromAuthz =
      typeof authz === 'string' && authz.toLowerCase().startsWith('bearer ')
        ? authz.slice(7).trim()
        : null;

    const token =
      req.headers['x-forwarded-access-token'] ||
      req.headers['x-databricks-oauth-token'] ||
      req.headers['x-databricks-token'] ||
      bearerFromAuthz ||
      null;

    // Local development fallback (no Databricks proxy headers)
    const isDev = process.env.NODE_ENV !== 'production';
    const effectiveEmail = userEmail || (isDev ? (process.env.DEV_USER_EMAIL || 'dev@example.com') : null);
    const effectiveUserId = userId || (isDev ? (process.env.DEV_USER_ID || 'dev-user') : null);

    if (!effectiveEmail) {
      return res.status(401).json({ error: 'Unauthenticated' });
    }

    const isAdmin = await checkAdminStatus(effectiveEmail);

    req.user = {
      email: effectiveEmail,
      id: effectiveUserId,
      isAdmin
    };

    // Create Databricks client with the token from request
    req.databricks = getDatabricksClient({ token });
    // Expose the user token for Lakebase Postgres auth (PGPASSWORD-style).
    req.userToken = token;
    
    next();
  } catch (error) {
    console.error('Auth middleware error:', error);
    return res.status(401).json({ error: 'Unauthenticated' });
  }
}

async function checkAdminStatus(email) {
  const adminEmails = (process.env.ADMIN_EMAILS || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  if (adminEmails.includes(email)) {
    return true;
  }
  // Optional dev override for quick local testing.
  if (process.env.NODE_ENV !== 'production' && process.env.DEV_IS_ADMIN === 'true') {
    return true;
  }
  return false;
}

function requireAdmin(req, res, next) {
  if (!req.user?.isAdmin) {
    return res.status(403).json({ error: 'Admin access required' });
  }
  next();
}

module.exports = { authMiddleware, requireAdmin };
