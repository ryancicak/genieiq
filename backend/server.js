/**
 * GenieIQ Server
 */

require('dotenv').config();

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const path = require('path');

const authRoutes = require('./routes/auth.js');
const spacesRoutes = require('./routes/spaces.js');
const adminRoutes = require('./routes/admin.js');
const { authMiddleware } = require('./middleware/auth.js');
const lakebase = require('./services/lakebase.js');

const app = express();
const PORT = process.env.DATABRICKS_APP_PORT || process.env.PORT || 3001;
const IS_PROD = process.env.NODE_ENV === 'production';

// API responses should never be cached in the browser/proxies (prevents “blank UI” after deploys).
app.set('etag', false);

// Middleware
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(morgan('combined'));
app.use(express.json());

app.use('/api', (req, res, next) => {
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  next();
});

// API Routes
app.use('/api/auth', authRoutes);
app.use('/api/spaces', authMiddleware, spacesRoutes);
app.use('/api/admin', authMiddleware, adminRoutes);

// Health check
app.get('/api/health', async (req, res) => {
  // Try to infer storage mode using the caller's proxy token when present.
  const userEmail =
    req.headers['x-forwarded-email'] ||
    req.headers['x-user-email'] ||
    req.headers['x-databricks-user-email'] ||
    null;
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

  const dbHealth = await lakebase
    .healthCheck({ userEmail, token })
    .catch(() => ({ status: 'not configured' }));
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    database: dbHealth,
    env: {
      hasHost: !!process.env.DATABRICKS_HOST,
      hasToken: !!process.env.DATABRICKS_TOKEN,
      nodeEnv: process.env.NODE_ENV
    }
  });
});

// Serve frontend only in production (dev uses Vite on :5173)
if (IS_PROD) {
  const frontendPath = path.join(__dirname, '../frontend/dist');

  app.use(express.static(frontendPath, {
    setHeaders(res, filePath) {
      // Prevent “stuck UI” after deploys: never cache the HTML shell.
      if (filePath.endsWith('index.html')) {
        res.setHeader('Cache-Control', 'no-store');
        return;
      }
      // Hashed assets can be cached aggressively.
      res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    }
  }));

  app.get('*', (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.sendFile(path.join(frontendPath, 'index.html'));
  });
}

app.listen(PORT, async () => {
  console.log(`
  ╔═══════════════════════════════════════════════╗
  ║   GenieIQ Server - Port ${PORT}                  ║
  ╚═══════════════════════════════════════════════╝
  `);
  
  // Auto-initialize Database
  await lakebase.initializeDatabase();
});

module.exports = app;
