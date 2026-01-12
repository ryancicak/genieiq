/**
 * Auth Routes
 */

const { Router } = require('express');
const { authMiddleware } = require('../middleware/auth.js');

const router = Router();

// GET /api/auth/me
router.get('/me', authMiddleware, (req, res) => {
  res.json({
    email: req.user.email,
    id: req.user.id,
    isAdmin: req.user.isAdmin
  });
});

// GET /api/auth/status
router.get('/status', (req, res) => {
  const userEmail =
    req.headers['x-forwarded-email'] ||
    req.headers['x-user-email'] ||
    req.headers['x-databricks-user-email'];
  const isDev = process.env.NODE_ENV !== 'production';
  const effectiveEmail = userEmail || (isDev ? (process.env.DEV_USER_EMAIL || 'dev@example.com') : null);

  res.json({
    authenticated: !!effectiveEmail,
    mode: isDev ? 'dev' : 'databricks-app'
  });
});

module.exports = router;
