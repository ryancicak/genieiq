-- GenieIQ Lakebase Setup
-- Run this in your Lakebase Provisioned instance (PostgreSQL-compatible)

-- Needed for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Main audit results table
CREATE TABLE IF NOT EXISTS audit_results (
  -- Primary key
  id                  SERIAL PRIMARY KEY,
  scan_id             UUID NOT NULL DEFAULT gen_random_uuid(),
  
  -- Identifiers
  space_id            VARCHAR(255) NOT NULL,
  space_name          VARCHAR(255) NOT NULL,
  space_description   TEXT,
  owner_email         VARCHAR(255),
  
  -- Scores
  total_score         INTEGER NOT NULL,
  maturity_level      VARCHAR(50) NOT NULL,
  
  -- Category breakdown
  foundation_score    INTEGER,
  foundation_max      INTEGER DEFAULT 30,
  data_setup_score    INTEGER,
  data_setup_max      INTEGER DEFAULT 25,
  sql_assets_score    INTEGER,
  sql_assets_max      INTEGER DEFAULT 25,
  optimization_score  INTEGER,
  optimization_max    INTEGER DEFAULT 20,

  -- Full breakdown object (mirrors app payload; allows future charts without schema changes)
  breakdown           JSONB,
  
  -- Detailed findings (JSONB for flexibility)
  findings            JSONB,
  next_steps          JSONB,
  
  -- Warehouse info
  warehouse_id        VARCHAR(255),
  warehouse_name      VARCHAR(255),
  warehouse_type      VARCHAR(50),
  is_serverless       BOOLEAN,
  
  -- Metadata
  scanned_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  scanned_by          VARCHAR(255),
  scan_duration_ms    INTEGER,
  
  -- Indexes for common queries
  CONSTRAINT valid_score CHECK (total_score >= 0 AND total_score <= 100)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_audit_space_id ON audit_results(space_id);
CREATE INDEX IF NOT EXISTS idx_audit_scanned_at ON audit_results(scanned_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_owner ON audit_results(owner_email);
CREATE INDEX IF NOT EXISTS idx_audit_score ON audit_results(total_score);

-- Per-user starred spaces (favorites)
CREATE TABLE IF NOT EXISTS space_stars (
  space_id    VARCHAR(255) NOT NULL,
  user_email  VARCHAR(255) NOT NULL,
  starred_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(space_id, user_email)
);
CREATE INDEX IF NOT EXISTS idx_space_stars_user ON space_stars(user_email);

-- Space discovery registry (to power "New spaces" UX when Genie API doesn't expose created_at)
CREATE TABLE IF NOT EXISTS spaces_seen (
  space_id      VARCHAR(255) PRIMARY KEY,
  first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  last_seen_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  last_name     VARCHAR(255)
);
CREATE INDEX IF NOT EXISTS idx_spaces_seen_first ON spaces_seen(first_seen_at DESC);

-- View for latest scores per space
CREATE OR REPLACE VIEW latest_scores AS
SELECT DISTINCT ON (space_id)
  *
FROM audit_results
ORDER BY space_id, scanned_at DESC;

-- View for org-level statistics
CREATE OR REPLACE VIEW org_stats AS
SELECT 
  COUNT(DISTINCT space_id) as total_spaces,
  ROUND(AVG(total_score)) as avg_score,
  COUNT(*) FILTER (WHERE total_score < 40) as critical_count,
  COUNT(*) FILTER (WHERE is_serverless = false) as shared_warehouse_count,
  COUNT(*) FILTER (WHERE maturity_level = 'optimized') as optimized_count,
  COUNT(*) FILTER (WHERE maturity_level = 'maturing') as maturing_count,
  COUNT(*) FILTER (WHERE maturity_level = 'developing') as developing_count,
  COUNT(*) FILTER (WHERE maturity_level = 'emerging') as emerging_count,
  MAX(scanned_at) as last_scan_time
FROM latest_scores;

-- Function to get score history for a space
CREATE OR REPLACE FUNCTION get_score_history(p_space_id VARCHAR, p_limit INTEGER DEFAULT 30)
RETURNS TABLE (
  scan_date DATE,
  total_score INTEGER,
  maturity_level VARCHAR,
  score_delta INTEGER
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    DATE(ar.scanned_at) as scan_date,
    ar.total_score,
    ar.maturity_level,
    ar.total_score - LAG(ar.total_score) OVER (ORDER BY ar.scanned_at) as score_delta
  FROM audit_results ar
  WHERE ar.space_id = p_space_id
  ORDER BY ar.scanned_at DESC
  LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Sample queries:
-- Get spaces needing attention:
-- SELECT space_id, space_name, owner_email, total_score FROM latest_scores WHERE total_score < 50 ORDER BY total_score;

-- Get score trend for a space:
-- SELECT * FROM get_score_history('space-123', 10);

-- Get weekly improvement leaders:
-- SELECT space_name, MIN(total_score) as start_score, MAX(total_score) as end_score, MAX(total_score) - MIN(total_score) as improvement
-- FROM audit_results WHERE scanned_at >= CURRENT_DATE - INTERVAL '7 days'
-- GROUP BY space_name HAVING MAX(total_score) - MIN(total_score) > 0 ORDER BY improvement DESC;

