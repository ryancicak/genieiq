/**
 * GenieIQ Scoring Rubric
 */

const RUBRIC = {
  foundation: {
    name: 'Foundation',
    maxPoints: 30,
    criteria: [
      {
        id: 'dedicated_warehouse',
        name: 'Serverless Warehouse with Headroom',
        points: 10,
        check: (space) => {
          const wh = space.warehouse;
          if (!wh) return false;

          // In the Databricks UI, users typically choose "Serverless" for best interactive latency.
          // Depending on API version/workspace, serverless can appear as:
          // - warehouse_type === 'SERVERLESS', or
          // - enable_serverless_compute === true
          const warehouseType = String(wh?.warehouse_type || '').toUpperCase();
          const isServerless =
            warehouseType === 'SERVERLESS' ||
            wh?.enable_serverless_compute === true;
          if (!isServerless) return false;

          // Autoscaling headroom:
          // - A very small max cluster cap can cause queueing under load.
          // - If min == max, there is no autoscaling headroom (fixed-size).
          const max = Number(wh?.max_num_clusters);
          const min = wh?.min_num_clusters == null ? null : Number(wh?.min_num_clusters);

          // If we can't read max clusters, treat as not passing (we cannot validate headroom).
          if (!Number.isFinite(max)) return false;

          // Keep this simple and conservative:
          // - max >= 5 provides reasonable burst capacity for typical shared workloads
          // - max must be greater than min if min is present (ensures actual autoscaling headroom)
          if (max < 5) return false;
          if (Number.isFinite(min) && max <= min) return false;

          return true;
        },
        recommendation: 'Use a serverless warehouse with autoscaling headroom (for example: max clusters 5+ and max > min) to avoid queueing under load.'
      },
      {
        id: 'warehouse_size_latency',
        name: 'Warehouse Sizing for Interactive Workloads',
        points: 5,
        check: (space) => {
          const wh = space.warehouse;
          if (!wh) return false;

          const sizeRaw = String(wh?.cluster_size || '').trim().toLowerCase();
          const max = Number(wh?.max_num_clusters);

          // If max clusters is high, smaller sizes can still perform well via parallelism.
          if (Number.isFinite(max) && max >= 10) return true;

          // Conservative: treat very small tiers as risky for interactive p95 latency.
          // Common strings include: "X-Small", "XSMALL", "2X-Small", etc.
          if (!sizeRaw) return false;
          const normalized = sizeRaw.replace(/\s+/g, '');
          const isVerySmall =
            normalized.includes('x-small') ||
            normalized.includes('xsmall') ||
            normalized.includes('2x-small') ||
            normalized.includes('2xsmall') ||
            normalized.includes('xxsmall') ||
            normalized.includes('2x') && normalized.includes('small');

          return !isVerySmall;
        },
        recommendation: 'Consider a larger serverless warehouse size (for example: Small or Medium) for better interactive latency, especially for shared or high-use spaces.'
      },
      {
        id: 'has_instructions',
        name: 'Text Instructions Defined',
        points: 10,
        check: (space) => space.instructions?.length > 0,
        recommendation: 'Add text instructions to guide Genie on how to respond.'
      },
      {
        id: 'lean_instructions',
        name: 'Instructions are Lean (<500 chars)',
        points: 5,
        check: (space) => {
          if (!space.instructions) return false;
          return space.instructions.length > 0 && space.instructions.length < 500;
        },
        recommendation: 'Keep instructions concise (under 500 characters).'
      }
    ]
  },

  dataSetup: {
    name: 'Data Setup',
    maxPoints: 25,
    criteria: [
      {
        id: 'has_join',
        name: 'At Least 1 Join Defined',
        points: 5,
        check: (space) => space.joins?.length >= 1,
        recommendation: 'Define at least one join between tables.'
      },
      {
        id: 'sample_questions_5',
        name: '5+ Sample Questions',
        points: 10,
        check: (space) => space.sampleQuestions?.length >= 5,
        recommendation: 'Add at least 5 sample questions.'
      },
      {
        id: 'table_descriptions',
        name: 'Tables Have Descriptions',
        points: 5,
        check: (space) => {
          if (!space.tables || space.tables.length === 0) return false;
          const described = space.tables.filter(t => t.description?.length > 0);
          return described.length / space.tables.length >= 0.5;
        },
        recommendation: 'Add descriptions to your tables in Genie (Data & descriptions).'
      },
      {
        id: 'column_descriptions',
        name: 'Key Columns Have Descriptions',
        points: 5,
        check: (space) => {
          if (!space.tables) return false;
          let total = 0, described = 0;
          space.tables.forEach(t => {
            t.columns?.forEach(c => {
              total++;
              if (c.description?.length > 0) described++;
            });
          });
          return total > 0 && (described / total) >= 0.3;
        },
        recommendation: 'Add descriptions to key columns in Genie (Data & descriptions).'
      }
    ]
  },

  sqlAssets: {
    name: 'SQL Assets',
    maxPoints: 25,
    criteria: [
      {
        id: 'sql_expressions_3',
        name: '3+ SQL Expressions',
        points: 10,
        check: (space) => space.sqlExpressions?.length >= 3,
        recommendation: 'Add at least 3 SQL expressions for calculated metrics.'
      },
      {
        id: 'sql_queries_3',
        name: '3+ Trusted SQL Queries',
        points: 10,
        check: (space) => space.sqlQueries?.length >= 3,
        recommendation: 'Add at least 3 trusted SQL queries.'
      },
      {
        id: 'sample_questions_10',
        name: '10+ Sample Questions',
        points: 5,
        check: (space) => space.sampleQuestions?.length >= 10,
        recommendation: 'Expand to 10+ sample questions.'
      }
    ]
  },

  optimization: {
    name: 'Optimization',
    maxPoints: 20,
    criteria: [
      {
        id: 'feedback_enabled',
        name: 'Feedback Collection Enabled',
        points: 5,
        check: (space) => space.feedbackEnabled !== false,
        recommendation: 'Enable thumbs up/down feedback.'
      },
      {
        id: 'sql_expressions_5',
        name: '5+ SQL Expressions',
        points: 5,
        check: (space) => space.sqlExpressions?.length >= 5,
        recommendation: 'Expand SQL expressions to 5+.'
      },
      {
        id: 'sql_queries_5',
        name: '5+ Trusted SQL Queries',
        points: 5,
        check: (space) => space.sqlQueries?.length >= 5,
        recommendation: 'Add more trusted queries.'
      },
      {
        id: 'iteration_documented',
        name: 'Space Description Documents Purpose',
        points: 5,
        check: (space) => space.description?.length >= 50,
        recommendation: 'Add a meaningful description explaining purpose.'
      }
    ]
  }
};

function calculateScore(space) {
  let totalScore = 0;
  const breakdown = {};
  const findings = [];

  for (const [categoryKey, category] of Object.entries(RUBRIC)) {
    let categoryScore = 0;

    for (const criterion of category.criteria) {
      let passed = false;
      try {
        passed = criterion.check(space);
      } catch (e) {
        passed = false;
      }

      if (passed) {
        categoryScore += criterion.points;
        totalScore += criterion.points;
      }

      findings.push({
        id: criterion.id,
        category: categoryKey,
        categoryName: category.name,
        name: criterion.name,
        passed,
        points: criterion.points,
        recommendation: passed ? null : criterion.recommendation
      });
    }

    breakdown[categoryKey] = {
      name: category.name,
      score: categoryScore,
      maxPoints: category.maxPoints
    };
  }

  let maturityLevel, maturityLabel;
  if (totalScore >= 76) {
    maturityLevel = 'optimized';
    maturityLabel = 'Optimized';
  } else if (totalScore >= 51) {
    maturityLevel = 'maturing';
    maturityLabel = 'Maturing';
  } else if (totalScore >= 31) {
    maturityLevel = 'developing';
    maturityLabel = 'Developing';
  } else {
    maturityLevel = 'emerging';
    maturityLabel = 'Emerging';
  }

  const nextSteps = findings
    .filter(f => !f.passed)
    .sort((a, b) => b.points - a.points)
    .slice(0, 5)
    .map(f => ({
      id: f.id,
      category: f.category,
      categoryName: f.categoryName,
      name: f.name,
      points: f.points,
      recommendation: f.recommendation
    }));

  return {
    totalScore,
    maxScore: 100,
    breakdown,
    findings,
    maturityLevel,
    maturityLabel,
    nextSteps
  };
}

function getStatus(score) {
  if (score >= 70) return 'healthy';
  if (score >= 40) return 'warning';
  return 'critical';
}

module.exports = { RUBRIC, calculateScore, getStatus };
