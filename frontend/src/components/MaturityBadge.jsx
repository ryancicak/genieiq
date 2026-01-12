import './MaturityBadge.css';

const MATURITY_CONFIG = {
  optimized: { label: 'Optimized', color: 'success' },
  maturing: { label: 'Maturing', color: 'accent' },
  developing: { label: 'Developing', color: 'warning' },
  emerging: { label: 'Emerging', color: 'critical' },
  unscanned: { label: 'Unscanned', color: 'muted' }
};

function MaturityBadge({ level }) {
  const key = level || 'unscanned';
  const config = MATURITY_CONFIG[key] || MATURITY_CONFIG.emerging;

  return (
    <span className={`maturity-badge maturity-badge--${config.color}`}>
      {config.label}
    </span>
  );
}

export default MaturityBadge;

