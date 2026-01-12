import './StatusBadge.css';

const STATUS_CONFIG = {
  healthy: { label: 'healthy', color: 'success' },
  warning: { label: 'warning', color: 'warning' },
  critical: { label: 'critical', color: 'critical' }
};

function StatusBadge({ status }) {
  const config = STATUS_CONFIG[status] || STATUS_CONFIG.warning;

  return (
    <span className={`status-badge status-badge--${config.color}`}>
      {config.label}
    </span>
  );
}

export default StatusBadge;

