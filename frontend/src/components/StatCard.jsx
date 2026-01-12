import './StatCard.css';

function StatCard({ value, label, index = 0 }) {
  return (
    <div className={`stat-card card animate-fade-in stagger-${index + 1}`}>
      <span className="stat-value">{value}</span>
      <span className="stat-label">{label}</span>
    </div>
  );
}

export default StatCard;

