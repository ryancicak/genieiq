import { Link } from 'react-router-dom';
import ScoreDisplay from './ScoreDisplay';
import MaturityBadge from './MaturityBadge';
import './SpaceCard.css';

function SpaceCard({ space, index = 0, onToggleStar, newThresholdDays = 7 }) {
  const { id, name, totalScore, maturityLevel, scannedAt, starred, firstSeenAt } = space;
  const hasScore = totalScore !== null && totalScore !== undefined && !Number.isNaN(Number(totalScore));

  const isNew = (() => {
    if (!firstSeenAt) return false;
    const t = Date.parse(firstSeenAt);
    if (!Number.isFinite(t)) return false;
    const days = Math.max(1, Number(newThresholdDays) || 7);
    return (Date.now() - t) <= days * 24 * 60 * 60 * 1000;
  })();

  const formatTime = (isoString) => {
    if (!isoString) return 'Never';
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 60) return `${diffMins} mins ago`;
    if (diffHours < 24) return `${diffHours} hours ago`;
    return `${diffDays} days ago`;
  };

  return (
    <Link 
      to={`/space/${id}`} 
      className={`space-card card card-clickable animate-fade-in stagger-${(index % 6) + 1}`}
    >
      <button
        type="button"
        className={`space-card-star ${starred ? 'is-starred' : ''}`}
        aria-label={starred ? 'Unstar space' : 'Star space'}
        title={starred ? 'Starred' : 'Star this space'}
        onClick={(e) => {
          e.preventDefault();
          e.stopPropagation();
          onToggleStar?.(!starred);
        }}
      >
        {starred ? '★' : '☆'}
      </button>

      <ScoreDisplay score={totalScore} size="medium" animate={false} />
      
      <div className="space-card-info">
        <h3 className="space-card-name">
          <span className="space-card-name-text">{name}</span>
          {isNew && <span className="space-card-new">New</span>}
        </h3>
        <MaturityBadge level={maturityLevel} />
        {!hasScore && (
          <span
            className="space-card-access"
            title="Needs CAN_EDIT on the space and UC READ on its tables for GenieIQ to score."
          >
            Needs access
          </span>
        )}
      </div>
      
      <p className="space-card-meta">
        {hasScore ? `Last scanned ${formatTime(scannedAt)}` : 'Not scored yet'}
      </p>
    </Link>
  );
}

export default SpaceCard;

