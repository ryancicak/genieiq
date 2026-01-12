import './ProgressBar.css';

function ProgressBar({ label, score, maxScore, index = 0 }) {
  const percentage = Math.round((score / maxScore) * 100);
  
  return (
    <div className={`progress-bar animate-fade-in stagger-${index + 1}`}>
      <div className="progress-bar-header">
        <span className="progress-bar-label">{label}</span>
        <span className="progress-bar-value">{score}/{maxScore}</span>
      </div>
      <div className="progress-bar-track">
        <div 
          className="progress-bar-fill" 
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  );
}

export default ProgressBar;

