import './ScoreDisplay.css';

function ScoreDisplay({ score, size = 'large', delta = null, animate = true }) {
  const sizeClass = `score-display--${size}`;
  const animateClass = animate ? 'animate-count-up' : '';
  const hasScore = score !== null && score !== undefined && !Number.isNaN(Number(score));

  return (
    <div className={`score-display ${sizeClass} ${animateClass}`}>
      <span className="score-value">{hasScore ? score : '-'}</span>
      {hasScore && delta !== null && delta !== 0 && (
        <span className={`score-delta ${delta > 0 ? 'positive' : 'negative'}`}>
          {delta > 0 ? '+' : ''}{delta} from last scan
        </span>
      )}
    </div>
  );
}

export default ScoreDisplay;

