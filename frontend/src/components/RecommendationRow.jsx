import './RecommendationRow.css';

function RecommendationRow({ step, index = 0 }) {
  const { name, points, recommendation } = step;

  return (
    <div className={`recommendation-row animate-fade-in stagger-${index + 1}`}>
      <div className="recommendation-points">+{points}</div>
      <div className="recommendation-content">
        <h4 className="recommendation-title">{name}</h4>
        {recommendation && (
          <p className="recommendation-desc">{recommendation}</p>
        )}
      </div>
      <div className="recommendation-arrow">→</div>
    </div>
  );
}

export default RecommendationRow;

