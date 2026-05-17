import { useEffect, useMemo, useState } from 'react';
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { FeatureImportance, ModelAnalysis } from './types';

const EMPTY_MODEL: ModelAnalysis = {
  best_score: 0,
  accuracy: 0,
  best_params: {},
  feature_importances: [],
};

const DEFAULT_FEATURE_COUNT = 5;

function App() {
  const [modelAnalysis, setModelAnalysis] = useState<ModelAnalysis>(EMPTY_MODEL);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedFeatures, setSelectedFeatures] = useState<string[]>([]);

  useEffect(() => {
    fetch('/api/model-analysis')
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Server returned ${response.status}`);
        }
        return response.json();
      })
      .then((data: ModelAnalysis) => {
        if (data && data.feature_importances) {
          setModelAnalysis(data);
        } else {
          setModelAnalysis(EMPTY_MODEL);
        }
        setIsLoading(false);
      })
      .catch((fetchError) => {
        console.error('Failed to load model analysis:', fetchError);
        // If live data is not available, fall back to an empty model so the UI shows an empty table/graph
        setModelAnalysis(EMPTY_MODEL);
        setError(null);
        setIsLoading(false);
      });
  }, []);

  useEffect(() => {
    if (modelAnalysis.feature_importances.length > 0 && selectedFeatures.length === 0) {
      setSelectedFeatures(
        modelAnalysis.feature_importances
          .slice(0, DEFAULT_FEATURE_COUNT)
          .map((item) => item.feature)
      );
    }
  }, [modelAnalysis, selectedFeatures]);

  const hasData = (modelAnalysis.feature_importances?.length || 0) > 0 || Object.keys(modelAnalysis.best_params || {}).length > 0;

  const performanceRows = [
    { label: 'Best Score', value: modelAnalysis.best_score.toFixed(4) },
    { label: 'Accuracy', value: modelAnalysis.accuracy.toFixed(4) },
  ];

  const availableFeatures = modelAnalysis.feature_importances.map((item) => item.feature);

  const selectedImportances = useMemo(
    () => modelAnalysis.feature_importances.filter((item) => selectedFeatures.includes(item.feature)),
    [selectedFeatures, modelAnalysis.feature_importances]
  );

  const toggleFeature = (feature: string) => {
    setSelectedFeatures((current) =>
      current.includes(feature)
        ? current.filter((name) => name !== feature)
        : [...current, feature]
    );
  };

  return (
    <div className="app-shell">
      <header className="app-header">
        <h1>NEO Object Harmfulness Model Performance</h1>
        <p>Explore model metrics and choose which feature importances to show in the graph.</p>
      </header>

      <section className="performance-card">
        <h2>Model Performance</h2>
        {isLoading ? (
          <p>Loading live model data...</p>
        ) : (
          <table>
            <tbody>
              {performanceRows.map((row) => (
                <tr key={row.label}>
                  <th>{row.label}</th>
                  <td>{row.value}</td>
                </tr>
              ))}
              <tr>
                <th>Best Params</th>
                <td>
                  <pre>{JSON.stringify(modelAnalysis.best_params, null, 2)}</pre>
                </td>
              </tr>
            </tbody>
          </table>
        )}

        {error ? <p style={{ color: '#b91c1c' }}>{error}</p> : null}
      </section>

      <section className="chart-card">
        <div className="chart-header">
          <div>
            <h2>Feature Importances</h2>
            <p>Select the features you want to visualize. Each score represents the importance of the feature in determining the harmfulnees of a NEO object.</p>
          </div>
          <div className="feature-selector">
            {availableFeatures.map((feature) => (
              <label key={feature} className="feature-checkbox">
                <input
                  type="checkbox"
                  checked={selectedFeatures.includes(feature)}
                  onChange={() => toggleFeature(feature)}
                />
                {feature}
              </label>
            ))}
          </div>
        </div>

        {selectedImportances.length === 0 ? (
          <div className="empty-state">Select at least one feature to render the chart.</div>
        ) : (
          <div className="chart-wrapper">
            <ResponsiveContainer width="100%" height={360}>
              <BarChart data={selectedImportances} margin={{ top: 24, right: 24, left: 0, bottom: 24 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="feature" tick={{ fontSize: 12 }} />
                <YAxis />
                <Tooltip formatter={(value: number) => value.toFixed(3)} />
                <Legend />
                <Bar dataKey="importance" name="Importance Score (Normalized)" fill="#2563eb" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </section>
    </div>
  );
}

export default App;
