export interface FeatureImportance {
  feature: string;
  importance: number;
}

export interface ModelAnalysis {
  best_score: number;
  accuracy: number;
  best_params: Record<string, number | string>;
  feature_importances: FeatureImportance[];
}
