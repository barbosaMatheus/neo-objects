import express from 'express';
import path from 'path';
import { Pool } from 'pg';

const port = Number(process.env.PORT || 4173);
const pool = new Pool({
  host: process.env.PGHOST || 'neo-db',
  user: process.env.PGUSER || 'neo',
  password: process.env.PGPASSWORD || 'neo123',
  database: process.env.PGDATABASE || 'neo_data',
  port: Number(process.env.PGPORT || 5432),
});

const app = express();
app.use(express.static(path.join(process.cwd(), 'dist')));

app.get('/api/model-analysis', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT best_score, accuracy, best_params, feature_importances
       FROM model_analysis
       ORDER BY created_at DESC
       LIMIT 1`
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'No model analysis found' });
    }

    return res.json(result.rows[0]);
  } catch (error) {
    console.error('Failed to fetch model analysis', error);
    return res.status(500).json({ error: 'Unable to load model analysis' });
  }
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(process.cwd(), 'dist', 'index.html'));
});

app.listen(port, () => {
  console.log(`Frontend server listening on port ${port}`);
});
