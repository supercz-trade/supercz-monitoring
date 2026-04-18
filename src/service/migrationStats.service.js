import { db } from "../infra/database.js";

export async function updateMigrationStats(baseAmount) {
  if (!baseAmount || baseAmount <= 0) return;

  await db.query(`
    UPDATE migration_stats
    SET
      avg_target = (
        (avg_target * sample_size + $1) / (sample_size + 1)
      ),
      sample_size = sample_size + 1,
      updated_at = NOW()
    WHERE id = 1
  `, [baseAmount]);
}