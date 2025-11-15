import { DataLoader } from './types';

export const jsonLoader: DataLoader = {
  id: 'json',
  canLoad: (fileName: string) =>
    fileName.endsWith('.json') || fileName.endsWith('.jsonl') || fileName.endsWith('.ndjson'),

  load: async (fileName, fileBytes, { db, connection }) => {
    // Register the file with DuckDB
    await db.registerFileBuffer(fileName, fileBytes);

    // Create a view using DuckDB's read_json function with auto-detect
    const relationName = 'json_data_' + Date.now();

    // Check if it's JSONL/NDJSON (newline-delimited JSON)
    const isJSONL = fileName.endsWith('.jsonl') || fileName.endsWith('.ndjson');

    const format = isJSONL ? 'newline_delimited' : 'auto';

    await connection.query(`
      CREATE OR REPLACE VIEW ${relationName} AS
      SELECT * FROM read_json('${fileName}',
        auto_detect=true,
        format='${format}',
        maximum_object_size=10485760
      )
    `);

    // Get column names from the created view
    const result = await connection.query(`SELECT * FROM ${relationName} LIMIT 1`);
    const columns = result.schema.fields.map((field) => field.name);

    return {
      columns,
      relationIdentifier: relationName,
    };
  },
};
