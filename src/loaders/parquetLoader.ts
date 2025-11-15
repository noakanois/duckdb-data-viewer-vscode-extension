import { DataLoader } from './types';
import { deriveRelationName, formatIdentifierForSql } from '../utils/sqlHelpers';

// 1. Define Parquet file extensions
const PARQUET_EXTENSIONS = /\.(parquet|parq)$/i;

export const parquetLoader: DataLoader = {
  id: 'parquet',
  canLoad(fileName: string) {
    // 2. Check against Parquet extensions
    return PARQUET_EXTENSIONS.test(fileName);
  },
  async load(fileName, fileBytes, context) {
    const { db, connection, updateStatus } = context;

    updateStatus('Registering Parquet file…');
    // 3. Register the file buffer just like the CSV loader
    await db.registerFileBuffer(fileName, fileBytes);

    // 4. Escape the file name for use in SQL
    const escapedFileName = fileName.replace(/'/g, "''");

    // 5. Use 'read_parquet' to inspect the file's schema
    // Parquet files are self-describing, so no 'header=true' is needed.
    const describeQuery = `DESCRIBE SELECT * FROM read_parquet('${escapedFileName}');`;
    updateStatus('Inspecting Parquet schema…');
    const describeResult = await connection.query(describeQuery);
    
    // 6. Get the column names from the describe result
    const columnDetails = describeResult
      .toArray()
      .map((row: any) => ({
        name: typeof row.column_name === 'string' ? row.column_name : undefined,
        type: typeof row.column_type === 'string' ? row.column_type : 'unknown',
      }))
      .filter((detail): detail is { name: string; type: string } =>
        typeof detail.name === 'string' && detail.name.length > 0
      );

    const columns = columnDetails.map((detail) => detail.name);

    if (columns.length === 0) {
      throw new Error('No columns were detected in this Parquet file.');
    }

    // 7. Derive the name for the view
    const relationName = deriveRelationName(fileName);
    const relationIdentifier = formatIdentifierForSql(relationName);

    // 8. Create the temporary view using 'read_parquet'
    updateStatus(`Creating '${relationName}' view…`);
    const createViewQuery = `
      CREATE OR REPLACE TEMP VIEW ${relationIdentifier} AS 
      SELECT * FROM read_parquet('${escapedFileName}');
    `;
    await connection.query(createViewQuery);

    // 9. Return the same result shape
    return {
      relationName,
      relationIdentifier,
      columns,
      columnDetails,
    };
  },
};