import * as duckdb from '@duckdb/duckdb-wasm';

export interface LoaderContext {
  db: duckdb.AsyncDuckDB;
  connection: duckdb.AsyncDuckDBConnection;
  updateStatus: (message: string) => void;
}

export interface ColumnDetail {
  name: string;
  type: string;
}

export interface LoadResult {
  relationName: string;
  relationIdentifier: string;
  columns: string[];
  columnDetails: ColumnDetail[];
}

export interface DataLoader {
  id: string;
  canLoad: (fileName: string) => boolean;
  load: (
    fileName: string,
    fileBytes: Uint8Array,
    context: LoaderContext
  ) => Promise<LoadResult>;
}
