import * as duckdb from '@duckdb/duckdb-wasm';
import { Table } from 'apache-arrow';
import { csvLoader } from './loaders/csvLoader';
import { arrowLoader } from './loaders/arrowLoader';
import { parquetLoader } from './loaders/parquetLoader';
import { DataLoader } from './loaders/types';
import { buildDefaultQuery } from './utils/sqlHelpers';

declare const acquireVsCodeApi: any;
const vscode = acquireVsCodeApi();

// Get UI elements
const status = document.getElementById('status');
const controls = document.getElementById('controls');
const resultsContainer = document.getElementById('results-container');
const sqlInput = document.getElementById('sql-input') as HTMLTextAreaElement;
const runButton = document.getElementById('run-query') as HTMLButtonElement;
const copySqlButton = document.getElementById('copy-sql') as HTMLButtonElement;
const statusWrapper = document.getElementById('status-wrapper');
const globalSearchInput = document.getElementById('global-search') as HTMLInputElement;
const rowCountLabel = document.getElementById('row-count');
const profileContainer = document.getElementById('profile-container') as HTMLDivElement | null;
const refreshProfileButton = document.getElementById('refresh-profile') as HTMLButtonElement | null;
const insightList = document.getElementById('insight-list') as HTMLDivElement | null;
const historyList = document.getElementById('history-list') as HTMLDivElement | null;

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
}

interface SchemaColumn {
  name: string;
  type: string;
  nullable: boolean;
}

interface InsightDefinition {
  id: string;
  label: string;
  description: string;
  sql: string;
}

interface ColumnProfile {
  name: string;
  type: string;
  fillRate: number;
  nullCount: number;
  distinctCount: number;
  minValue: string;
  maxValue: string;
  averageValue: string;
  sampleValues: string[];
}

interface QueryHistoryEntry {
  sql: string;
  timestamp: number;
}

let db: duckdb.AsyncDuckDB | null = null;
let connection: duckdb.AsyncDuckDBConnection | null = null;
let duckdbInitializationPromise: Promise<void> | null = null;
let currentTableData: TableData | null = null;
let columnFilters: string[] = [];
let globalFilter = '';
let sortState: { columnIndex: number; direction: SortDirection } = { columnIndex: -1, direction: null };
let tableBodyElement: HTMLTableSectionElement | null = null;
let copyTimeoutHandle: number | null = null;
let activeRelation: { identifier: string; columns: string[] } | null = null;
let schemaColumns: SchemaColumn[] = [];
let queryHistory: QueryHistoryEntry[] = [];
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];
const PROFILE_COLUMN_LIMIT = 25;
const HISTORY_LIMIT = 50;

// --- Event Listeners (Moved to top) ---

// Listen for messages from the extension
window.addEventListener('message', (event: any) => {
  const message = event.data;
  if (message.command === 'init') {
    ensureDuckDBInitialized(message.bundles).catch(reportError);
  } else if (message.command === 'loadFile') {
    handleFileLoad(message.fileName, message.fileData).catch(reportError);
  } else if (message.command === 'error') {
    reportError(message.message);
  }
});

// Listen for the "Run" button click
runButton.addEventListener('click', () => {
  runQuery(sqlInput.value).catch(reportError);
});

// Allow Cmd/Ctrl + Enter to run the query
sqlInput.addEventListener('keydown', (event: KeyboardEvent) => {
  const isSubmitShortcut = event.key === 'Enter' && (event.metaKey || event.ctrlKey);
  if (isSubmitShortcut) {
    event.preventDefault();
    runQuery(sqlInput.value).catch(reportError);
  }
});

// Global search box to filter visible rows
if (globalSearchInput) {
  globalSearchInput.addEventListener('input', () => {
    globalFilter = globalSearchInput.value;
    applyTableState();
  });
}

// Copy SQL to the clipboard for quick sharing
if (copySqlButton) {
  copySqlButton.addEventListener('click', async () => {
    try {
      const didCopy = await copyTextToClipboard(sqlInput.value);
      if (didCopy) {
        flashCopyState();
      } else {
        updateStatus('Clipboard access is not available in this environment.');
      }
    } catch (err) {
      updateStatus('Copy to clipboard is unavailable in this context.');
      console.warn('[Webview] Clipboard copy failed', err);
    }
  });
}

if (refreshProfileButton) {
  refreshProfileButton.addEventListener('click', () => {
    refreshColumnProfiles(true).catch(reportError);
  });
}

// --- Core Functions ---

function createDuckDBWorker(workerSource: string, workerUrl: string): { worker: Worker; cleanup: () => void } {
  updateStatus('Creating DuckDB worker from source...');
  const bootstrap = `
      self.window = self;
      self.document = { currentScript: { src: ${JSON.stringify(workerUrl)} } };
  `;
  const blob = new Blob([bootstrap, '\n', workerSource], { type: 'application/javascript' });
  const blobUrl = URL.createObjectURL(blob);
  return {
      worker: new Worker(blobUrl),
      cleanup: () => URL.revokeObjectURL(blobUrl),
  };
}

async function ensureDuckDBInitialized(bundles: duckdb.DuckDBBundles) {
  if (connection) {
    updateStatus('DuckDB ready. Waiting for file data…');
    vscode.postMessage({ command: 'duckdb-ready' });
    return;
  }

  if (!duckdbInitializationPromise) {
    duckdbInitializationPromise = bootstrapDuckDB(bundles).catch((error) => {
      duckdbInitializationPromise = null;
      throw error;
    });
  }

  await duckdbInitializationPromise;
}

async function bootstrapDuckDB(bundles: duckdb.DuckDBBundles) {
  try {
    updateStatus('Selecting DuckDB bundle...');
    const selectedBundle = await duckdb.selectBundle(bundles);
    
    if (!selectedBundle.mainWorker || typeof selectedBundle.mainWorker !== 'string') {
        throw new Error('Selected bundle has no worker source.');
    }
    if (!selectedBundle.mainModule) {
        throw new Error('Selected bundle has no WASM module URL.');
    }

    const workerUrl = selectedBundle.mainModule.replace('.wasm', '.worker.js');
    const { worker, cleanup } = createDuckDBWorker(selectedBundle.mainWorker, workerUrl);
    
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);
    
    updateStatus('Instantiating DuckDB...');
    await db.instantiate(selectedBundle.mainModule, selectedBundle.pthreadWorker);
    cleanup();
    
    updateStatus('Opening DuckDB...');
    await db.open({ path: ':memory:' });
    
    updateStatus('Connecting to DuckDB...');
    connection = await db.connect();
    
    updateStatus('Installing extensions...');
    await connection.query("INSTALL parquet; LOAD parquet;");
    await connection.query("INSTALL sqlite; LOAD sqlite;");
  
    updateStatus('DuckDB ready. Waiting for file data…');
    vscode.postMessage({ command: 'duckdb-ready' });

  } catch (e) {
    reportError(e);
  }
}

async function handleFileLoad(fileName: string, fileData: any) {
  if (!db || !connection) {
    throw new Error('DuckDB is not initialized.');
  }

  const fileBytes = extractFileBytes(fileData);
  if (fileBytes.length === 0) {
    throw new Error('File is empty (0 bytes).');
  }

  const loader = selectLoader(fileName);
  updateStatus(`Preparing ${loader.id.toUpperCase()} data for ${fileName}…`);
  const loadResult = await loader.load(fileName, fileBytes, {
    db,
    connection,
    updateStatus,
  });

  activeRelation = {
    identifier: loadResult.relationIdentifier,
    columns: [...loadResult.columns],
  };
  schemaColumns = [];
  if (insightList) {
    insightList.innerHTML = '<div class="empty-subtle">Forging turbo insights…</div>';
  }
  if (profileContainer) {
    profileContainer.innerHTML = '<div class="empty-subtle">Scanning columns at hyperspeed…</div>';
  }

  const defaultQuery = buildDefaultQuery(loadResult.columns, loadResult.relationIdentifier);
  sqlInput.value = defaultQuery;
  sqlInput.placeholder = `Example: ${defaultQuery}`;

  if (controls) {
    controls.style.display = 'flex';
  }
  if (resultsContainer) {
    resultsContainer.style.display = 'block';
  }

  const schemaPromise = fetchSchema(loadResult.relationIdentifier);

  await runQuery(defaultQuery);

  try {
    schemaColumns = await schemaPromise;
  } catch (schemaError) {
    console.warn('[Webview] Schema introspection failed', schemaError);
    updateStatus('Loaded data, but schema introspection failed. Insights may be limited.');
    schemaColumns = [];
  }

  renderInsightList(generateInsightQueries(loadResult.relationIdentifier, schemaColumns, loadResult.columns));
  await refreshColumnProfiles();
  updateStatus('Turbo systems engaged. Trigger an insight or craft your own SQL.');
}

function selectLoader(fileName: string): DataLoader {
  return DATA_LOADERS.find((loader) => loader.canLoad(fileName)) ?? csvLoader;
}

function extractFileBytes(fileData: any): Uint8Array {
  if (fileData instanceof Uint8Array) {
    return fileData;
  }
  if (fileData?.data instanceof ArrayBuffer) {
    return new Uint8Array(fileData.data);
  }
  if (Array.isArray(fileData?.data)) {
    return new Uint8Array(fileData.data);
  }
  if (fileData instanceof ArrayBuffer) {
    return new Uint8Array(fileData);
  }
  if (fileData?.buffer instanceof ArrayBuffer) {
    return new Uint8Array(fileData.buffer);
  }
  throw new Error('Unable to read file bytes from message.');
}

async function runQuery(sql: string) {
  if (!connection) {
    throw new Error("No database connection.");
  }

  const normalizedSql = sql.trim();
  if (!normalizedSql) {
    updateStatus('Enter a SQL query to run.');
    return;
  }

  // Show status bar for "Running query..."
  updateStatus('Running query...');
  runButton.disabled = true;

  try {
    const result = await connection.query(normalizedSql);
    renderResults(result);
    recordQuery(normalizedSql);

    // --- CHANGE ---
    // Hide the status bar on success
    if (statusWrapper) {
      statusWrapper.style.display = 'none';
    }
    // ---

  } catch (e) {
    reportError(e); // reportError will show the status bar
  } finally {
    runButton.disabled = false;
  }
}

function renderResults(table: Table | null) {
  if (!resultsContainer) {
    return;
  }

  if (!table || table.numRows === 0) {
    resultsContainer.innerHTML = '<div class="empty-state">Query completed. No rows returned.</div>';
    currentTableData = null;
    tableBodyElement = null;
    updateRowCount(0, 0);
    return;
  }

  const rows: TableRow[] = [];
  const columns = table.schema.fields.map((field) => field.name);

  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i);
    if (!row) {
      continue;
    }

    const raw: any[] = [];
    const display: string[] = [];
    for (const field of table.schema.fields) {
      const value = row[field.name];
      raw.push(value);
      display.push(formatCell(value));
    }
    rows.push({ raw, display });
  }

  currentTableData = { columns, rows };
  columnFilters = columns.map(() => '');
  globalFilter = '';
  sortState = { columnIndex: -1, direction: null };
  if (globalSearchInput) {
    globalSearchInput.value = '';
  }

  buildTableSkeleton(columns);
  applyTableState();

  resultsContainer.style.display = 'block';
  resultsContainer.scrollTop = 0;
}

function buildTableSkeleton(columns: string[]) {
  if (!resultsContainer) {
    return;
  }

  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  headerRow.className = 'column-row';

  columns.forEach((column, index) => {
    const th = document.createElement('th');
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'header-button';

    const label = document.createElement('span');
    label.textContent = column;
    const indicator = document.createElement('span');
    indicator.className = 'sort-indicator';

    button.append(label, indicator);
    button.addEventListener('click', () => toggleSort(index));
    th.appendChild(button);
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  const filterRow = document.createElement('tr');
  filterRow.className = 'filter-row';
  columns.forEach((column, index) => {
    const th = document.createElement('th');
    const input = document.createElement('input');
    input.type = 'search';
    input.placeholder = 'Filter';
    input.value = columnFilters[index] ?? '';
    input.setAttribute('aria-label', `Filter column ${column}`);
    input.addEventListener('input', () => {
      columnFilters[index] = input.value;
      applyTableState();
    });
    th.appendChild(input);
    filterRow.appendChild(th);
  });
  thead.appendChild(filterRow);

  const tbody = document.createElement('tbody');
  table.appendChild(thead);
  table.appendChild(tbody);

  resultsContainer.innerHTML = '';
  resultsContainer.appendChild(table);
  tableBodyElement = tbody;
  syncColumnHeaderHeight(headerRow);
}

function applyTableState() {
  if (!currentTableData || !tableBodyElement) {
    return;
  }
  const tbody = tableBodyElement;

  const normalizedGlobal = globalFilter.trim().toLowerCase();
  const normalizedFilters = columnFilters.map((value) => value.trim().toLowerCase());

  let visibleRows = currentTableData.rows.filter((row) => {
    if (normalizedGlobal) {
      const hasMatch = row.display.some((cell) => cell.toLowerCase().includes(normalizedGlobal));
      if (!hasMatch) {
        return false;
      }
    }
    return normalizedFilters.every((filter, idx) => {
      if (!filter) {
        return true;
      }
      return (row.display[idx] ?? '').toLowerCase().includes(filter);
    });
  });

  if (sortState.direction && sortState.columnIndex >= 0) {
    const directionMultiplier = sortState.direction === 'asc' ? 1 : -1;
    const sortIndex = sortState.columnIndex;
    visibleRows = [...visibleRows].sort((a, b) => {
      const comparison = compareValues(
        a.raw[sortIndex],
        b.raw[sortIndex],
        a.display[sortIndex],
        b.display[sortIndex]
      );
      return comparison * directionMultiplier;
    });
  } else {
    visibleRows = [...visibleRows];
  }

  tbody.innerHTML = '';

  if (visibleRows.length === 0) {
    const emptyRow = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = currentTableData.columns.length || 1;
    cell.textContent = 'No rows match the current filters.';
    cell.className = 'empty-row';
    emptyRow.appendChild(cell);
    tbody.appendChild(emptyRow);
  } else {
    visibleRows.forEach((row) => {
      const tr = document.createElement('tr');
      row.display.forEach((value) => {
        const td = document.createElement('td');
        td.textContent = value;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  }

  updateRowCount(visibleRows.length, currentTableData.rows.length);
  refreshSortIndicators();
}

function syncColumnHeaderHeight(headerRow: HTMLTableRowElement) {
  window.requestAnimationFrame(() => {
    const height = headerRow.getBoundingClientRect().height;
    if (height > 0) {
      document.documentElement.style.setProperty('--column-header-height', `${height}px`);
    }
  });
}

function toggleSort(columnIndex: number) {
  if (sortState.columnIndex === columnIndex) {
    if (sortState.direction === 'asc') {
      sortState.direction = 'desc';
    } else if (sortState.direction === 'desc') {
      sortState = { columnIndex: -1, direction: null };
    } else {
      sortState.direction = 'asc';
    }
  } else {
    sortState = { columnIndex, direction: 'asc' };
  }
  applyTableState();
}

function refreshSortIndicators() {
  if (!resultsContainer) {
    return;
  }

  const headerButtons = Array.from(resultsContainer.querySelectorAll<HTMLButtonElement>('.header-button'));
  headerButtons.forEach((button, index) => {
    if (sortState.columnIndex === index && sortState.direction) {
      button.dataset.sort = sortState.direction;
    } else {
      delete button.dataset.sort;
    }
  });
}

function updateRowCount(visible: number, total: number) {
  if (!rowCountLabel) {
    return;
  }
  if (total <= 0) {
    rowCountLabel.textContent = 'No rows available.';
    return;
  }

  const visibilityRatio = total > 0 ? (visible / total) * 100 : 0;
  const formattedVisible = formatCount(visible);
  const formattedTotal = formatCount(total);
  rowCountLabel.textContent = `${formattedVisible} visible of ${formattedTotal} rows (${visibilityRatio.toFixed(1)}% in view)`;
}

function compareValues(a: any, b: any, aDisplay: string, bDisplay: string): number {
  if (a === b) {
    return 0;
  }

  const aIsNumber = typeof a === 'number' && Number.isFinite(a);
  const bIsNumber = typeof b === 'number' && Number.isFinite(b);
  if (aIsNumber && bIsNumber) {
    return a < b ? -1 : 1;
  }

  const aIsDate = a instanceof Date;
  const bIsDate = b instanceof Date;
  if (aIsDate && bIsDate) {
    return a.getTime() - b.getTime();
  }

  const textA = (aDisplay ?? '').toLowerCase();
  const textB = (bDisplay ?? '').toLowerCase();
  return textA.localeCompare(textB, undefined, { numeric: true, sensitivity: 'base' });
}

function formatCell(value: any): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value.toString() : '';
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

async function copyTextToClipboard(text: string): Promise<boolean> {
  try {
    const clipboard = navigator.clipboard;
    if (!clipboard) {
      return false;
    }
    await clipboard.writeText(text);
    return true;
  } catch (error) {
    console.warn('[Webview] Clipboard interaction failed', error);
    return false;
  }
}

function renderInsightList(insights: InsightDefinition[]) {
  if (!insightList) {
    return;
  }

  if (!insights.length) {
    insightList.innerHTML = '<div class="empty-subtle">No automated insights available for this dataset.</div>';
    return;
  }

  insightList.innerHTML = '';
  insights.forEach((insight) => {
    const card = document.createElement('div');
    card.className = 'insight-card';

    const title = document.createElement('h3');
    title.textContent = insight.label;
    const description = document.createElement('p');
    description.textContent = insight.description;

    const preview = document.createElement('pre');
    preview.className = 'history-preview';
    preview.textContent = insight.sql;

    const actions = document.createElement('div');
    actions.className = 'insight-actions';

    const igniteButton = document.createElement('button');
    igniteButton.textContent = 'Ignite Insight';
    igniteButton.addEventListener('click', () => {
      sqlInput.value = insight.sql;
      runQuery(insight.sql).catch(reportError);
    });

    const stageButton = document.createElement('button');
    stageButton.textContent = 'Load into SQL Lab';
    stageButton.className = 'ghost-button';
    stageButton.addEventListener('click', () => {
      sqlInput.value = insight.sql;
      updateStatus('Insight loaded into SQL Lab. Tweak and fire when ready.');
    });

    actions.append(igniteButton, stageButton);
    card.append(title, description, preview, actions);
    insightList.appendChild(card);
  });
}

function generateInsightQueries(
  relationIdentifier: string,
  schema: SchemaColumn[],
  fallbackColumns: string[],
): InsightDefinition[] {
  const insights: InsightDefinition[] = [];
  const columns = schema.length ? schema.map((column) => column.name) : fallbackColumns;
  const numericColumns = schema.filter((column) => isNumericType(column.type));
  const textColumns = schema.filter((column) => isTextType(column.type));
  const temporalColumns = schema.filter((column) => isTemporalType(column.type));

  if (columns.length) {
    insights.push({
      id: 'null-radar',
      label: 'Null Void Radar',
      description: 'Ranks every column by emptiness to surface data quality landmines.',
      sql: buildNullRadarQuery(relationIdentifier, columns),
    });
  }

  insights.push({
    id: 'row-pulse',
    label: 'Row Pulse Scan',
    description: 'Total row count plus rolling averages for the loudest numeric signals.',
    sql: buildRowPulseQuery(relationIdentifier, numericColumns),
  });

  if (numericColumns.length > 0) {
    const primaryNumeric = numericColumns[0];
    insights.push({
      id: `distribution-${primaryNumeric.name}`,
      label: `Distribution Reactor · ${primaryNumeric.name}`,
      description: 'Quartiles, spread, and volatility for the headline numeric column.',
      sql: buildNumericDistributionQuery(relationIdentifier, primaryNumeric.name),
    });
  }

  if (numericColumns.length > 1) {
    const [a, b] = numericColumns;
    insights.push({
      id: `correlation-${a.name}-${b.name}`,
      label: `Correlation Collider · ${a.name} ↔ ${b.name}`,
      description: 'Pearson correlation to test whether your top measures move in sync.',
      sql: buildCorrelationQuery(relationIdentifier, a.name, b.name),
    });
  }

  if (textColumns.length > 0) {
    const primaryText = textColumns[0];
    insights.push({
      id: `frequency-${primaryText.name}`,
      label: `Category Frequency Blast · ${primaryText.name}`,
      description: 'Top categories with share of rows to spotlight runaway values.',
      sql: buildCategoryFrequencyQuery(relationIdentifier, primaryText.name),
    });
  }

  if (temporalColumns.length > 0) {
    const temporal = temporalColumns[0];
    insights.push({
      id: `timeline-${temporal.name}`,
      label: `Timeline Waveform · ${temporal.name}`,
      description: 'Daily row counts to expose surges, droughts, and seasonality.',
      sql: buildTimelineQuery(relationIdentifier, temporal.name),
    });
  }

  insights.push({
    id: 'random-projection',
    label: 'Random Projection Sample',
    description: '200-row randomized slice to eyeball anomalies instantly.',
    sql: buildRandomProjectionQuery(relationIdentifier),
  });

  return insights;
}

function buildNullRadarQuery(relationIdentifier: string, columns: string[]): string {
  const unionSections = columns.map((column) => {
    const identifier = formatIdentifierForSql(column);
    const label = escapeSqlString(column);
    return `SELECT '${label}' AS column_name,\n       COUNT(*) AS total_rows,\n       SUM(CASE WHEN ${identifier} IS NULL THEN 1 ELSE 0 END) AS null_rows,\n       ROUND(100.0 * SUM(CASE WHEN ${identifier} IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS null_pct\nFROM base`;
  });

  return `WITH base AS (\n  SELECT * FROM ${relationIdentifier}\n)\n${unionSections.join('\nUNION ALL\n')}\nORDER BY null_pct DESC;`;
}

function buildRowPulseQuery(relationIdentifier: string, numericColumns: SchemaColumn[]): string {
  const aggregations: string[] = ['COUNT(*) AS total_rows'];
  numericColumns.slice(0, 3).forEach((column) => {
    const identifier = formatIdentifierForSql(column.name);
    const alias = formatIdentifierForSql(`avg_${toSnakeCase(column.name)}`);
    aggregations.push(`AVG(${identifier}) AS ${alias}`);
  });

  return `SELECT\n  ${aggregations.join(',\n  ')}\nFROM ${relationIdentifier};`;
}

function buildNumericDistributionQuery(relationIdentifier: string, columnName: string): string {
  const identifier = formatIdentifierForSql(columnName);
  return `SELECT\n  MIN(${identifier}) AS min_value,\n  QUANTILE_CONT(${identifier}, 0.25) AS q1,\n  QUANTILE_CONT(${identifier}, 0.5) AS median,\n  QUANTILE_CONT(${identifier}, 0.75) AS q3,\n  MAX(${identifier}) AS max_value,\n  AVG(${identifier}) AS avg_value,\n  STDDEV_POP(${identifier}) AS stddev\nFROM ${relationIdentifier}\nWHERE ${identifier} IS NOT NULL;`;
}

function buildCorrelationQuery(relationIdentifier: string, columnA: string, columnB: string): string {
  const a = formatIdentifierForSql(columnA);
  const b = formatIdentifierForSql(columnB);
  const alias = formatIdentifierForSql(`corr_${toSnakeCase(columnA)}_${toSnakeCase(columnB)}`);
  return `SELECT\n  CORR(${a}, ${b}) AS ${alias}\nFROM ${relationIdentifier}\nWHERE ${a} IS NOT NULL AND ${b} IS NOT NULL;`;
}

function buildCategoryFrequencyQuery(relationIdentifier: string, columnName: string): string {
  const identifier = formatIdentifierForSql(columnName);
  return `WITH base AS (SELECT COUNT(*) AS total_rows FROM ${relationIdentifier})\nSELECT\n  ${identifier} AS value,\n  COUNT(*) AS frequency,\n  ROUND(100.0 * COUNT(*) / NULLIF((SELECT total_rows FROM base), 0), 2) AS pct_share\nFROM ${relationIdentifier}\nWHERE ${identifier} IS NOT NULL\nGROUP BY 1\nORDER BY frequency DESC\nLIMIT 25;`;
}

function buildTimelineQuery(relationIdentifier: string, columnName: string): string {
  const identifier = formatIdentifierForSql(columnName);
  return `SELECT\n  DATE_TRUNC('day', ${identifier}) AS day,\n  COUNT(*) AS rows\nFROM ${relationIdentifier}\nWHERE ${identifier} IS NOT NULL\nGROUP BY 1\nORDER BY day;`;
}

function buildRandomProjectionQuery(relationIdentifier: string): string {
  return `SELECT *\nFROM ${relationIdentifier}\nORDER BY RANDOM()\nLIMIT 200;`;
}

async function fetchSchema(relationIdentifier: string): Promise<SchemaColumn[]> {
  if (!connection) {
    throw new Error('No database connection.');
  }
  const pragma = await connection.query(`PRAGMA table_info(${relationIdentifier});`);
  return tableToObjects(pragma).map((row: any) => {
    const notNullRaw = row.notnull;
    const notNull = typeof notNullRaw === 'boolean'
      ? notNullRaw
      : typeof notNullRaw === 'number'
        ? notNullRaw === 1
        : typeof notNullRaw === 'bigint'
          ? notNullRaw === BigInt(1)
          : false;
    return {
      name: String(row.name ?? ''),
      type: String(row.type ?? 'UNKNOWN'),
      nullable: !notNull,
    };
  });
}

async function refreshColumnProfiles(triggeredByUser = false) {
  if (!activeRelation || !profileContainer) {
    return;
  }

  if (!schemaColumns.length) {
    profileContainer.innerHTML = '<div class="empty-subtle">Schema information unavailable. Run a query to hydrate the profile.</div>';
    return;
  }

  const truncated = schemaColumns.length > PROFILE_COLUMN_LIMIT;
  const targetSchema = schemaColumns.slice(0, PROFILE_COLUMN_LIMIT);
  profileContainer.innerHTML = '<div class="empty-subtle">Crunching column diagnostics…</div>';
  if (triggeredByUser) {
    updateStatus('Refreshing the hyper-profiler…');
  }

  try {
    const profiles = await computeColumnProfiles(activeRelation.identifier, targetSchema);
    renderColumnProfiles(profiles, truncated);
    if (triggeredByUser) {
      updateStatus('Hyper-profiler refreshed.');
    }
  } catch (error) {
    profileContainer.innerHTML = `<div class="empty-subtle">Profiling failed: ${error instanceof Error ? error.message : String(error)}</div>`;
    throw error;
  }
}

async function computeColumnProfiles(
  relationIdentifier: string,
  columns: SchemaColumn[],
): Promise<ColumnProfile[]> {
  if (!connection) {
    throw new Error('No database connection.');
  }

  const profiles: ColumnProfile[] = [];
  for (const column of columns) {
    const identifier = formatIdentifierForSql(column.name);
    const statsQuery = `SELECT\n      COUNT(*) AS total_rows,\n      COUNT(${identifier}) AS non_nulls,\n      COUNT(DISTINCT ${identifier}) AS distinct_count,\n      MIN(${identifier}) AS min_value,\n      MAX(${identifier}) AS max_value,\n      AVG(${identifier}) AS avg_value\n    FROM ${relationIdentifier};`;
    const statsRow = tableToObjects(await connection.query(statsQuery))[0] ?? {};

    const totalRows = toNumber(statsRow.total_rows);
    const nonNulls = toNumber(statsRow.non_nulls);
    const distinct = toNumber(statsRow.distinct_count);
    const nullCount = Math.max(0, totalRows - nonNulls);
    const fillRate = totalRows > 0 ? (nonNulls / totalRows) * 100 : 0;

    const sampleQuery = `SELECT ${identifier} AS value FROM ${relationIdentifier} WHERE ${identifier} IS NOT NULL LIMIT 3;`;
    const sampleValues = tableToObjects(await connection.query(sampleQuery))
      .map((row: any) => formatCell(row.value))
      .filter((value: string) => value.length > 0);

    const avgValue = statsRow.avg_value;

    profiles.push({
      name: column.name,
      type: column.type,
      fillRate,
      nullCount,
      distinctCount: distinct,
      minValue: formatCell(statsRow.min_value),
      maxValue: formatCell(statsRow.max_value),
      averageValue: isNumericType(column.type)
        ? formatNumericValue(avgValue)
        : '',
      sampleValues,
    });
  }

  return profiles;
}

function renderColumnProfiles(profiles: ColumnProfile[], truncated: boolean) {
  if (!profileContainer) {
    return;
  }

  if (!profiles.length) {
    profileContainer.innerHTML = '<div class="empty-subtle">No column metrics available for this dataset.</div>';
    return;
  }

  profileContainer.innerHTML = '';
  profiles.forEach((profile) => {
    const card = document.createElement('div');
    card.className = 'profile-card';

    const title = document.createElement('h3');
    title.textContent = profile.name;

    const meta = document.createElement('div');
    meta.className = 'profile-meta';
    meta.textContent = `${profile.type.toUpperCase()} • ${formatPercent(profile.fillRate)} filled`;

    const progress = document.createElement('div');
    progress.className = 'profile-progress';
    const progressValue = document.createElement('span');
    progressValue.style.width = `${Math.max(0, Math.min(100, profile.fillRate))}%`;
    progress.appendChild(progressValue);

    const metrics = document.createElement('div');
    metrics.className = 'profile-metrics';
    metrics.append(createMetric('Distinct', formatCount(profile.distinctCount)));
    metrics.append(createMetric('Nulls', formatCount(profile.nullCount)));
    if (profile.minValue) {
      metrics.append(createMetric('Min', profile.minValue));
    }
    if (profile.maxValue) {
      metrics.append(createMetric('Max', profile.maxValue));
    }
    if (profile.averageValue) {
      metrics.append(createMetric('Avg', profile.averageValue));
    }
    const samples = createMetric('Samples', profile.sampleValues.length ? profile.sampleValues.join(', ') : '—');
    samples.style.gridColumn = '1 / -1';
    metrics.append(samples);

    card.append(title, meta, progress, metrics);
    profileContainer.appendChild(card);
  });

  if (truncated) {
    const note = document.createElement('div');
    note.className = 'empty-subtle';
    note.textContent = `Showing first ${PROFILE_COLUMN_LIMIT} columns for warp-speed diagnostics.`;
    profileContainer.appendChild(note);
  }
}

function createMetric(label: string, value: string): HTMLDivElement {
  const wrapper = document.createElement('div');
  wrapper.className = 'profile-metric';
  const strong = document.createElement('strong');
  strong.textContent = label;
  const span = document.createElement('span');
  span.textContent = value || '—';
  wrapper.append(strong, span);
  return wrapper;
}

function recordQuery(sql: string) {
  const normalized = sql.trim();
  if (!normalized) {
    return;
  }

  const existingIndex = queryHistory.findIndex((entry) => entry.sql === normalized);
  if (existingIndex >= 0) {
    queryHistory.splice(existingIndex, 1);
  }

  queryHistory.unshift({ sql: normalized, timestamp: Date.now() });
  if (queryHistory.length > HISTORY_LIMIT) {
    queryHistory = queryHistory.slice(0, HISTORY_LIMIT);
  }

  renderHistory();
}

function renderHistory() {
  if (!historyList) {
    return;
  }

  if (!queryHistory.length) {
    historyList.innerHTML = '<div class="empty-subtle">No queries yet. Fire off a SQL blast to seed the timeline.</div>';
    return;
  }

  historyList.innerHTML = '';
  queryHistory.forEach((entry, index) => {
    historyList.appendChild(createHistoryItem(entry, index));
  });
}

function createHistoryItem(entry: QueryHistoryEntry, index: number): HTMLDivElement {
  const item = document.createElement('div');
  item.className = 'history-item';

  const title = document.createElement('h3');
  const time = new Date(entry.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  title.textContent = `Query #${index + 1} · ${time}`;

  const preview = document.createElement('pre');
  preview.className = 'history-preview';
  preview.textContent = entry.sql;

  const actions = document.createElement('div');
  actions.className = 'history-actions';

  const replayButton = document.createElement('button');
  replayButton.textContent = 'Replay';
  replayButton.addEventListener('click', () => {
    sqlInput.value = entry.sql;
    runQuery(entry.sql).catch(reportError);
  });

  const stageButton = document.createElement('button');
  stageButton.textContent = 'Load into SQL Lab';
  stageButton.className = 'ghost-button';
  stageButton.addEventListener('click', () => {
    sqlInput.value = entry.sql;
    updateStatus('Query staged in SQL Lab. Modify and relaunch when ready.');
  });

  const copyButton = document.createElement('button');
  copyButton.textContent = 'Copy';
  copyButton.className = 'ghost-button';
  copyButton.addEventListener('click', async () => {
    const didCopy = await copyTextToClipboard(entry.sql);
    if (!didCopy) {
      updateStatus('Clipboard access is not available in this environment.');
    }
  });

  actions.append(replayButton, stageButton, copyButton);
  item.append(title, preview, actions);
  return item;
}

function tableToObjects(table: Table | null): any[] {
  if (!table) {
    return [];
  }
  const anyTable = table as any;
  if (typeof anyTable.toArray === 'function') {
    return anyTable.toArray();
  }
  const rows: any[] = [];
  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i);
    if (row) {
      rows.push(row);
    }
  }
  return rows;
}

function formatCount(value: number): string {
  if (!Number.isFinite(value)) {
    return '0';
  }
  return Math.round(value).toLocaleString();
}

function formatPercent(value: number): string {
  if (!Number.isFinite(value)) {
    return '0%';
  }
  return `${value.toFixed(1)}%`;
}

function formatNumericValue(value: any, maximumFractionDigits = 4): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'bigint') {
    return Number(value).toLocaleString();
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return '';
    }
    return value.toLocaleString(undefined, { maximumFractionDigits });
  }
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return numeric.toLocaleString(undefined, { maximumFractionDigits });
  }
  return String(value);
}

function toNumber(value: any): number {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0;
  }
  if (typeof value === 'bigint') {
    return Number(value);
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function toSnakeCase(value: string): string {
  let sanitized = value.replace(/[^A-Za-z0-9]+/g, '_').replace(/_{2,}/g, '_').replace(/^_+|_+$/g, '');
  if (!sanitized) {
    sanitized = 'value';
  }
  if (/^[0-9]/.test(sanitized)) {
    sanitized = `c_${sanitized}`;
  }
  return sanitized.toLowerCase();
}

function formatIdentifierForSql(identifier: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)) {
    return identifier;
  }
  return `"${identifier.replace(/"/g, '""')}"`;
}

function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}

function isNumericType(type: string): boolean {
  return /(DOUBLE|FLOAT|REAL|INT|DECIMAL|NUMERIC|HUGEINT|BIGINT|UBIGINT|SMALLINT|TINYINT)/i.test(type);
}

function isTemporalType(type: string): boolean {
  return /(DATE|TIME|TIMESTAMP)/i.test(type);
}

function isTextType(type: string): boolean {
  return /(CHAR|TEXT|STRING|VARCHAR|UUID)/i.test(type);
}

// ---
// Helpers
// ---
function flashCopyState() {
  if (!copySqlButton) {
    return;
  }
  const originalLabel = copySqlButton.textContent ?? 'Copy SQL';
  copySqlButton.textContent = 'Copied!';
  copySqlButton.disabled = true;
  if (copyTimeoutHandle) {
    window.clearTimeout(copyTimeoutHandle);
  }
  copyTimeoutHandle = window.setTimeout(() => {
    copySqlButton!.textContent = originalLabel;
    copySqlButton!.disabled = false;
  }, 1200);
}

function updateStatus(message: string) {
  // Always make the status bar visible when updating
  if (statusWrapper) {
    statusWrapper.style.display = 'block';
  }
  if (status) {
    status.textContent = message;
    status.classList.remove('error'); // Remove error style if it was there
  }
}
function reportError(e: any) {
  const message = e instanceof Error ? e.message : String(e);
  
  // Always make the status bar visible for errors
  if (statusWrapper) {
    statusWrapper.style.display = 'block';
  }
  if (status) {
    status.textContent = `Error: ${message}`;
    status.classList.add('error'); // Add a red error style
  }
  console.error(`[Error] ${message}`, e);
}

// Send the 'ready' signal to the extension to start the handshake
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });
