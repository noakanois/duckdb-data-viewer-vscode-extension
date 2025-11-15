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
const materializeViewButton = document.getElementById('materialize-view') as HTMLButtonElement | null;
const insightsList = document.getElementById('insights-list');
const insightsEmpty = document.getElementById('insights-empty');
const historyList = document.getElementById('history-list');
const historyEmpty = document.getElementById('history-empty');
const clearHistoryButton = document.getElementById('clear-history') as HTMLButtonElement | null;
const viewsList = document.getElementById('views-list');
const viewsEmpty = document.getElementById('views-empty');

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
}

interface ColumnInsight {
  column: string;
  type: string;
  headline: string;
  meta: string[];
}

interface QueryHistoryEntry {
  sql: string;
  timestamp: number;
  rowCount: number;
  durationMs: number;
}

interface MaterializedViewEntry {
  name: string;
  sourceSql: string;
  createdAt: number;
}

interface ColumnStats {
  total: number;
  nulls: number;
  numericCount: number;
  sum: number;
  min: number;
  max: number;
  dateCount: number;
  earliest: number;
  latest: number;
  uniqueValues: Map<string, number>;
  typeHits: Set<string>;
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
let statusFadeTimeout: number | null = null;
let currentRelationIdentifier: string | null = null;
let currentRelationName: string | null = null;
const queryHistory: QueryHistoryEntry[] = [];
const materializedViews: MaterializedViewEntry[] = [];
const MAX_HISTORY_ENTRIES = 25;
const MAX_TRACKED_UNIQUES = 120;
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];

if (insightsList && insightsEmpty) {
  togglePanelState(insightsList, insightsEmpty, false);
}
if (historyList && historyEmpty) {
  togglePanelState(historyList, historyEmpty, false);
}
if (viewsList && viewsEmpty) {
  togglePanelState(viewsList, viewsEmpty, false);
}

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
      const clipboard = navigator.clipboard;
      if (!clipboard) {
        updateStatus('Clipboard access is not available in this environment.');
        return;
      }
      await clipboard.writeText(sqlInput.value);
      flashCopyState();
    } catch (err) {
      updateStatus('Copy to clipboard is unavailable in this context.');
      console.warn('[Webview] Clipboard copy failed', err);
    }
  });
}

if (materializeViewButton) {
  materializeViewButton.addEventListener('click', () => {
    materializeCurrentQuery().catch(reportError);
  });
}

if (clearHistoryButton) {
  clearHistoryButton.addEventListener('click', () => {
    clearQueryHistory(true);
  });
}

if (historyList) {
  historyList.addEventListener('click', (event) => {
    const target = (event.target as HTMLElement).closest('button[data-action]');
    if (!target) {
      return;
    }
    const action = target.getAttribute('data-action');
    const sql = target.getAttribute('data-sql');
    if (!action || !sql) {
      return;
    }
    if (action === 'run') {
      sqlInput.value = sql;
      runQuery(sql).catch(reportError);
    } else if (action === 'copy') {
      navigator.clipboard?.writeText(sql).catch(() => {
        updateStatus('Clipboard access is not available.');
      });
    }
  });
}

if (viewsList) {
  viewsList.addEventListener('click', (event) => {
    const target = (event.target as HTMLElement).closest('button[data-action]');
    if (!target) {
      return;
    }
    const action = target.getAttribute('data-action');
    const viewName = target.getAttribute('data-view');
    if (!action || !viewName) {
      return;
    }
    if (action === 'preview') {
      previewView(viewName).catch(reportError);
    } else if (action === 'drop') {
      dropView(viewName).catch(reportError);
    }
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

  await clearAllTempViews();
  clearQueryHistory();

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

  currentRelationIdentifier = loadResult.relationIdentifier;
  currentRelationName = loadResult.relationName;
  if (rowCountLabel) {
    rowCountLabel.textContent = `Dataset: ${loadResult.relationName} — awaiting query…`;
  }
  updateInsightsPanel([]);

  const defaultQuery = buildDefaultQuery(loadResult.columns, loadResult.relationIdentifier);
  sqlInput.value = defaultQuery;
  sqlInput.placeholder = `Example: ${defaultQuery}`;

  if (controls) {
    controls.style.display = 'flex';
  }
  if (resultsContainer) {
    resultsContainer.style.display = 'block';
  }

  await runQuery(defaultQuery);
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

  const trimmedSql = sql.trim();
  if (!trimmedSql) {
    updateStatus('Enter a SQL query to run.');
    return;
  }

  // Show status bar for "Running query..."
  updateStatus('Running query...');
  runButton.disabled = true;

  try {
    const startTime = performance.now();
    const result = await connection.query(trimmedSql);
    const durationMs = performance.now() - startTime;
    const insights = renderResults(result);
    updateInsightsPanel(insights);

    const rowCount = result?.numRows ?? 0;
    recordQueryHistory(trimmedSql, rowCount, durationMs);
    flashStatusAndFade(`Query complete in ${formatDuration(durationMs)} · ${formatRowCount(rowCount)}`);

  } catch (e) {
    reportError(e); // reportError will show the status bar
  } finally {
    runButton.disabled = false;
  }
}

function renderResults(table: Table | null): ColumnInsight[] {
  if (!resultsContainer) {
    return [];
  }

  if (!table || table.numRows === 0) {
    resultsContainer.innerHTML = '<div class="empty-state">Query completed. No rows returned.</div>';
    currentTableData = null;
    tableBodyElement = null;
    updateRowCount(0, 0);
    return [];
  }

  const rows: TableRow[] = [];
  const columns = table.schema.fields.map((field) => field.name);
  const stats = columns.map(() => createEmptyColumnStats());

  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i);
    if (!row) {
      continue;
    }

    const raw: any[] = [];
    const display: string[] = [];
    for (const [fieldIndex, field] of table.schema.fields.entries()) {
      const value = row[field.name];
      const formatted = formatCell(value);
      raw.push(value);
      display.push(formatted);
      updateColumnStats(stats[fieldIndex], value, formatted);
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
  return createColumnInsights(columns, stats, table.numRows);
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
  const datasetLabel = currentRelationName ? `${currentRelationName} · ` : '';
  const base = total > 0
    ? `Showing ${formatInteger(visible)} of ${formatInteger(total)} rows`
    : `Showing ${formatInteger(visible)} rows`;
  const filtered = total > 0 && visible !== total ? ' (filtered)' : '';
  rowCountLabel.textContent = `${datasetLabel}${base}${filtered}`;
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

function createEmptyColumnStats(): ColumnStats {
  return {
    total: 0,
    nulls: 0,
    numericCount: 0,
    sum: 0,
    min: Number.POSITIVE_INFINITY,
    max: Number.NEGATIVE_INFINITY,
    dateCount: 0,
    earliest: Number.POSITIVE_INFINITY,
    latest: Number.NEGATIVE_INFINITY,
    uniqueValues: new Map<string, number>(),
    typeHits: new Set<string>(),
  };
}

function updateColumnStats(stats: ColumnStats, value: any, formatted: string) {
  stats.total++;
  if (value === null || value === undefined || (typeof value === 'number' && Number.isNaN(value))) {
    stats.nulls++;
    return;
  }

  const typeLabel = value instanceof Date ? 'date' : typeof value;
  stats.typeHits.add(typeLabel);

  if (typeLabel === 'number' && Number.isFinite(value)) {
    stats.numericCount++;
    stats.sum += value;
    stats.min = Math.min(stats.min, value);
    stats.max = Math.max(stats.max, value);
  } else if (value instanceof Date) {
    stats.dateCount++;
    const timestamp = value.getTime();
    stats.earliest = Math.min(stats.earliest, timestamp);
    stats.latest = Math.max(stats.latest, timestamp);
  }

  const key = formatted === '' ? '(empty)' : formatted;
  const current = stats.uniqueValues.get(key);
  if (current !== undefined) {
    stats.uniqueValues.set(key, current + 1);
  } else if (stats.uniqueValues.size < MAX_TRACKED_UNIQUES) {
    stats.uniqueValues.set(key, 1);
  }
}

function createColumnInsights(columns: string[], stats: ColumnStats[], totalRows: number): ColumnInsight[] {
  return columns.map((column, index) => {
    const stat = stats[index];
    const nonNullCount = stat.total - stat.nulls;
    const uniqueCount = stat.uniqueValues.size;
    const coverageRatio = stat.total > 0 ? nonNullCount / stat.total : 0;

    let type = 'Mixed';
    let headline = '';
    const meta: string[] = [];

    if (stat.total === 0 || nonNullCount === 0) {
      type = 'Empty';
      headline = 'All values are NULL or empty.';
    } else if (stat.numericCount > 0 && stat.numericCount + stat.nulls === stat.total) {
      type = 'Numeric';
      const minValue = Number.isFinite(stat.min) ? formatNumber(stat.min) : 'n/a';
      const maxValue = Number.isFinite(stat.max) ? formatNumber(stat.max) : 'n/a';
      const average = stat.numericCount > 0 ? formatNumber(stat.sum / stat.numericCount) : 'n/a';
      headline = `Avg ${average} · Range ${minValue} → ${maxValue}`;
      meta.push(`Numeric values ${formatInteger(stat.numericCount)}`);
    } else if (stat.dateCount > 0 && stat.dateCount + stat.nulls === stat.total) {
      type = 'Temporal';
      const earliest = Number.isFinite(stat.earliest) ? formatDate(stat.earliest) : 'n/a';
      const latest = Number.isFinite(stat.latest) ? formatDate(stat.latest) : 'n/a';
      headline = `Span ${earliest} → ${latest}`;
      meta.push(`Temporal values ${formatInteger(stat.dateCount)}`);
    } else if (stat.typeHits.size === 1 && stat.typeHits.has('boolean')) {
      type = 'Boolean';
      const topValues = describeTopValues(stat, nonNullCount);
      headline = topValues.length ? `Distribution ${topValues.join(', ')}` : 'Boolean split unavailable';
    } else if (stat.typeHits.size === 1 && stat.typeHits.has('string')) {
      type = 'Text';
      const topValues = describeTopValues(stat, nonNullCount);
      headline = topValues.length ? `Top values: ${topValues.join(', ')}` : 'All values unique.';
    } else {
      type = 'Mixed';
      const topValues = describeTopValues(stat, nonNullCount);
      headline = topValues.length ? `Leaders: ${topValues.join(', ')}` : 'Mixed data detected.';
    }

    meta.push(`Coverage ${formatPercent(coverageRatio)}`);
    if (stat.nulls > 0) {
      meta.push(`${formatInteger(stat.nulls)} null${stat.nulls === 1 ? '' : 's'}`);
    }
    if (uniqueCount > 0) {
      meta.push(`${formatInteger(uniqueCount)} unique`);
    }

    return { column, type, headline, meta };
  });
}

function describeTopValues(stat: ColumnStats, nonNullCount: number): string[] {
  if (nonNullCount === 0) {
    return [];
  }
  const sorted = Array.from(stat.uniqueValues.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3);
  return sorted.map(([value, count]) => {
    const label = value === '(empty)' ? '∅ empty' : value;
    return `${label} (${formatPercent(count / nonNullCount)})`;
  });
}

function updateInsightsPanel(insights: ColumnInsight[]) {
  if (!insightsList || !insightsEmpty) {
    return;
  }
  insightsList.innerHTML = '';
  if (insights.length === 0) {
    togglePanelState(insightsList, insightsEmpty, false);
    return;
  }
  togglePanelState(insightsList, insightsEmpty, true);

  insights.forEach((insight) => {
    const li = document.createElement('li');
    const heading = document.createElement('div');
    heading.className = 'list-heading';
    const columnSpan = document.createElement('span');
    columnSpan.textContent = insight.column;
    const typeBadge = document.createElement('span');
    typeBadge.className = 'meta-pill';
    typeBadge.textContent = insight.type;
    heading.append(columnSpan, typeBadge);

    const headline = document.createElement('div');
    headline.className = 'list-headline';
    headline.textContent = insight.headline;
    li.append(heading, headline);

    if (insight.meta.length > 0) {
      const metaRow = document.createElement('div');
      metaRow.className = 'list-meta';
      insight.meta.forEach((metaItem) => {
        const pill = document.createElement('span');
        pill.className = 'meta-pill';
        pill.textContent = metaItem;
        metaRow.appendChild(pill);
      });
      li.appendChild(metaRow);
    }

    insightsList.appendChild(li);
  });
}

function togglePanelState(list: HTMLElement, placeholder: HTMLElement, showList: boolean) {
  list.style.display = showList ? 'flex' : 'none';
  placeholder.style.display = showList ? 'none' : 'block';
}

function recordQueryHistory(sql: string, rowCount: number, durationMs: number) {
  const normalized = sql.trim();
  if (!normalized) {
    return;
  }

  const existingIndex = queryHistory.findIndex((entry) => entry.sql === normalized);
  if (existingIndex >= 0) {
    queryHistory.splice(existingIndex, 1);
  }

  queryHistory.unshift({
    sql: normalized,
    timestamp: Date.now(),
    rowCount,
    durationMs,
  });

  if (queryHistory.length > MAX_HISTORY_ENTRIES) {
    queryHistory.length = MAX_HISTORY_ENTRIES;
  }

  renderHistoryList();
}

function renderHistoryList() {
  if (!historyList || !historyEmpty) {
    return;
  }

  historyList.innerHTML = '';
  if (queryHistory.length === 0) {
    togglePanelState(historyList, historyEmpty, false);
    return;
  }

  togglePanelState(historyList, historyEmpty, true);

  queryHistory.forEach((entry) => {
    const li = document.createElement('li');

    const heading = document.createElement('div');
    heading.className = 'list-heading';
    const timeLabel = document.createElement('span');
    timeLabel.textContent = formatRelativeTime(entry.timestamp);
    const badge = document.createElement('span');
    badge.className = 'meta-pill';
    badge.textContent = `${formatRowCount(entry.rowCount)} · ${formatDuration(entry.durationMs)}`;
    heading.append(timeLabel, badge);

    const sqlPreview = document.createElement('pre');
    sqlPreview.className = 'sql-snippet';
    sqlPreview.textContent = entry.sql;

    const actions = document.createElement('div');
    actions.className = 'list-actions';

    const runButtonEl = document.createElement('button');
    runButtonEl.textContent = 'Run';
    runButtonEl.setAttribute('data-action', 'run');
    runButtonEl.setAttribute('data-sql', entry.sql);

    const copyButtonEl = document.createElement('button');
    copyButtonEl.textContent = 'Copy';
    copyButtonEl.className = 'secondary-button';
    copyButtonEl.setAttribute('data-action', 'copy');
    copyButtonEl.setAttribute('data-sql', entry.sql);

    actions.append(runButtonEl, copyButtonEl);

    li.append(heading, sqlPreview, actions);
    historyList.appendChild(li);
  });
}

function clearQueryHistory(announce = false) {
  queryHistory.length = 0;
  renderHistoryList();
  if (announce) {
    flashStatusAndFade('Query history cleared.');
  }
}

function renderViews() {
  if (!viewsList || !viewsEmpty) {
    return;
  }

  viewsList.innerHTML = '';
  if (materializedViews.length === 0) {
    togglePanelState(viewsList, viewsEmpty, false);
    return;
  }

  togglePanelState(viewsList, viewsEmpty, true);

  materializedViews.forEach((view) => {
    const li = document.createElement('li');

    const heading = document.createElement('div');
    heading.className = 'list-heading';
    const nameSpan = document.createElement('span');
    nameSpan.textContent = view.name;
    const timeBadge = document.createElement('span');
    timeBadge.className = 'meta-pill';
    timeBadge.textContent = formatRelativeTime(view.createdAt);
    heading.append(nameSpan, timeBadge);

    const sqlPreview = document.createElement('pre');
    sqlPreview.className = 'sql-snippet';
    sqlPreview.textContent = view.sourceSql;

    const actions = document.createElement('div');
    actions.className = 'list-actions';

    const previewButton = document.createElement('button');
    previewButton.textContent = 'Preview';
    previewButton.setAttribute('data-action', 'preview');
    previewButton.setAttribute('data-view', view.name);

    const dropButton = document.createElement('button');
    dropButton.textContent = 'Drop';
    dropButton.className = 'secondary-button';
    dropButton.setAttribute('data-action', 'drop');
    dropButton.setAttribute('data-view', view.name);

    actions.append(previewButton, dropButton);

    li.append(heading, sqlPreview, actions);
    viewsList.appendChild(li);
  });
}

async function materializeCurrentQuery() {
  if (!connection) {
    throw new Error('No database connection.');
  }

  const sql = sqlInput.value.trim();
  if (!sql) {
    updateStatus('Enter a SQL query before materializing a view.');
    return;
  }

  const suggestion = generateViewSuggestion();
  const name = window.prompt('Name for the temporary DuckDB view', suggestion);
  if (!name) {
    return;
  }

  const normalized = name.trim();
  if (!normalized) {
    updateStatus('View name cannot be empty.');
    return;
  }

  const statement = `CREATE OR REPLACE TEMP VIEW ${escapeIdentifier(normalized)} AS ${sql}`;
  await connection.query(statement);

  const existingIndex = materializedViews.findIndex((entry) => entry.name === normalized);
  if (existingIndex >= 0) {
    materializedViews.splice(existingIndex, 1);
  }

  materializedViews.unshift({
    name: normalized,
    sourceSql: sql,
    createdAt: Date.now(),
  });

  renderViews();
  flashStatusAndFade(`Temp view ${normalized} is ready.`);
}

function generateViewSuggestion(): string {
  const base = (currentRelationIdentifier ?? currentRelationName ?? 'query_view')
    .replace(/[^A-Za-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase() || 'query_view';

  let candidate = `${base}_view`;
  let counter = 1;
  while (materializedViews.some((view) => view.name === candidate)) {
    counter++;
    candidate = `${base}_view_${counter}`;
  }
  return candidate;
}

async function previewView(viewName: string) {
  const previewSql = `SELECT * FROM ${escapeIdentifier(viewName)} LIMIT 200`;
  sqlInput.value = `${previewSql};`;
  await runQuery(previewSql);
}

async function dropView(viewName: string) {
  if (!connection) {
    throw new Error('No database connection.');
  }

  await connection.query(`DROP VIEW IF EXISTS ${escapeIdentifier(viewName)}`);
  const index = materializedViews.findIndex((view) => view.name === viewName);
  if (index >= 0) {
    materializedViews.splice(index, 1);
  }
  renderViews();
  flashStatusAndFade(`Dropped view ${viewName}.`);
}

async function clearAllTempViews() {
  if (materializedViews.length === 0) {
    return;
  }
  if (!connection) {
    materializedViews.length = 0;
    renderViews();
    return;
  }

  const names = materializedViews.map((view) => view.name);
  materializedViews.length = 0;
  for (const name of names) {
    try {
      await connection.query(`DROP VIEW IF EXISTS ${escapeIdentifier(name)}`);
    } catch (err) {
      console.warn('Failed to drop view', name, err);
    }
  }
  renderViews();
}

function flashStatusAndFade(message: string) {
  updateStatus(message);
  if (statusFadeTimeout) {
    window.clearTimeout(statusFadeTimeout);
  }
  statusFadeTimeout = window.setTimeout(() => {
    if (statusWrapper) {
      statusWrapper.style.display = 'none';
    }
    statusFadeTimeout = null;
  }, 3500);
}

function formatDuration(ms: number): string {
  if (ms < 1) {
    return '0 ms';
  }
  if (ms < 1000) {
    return `${Math.round(ms)} ms`;
  }
  if (ms < 60000) {
    return `${(ms / 1000).toFixed(2)} s`;
  }
  return `${(ms / 60000).toFixed(2)} min`;
}

const integerFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const decimalFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
const percentFormatter = new Intl.NumberFormat(undefined, { style: 'percent', maximumFractionDigits: 1 });

function formatRowCount(count: number): string {
  return count === 1 ? '1 row' : `${formatInteger(count)} rows`;
}

function formatInteger(value: number): string {
  return integerFormatter.format(Math.max(0, Math.round(value)));
}

function formatNumber(value: number): string {
  return decimalFormatter.format(value);
}

function formatPercent(value: number): string {
  return percentFormatter.format(Math.max(0, value));
}

function formatRelativeTime(timestamp: number): string {
  const diffMs = Date.now() - timestamp;
  const diffSeconds = Math.floor(diffMs / 1000);
  if (diffSeconds < 5) {
    return 'Just now';
  }
  if (diffSeconds < 60) {
    return `${diffSeconds}s ago`;
  }
  const diffMinutes = Math.floor(diffSeconds / 60);
  if (diffMinutes < 60) {
    return `${diffMinutes}m ago`;
  }
  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours}h ago`;
  }
  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 7) {
    return `${diffDays}d ago`;
  }
  return new Date(timestamp).toLocaleDateString();
}

function formatDate(timestamp: number): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return 'n/a';
  }
  return date.toISOString().replace('T', ' ').replace('Z', ' UTC');
}

function escapeIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
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
  if (statusFadeTimeout) {
    window.clearTimeout(statusFadeTimeout);
    statusFadeTimeout = null;
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
  if (statusFadeTimeout) {
    window.clearTimeout(statusFadeTimeout);
    statusFadeTimeout = null;
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
