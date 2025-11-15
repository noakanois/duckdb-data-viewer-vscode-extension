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
const cosmicTagline = document.getElementById('cosmic-tagline');
const hyperLabel = document.getElementById('hyper-label');
const dreamscapeStatus = document.getElementById('dreamscape-status');
const moodRingCore = document.getElementById('mood-ring-core') as HTMLDivElement | null;
const moodRingLabel = document.getElementById('mood-ring-label');
const columnNebula = document.getElementById('column-nebula');
const sqlSpellbook = document.getElementById('sql-spellbook');

const COSMIC_INTROS = [
  'Amplifying your data aura…',
  'Tuning hyperspatial resonances…',
  'Summoning rows from the void…',
  'Aligning qubits with cosmic schemas…',
  'Mapping constellations of columns…',
];

const MOOD_TITLES = [
  'Nebula Serenade',
  'Hypernova Groove',
  'Gravity Disco',
  'Binary Aurora',
  'Quantum Chillwave',
  'Plasma Ballet',
];

const SPELLBOOK_INTROS = [
  'Project a kaleidoscope of columns',
  'Harmonize aggregates with cosmic beats',
  'Slice dimensions with galactic precision',
  'Distill the brightest stars from your table',
];

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
}

let db: duckdb.AsyncDuckDB | null = null;
let connection: duckdb.AsyncDuckDBConnection | null = null;
let duckdbInitializationPromise: Promise<void> | null = null;
let currentTableData: TableData | null = null;
let currentRelationIdentifier: string | null = null;
let columnFilters: string[] = [];
let globalFilter = '';
let sortState: { columnIndex: number; direction: SortDirection } = { columnIndex: -1, direction: null };
let tableBodyElement: HTMLTableSectionElement | null = null;
let copyTimeoutHandle: number | null = null;
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];

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

  currentRelationIdentifier = loadResult.relationIdentifier;

  if (cosmicTagline) {
    const intro = COSMIC_INTROS[Math.floor(Math.random() * COSMIC_INTROS.length)] ?? 'Preparing your dataset…';
    cosmicTagline.textContent = `${intro} (${fileName})`;
  }
  if (hyperLabel) {
    hyperLabel.textContent = 'Dreamstate calibrating';
  }
  serenadeSpellbookIntro();

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
  
  // Show status bar for "Running query..."
  updateStatus('Running query...');
  runButton.disabled = true;
  if (hyperLabel) {
    hyperLabel.textContent = 'Dreamstate oscillating';
  }

  try {
    const result = await connection.query(sql);
    renderResults(result);
    
    // --- CHANGE ---
    // Hide the status bar on success
    if (statusWrapper) {
      statusWrapper.style.display = 'none';
    }
    if (hyperLabel) {
      hyperLabel.textContent = 'Dreamstate euphoric';
    }
    // ---

  } catch (e) {
    reportError(e); // reportError will show the status bar
    if (hyperLabel) {
      hyperLabel.textContent = 'Dreamstate disrupted';
    }
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
    lullDreamscape('Cosmic silence – no rows echoed back.');
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
  igniteDreamscape(currentTableData);
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
  if (dreamscapeStatus && currentTableData) {
    dreamscapeStatus.textContent = `Orbiting ${currentTableData.columns.length} constellations · ${visibleRows.length.toLocaleString()} of ${currentTableData.rows.length.toLocaleString()} rows shimmering`;
  }
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
  if (total === 0) {
    rowCountLabel.textContent = 'No rows shimmering in this dimension';
    return;
  }

  const visibility = Math.round((visible / total) * 100);
  rowCountLabel.innerHTML = `<span class="row-count-intensity">${visible.toLocaleString()}</span> / ${total.toLocaleString()} ROWS · ${visibility}% VISIBLE`;
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

function serenadeSpellbookIntro() {
  if (!sqlSpellbook) {
    return;
  }
  sqlSpellbook.innerHTML = '';
  const intro = document.createElement('li');
  intro.textContent = SPELLBOOK_INTROS[Math.floor(Math.random() * SPELLBOOK_INTROS.length)] ?? 'Channeling SQL muses…';
  intro.style.opacity = '0.7';
  sqlSpellbook.appendChild(intro);
}

function igniteDreamscape(tableData: TableData | null) {
  if (!tableData || !moodRingCore || !moodRingLabel || !dreamscapeStatus) {
    return;
  }

  const columnCount = tableData.columns.length;
  const rowCount = tableData.rows.length;
  const totalCells = Math.max(1, columnCount * rowCount);

  let numericCount = 0;
  let nullishCount = 0;
  let textCount = 0;

  tableData.rows.forEach((row) => {
    row.raw.forEach((value) => {
      if (value === null || value === undefined) {
        nullishCount += 1;
      } else if (typeof value === 'number' && Number.isFinite(value)) {
        numericCount += 1;
      } else if (typeof value === 'string') {
        textCount += 1;
      }
    });
  });

  const numericRatio = numericCount / totalCells;
  const nullRatio = nullishCount / totalCells;
  const hue = Math.round((columnCount * 47 + rowCount) % 360);
  const saturation = Math.round(40 + numericRatio * 60);
  const lightness = Math.round(48 + (0.5 - nullRatio) * 20);

  moodRingCore.style.setProperty('--mood-hue', String(hue));
  moodRingCore.style.setProperty('--mood-saturation', `${Math.min(100, Math.max(30, saturation))}%`);
  moodRingCore.style.setProperty('--mood-lightness', `${Math.min(80, Math.max(30, lightness))}%`);

  const title = MOOD_TITLES[Math.floor(Math.random() * MOOD_TITLES.length)] ?? 'Data Reverie';
  moodRingLabel.textContent = `${title} · ${columnCount} columns · ${rowCount.toLocaleString()} rows`;
  dreamscapeStatus.textContent = `Orbiting ${columnCount} constellations · ${rowCount.toLocaleString()} records streaming`;

  if (cosmicTagline) {
    const intensity = (1 - nullRatio + numericRatio).toFixed(2);
    cosmicTagline.textContent = `Hypercolor index ${intensity} · Columns vibrating at ${columnCount} frequencies`;
  }

  renderNebula(tableData);
  renderSpellbook(tableData);
}

function lullDreamscape(reason: string) {
  if (dreamscapeStatus) {
    dreamscapeStatus.textContent = reason;
  }
  if (moodRingLabel) {
    moodRingLabel.textContent = 'Moodless void';
  }
  if (moodRingCore) {
    moodRingCore.style.setProperty('--mood-lightness', '45%');
    moodRingCore.style.setProperty('--mood-saturation', '25%');
  }
  serenadeSpellbookIntro();
  if (columnNebula) {
    columnNebula.innerHTML = '';
  }
  if (cosmicTagline) {
    cosmicTagline.textContent = 'Awaiting cosmic dataset alignment…';
  }
}

function renderNebula(tableData: TableData) {
  if (!columnNebula) {
    return;
  }

  columnNebula.innerHTML = '';
  const sampleRow = tableData.rows[0]?.display ?? [];

  tableData.columns.forEach((column, index) => {
    const chip = document.createElement('button');
    chip.type = 'button';
    chip.className = 'nebula-chip';
    chip.textContent = column;
    const sampleValue = sampleRow[index];
    if (sampleValue) {
      chip.title = `Sample: ${sampleValue}`;
    }
    chip.addEventListener('click', () => {
      const relation = currentRelationIdentifier ?? 'data';
      const incantation = `SELECT "${column}" AS column_value, COUNT(*) AS appearances FROM ${relation} GROUP BY 1 ORDER BY appearances DESC LIMIT 25;`;
      sqlInput.value = incantation;
      sqlInput.focus();
    });
    columnNebula.appendChild(chip);
  });
}

function renderSpellbook(tableData: TableData) {
  if (!sqlSpellbook) {
    return;
  }

  const spells = conjureSpells(tableData);
  sqlSpellbook.innerHTML = '';
  if (spells.length === 0) {
    serenadeSpellbookIntro();
    return;
  }
  spells.forEach((spell) => {
    const item = document.createElement('li');
    item.innerHTML = `<strong>${spell.title}</strong><br><code>${spell.sql}</code>`;
    item.addEventListener('click', () => {
      sqlInput.value = spell.sql;
      sqlInput.focus();
    });
    sqlSpellbook.appendChild(item);
  });
}

function conjureSpells(tableData: TableData): { title: string; sql: string }[] {
  const relation = currentRelationIdentifier ?? 'data';
  const columns = tableData.columns;
  const spells: { title: string; sql: string }[] = [];

  if (columns.length === 0) {
    return spells;
  }

  const firstColumn = columns[0];
  const randomColumn = columns[Math.floor(Math.random() * columns.length)] ?? firstColumn;
  let numericIndex = -1;
  for (let colIndex = 0; colIndex < columns.length; colIndex++) {
    const hasNumeric = tableData.rows.some((row) => {
      const cell = row.raw[colIndex];
      return typeof cell === 'number' && Number.isFinite(cell);
    });
    if (hasNumeric) {
      numericIndex = colIndex;
      break;
    }
  }
  const numericName = numericIndex >= 0 ? columns[numericIndex] : null;

  const projectionColumns = columns.slice(0, Math.min(3, columns.length)).map((name) => `"${name}"`).join(', ');
  const projectionSelection = projectionColumns || '*';

  spells.push({
    title: 'Prism Split',
    sql: `SELECT ${projectionSelection} FROM ${relation} LIMIT 33;`,
  });

  if (numericName) {
    spells.push({
      title: 'Gravitational Pulse',
      sql: `SELECT MIN("${numericName}") AS min_val, AVG("${numericName}") AS avg_val, MAX("${numericName}") AS max_val FROM ${relation};`,
    });
  }

  spells.push({
    title: 'Nova Filter',
    sql: `SELECT * FROM ${relation} WHERE "${randomColumn}" IS NOT NULL LIMIT 50;`,
  });

  spells.push({
    title: 'Constellation Count',
    sql: `SELECT "${firstColumn}", COUNT(*) AS frequency FROM ${relation} GROUP BY 1 ORDER BY frequency DESC LIMIT 25;`,
  });

  return spells;
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
  lullDreamscape(`Distress signal: ${message}`);
  console.error(`[Error] ${message}`, e);
}

// Send the 'ready' signal to the extension to start the handshake
serenadeSpellbookIntro();
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });
