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

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

type ColumnCategory = 'number' | 'string' | 'boolean' | 'temporal' | 'other';

interface ColumnMetadata {
  name: string;
  arrowType: string;
  category: ColumnCategory;
  emoji: string;
  tagline: string;
}

interface TableData {
  columns: string[];
  rows: TableRow[];
  columnMetadata: ColumnMetadata[];
}

let db: duckdb.AsyncDuckDB | null = null;
let connection: duckdb.AsyncDuckDBConnection | null = null;
let duckdbInitializationPromise: Promise<void> | null = null;
let currentTableData: TableData | null = null;
let currentRelation: { name: string; identifier: string } | null = null;
let columnFilters: string[] = [];
let globalFilter = '';
let sortState: { columnIndex: number; direction: SortDirection } = { columnIndex: -1, direction: null };
let tableBodyElement: HTMLTableSectionElement | null = null;
let copyTimeoutHandle: number | null = null;
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];

// Hyperdrive UI references
const hyperdriveBanner = document.getElementById('hyperdrive-banner');
const toggleHyperdriveButton = document.getElementById('toggle-hyperdrive') as HTMLButtonElement | null;
const chaosQueryButton = document.getElementById('chaos-query') as HTMLButtonElement | null;
const querySuggestionsList = document.getElementById('query-suggestions');
const dataProphecyElement = document.getElementById('data-prophecy');
const remixProphecyButton = document.getElementById('remix-prophecy') as HTMLButtonElement | null;
const columnGlyphsElement = document.getElementById('column-glyphs');
const cosmicMoodElement = document.getElementById('cosmic-mood');
const hyperdriveBadgeElement = document.getElementById('hyperdrive-badge');

let lastSparkleTimestamp = 0;

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

if (toggleHyperdriveButton) {
  toggleHyperdriveButton.addEventListener('click', () => {
    toggleHyperdrive();
  });
}

if (chaosQueryButton) {
  chaosQueryButton.addEventListener('click', () => {
    unleashChaosQuery().catch(reportError);
  });
}

if (remixProphecyButton) {
  remixProphecyButton.addEventListener('click', () => {
    updateDataProphecy(true);
  });
}

if (querySuggestionsList) {
  querySuggestionsList.addEventListener('click', (event) => {
    const target = event.target as HTMLElement;
    if (target?.dataset?.sql) {
      const sql = target.dataset.sql;
      sqlInput.value = sql;
      runQuery(sql).catch(reportError);
      spawnSparkleTrail(event as PointerEvent);
    }
  });
}

document.addEventListener('pointermove', (event) => {
  const now = Date.now();
  if (!document.body.classList.contains('hyperdrive')) {
    return;
  }
  if (now - lastSparkleTimestamp < 120) {
    return;
  }
  lastSparkleTimestamp = now;
  spawnSparkleTrail(event);
});

updateCosmicControlsAvailability();

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

  const defaultQuery = buildDefaultQuery(loadResult.columns, loadResult.relationIdentifier);
  sqlInput.value = defaultQuery;
  sqlInput.placeholder = `Example: ${defaultQuery}`;

  currentRelation = {
    name: loadResult.relationName,
    identifier: loadResult.relationIdentifier,
  };
  updateCosmicControlsAvailability();
  updateCosmicMood();

  if (controls) controls.style.display = 'flex';
  if (resultsContainer) resultsContainer.style.display = 'block';

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

  try {
    const result = await connection.query(sql);
    renderResults(result);
    
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
    resetCosmicPanels();
    return;
  }

  const rows: TableRow[] = [];
  const columns = table.schema.fields.map((field) => field.name);
  const sampleValues = columns.map(() => new Set<string>());

  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i);
    if (!row) continue;

    const raw: any[] = [];
    const display: string[] = [];
    table.schema.fields.forEach((field, columnIndex) => {
      const value = row[field.name];
      raw.push(value);
      const formatted = formatCell(value);
      display.push(formatted);
      if (formatted && sampleValues[columnIndex].size < 3) {
        sampleValues[columnIndex].add(formatted);
      }
    });
    rows.push({ raw, display });
  }

  const columnMetadata = buildColumnMetadata(table, sampleValues);

  currentTableData = { columns, rows, columnMetadata };
  columnFilters = columns.map(() => '');
  globalFilter = '';
  sortState = { columnIndex: -1, direction: null };
  if (globalSearchInput) {
    globalSearchInput.value = '';
    globalSearchInput.placeholder = 'Search the data nebula…';
  }

  buildTableSkeleton(columns);
  applyTableState();

  resultsContainer.style.display = 'block';
  resultsContainer.scrollTop = 0;
  populateQuerySpellbook();
  updateColumnGlyphs();
  updateDataProphecy();
  updateCosmicMood();
  updateCosmicAura(table.numRows, columns.length);
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

    const labelWrapper = document.createElement('span');
    labelWrapper.className = 'column-label-wrapper';
    const glyph = document.createElement('span');
    glyph.className = 'glyph-icon';
    const metadata = currentTableData?.columnMetadata[index];
    glyph.textContent = metadata?.emoji ?? '⬡';
    const label = document.createElement('span');
    label.textContent = column;
    labelWrapper.append(glyph, label);
    const indicator = document.createElement('span');
    indicator.className = 'sort-indicator';

    button.append(labelWrapper, indicator);
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
    const metadata = currentTableData?.columnMetadata[index];
    input.placeholder = metadata ? `Filter ${metadata.category} vibes` : 'Filter';
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
      if (!filter) return true;
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
  if (total === 0) {
    rowCountLabel.textContent = 'No rows shimmering yet.';
    return;
  }

  const vibes = ['luminous', 'hyperspatial', 'electric', 'chromatic', 'quantum'];
  const vibe = vibes[(visible + total) % vibes.length];
  rowCountLabel.innerHTML = `<strong>${visible.toLocaleString()}</strong> ${vibe} rows visible · ${total.toLocaleString()} total in orbit`;
}

function compareValues(a: any, b: any, aDisplay: string, bDisplay: string): number {
  if (a === b) return 0;

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

function buildColumnMetadata(table: Table, sampleValues: Set<string>[]): ColumnMetadata[] {
  return table.schema.fields.map((field, index) => {
    const typeLabel = typeof field.type?.toString === 'function' ? field.type.toString() : 'unknown';
    const category = categorizeArrowType(typeLabel);
    const emoji = pickEmojiForCategory(category);
    const samples = Array.from(sampleValues[index] ?? []);
    const tagline = craftColumnTagline(field.name, category, samples, typeLabel);
    return {
      name: field.name,
      arrowType: typeLabel,
      category,
      emoji,
      tagline,
    };
  });
}

function categorizeArrowType(typeLabel: string): ColumnCategory {
  const normalized = typeLabel.toLowerCase();
  if (normalized.includes('int') || normalized.includes('float') || normalized.includes('decimal') || normalized.includes('double')) {
    return 'number';
  }
  if (normalized.includes('bool')) {
    return 'boolean';
  }
  if (normalized.includes('date') || normalized.includes('time') || normalized.includes('timestamp')) {
    return 'temporal';
  }
  if (normalized.includes('utf') || normalized.includes('string') || normalized.includes('binary')) {
    return 'string';
  }
  return 'other';
}

function pickEmojiForCategory(category: ColumnCategory): string {
  switch (category) {
    case 'number':
      return '🔢';
    case 'string':
      return '🔤';
    case 'boolean':
      return '🌓';
    case 'temporal':
      return '⏳';
    default:
      return '🧬';
  }
}

function craftColumnTagline(name: string, category: ColumnCategory, samples: string[], typeLabel: string): string {
  const sampleSnippet = samples.length ? `e.g. ${samples.slice(0, 2).join(' · ')}` : `type ${typeLabel}`;
  const channelPhrases: Record<ColumnCategory, string[]> = {
    number: ['calibrates gravity wells', 'powers nebula math', 'tracks cosmic frequencies'],
    string: ['whispers galactic names', 'stores cosmic myths', 'encodes interstellar lore'],
    boolean: ['toggles wormholes', 'flips reality switches', 'controls starlight or shadow'],
    temporal: ['measures chronostreams', 'anchors timeline echoes', 'charts orbital dawns'],
    other: ['houses quantum curios', 'guards enigmatic relics', 'contains uncharted matter'],
  };
  const phrases = channelPhrases[category];
  const descriptor = phrases[Math.floor(Math.random() * phrases.length)];
  return `${descriptor}; ${sampleSnippet}`;
}

function formatColumnIdentifier(column: string): string {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(column) ? column : `"${column.replace(/"/g, '""')}"`;
}

function populateQuerySpellbook() {
  if (!querySuggestionsList) {
    return;
  }
  if (!currentTableData || !currentRelation) {
    querySuggestionsList.innerHTML = '<li>Awaiting data to brew spells…</li>';
    return;
  }

  const suggestions = generateQuerySuggestions();
  if (!suggestions.length) {
    querySuggestionsList.innerHTML = '<li>The spellbook is momentarily blank.</li>';
    return;
  }

  querySuggestionsList.innerHTML = '';
  suggestions.forEach((suggestion) => {
    const li = document.createElement('li');
    const description = document.createElement('div');
    description.textContent = suggestion.label;
    const actions = document.createElement('div');
    actions.className = 'suggestion-actions';
    const sqlPreview = document.createElement('code');
    sqlPreview.textContent = suggestion.preview;
    const castButton = document.createElement('button');
    castButton.type = 'button';
    castButton.textContent = 'Cast spell';
    castButton.dataset.sql = suggestion.sql;
    actions.append(sqlPreview, castButton);
    li.append(description, actions);
    querySuggestionsList.appendChild(li);
  });
}

function generateQuerySuggestions(): Array<{ label: string; sql: string; preview: string }> {
  if (!currentTableData || !currentRelation) {
    return [];
  }

  const { columnMetadata } = currentTableData;
  const sampleSize = Math.max(currentTableData.rows.length, 1);
  const limit = Math.min(50, Math.max(7, Math.round(sampleSize / 4)));
  const base = currentRelation.identifier;

  const numericColumn = columnMetadata.find((meta) => meta.category === 'number');
  const temporalColumn = columnMetadata.find((meta) => meta.category === 'temporal');
  const stringColumn = columnMetadata.find((meta) => meta.category === 'string');

  const suggestions: Array<{ label: string; sql: string; preview: string }> = [
    {
      label: 'Sample the cosmic lattice',
      sql: `SELECT * FROM ${base}\nORDER BY RANDOM()\nLIMIT ${limit};`,
      preview: `Random ${limit} rows`,
    },
    {
      label: 'Count the luminous bodies',
      sql: `SELECT COUNT(*) AS cosmic_count\nFROM ${base};`,
      preview: 'COUNT(*) cosmos',
    },
  ];

  if (numericColumn) {
    const numericIdentifier = formatColumnIdentifier(numericColumn.name);
    suggestions.push({
      label: `Amplify ${numericColumn.name} harmonics`,
      sql: `SELECT ${numericIdentifier}, AVG(${numericIdentifier}) AS avg_${numericColumn.name}\nFROM ${base}\nGROUP BY ${numericIdentifier}\nORDER BY avg_${numericColumn.name} DESC\nLIMIT 15;`,
      preview: `AVG(${numericColumn.name})`,
    });
  }

  if (temporalColumn && stringColumn) {
    const temporalIdentifier = formatColumnIdentifier(temporalColumn.name);
    const stringIdentifier = formatColumnIdentifier(stringColumn.name);
    suggestions.push({
      label: `Plot ${temporalColumn.name} constellations by ${stringColumn.name}`,
      sql: `SELECT ${stringIdentifier}, MIN(${temporalIdentifier}) AS first_${temporalColumn.name}, MAX(${temporalIdentifier}) AS last_${temporalColumn.name}\nFROM ${base}\nGROUP BY ${stringIdentifier}\nORDER BY last_${temporalColumn.name} DESC\nLIMIT 25;`,
      preview: `MIN/MAX ${temporalColumn.name}`,
    });
  }

  if (suggestions.length < 4 && stringColumn) {
    const stringIdentifier = formatColumnIdentifier(stringColumn.name);
    suggestions.push({
      label: `Find the rarest ${stringColumn.name} sigils`,
      sql: `SELECT ${stringIdentifier}, COUNT(*) AS appearances\nFROM ${base}\nGROUP BY ${stringIdentifier}\nORDER BY appearances ASC\nLIMIT 20;`,
      preview: `Rare ${stringColumn.name}`,
    });
  }

  return suggestions;
}

function updateColumnGlyphs() {
  if (!columnGlyphsElement) {
    return;
  }
  if (!currentTableData) {
    columnGlyphsElement.textContent = 'Load data to decode glyphs.';
    return;
  }

  columnGlyphsElement.innerHTML = '';
  currentTableData.columnMetadata.forEach((meta) => {
    const wrapper = document.createElement('div');
    wrapper.className = 'glyph-item';
    const icon = document.createElement('span');
    icon.className = 'glyph-icon';
    icon.textContent = meta.emoji;
    const copy = document.createElement('div');
    const heading = document.createElement('strong');
    heading.textContent = meta.name;
    const tagline = document.createElement('div');
    tagline.className = 'glyph-copy';
    tagline.textContent = meta.tagline;
    copy.append(heading, tagline);
    wrapper.append(icon, copy);
    columnGlyphsElement.appendChild(wrapper);
  });
}

function updateDataProphecy(remix = false) {
  if (!dataProphecyElement) {
    return;
  }
  if (!currentTableData || !currentRelation) {
    dataProphecyElement.textContent = 'No omens detected yet.';
    return;
  }

  const totalRows = currentTableData.rows.length;
  const columnCount = currentTableData.columns.length;
  if (currentTableData.columnMetadata.length === 0) {
    dataProphecyElement.textContent = `The dataset "${currentRelation.name}" is a silent void.`;
    return;
  }
  const auraMood = ['radiant', 'rebellious', 'dreaming', 'phase-shifting', 'cosmic'];
  const baseMoodIndex = (totalRows + columnCount) % auraMood.length;
  const selectedMood = remix ? auraMood[Math.floor(Math.random() * auraMood.length)] : auraMood[baseMoodIndex];
  const highlightedColumn = currentTableData.columnMetadata[Math.floor(Math.random() * currentTableData.columnMetadata.length)];
  const prophecy = `The ${selectedMood} dataset "${currentRelation.name}" spans ${columnCount} glyphs and ${totalRows.toLocaleString()} rows. Column ${highlightedColumn.emoji} ${highlightedColumn.name} ${highlightedColumn.tagline}.`;
  dataProphecyElement.textContent = prophecy;
}

function updateCosmicMood() {
  if (!cosmicMoodElement) {
    return;
  }
  if (!currentTableData) {
    cosmicMoodElement.textContent = 'Load a data file to awaken the nebula.';
    return;
  }

  const descriptors = ['vibrating', 'howling', 'glimmering', 'supersonic', 'kaleidoscopic'];
  const descriptor = descriptors[currentTableData.columns.length % descriptors.length];
  const rowEnergy = currentTableData.rows.length.toLocaleString();
  cosmicMoodElement.textContent = `Nebula status: ${descriptor}. ${rowEnergy} rows are bending around your cursor.`;
}

function updateCosmicAura(totalRows: number, columns: number) {
  const hue = (columns * 47 + totalRows) % 360;
  document.body.style.setProperty('--aura-hue', `${hue}deg`);
  if (hyperdriveBadgeElement) {
    hyperdriveBadgeElement.textContent = `Aura hue calibrated to ${hue}°. ${columns} glyphs detected.`;
  }
}

async function unleashChaosQuery() {
  if (!currentRelation) {
    updateStatus('No relation loaded yet.');
    return;
  }
  const sql = buildChaosQuery();
  sqlInput.value = sql;
  await runQuery(sql);
}

function buildChaosQuery(): string {
  if (!currentRelation || !currentTableData) {
    return sqlInput.value;
  }
  const limitOptions = [13, 21, 34, 55, 89];
  const limit = limitOptions[Math.floor(Math.random() * limitOptions.length)];
  const randomColumn = currentTableData.columns[Math.floor(Math.random() * currentTableData.columns.length)];
  const direction = Math.random() > 0.5 ? 'ASC' : 'DESC';
  const randomIdentifier = formatColumnIdentifier(randomColumn);
  return `SELECT * FROM ${currentRelation.identifier}\nORDER BY ${randomIdentifier} ${direction}, RANDOM()\nLIMIT ${limit};`;
}

function toggleHyperdrive() {
  const engaged = document.body.classList.toggle('hyperdrive');
  if (hyperdriveBanner) {
    hyperdriveBanner.style.display = engaged ? 'block' : 'none';
  }
  if (toggleHyperdriveButton) {
    toggleHyperdriveButton.textContent = engaged ? 'Disengage Hyperdrive' : 'Ignite Hyperdrive';
  }
  if (hyperdriveBadgeElement) {
    hyperdriveBadgeElement.textContent = engaged ? 'Hyperdrive humming with cosmic code.' : 'Awaiting ignition spark…';
  }
  if (engaged) {
    spawnSparkleTrail();
  }
}

function spawnSparkleTrail(event?: PointerEvent) {
  const sparkle = document.createElement('div');
  sparkle.className = 'sparkle-trail';
  const x = event?.clientX ?? Math.random() * window.innerWidth;
  const y = event?.clientY ?? Math.random() * window.innerHeight;
  sparkle.style.left = `${x}px`;
  sparkle.style.top = `${y}px`;
  document.body.appendChild(sparkle);
  window.setTimeout(() => sparkle.remove(), 1000);
}

function resetCosmicPanels() {
  populateQuerySpellbook();
  if (columnGlyphsElement) {
    columnGlyphsElement.textContent = 'Load data to decode glyphs.';
  }
  if (cosmicMoodElement) {
    cosmicMoodElement.textContent = 'Load a data file to awaken the nebula.';
  }
  if (dataProphecyElement) {
    dataProphecyElement.textContent = 'No omens detected yet.';
  }
}

function updateCosmicControlsAvailability() {
  if (chaosQueryButton) {
    chaosQueryButton.disabled = !currentRelation;
    chaosQueryButton.title = currentRelation ? 'Fire a randomized SQL incantation.' : 'Load a file to summon chaos queries.';
  }
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
