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

// REVOLUTIONARY UI ELEMENTS
const matrixCanvas = document.getElementById('matrix-canvas') as HTMLCanvasElement;
const particleCanvas = document.getElementById('particle-canvas') as HTMLCanvasElement;
const vizCanvas = document.getElementById('viz-canvas') as HTMLCanvasElement;
const aiAssistant = document.getElementById('ai-assistant') as HTMLDivElement;
const godModeIndicator = document.getElementById('god-mode-indicator') as HTMLDivElement;
const historyTimeline = document.getElementById('history-timeline') as HTMLDivElement;
const cyberpunkToggle = document.getElementById('cyberpunk-toggle') as HTMLButtonElement;
const holographicToggle = document.getElementById('holographic-toggle') as HTMLButtonElement;
const soundToggle = document.getElementById('sound-toggle') as HTMLButtonElement;
const matrixToggle = document.getElementById('matrix-toggle') as HTMLButtonElement;
const visualize3DBtn = document.getElementById('visualize-3d') as HTMLButtonElement;
const closeVizBtn = document.getElementById('close-viz') as HTMLButtonElement;

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
}

interface Particle {
  x: number;
  y: number;
  vx: number;
  vy: number;
  life: number;
  color: string;
  size: number;
}

interface QueryHistoryItem {
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
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];

// REVOLUTIONARY STATE
let matrixActive = false;
let cyberpunkMode = false;
let holographicMode = false;
let soundEnabled = false;
let godMode = false;
let konamiCode: string[] = [];
const konamiSequence = ['ArrowUp', 'ArrowUp', 'ArrowDown', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'ArrowLeft', 'ArrowRight', 'b', 'a'];
let particles: Particle[] = [];
let matrixColumns: { x: number; y: number; speed: number; chars: string }[] = [];
let queryHistory: QueryHistoryItem[] = [];
let achievementCount = 0;
let audioContext: AudioContext | null = null;

// --- REVOLUTIONARY FEATURES ---

// MATRIX RAIN EFFECT
function initMatrixRain() {
  if (!matrixCanvas) return;
  matrixCanvas.width = window.innerWidth;
  matrixCanvas.height = window.innerHeight;

  const columnWidth = 20;
  const numColumns = Math.floor(matrixCanvas.width / columnWidth);

  matrixColumns = [];
  for (let i = 0; i < numColumns; i++) {
    matrixColumns.push({
      x: i * columnWidth,
      y: Math.random() * matrixCanvas.height,
      speed: Math.random() * 5 + 2,
      chars: '01'
    });
  }
}

function animateMatrix() {
  if (!matrixCanvas || !matrixActive) return;

  const ctx = matrixCanvas.getContext('2d');
  if (!ctx) return;

  ctx.fillStyle = 'rgba(0, 0, 0, 0.05)';
  ctx.fillRect(0, 0, matrixCanvas.width, matrixCanvas.height);

  ctx.fillStyle = '#0f0';
  ctx.font = '16px monospace';

  matrixColumns.forEach(column => {
    const char = column.chars[Math.floor(Math.random() * column.chars.length)];
    ctx.fillText(char, column.x, column.y);

    column.y += column.speed;
    if (column.y > matrixCanvas.height) {
      column.y = 0;
    }
  });

  requestAnimationFrame(animateMatrix);
}

// PARTICLE EXPLOSION SYSTEM
function createExplosion(x: number, y: number) {
  const colors = ['#00ffff', '#ff00ff', '#ffff00', '#00ff00', '#ff0000'];

  for (let i = 0; i < 50; i++) {
    const angle = (Math.PI * 2 * i) / 50;
    const speed = Math.random() * 5 + 2;
    particles.push({
      x,
      y,
      vx: Math.cos(angle) * speed,
      vy: Math.sin(angle) * speed,
      life: 1,
      color: colors[Math.floor(Math.random() * colors.length)],
      size: Math.random() * 5 + 2
    });
  }

  if (!particles.length) {
    requestAnimationFrame(animateParticles);
  }
}

function animateParticles() {
  if (!particleCanvas) return;

  const ctx = particleCanvas.getContext('2d');
  if (!ctx) return;

  ctx.clearRect(0, 0, particleCanvas.width, particleCanvas.height);

  particles = particles.filter(p => {
    p.x += p.vx;
    p.y += p.vy;
    p.vy += 0.2; // gravity
    p.life -= 0.01;

    if (p.life > 0) {
      ctx.globalAlpha = p.life;
      ctx.fillStyle = p.color;
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
      ctx.fill();
      return true;
    }
    return false;
  });

  if (particles.length > 0) {
    requestAnimationFrame(animateParticles);
  }
}

// ACHIEVEMENT SYSTEM
function showAchievement(message: string) {
  const achievement = document.createElement('div');
  achievement.className = 'achievement';
  achievement.textContent = `🏆 ${message}`;
  document.body.appendChild(achievement);

  playSound(800, 0.1);
  setTimeout(() => playSound(1000, 0.1), 100);

  setTimeout(() => {
    achievement.style.opacity = '0';
    setTimeout(() => achievement.remove(), 300);
  }, 3000);
}

// DATA SONIFICATION
function playSonification(data: number[]) {
  if (!soundEnabled || !audioContext) return;

  const now = audioContext.currentTime;
  data.slice(0, 10).forEach((value, i) => {
    const frequency = 200 + (value % 1000);
    playSound(frequency, 0.05, now + i * 0.1);
  });
}

function playSound(frequency: number, duration: number, startTime?: number) {
  if (!soundEnabled) return;

  if (!audioContext) {
    audioContext = new AudioContext();
  }

  const oscillator = audioContext.createOscillator();
  const gainNode = audioContext.createGain();

  oscillator.connect(gainNode);
  gainNode.connect(audioContext.destination);

  oscillator.frequency.value = frequency;
  oscillator.type = 'sine';

  const start = startTime || audioContext.currentTime;
  gainNode.gain.setValueAtTime(0.1, start);
  gainNode.gain.exponentialRampToValueAtTime(0.01, start + duration);

  oscillator.start(start);
  oscillator.stop(start + duration);
}

// 3D VISUALIZATION
function visualize3D() {
  if (!vizCanvas || !currentTableData) return;

  vizCanvas.classList.add('active');
  closeVizBtn.classList.add('active');

  const ctx = vizCanvas.getContext('2d');
  if (!ctx) return;

  vizCanvas.width = window.innerWidth * 0.8;
  vizCanvas.height = window.innerHeight * 0.8;

  // Simple 3D cube rotation effect
  let rotation = 0;
  const animate3D = () => {
    if (!vizCanvas.classList.contains('active')) return;

    ctx.fillStyle = 'rgba(0, 0, 0, 0.9)';
    ctx.fillRect(0, 0, vizCanvas.width, vizCanvas.height);

    const centerX = vizCanvas.width / 2;
    const centerY = vizCanvas.height / 2;
    const size = 200;

    // Draw rotating data points
    currentTableData!.rows.slice(0, 100).forEach((row, i) => {
      const angle = (i / 100) * Math.PI * 2 + rotation;
      const radius = size + (i % 10) * 20;
      const x = centerX + Math.cos(angle) * radius;
      const y = centerY + Math.sin(angle) * radius;
      const z = Math.sin(rotation + i * 0.1) * 50;

      const scale = 1 + z / 200;
      const hue = (i * 360 / 100 + rotation * 50) % 360;

      ctx.fillStyle = `hsla(${hue}, 100%, 50%, 0.8)`;
      ctx.beginPath();
      ctx.arc(x, y, 5 * scale, 0, Math.PI * 2);
      ctx.fill();
    });

    rotation += 0.02;
    requestAnimationFrame(animate3D);
  };

  animate3D();
  showAchievement('3D Visualization Master!');
}

// AI ASSISTANT SUGGESTIONS
const sqlSuggestions = [
  "Try: SELECT * FROM data WHERE column > 100",
  "How about: SELECT COUNT(*) GROUP BY category",
  "Consider: SELECT AVG(value) FROM data",
  "Experiment with: SELECT * ORDER BY date DESC LIMIT 10",
  "Pro tip: Use DISTINCT to find unique values!",
  "Advanced: Try a JOIN if you have multiple tables",
  "Aggregate: SUM(), AVG(), MIN(), MAX() are your friends!",
  "Filter wisely: WHERE conditions speed up queries",
];

function showAISuggestion() {
  const suggestion = sqlSuggestions[Math.floor(Math.random() * sqlSuggestions.length)];

  const bubble = document.createElement('div');
  bubble.className = 'ai-bubble';
  bubble.textContent = suggestion;
  document.body.appendChild(bubble);

  aiAssistant.classList.add('talking');

  setTimeout(() => {
    bubble.style.opacity = '0';
    aiAssistant.classList.remove('talking');
    setTimeout(() => bubble.remove(), 300);
  }, 4000);
}

// QUERY HISTORY TIMELINE
function addToHistory(sql: string) {
  queryHistory.unshift({ sql, timestamp: Date.now() });
  if (queryHistory.length > 10) queryHistory.pop();

  updateHistoryTimeline();
}

function updateHistoryTimeline() {
  if (!historyTimeline) return;

  historyTimeline.innerHTML = '';
  queryHistory.forEach((item, index) => {
    const historyItem = document.createElement('div');
    historyItem.className = 'history-item';
    historyItem.textContent = `${queryHistory.length - index}`;
    historyItem.title = item.sql;
    historyItem.onclick = () => {
      sqlInput.value = item.sql;
      runQuery(item.sql).catch(reportError);
    };
    historyTimeline.appendChild(historyItem);
  });

  if (queryHistory.length > 0) {
    historyTimeline.classList.add('active');
  }
}

// KONAMI CODE DETECTOR
document.addEventListener('keydown', (e) => {
  konamiCode.push(e.key);
  if (konamiCode.length > konamiSequence.length) {
    konamiCode.shift();
  }

  if (konamiCode.join(',') === konamiSequence.join(',')) {
    activateGodMode();
  }
});

function activateGodMode() {
  godMode = true;
  godModeIndicator.classList.add('active');

  // Activate ALL modes
  cyberpunkMode = true;
  holographicMode = true;
  matrixActive = true;
  soundEnabled = true;

  document.body.classList.add('cyberpunk', 'holographic');
  matrixCanvas.classList.add('active');
  cyberpunkToggle.classList.add('active');
  holographicToggle.classList.add('active');
  matrixToggle.classList.add('active');
  soundToggle.classList.add('active');

  showAchievement('GOD MODE ACTIVATED! You are unstoppable!');

  // Mega explosion
  for (let i = 0; i < 10; i++) {
    setTimeout(() => {
      createExplosion(
        Math.random() * window.innerWidth,
        Math.random() * window.innerHeight
      );
    }, i * 100);
  }
}

// --- Event Listeners (Revolutionary) ---

cyberpunkToggle.addEventListener('click', () => {
  cyberpunkMode = !cyberpunkMode;
  document.body.classList.toggle('cyberpunk');
  cyberpunkToggle.classList.toggle('active');
  if (cyberpunkMode) showAchievement('Welcome to the Cyberpunk Future!');
});

holographicToggle.addEventListener('click', () => {
  holographicMode = !holographicMode;
  document.body.classList.toggle('holographic');
  holographicToggle.classList.toggle('active');
  if (holographicMode) showAchievement('Holographic Mode Engaged!');
});

soundToggle.addEventListener('click', () => {
  soundEnabled = !soundEnabled;
  soundToggle.classList.toggle('active');
  if (soundEnabled) {
    showAchievement('Sound Effects Enabled!');
    playSound(440, 0.2);
  }
});

matrixToggle.addEventListener('click', () => {
  matrixActive = !matrixActive;
  matrixCanvas.classList.toggle('active');
  matrixToggle.classList.toggle('active');

  if (matrixActive) {
    initMatrixRain();
    animateMatrix();
    showAchievement('Entering The Matrix...');
  }
});

visualize3DBtn.addEventListener('click', () => {
  visualize3D();
});

closeVizBtn.addEventListener('click', () => {
  vizCanvas.classList.remove('active');
  closeVizBtn.classList.remove('active');
});

aiAssistant.addEventListener('click', () => {
  showAISuggestion();
  playSound(600, 0.1);
});

// Initialize canvases
if (particleCanvas) {
  particleCanvas.width = window.innerWidth;
  particleCanvas.height = window.innerHeight;
}

window.addEventListener('resize', () => {
  if (matrixCanvas) {
    matrixCanvas.width = window.innerWidth;
    matrixCanvas.height = window.innerHeight;
    initMatrixRain();
  }
  if (particleCanvas) {
    particleCanvas.width = window.innerWidth;
    particleCanvas.height = window.innerHeight;
  }
});

// --- Event Listeners (Original) ---

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
      playSound(800, 0.1);
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

    showAchievement('DuckDB Initialized!');

  } catch (e) {
    reportError(e);
  }
}

async function handleFileLoad(fileName: string, fileData: any) {
  if (!db || !connection) {
    throw new Error('DuckDB is not initialized.');
  }

  // Activate matrix during loading
  if (!matrixActive && Math.random() > 0.5) {
    matrixActive = true;
    matrixCanvas.classList.add('active');
    initMatrixRain();
    animateMatrix();
    setTimeout(() => {
      matrixActive = false;
      matrixCanvas.classList.remove('active');
    }, 3000);
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

  if (controls) controls.style.display = 'flex';
  if (resultsContainer) resultsContainer.style.display = 'block';

  await runQuery(defaultQuery);

  achievementCount++;
  if (achievementCount === 1) showAchievement('First File Loaded!');
  if (achievementCount === 5) showAchievement('Data Explorer - 5 files!');
  if (achievementCount === 10) showAchievement('Data Master - 10 files!');
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

  updateStatus('Running query...');
  runButton.disabled = true;

  try {
    const result = await connection.query(sql);
    renderResults(result);

    // Add to history
    addToHistory(sql);

    // REVOLUTIONARY SUCCESS EFFECTS
    if (statusWrapper) {
      statusWrapper.style.display = 'none';
    }

    // Particle explosion at random location
    createExplosion(
      Math.random() * window.innerWidth,
      Math.random() * window.innerHeight
    );

    // Sound effect
    playSound(523.25, 0.1); // C note
    setTimeout(() => playSound(659.25, 0.1), 100); // E note

    // Data sonification
    if (result && result.numRows > 0 && soundEnabled) {
      const firstRow = result.get(0);
      if (firstRow) {
        const numericValues: number[] = [];
        result.schema.fields.forEach(field => {
          const val = firstRow[field.name];
          if (typeof val === 'number') numericValues.push(val);
        });
        if (numericValues.length > 0) {
          setTimeout(() => playSonification(numericValues), 200);
        }
      }
    }

    // Achievement checks
    if (sql.toLowerCase().includes('join')) {
      showAchievement('JOIN Master!');
    }
    if (sql.toLowerCase().includes('group by')) {
      showAchievement('Aggregation Expert!');
    }
    if (sql.toLowerCase().match(/\b(sum|avg|max|min|count)\b/i)) {
      showAchievement('Function Wizard!');
    }

  } catch (e) {
    reportError(e);
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
    if (!row) continue;

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
  playSound(400, 0.05);
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
  if (visible === total) {
    rowCountLabel.textContent = `${total} rows`;
  } else {
    rowCountLabel.textContent = `${visible} of ${total} rows`;
  }
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
  if (statusWrapper) {
    statusWrapper.style.display = 'block';
  }
  if (status) {
    status.textContent = message;
    status.classList.remove('error');
  }
}

function reportError(e: any) {
  const message = e instanceof Error ? e.message : String(e);

  if (statusWrapper) {
    statusWrapper.style.display = 'block';
  }
  if (status) {
    status.textContent = `Error: ${message}`;
    status.classList.add('error');
  }
  console.error(`[Error] ${message}`, e);

  // Error sound
  playSound(200, 0.3);
}

// Send the 'ready' signal to the extension to start the handshake
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });

// Welcome message
setTimeout(() => {
  if (aiAssistant) {
    const welcomeBubble = document.createElement('div');
    welcomeBubble.className = 'ai-bubble';
    welcomeBubble.textContent = '👋 Welcome! Try the Cyberpunk mode or enter the Konami code for a surprise! ↑↑↓↓←→←→BA';
    document.body.appendChild(welcomeBubble);

    setTimeout(() => {
      welcomeBubble.style.opacity = '0';
      setTimeout(() => welcomeBubble.remove(), 300);
    }, 8000);
  }
}, 1000);
