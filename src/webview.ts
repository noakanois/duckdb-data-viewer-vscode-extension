import * as duckdb from '@duckdb/duckdb-wasm';
import { Table } from 'apache-arrow';

declare const acquireVsCodeApi: any;
const vscode = acquireVsCodeApi();

// Get UI elements
const status = document.getElementById('status');
const controls = document.getElementById('controls');
const resultsContainer = document.getElementById('results-container');
const sqlInput = document.getElementById('sql-input') as HTMLTextAreaElement;
const runButton = document.getElementById('run-query') as HTMLButtonElement;
const statusWrapper = document.getElementById('status-wrapper');

let db: duckdb.AsyncDuckDB | null = null;
let connection: duckdb.AsyncDuckDBConnection | null = null;
const VIEW_NAME = "my_data"; 

// --- Event Listeners (Moved to top) ---

// Listen for messages from the extension
window.addEventListener('message', (event: any) => {
  const message = event.data;
  console.log('[Webview] Received message:', message.command); // DEBUG
  
  if (message.command === 'init') {
    bootstrapDuckDB(message.bundles);
  } else if (message.command === 'loadFile') {
    console.log('[Debug] Received fileData:', message.fileData); // DEBUG
    handleFileLoad(message.fileName, message.fileData).catch(reportError);
  } else if (message.command === 'error') {
    reportError(message.message);
  }
});

// Listen for the "Run" button click
runButton.addEventListener('click', () => {
  runQuery(sqlInput.value).catch(reportError);
});

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
    
    updateStatus('DuckDB ready. Waiting for file data…');
    vscode.postMessage({ command: 'duckdb-ready' });

  } catch (e) {
    reportError(e);
  }
}

async function handleFileLoad(fileName: string, fileData: any) {
  if (!db || !connection) {
    throw new Error("DuckDB is not initialized.");
  }

  // 1. --- THIS IS THE FIX ---
  // We must access the .data property of the received object
  const fileBytes = new Uint8Array(fileData.data);
  // -------------------------
  
  // --- DEBUG LOGS ---
  console.log(`[Debug] Reconstructed fileBytes. Size: ${fileBytes.length} bytes`);
  if (fileBytes.length === 0) {
      reportError("File is empty (0 bytes).");
      return;
  }
  const headerSnippet = new TextDecoder().decode(fileBytes.slice(0, 100));
  console.log(`[Debug] File Header Snippet:\n${headerSnippet}`);
  updateStatus(`File size: ${fileBytes.length} bytes. Header: ${headerSnippet.split('\n')[0]}`);
  // ---

  // 2. Register the file buffer
  console.log(`[Debug] Registering file: ${fileName}`);
  await db.registerFileBuffer(fileName, fileBytes); 
  
  // 3. --- NEW DEBUG STEP ---
  try {
    updateStatus('Debugging: Describing CSV structure...');
    // We will use header=true to be safe
    const describeQuery = `DESCRIBE SELECT * FROM read_csv('${fileName}', header=true);`;
    console.log(`[Debug] Running query: ${describeQuery}`);
    
    const describeResult = await connection.query(describeQuery);
    const describeArray = describeResult.toArray();
    console.log('[Debug] DESCRIBE query result:', describeArray);

    if (describeArray.length === 0) {
      reportError(`DuckDB's read_csv could not find any columns.`);
      return;
    }
    
    renderResults(describeResult);

  } catch (e) {
    reportError(e);
    return;
  }
  // --- END DEBUG STEP ---
  
  // 4. Create a view from the CSV file
  updateStatus(`Creating view '${VIEW_NAME}' from ${fileName}...`);
  const createViewQuery = `
    CREATE OR REPLACE TEMP VIEW ${VIEW_NAME} AS 
    SELECT * FROM read_csv('${fileName}', header=true);
  `;
  console.log(`[Debug] Running query: ${createViewQuery}`);
  await connection.query(createViewQuery);
  
  // 5. Set the default query to select from the VIEW
  const defaultQuery = `SELECT * FROM ${VIEW_NAME} LIMIT 10;`;
  sqlInput.value = defaultQuery;
  
  // 6. Show the UI
  if (controls) controls.style.display = 'flex';
  if (resultsContainer) resultsContainer.style.display = 'block';

  // 7. Automatically run the default query
  console.log(`[Debug] Running default query: ${defaultQuery}`);
  await runQuery(defaultQuery);
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
    console.log('[Debug] Query result:', result.toArray());
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
  if (!resultsContainer) return;
  
  if (!table || table.numRows === 0) {
    console.log('[Debug] renderResults: Query returned no rows.');
    resultsContainer.innerHTML = '<p>Query completed. No rows returned.</p>';
    return;
  }

  const headers = table.schema.fields.map((field) => field.name);
  let html = '<table><thead><tr>';
  html += headers.map(h => `<th>${escapeHtml(h)}</th>`).join('');
  html += '</tr></thead><tbody>';

  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i);
    if (!row) continue;
    
    html += '<tr>';
    for (const field of table.schema.fields) {
      const value = row[field.name];
      html += `<td>${escapeHtml(String(value ?? 'NULL'))}</td>`;
    }
    html += '</tr>';
  }

  html += '</tbody></table>';
  resultsContainer.innerHTML = html;
}

// ---
// Helpers
// ---
function updateStatus(message: string) {
  // Always make the status bar visible when updating
  if (statusWrapper) {
    statusWrapper.style.display = 'block';
  }
  if (status) {
    status.textContent = message;
    status.classList.remove('error'); // Remove error style if it was there
  }
  console.log(`[Status] ${message}`);
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

function escapeHtml(str: string): string {
  if (str === null || str === undefined) {
    return 'NULL';
  }
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}