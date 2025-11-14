import * as vscode from 'vscode';
import * as path from 'path';
import { promises as fs } from 'fs';

// The new command ID from package.json
const COMMAND_ID = 'duckdb-viewer.viewFile';

export function activate(context: vscode.ExtensionContext) {
  
  // The command now receives a 'uri' from the right-click menu
  let disposable = vscode.commands.registerCommand(COMMAND_ID, async (uri: vscode.Uri) => {
    
    // Handle if the command is run without a file
    if (!uri) {
      vscode.window.showWarningMessage("Please right-click a file from the explorer to use this command.");
      return;
    }

    const fileName = path.basename(uri.fsPath);

    const panel = vscode.window.createWebviewPanel(
      'duckdbDataViewer',
      `DuckDB: ${fileName}`, // Panel title
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'dist')]
      }
    );

    panel.webview.html = await getWebviewHtml(context, panel.webview);

    // This is our 2-stage handshake
    panel.webview.onDidReceiveMessage(
      async (message) => {
        // Step 1: Webview is ready, send the assets
        if (message.command === 'ready') {
          try {
            const bundles = await prepareDuckDBBundles(context, panel.webview);
            panel.webview.postMessage({ command: 'init', bundles });
          } catch (e) {
            panel.webview.postMessage({ 
              command: 'error', 
              message: e instanceof Error ? e.message : String(e) 
            });
          }
        }
        
        // Step 2: DuckDB is initialized, send the file data
        if (message.command === 'duckdb-ready') {
          try {
            const fileBytes = await vscode.workspace.fs.readFile(uri);
            panel.webview.postMessage({
              command: 'loadFile',
              fileName: fileName,
              fileData: fileBytes
            });
          } catch (e) {
             panel.webview.postMessage({ 
              command: 'error', 
              message: e instanceof Error ? `Failed to read file: ${e.message}` : String(e) 
            });
          }
        }
      },
      undefined,
      context.subscriptions
    );
  });

  context.subscriptions.push(disposable);
}

// Helper to read a worker file from dist into a string
async function readWorkerSource(context: vscode.ExtensionContext, fileName: string): Promise<string> {
  const workerUri = vscode.Uri.joinPath(context.extensionUri, 'dist', fileName);
  const workerBytes = await vscode.workspace.fs.readFile(workerUri);
  return new TextDecoder().decode(workerBytes);
}

// Prepare all asset paths and worker source code
async function prepareDuckDBBundles(context: vscode.ExtensionContext, webview: vscode.Webview) {
  const mvpWasmUrl = webview.asWebviewUri(vscode.Uri.joinPath(context.extensionUri, 'dist', 'duckdb-mvp.wasm')).toString();
  const ehWasmUrl = webview.asWebviewUri(vscode.Uri.joinPath(context.extensionUri, 'dist', 'duckdb-eh.wasm')).toString();
  const mvpWorkerSource = await readWorkerSource(context, 'duckdb-browser-mvp.worker.js');
  const ehWorkerSource = await readWorkerSource(context, 'duckdb-browser-eh.worker.js');

  return {
    mvp: {
      mainModule: mvpWasmUrl,
      mainWorker: mvpWorkerSource,
    },
    eh: {
      mainModule: ehWasmUrl,
      mainWorker: ehWorkerSource,
    },
  };
}

// Reads the HTML template from disk
async function getWebviewHtml(context: vscode.ExtensionContext, webview: vscode.Webview): Promise<string> {
  const htmlPath = vscode.Uri.joinPath(context.extensionUri, 'src', 'webview.html');
  const template = await vscode.workspace.fs.readFile(htmlPath);
  const nonce = generateNonce();
  const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(context.extensionUri, 'dist', 'webview.js'));
  const toolkitUri = webview.asWebviewUri(vscode.Uri.joinPath(context.extensionUri, 'dist', 'toolkit.js'));

  return new TextDecoder().decode(template)
    .replace(/{{nonce}}/g, nonce)
    .replace(/{{csp_source}}/g, webview.cspSource)
    .replace(/{{webview_script_uri}}/g, scriptUri.toString())
    .replace(/{{toolkit_uri}}/g, toolkitUri.toString());
}

function generateNonce(): string {
  let text = '';
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  for (let i = 0; i < 32; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}

export function deactivate() {}
