import * as vscode from 'vscode';
import * as path from 'path';

// The new command ID from package.json
const COMMAND_ID = 'duckdb-viewer.viewFile';

export function activate(context: vscode.ExtensionContext) {
  context.subscriptions.push(
    vscode.commands.registerCommand(COMMAND_ID, (uri: vscode.Uri) => {
      if (!uri) {
        // If the command is run from the command palette, prompt the user for a file.
        promptForFile().then((selectedUri) => {
          if (selectedUri) {
            ViewerPanel.getOrCreate(context).then((panel) => panel.loadFile(selectedUri));
          }
        });
      } else {
        // If the command is run from the context menu, use the provided URI.
        ViewerPanel.getOrCreate(context).then((panel) => panel.loadFile(uri));
      }
    })
  );
}
class ViewerPanel {
  private static panel: ViewerPanel | undefined;
  private readonly panel: vscode.WebviewPanel;
  private readonly context: vscode.ExtensionContext;
  private duckdbReady = false;
  private pendingFiles: vscode.Uri[] = [];

  private constructor(context: vscode.ExtensionContext) {
    this.context = context;
    this.panel = vscode.window.createWebviewPanel(
      'duckdbDataViewer',
      'DuckDB Data Viewer',
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'dist')],
      }
    );

    this.panel.onDidDispose(() => {
      ViewerPanel.panel = undefined;
    });

    this.panel.webview.onDidReceiveMessage(async (message) => {
      switch (message.command) {
        case 'ready':
          this.initializeDuckDB();
          break;
        case 'duckdb-ready':
          this.duckdbReady = true;
          this.deliverPendingFiles();
          break;
        case 'add-data-source':
          this.promptForFileAndLoad();
          break;
      }
    });
  }

  public static async getOrCreate(context: vscode.ExtensionContext): Promise<ViewerPanel> {
    if (!ViewerPanel.panel) {
      ViewerPanel.panel = new ViewerPanel(context);
      await ViewerPanel.panel.initializeWebview();
    }
    ViewerPanel.panel.panel.reveal(vscode.ViewColumn.One);
    return ViewerPanel.panel;
  }

  private async initializeWebview() {
    this.panel.webview.html = await getWebviewHtml(this.context, this.panel.webview);
  }

  private async initializeDuckDB() {
    try {
      const bundles = await prepareDuckDBBundles(this.context, this.panel.webview);
      this.panel.webview.postMessage({ command: 'init', bundles });
    } catch (e) {
      this.handleError(e);
    }
  }

  public loadFile(uri: vscode.Uri) {
    this.pendingFiles.push(uri);
    this.deliverPendingFiles();
  }

  private async deliverPendingFiles() {
    if (!this.duckdbReady || this.pendingFiles.length === 0) {
      return;
    }
    const urisToLoad = [...this.pendingFiles];
    this.pendingFiles = [];

    for (const uri of urisToLoad) {
      try {
        const fileName = path.basename(uri.fsPath);
        const fileBytes = await vscode.workspace.fs.readFile(uri);
        this.panel.webview.postMessage({
          command: 'loadFile',
          fileName,
          fileData: fileBytes,
        });
      } catch (e) {
        this.handleError(e);
      }
    }
  }

  private async promptForFileAndLoad() {
    const uri = await promptForFile();
    if (uri) {
      this.loadFile(uri);
    }
  }

  private handleError(e: unknown) {
    const message = e instanceof Error ? `Failed to read file: ${e.message}` : String(e);
    this.panel.webview.postMessage({
      command: 'error',
      message,
    });
    vscode.window.showErrorMessage(message);
  }
}

async function promptForFile(): Promise<vscode.Uri | undefined> {
  const options: vscode.OpenDialogOptions = {
    canSelectMany: false,
    openLabel: 'Select a data file',
    filters: {
      'Data files': ['csv', 'parquet', 'sqlite', 'arrow'],
    },
  };
  const fileUris = await vscode.window.showOpenDialog(options);
  return fileUris?.[0];
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
