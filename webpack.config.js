//@ts-check
'use strict';

const path = require('path');
const CopyPlugin = require('copy-webpack-plugin');

//@ts-check
/** @typedef {import('webpack').Configuration} WebpackConfig **/

/** @type WebpackConfig */
const baseConfig = {
  mode: 'none',
  devtool: 'nosources-source-map',
  infrastructureLogging: {
    level: 'log', // enables logging required for problem matchers
  },
  resolve: {
    extensions: ['.ts', '.js'],
  },
  module: {
    rules: [
      {
        test: /\.ts$/,
        exclude: /node_modules/,
        use: [
          {
            loader: 'ts-loader',
          },
        ],
      },
    ],
  },
};

/** @type WebpackConfig */
const extensionConfig = {
  ...baseConfig,
  target: 'node', // <-- This is for the extension
  entry: {
    extension: './src/extension.ts',
  },
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: '[name].js',
    libraryTarget: 'commonjs2',
  },
  externals: {
    vscode: 'commonjs vscode', // The 'vscode' module is special
  },
  plugins: [
    // Copy DuckDB assets
    new CopyPlugin({
      patterns: [
        {
          from: 'node_modules/@duckdb/duckdb-wasm/dist/*.{wasm,worker.js}',
          to: '[name][ext]',
        },
        {
          from: 'node_modules/@vscode/webview-ui-toolkit/dist/toolkit.js',
          to: 'toolkit.js', // This copies it to dist/toolkit.js
        },
      ],
    }),
  ],
};

/** @type WebpackConfig */
const webviewConfig = {
  ...baseConfig,
  target: 'web', // <-- This is for the webview
  entry: {
    webview: './src/webview.ts',
  },
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: '[name].js',
  },
};

// Export both configurations
module.exports = [extensionConfig, webviewConfig];