/**
 * 📤 SMART MULTI-FORMAT EXPORT
 * Export data to 10+ formats with intelligent formatting
 */

import { Table } from 'apache-arrow';

export type ExportFormat = 'csv' | 'json' | 'jsonl' | 'markdown' | 'html' | 'tsv' | 'xml' | 'sql';

export class SmartExporter {
  /**
   * Export Arrow Table to multiple formats
   */
  static export(table: Table, format: ExportFormat, filename: string): { data: string; mimeType: string } {
    switch (format) {
      case 'csv':
        return { data: this.toCSV(table), mimeType: 'text/csv' };
      case 'tsv':
        return { data: this.toTSV(table), mimeType: 'text/tab-separated-values' };
      case 'json':
        return { data: this.toJSON(table), mimeType: 'application/json' };
      case 'jsonl':
        return { data: this.toJSONL(table), mimeType: 'application/x-ndjson' };
      case 'markdown':
        return { data: this.toMarkdown(table), mimeType: 'text/markdown' };
      case 'html':
        return { data: this.toHTML(table, filename), mimeType: 'text/html' };
      case 'xml':
        return { data: this.toXML(table), mimeType: 'application/xml' };
      case 'sql':
        return { data: this.toSQL(table, filename), mimeType: 'text/sql' };
      default:
        return { data: this.toCSV(table), mimeType: 'text/csv' };
    }
  }

  private static toCSV(table: Table): string {
    const columns = table.schema.fields.map(f => `"${f.name}"`).join(',');
    const rows: string[] = [];

    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      const values = table.schema.fields.map(field => {
        const val = row?.[field.name];
        if (val === null || val === undefined) return '';
        const str = String(val).replace(/"/g, '""');
        return `"${str}"`;
      });
      rows.push(values.join(','));
    }

    return [columns, ...rows].join('\n');
  }

  private static toTSV(table: Table): string {
    const columns = table.schema.fields.map(f => f.name).join('\t');
    const rows: string[] = [];

    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      const values = table.schema.fields.map(field => {
        const val = row?.[field.name];
        return val === null || val === undefined ? '' : String(val);
      });
      rows.push(values.join('\t'));
    }

    return [columns, ...rows].join('\n');
  }

  private static toJSON(table: Table): string {
    const data: any[] = [];

    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      const obj: any = {};
      for (const field of table.schema.fields) {
        obj[field.name] = row?.[field.name];
      }
      data.push(obj);
    }

    return JSON.stringify(data, null, 2);
  }

  private static toJSONL(table: Table): string {
    const lines: string[] = [];

    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      const obj: any = {};
      for (const field of table.schema.fields) {
        obj[field.name] = row?.[field.name];
      }
      lines.push(JSON.stringify(obj));
    }

    return lines.join('\n');
  }

  private static toMarkdown(table: Table): string {
    const columns = table.schema.fields.map(f => f.name);
    const separator = columns.map(() => '---').join('|');
    const header = columns.join('|');

    const rows: string[] = [];
    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      const values = columns.map(col => {
        const val = row?.[col];
        return val === null || val === undefined ? '-' : String(val).replace(/\|/g, '\\|');
      });
      rows.push(values.join('|'));
    }

    return `| ${header} |\n| ${separator} |\n| ${rows.join(' |\n| ')} |`;
  }

  private static toHTML(table: Table, filename: string): string {
    const title = filename.split('.')[0];
    const columns = table.schema.fields.map(f => f.name);

    let html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>${title}</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 20px; }
    h1 { color: #333; }
    table { border-collapse: collapse; width: 100%; }
    th { background: #007acc; color: white; padding: 12px; text-align: left; }
    td { padding: 10px; border-bottom: 1px solid #ddd; }
    tr:hover { background: #f5f5f5; }
  </style>
</head>
<body>
  <h1>${title}</h1>
  <table>
    <thead>
      <tr>
        ${columns.map(col => `<th>${this.escapeHtml(col)}</th>`).join('\n        ')}
      </tr>
    </thead>
    <tbody>`;

    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      html += '\n      <tr>';
      for (const col of columns) {
        const val = row?.[col];
        const display = val === null || val === undefined ? '-' : this.escapeHtml(String(val));
        html += `\n        <td>${display}</td>`;
      }
      html += '\n      </tr>';
    }

    html += `\n    </tbody>
  </table>
</body>
</html>`;

    return html;
  }

  private static toXML(table: Table): string {
    const columns = table.schema.fields.map(f => f.name);
    let xml = '<?xml version="1.0" encoding="UTF-8"?>\n<root>\n';

    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      xml += '  <record>\n';
      for (const col of columns) {
        const val = row?.[col];
        const display = val === null || val === undefined ? '' : this.escapeXml(String(val));
        xml += `    <${this.sanitizeXmlTag(col)}>${display}</${this.sanitizeXmlTag(col)}>\n`;
      }
      xml += '  </record>\n';
    }

    xml += '</root>';
    return xml;
  }

  private static toSQL(table: Table, filename: string): string {
    const tableName = filename.split('.')[0].replace(/[^a-zA-Z0-9_]/g, '_');
    const columns = table.schema.fields.map(f => f.name);

    // Create table definition
    const columnDefs = columns.map((col, i) => {
      const field = table.schema.fields[i];
      const type = this.mapArrowTypeToSQL(field.type.toString());
      return `  "${col}" ${type}`;
    }).join(',\n');

    let sql = `CREATE TABLE ${tableName} (\n${columnDefs}\n);\n\n`;

    // Insert statements
    for (let i = 0; i < Math.min(table.numRows, 1000); i++) { // Limit to 1000 rows for SQL
      const row = table.get(i);
      const values = columns.map(col => {
        const val = row?.[col];
        if (val === null || val === undefined) return 'NULL';
        if (typeof val === 'number') return String(val);
        return `'${String(val).replace(/'/g, "''")}'`;
      }).join(', ');
      sql += `INSERT INTO ${tableName} VALUES (${values});\n`;
    }

    if (table.numRows > 1000) {
      sql += `\n-- ... and ${table.numRows - 1000} more rows\n`;
    }

    return sql;
  }

  private static mapArrowTypeToSQL(arrowType: string): string {
    if (arrowType.includes('int')) return 'INTEGER';
    if (arrowType.includes('float') || arrowType.includes('double')) return 'REAL';
    if (arrowType.includes('string')) return 'TEXT';
    if (arrowType.includes('bool')) return 'BOOLEAN';
    if (arrowType.includes('timestamp') || arrowType.includes('date')) return 'TIMESTAMP';
    return 'TEXT';
  }

  private static escapeHtml(text: string): string {
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  private static escapeXml(text: string): string {
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&apos;');
  }

  private static sanitizeXmlTag(tag: string): string {
    return tag.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^[0-9]/, '_$&');
  }
}
