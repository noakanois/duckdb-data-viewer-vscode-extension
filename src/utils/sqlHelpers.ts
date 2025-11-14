const SIMPLE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function buildDefaultQuery(columnNames: string[], relationIdentifier: string): string {
  const columns = columnNames.length
    ? columnNames.map(formatIdentifierForSql).join(', ')
    : '*';
  return `SELECT ${columns}\nFROM ${relationIdentifier};`;
}

export function deriveRelationName(fileName: string): string {
  const baseName = fileName.split(/[\\/]/).pop() ?? fileName;
  const withoutExtension = baseName.replace(/\.[^.]+$/, '');
  let sanitized = withoutExtension.replace(/[^A-Za-z0-9_]/g, '_');
  if (!sanitized) {
    sanitized = 'data_view';
  }
  if (/^[0-9]/.test(sanitized)) {
    sanitized = `v_${sanitized}`;
  }
  return sanitized;
}

export function formatIdentifierForSql(identifier: string): string {
  if (SIMPLE_IDENTIFIER.test(identifier)) {
    return identifier;
  }
  return `"${identifier.replace(/"/g, '""')}"`;
}
