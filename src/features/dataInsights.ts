/**
 * 🧠 AI-POWERED DATA INSIGHTS ANALYZER
 * Auto-generates insights about your data: anomalies, patterns, distributions
 */

import { Table } from 'apache-arrow';

export interface DataInsight {
  type: 'nulls' | 'duplicates' | 'distribution' | 'correlation' | 'anomaly' | 'cardinality';
  title: string;
  description: string;
  severity: 'info' | 'warning' | 'critical';
  icon: string;
}

export class DataInsightsAnalyzer {
  /**
   * Analyzes table data and returns interesting insights
   */
  static analyzeTable(table: Table): DataInsight[] {
    const insights: DataInsight[] = [];
    const columns = table.schema.fields.map(f => f.name);

    // Check for null/missing values
    for (const colName of columns) {
      const nullCount = this.countNulls(table, colName);
      if (nullCount > table.numRows * 0.2) {
        insights.push({
          type: 'nulls',
          title: `⚠️ High Null Rate: ${colName}`,
          description: `${nullCount} null values (${((nullCount / table.numRows) * 100).toFixed(1)}%) - consider data quality issues`,
          severity: 'warning',
          icon: '🕳️'
        });
      }
    }

    // Check for duplicates
    for (const colName of columns) {
      const dupeCount = this.countDuplicates(table, colName);
      if (dupeCount > table.numRows * 0.3) {
        insights.push({
          type: 'duplicates',
          title: `🔄 Many Duplicates: ${colName}`,
          description: `${dupeCount} duplicate values - might want to check for data integrity`,
          severity: 'info',
          icon: '🔁'
        });
      }
    }

    // High cardinality warning
    for (const colName of columns) {
      const cardinality = this.countUnique(table, colName);
      if (cardinality > table.numRows * 0.95) {
        insights.push({
          type: 'cardinality',
          title: `🎲 High Cardinality: ${colName}`,
          description: `${cardinality} unique values - likely an ID or key column`,
          severity: 'info',
          icon: '🔑'
        });
      }
    }

    // Numeric column insights
    for (const field of table.schema.fields) {
      const colName = field.name;
      const type = field.type.toString();
      if (type.includes('int') || type.includes('float') || type.includes('double')) {
        const stats = this.getNumericStats(table, colName);
        if (stats && stats.maxValue > stats.minValue * 100) {
          insights.push({
            type: 'distribution',
            title: `📊 Wide Range: ${colName}`,
            description: `Values range from ${stats.minValue} to ${stats.maxValue} (${(stats.maxValue / stats.minValue).toFixed(1)}x difference)`,
            severity: 'info',
            icon: '📈'
          });
        }
      }
    }

    return insights;
  }

  private static countNulls(table: Table, columnName: string): number {
    let count = 0;
    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      if (row && row[columnName] == null) {
        count++;
      }
    }
    return count;
  }

  private static countDuplicates(table: Table, columnName: string): number {
    const seen = new Set<any>();
    let dupes = 0;
    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      if (row) {
        const val = row[columnName];
        if (seen.has(val)) {
          dupes++;
        }
        seen.add(val);
      }
    }
    return dupes;
  }

  private static countUnique(table: Table, columnName: string): number {
    const seen = new Set<any>();
    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      if (row && row[columnName] != null) {
        seen.add(row[columnName]);
      }
    }
    return seen.size;
  }

  private static getNumericStats(table: Table, columnName: string) {
    let min = Infinity;
    let max = -Infinity;
    let count = 0;

    for (let i = 0; i < table.numRows; i++) {
      const row = table.get(i);
      if (row && row[columnName] != null && typeof row[columnName] === 'number') {
        const val = row[columnName];
        min = Math.min(min, val);
        max = Math.max(max, val);
        count++;
      }
    }

    return count > 0 ? { minValue: min, maxValue: max, count } : null;
  }
}
