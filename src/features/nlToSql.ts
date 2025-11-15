/**
 * 🧠 NATURAL LANGUAGE TO SQL CONVERTER
 * Translates English descriptions into SQL queries using smart pattern matching
 */

export interface SQLSuggestion {
  sql: string;
  explanation: string;
  confidence: number;
}

export class NLToSQLConverter {
  private columns: string[] = [];
  private tableName: string = '';

  constructor(columns: string[], tableName: string) {
    this.columns = columns;
    this.tableName = tableName;
  }

  /**
   * Generate SQL suggestions from natural language input
   */
  generateSuggestions(input: string): SQLSuggestion[] {
    const suggestions: SQLSuggestion[] = [];
    const lowerInput = input.toLowerCase().trim();

    // Pattern: "show me X"
    if (lowerInput.match(/^show me/i) || lowerInput.match(/^get/i) || lowerInput.match(/^fetch/i)) {
      suggestions.push(this.suggestSelectAll());
    }

    // Pattern: "count of", "how many"
    if (lowerInput.match(/count|how many|total|aggregate/i)) {
      suggestions.push(...this.suggestCount(input));
    }

    // Pattern: "where X equals/is Y"
    if (lowerInput.match(/where|filter|with|where.*=|equals|is /i)) {
      suggestions.push(...this.suggestFilters(input));
    }

    // Pattern: "sorted by X" / "order by X"
    if (lowerInput.match(/sort|order|by|ascending|descending|top|bottom|largest|smallest/i)) {
      suggestions.push(...this.suggestSort(input));
    }

    // Pattern: "group by X"
    if (lowerInput.match(/group|by|per|for each|breakdown|distinct/i)) {
      suggestions.push(...this.suggestGroupBy(input));
    }

    // Pattern: "average/sum/min/max"
    if (lowerInput.match(/average|avg|sum|total|minimum|maximum|min|max/i)) {
      suggestions.push(...this.suggestAggregations(input));
    }

    // Pattern: "unique/distinct"
    if (lowerInput.match(/unique|distinct|different/i)) {
      suggestions.push(...this.suggestDistinct(input));
    }

    // If we found matches, return top 3 by confidence
    return suggestions
      .sort((a, b) => b.confidence - a.confidence)
      .slice(0, 3);
  }

  private suggestSelectAll(): SQLSuggestion {
    return {
      sql: `SELECT * FROM ${this.tableName} LIMIT 100;`,
      explanation: 'Show first 100 rows',
      confidence: 0.8
    };
  }

  private suggestCount(input: string): SQLSuggestion[] {
    const suggestions: SQLSuggestion[] = [];
    const col = this.findColumn(input);

    // Count all rows
    suggestions.push({
      sql: `SELECT COUNT(*) as total_rows FROM ${this.tableName};`,
      explanation: 'Total number of rows',
      confidence: 0.9
    });

    // Count distinct
    if (col) {
      suggestions.push({
        sql: `SELECT COUNT(DISTINCT ${col}) as unique_values FROM ${this.tableName};`,
        explanation: `Unique values in ${col}`,
        confidence: 0.85
      });
    }

    return suggestions;
  }

  private suggestFilters(input: string): SQLSuggestion[] {
    const suggestions: SQLSuggestion[] = [];
    const col = this.findColumn(input);

    if (col) {
      // Extract potential values
      const values = this.extractValues(input);

      if (values.length > 0) {
        const valueStr = values.map(v => `'${v}'`).join(', ');
        suggestions.push({
          sql: `SELECT * FROM ${this.tableName} WHERE ${col} IN (${valueStr});`,
          explanation: `Filter where ${col} is ${values.join(' or ')}`,
          confidence: 0.8
        });
      }

      // Greater/less than patterns
      if (input.match(/greater than|more than|>/i)) {
        suggestions.push({
          sql: `SELECT * FROM ${this.tableName} WHERE ${col} > 0 ORDER BY ${col} DESC;`,
          explanation: `Filter where ${col} is greater than a value`,
          confidence: 0.7
        });
      }

      if (input.match(/less than|fewer than|</i)) {
        suggestions.push({
          sql: `SELECT * FROM ${this.tableName} WHERE ${col} < 1000 ORDER BY ${col};`,
          explanation: `Filter where ${col} is less than a value`,
          confidence: 0.7
        });
      }
    }

    return suggestions;
  }

  private suggestSort(input: string): SQLSuggestion[] {
    const suggestions: SQLSuggestion[] = [];
    const col = this.findColumn(input);

    if (col) {
      const isDesc = input.match(/descending|desc|largest|top|highest|most/i);
      const order = isDesc ? 'DESC' : 'ASC';
      const orderWord = isDesc ? 'descending' : 'ascending';

      suggestions.push({
        sql: `SELECT * FROM ${this.tableName} ORDER BY ${col} ${order} LIMIT 100;`,
        explanation: `Sort by ${col} ${orderWord}`,
        confidence: 0.9
      });
    }

    return suggestions;
  }

  private suggestGroupBy(input: string): SQLSuggestion[] {
    const suggestions: SQLSuggestion[] = [];
    const col = this.findColumn(input);

    if (col) {
      suggestions.push({
        sql: `SELECT ${col}, COUNT(*) as count FROM ${this.tableName} GROUP BY ${col} ORDER BY count DESC;`,
        explanation: `Group by ${col} and count`,
        confidence: 0.85
      });

      // Also suggest with sum/avg if numeric
      suggestions.push({
        sql: `SELECT ${col}, COUNT(*) as count, AVG(${col}) as avg_value FROM ${this.tableName} GROUP BY ${col};`,
        explanation: `Group by ${col} with aggregations`,
        confidence: 0.75
      });
    }

    return suggestions;
  }

  private suggestAggregations(input: string): SQLSuggestion[] {
    const suggestions: SQLSuggestion[] = [];
    const col = this.findColumn(input);

    if (col) {
      if (input.match(/average|avg/i)) {
        suggestions.push({
          sql: `SELECT AVG(${col}) as average FROM ${this.tableName};`,
          explanation: `Average of ${col}`,
          confidence: 0.9
        });
      }

      if (input.match(/sum|total/i)) {
        suggestions.push({
          sql: `SELECT SUM(${col}) as total FROM ${this.tableName};`,
          explanation: `Sum of ${col}`,
          confidence: 0.9
        });
      }

      if (input.match(/min|minimum|smallest|lowest/i)) {
        suggestions.push({
          sql: `SELECT MIN(${col}) as minimum FROM ${this.tableName};`,
          explanation: `Minimum value of ${col}`,
          confidence: 0.9
        });
      }

      if (input.match(/max|maximum|largest|highest|biggest/i)) {
        suggestions.push({
          sql: `SELECT MAX(${col}) as maximum FROM ${this.tableName};`,
          explanation: `Maximum value of ${col}`,
          confidence: 0.9
        });
      }
    }

    return suggestions;
  }

  private suggestDistinct(input: string): SQLSuggestion[] {
    const suggestions: SQLSuggestion[] = [];
    const col = this.findColumn(input);

    if (col) {
      suggestions.push({
        sql: `SELECT DISTINCT ${col} FROM ${this.tableName};`,
        explanation: `All unique values of ${col}`,
        confidence: 0.9
      });
    }

    return suggestions;
  }

  private findColumn(input: string): string | null {
    const lowerInput = input.toLowerCase();

    for (const col of this.columns) {
      const colLower = col.toLowerCase();
      if (lowerInput.includes(colLower)) {
        return col;
      }
    }

    // Try fuzzy matching for partial names
    for (const col of this.columns) {
      const words = col.toLowerCase().split(/[-_\s]/);
      for (const word of words) {
        if (word.length > 2 && lowerInput.includes(word)) {
          return col;
        }
      }
    }

    return null;
  }

  private extractValues(input: string): string[] {
    const quoted = input.match(/'([^']*)'/g);
    if (quoted) {
      return quoted.map(q => q.slice(1, -1));
    }

    // Extract quoted words after "is", "equals", "like"
    const match = input.match(/(is|equals|like|=)\s+([a-zA-Z0-9]+)/i);
    return match ? [match[2]] : [];
  }
}
