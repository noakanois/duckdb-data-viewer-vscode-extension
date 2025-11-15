/**
 * 🎨 VISUAL QUERY BUILDER
 * Drag-and-drop SQL query constructor with real-time preview
 */

export interface QueryCondition {
  column: string;
  operator: '=' | '!=' | '>' | '<' | '>=' | '<=' | 'LIKE' | 'IN';
  value: string;
}

export interface QueryConfig {
  select: string[];
  from: string;
  where: QueryCondition[];
  groupBy: string[];
  orderBy: { column: string; direction: 'ASC' | 'DESC' }[];
  limit?: number;
}

export class VisualQueryBuilder {
  private columns: string[];
  private tableName: string;
  private config: QueryConfig;

  constructor(columns: string[], tableName: string) {
    this.columns = columns;
    this.tableName = tableName;
    this.config = {
      select: columns,
      from: tableName,
      where: [],
      groupBy: [],
      orderBy: [],
      limit: 100
    };
  }

  /**
   * Generate UI for building queries visually
   */
  renderUI(): HTMLElement {
    const container = document.createElement('div');
    container.style.cssText = `
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      gap: 12px;
      padding: 16px;
      background: rgba(0, 0, 0, 0.2);
      border-radius: 8px;
    `;

    // SELECT clause
    container.appendChild(this.renderSelectSection());

    // WHERE clause
    container.appendChild(this.renderWhereSection());

    // GROUP BY clause
    container.appendChild(this.renderGroupBySection());

    // ORDER BY clause
    container.appendChild(this.renderOrderBySection());

    // LIMIT clause
    container.appendChild(this.renderLimitSection());

    return container;
  }

  private renderSelectSection(): HTMLElement {
    const section = document.createElement('div');
    section.style.cssText = `
      padding: 12px;
      background: rgba(0, 180, 255, 0.1);
      border-left: 3px solid #00b4ff;
      border-radius: 4px;
    `;

    const title = document.createElement('div');
    title.style.cssText = 'font-weight: bold; color: #00b4ff; margin-bottom: 8px;';
    title.textContent = '📋 SELECT Columns';
    section.appendChild(title);

    const columnsDiv = document.createElement('div');
    columnsDiv.style.cssText = 'display: flex; flex-wrap: wrap; gap: 6px;';

    for (const col of this.columns) {
      const tag = document.createElement('div');
      tag.style.cssText = `
        padding: 4px 8px;
        background: #00b4ff;
        color: white;
        border-radius: 4px;
        font-size: 12px;
        cursor: grab;
        user-select: none;
      `;
      tag.textContent = col;
      tag.draggable = true;

      tag.addEventListener('dragstart', (e) => {
        (e.dataTransfer as DataTransfer).effectAllowed = 'copy';
        (e.dataTransfer as DataTransfer).setData('column', col);
      });

      columnsDiv.appendChild(tag);
    }

    section.appendChild(columnsDiv);

    // Show selected
    const selected = document.createElement('div');
    selected.style.cssText = 'font-size: 12px; color: #888; margin-top: 8px;';
    selected.textContent = `${this.config.select.length} columns selected`;
    section.appendChild(selected);

    return section;
  }

  private renderWhereSection(): HTMLElement {
    const section = document.createElement('div');
    section.style.cssText = `
      padding: 12px;
      background: rgba(255, 100, 0, 0.1);
      border-left: 3px solid #ff6400;
      border-radius: 4px;
    `;

    const title = document.createElement('div');
    title.style.cssText = 'font-weight: bold; color: #ff6400; margin-bottom: 8px;';
    title.textContent = '🔍 WHERE Conditions';
    section.appendChild(title);

    const info = document.createElement('div');
    info.style.cssText = 'font-size: 12px; color: #888;';
    info.textContent = this.config.where.length === 0
      ? 'No filters applied'
      : `${this.config.where.length} condition(s)`;
    section.appendChild(info);

    if (this.config.where.length > 0) {
      const conditions = document.createElement('div');
      conditions.style.cssText = 'display: flex; flex-direction: column; gap: 4px; margin-top: 8px;';

      for (const cond of this.config.where) {
        const condDiv = document.createElement('div');
        condDiv.style.cssText = 'font-size: 11px; padding: 4px; background: rgba(255, 255, 255, 0.1); border-radius: 3px;';
        condDiv.textContent = `${cond.column} ${cond.operator} ${cond.value}`;
        conditions.appendChild(condDiv);
      }

      section.appendChild(conditions);
    }

    return section;
  }

  private renderGroupBySection(): HTMLElement {
    const section = document.createElement('div');
    section.style.cssText = `
      padding: 12px;
      background: rgba(0, 255, 100, 0.1);
      border-left: 3px solid #00ff64;
      border-radius: 4px;
    `;

    const title = document.createElement('div');
    title.style.cssText = 'font-weight: bold; color: #00ff64; margin-bottom: 8px;';
    title.textContent = '📊 GROUP BY';
    section.appendChild(title);

    const info = document.createElement('div');
    info.style.cssText = 'font-size: 12px; color: #888;';
    info.textContent = this.config.groupBy.length === 0
      ? 'No grouping'
      : `Group by: ${this.config.groupBy.join(', ')}`;
    section.appendChild(info);

    return section;
  }

  private renderOrderBySection(): HTMLElement {
    const section = document.createElement('div');
    section.style.cssText = `
      padding: 12px;
      background: rgba(255, 0, 255, 0.1);
      border-left: 3px solid #ff00ff;
      border-radius: 4px;
    `;

    const title = document.createElement('div');
    title.style.cssText = 'font-weight: bold; color: #ff00ff; margin-bottom: 8px;';
    title.textContent = '📈 ORDER BY';
    section.appendChild(title);

    const info = document.createElement('div');
    info.style.cssText = 'font-size: 12px; color: #888;';
    info.textContent = this.config.orderBy.length === 0
      ? 'No sorting'
      : `Sort: ${this.config.orderBy.map(o => `${o.column} ${o.direction}`).join(', ')}`;
    section.appendChild(info);

    return section;
  }

  private renderLimitSection(): HTMLElement {
    const section = document.createElement('div');
    section.style.cssText = `
      padding: 12px;
      background: rgba(255, 255, 0, 0.1);
      border-left: 3px solid #ffff00;
      border-radius: 4px;
    `;

    const title = document.createElement('div');
    title.style.cssText = 'font-weight: bold; color: #ffff00; margin-bottom: 8px;';
    title.textContent = '⏱️ LIMIT';
    section.appendChild(title);

    const limitDiv = document.createElement('div');
    limitDiv.style.cssText = 'display: flex; gap: 8px; align-items: center;';

    const input = document.createElement('input');
    input.type = 'number';
    input.value = String(this.config.limit || 100);
    input.min = '1';
    input.max = '10000';
    input.style.cssText = `
      padding: 4px 8px;
      border: 1px solid #ffff00;
      border-radius: 4px;
      background: rgba(0, 0, 0, 0.3);
      color: white;
      width: 80px;
    `;

    input.addEventListener('change', (e) => {
      this.config.limit = parseInt((e.target as HTMLInputElement).value);
    });

    limitDiv.appendChild(input);
    limitDiv.appendChild(document.createTextNode('rows'));

    section.appendChild(limitDiv);

    return section;
  }

  /**
   * Build SQL from current configuration
   */
  buildSQL(): string {
    let sql = `SELECT ${this.config.select.join(', ')} FROM ${this.config.from}`;

    if (this.config.where.length > 0) {
      const conditions = this.config.where
        .map(c => `${c.column} ${c.operator} ${this.formatValue(c.value)}`)
        .join(' AND ');
      sql += ` WHERE ${conditions}`;
    }

    if (this.config.groupBy.length > 0) {
      sql += ` GROUP BY ${this.config.groupBy.join(', ')}`;
    }

    if (this.config.orderBy.length > 0) {
      const orderClauses = this.config.orderBy
        .map(o => `${o.column} ${o.direction}`)
        .join(', ');
      sql += ` ORDER BY ${orderClauses}`;
    }

    if (this.config.limit) {
      sql += ` LIMIT ${this.config.limit}`;
    }

    sql += ';';

    return sql;
  }

  /**
   * Add a where condition
   */
  addCondition(column: string, operator: string, value: string) {
    this.config.where.push({
      column,
      operator: operator as any,
      value
    });
  }

  /**
   * Add a group by clause
   */
  addGroupBy(column: string) {
    if (!this.config.groupBy.includes(column)) {
      this.config.groupBy.push(column);
    }
  }

  /**
   * Add an order by clause
   */
  addOrderBy(column: string, direction: 'ASC' | 'DESC' = 'ASC') {
    this.config.orderBy.push({ column, direction });
  }

  private formatValue(value: string): string {
    if (value.match(/^\d+$/)) {
      return value; // Number
    }
    return `'${value.replace(/'/g, "''")}'`; // String
  }
}
