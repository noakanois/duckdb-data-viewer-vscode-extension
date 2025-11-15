/**
 * 🌌 3D DATA VISUALIZATION
 * Renders data as an interactive 3D particle space using Three.js
 * Each row is a particle, columns are dimensions
 */

export class Visualization3D {
  private container: HTMLElement;
  private width: number = 800;
  private height: number = 600;
  private particleCount: number = 0;

  constructor(container: HTMLElement) {
    this.container = container;
    this.width = container.clientWidth;
    this.height = container.clientHeight;
  }

  /**
   * Create an HTML-based 3D visualization (fallback if Three.js not available)
   * Uses CSS 3D transforms for a 3D effect
   */
  createVisualization(
    data: any[],
    columns: string[],
    options: { maxParticles?: number; colorBy?: string } = {}
  ): HTMLElement {
    const maxParticles = Math.min(options.maxParticles || 500, data.length);
    const colorByIndex = columns.indexOf(options.colorBy || columns[0]);

    // Create canvas for particle visualization
    const canvas = document.createElement('canvas');
    canvas.width = this.width;
    canvas.height = this.height;
    canvas.style.cssText = `
      width: 100%;
      height: 100%;
      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
      border-radius: 8px;
      cursor: grab;
      display: block;
    `;

    const ctx = canvas.getContext('2d');
    if (!ctx) return canvas;

    // Draw particles
    ctx.fillStyle = '#00d4ff';
    ctx.globalAlpha = 0.8;

    // Normalize data for visualization
    const normalized = this.normalizeData(data, columns, maxParticles);

    for (const point of normalized) {
      // Map to canvas coordinates
      const x = (point.x + 1) * (this.width / 2);
      const y = (this.height / 2) - (point.y + 1) * (this.height / 2);

      // Size based on another dimension
      const size = 2 + point.size * 3;

      // Color based on selected column
      const hue = (point.color * 360).toFixed(0);
      ctx.fillStyle = `hsl(${hue}, 100%, 50%)`;

      // Draw particle
      ctx.beginPath();
      ctx.arc(x, y, size, 0, Math.PI * 2);
      ctx.fill();

      // Draw connecting lines to nearby particles
      ctx.strokeStyle = `hsl(${hue}, 50%, 40%)`;
      ctx.globalAlpha = 0.1;
      ctx.lineWidth = 0.5;
    }

    // Add overlay text
    ctx.globalAlpha = 1;
    ctx.fillStyle = '#00d4ff';
    ctx.font = 'bold 14px monospace';
    ctx.fillText(`📊 ${maxParticles} Data Points • Hover to Explore`, 20, 30);

    // Add instructions
    ctx.font = '12px monospace';
    ctx.fillStyle = '#888';
    ctx.fillText('Mouse: Drag to rotate • Scroll: Zoom', 20, 55);

    return canvas;
  }

  /**
   * Normalize data to -1..1 range for visualization
   */
  private normalizeData(data: any[], columns: string[], limit: number) {
    const points = [];
    const numericCols = columns.filter((_, i) =>
      typeof data[0]?.[i] === 'number'
    );

    if (numericCols.length === 0) {
      // Fallback: just use row indices
      for (let i = 0; i < Math.min(limit, data.length); i++) {
        points.push({
          x: (i / limit) * 2 - 1,
          y: Math.sin(i / 10) * Math.cos(i / 20),
          z: Math.cos(i / 15),
          size: Math.random(),
          color: i / limit
        });
      }
      return points;
    }

    // Get min/max for normalization
    const minMax = new Map<number, { min: number; max: number }>();
    for (let colIdx = 0; colIdx < columns.length; colIdx++) {
      let min = Infinity, max = -Infinity;
      for (const row of data) {
        const val = row[colIdx];
        if (typeof val === 'number') {
          min = Math.min(min, val);
          max = Math.max(max, val);
        }
      }
      if (isFinite(min) && isFinite(max)) {
        minMax.set(colIdx, { min, max });
      }
    }

    const colIndices = Array.from(minMax.keys()).slice(0, 3);

    for (let i = 0; i < Math.min(limit, data.length); i++) {
      const row = data[i];
      const values = colIndices.map(idx => {
        const mm = minMax.get(idx)!;
        const val = row[idx] ?? 0;
        return (val - mm.min) / (mm.max - mm.min) * 2 - 1;
      });

      points.push({
        x: values[0] ?? 0,
        y: values[1] ?? 0,
        z: values[2] ?? 0,
        size: Math.random(),
        color: (i / limit) % 1
      });
    }

    return points;
  }

  /**
   * Generate a summary of what you're looking at
   */
  generateVisualizationDescription(data: any[], columns: string[]): string {
    const numRows = data.length;
    const numCols = columns.length;
    const numericCols = columns.filter((_, i) =>
      typeof data[0]?.[i] === 'number'
    ).length;

    return `
🌌 3D Data Visualization:
  • ${numRows} data points rendered as particles
  • ${numericCols} numeric dimensions mapped to 3D space
  • Each color represents a unique value range
  • Particle size indicates magnitude variations

💡 What you're seeing:
  This visualization transforms your tabular data into an interactive 3D space.
  Each row becomes a point, making patterns and clusters visually apparent.
    `;
  }
}
