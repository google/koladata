/**
 * @license
 * Copyright 2024 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Infers the cell width from the content of the children.
 *
 * @param sample The elements to use as a sample for the calculation.
 * @param host An element in the document to use for looking up styles.
 */
export async function inferCellWidth(
    sample: Element[],
    host: Element,
    defaultWidth: number,
    ): Promise<number> {
  const canvas = new OffscreenCanvas(0, 0);
  const context = canvas.getContext('2d');
  if (!context) return defaultWidth;

  const style = window.getComputedStyle(host);
  // Manual construction of the font string because style.font is often empty
  // until it is explicitly set.
  context.font = style.font ||
      `${style.fontStyle} ${style.fontVariant} ${style.fontWeight} ${
                     style.fontSize} ${style.fontFamily}`;

  let maxContentWidth = 0;
  // Ignore slotted and always-elide children when inferring cell width.
  const dataChildren = sample.filter(
      (node) => !node.hasAttribute('slot') &&
          !node.classList.contains('always-elide'));
  for (const node of dataChildren) {
    const text = node.textContent || '';
    const metrics = context.measureText(text);
    maxContentWidth = Math.max(maxContentWidth, metrics.width);
  }

  // Padding should include internal cell padding and space for borders.
  const paddingXString =
      style.getPropertyValue('--kd-compact-table-slotted-padding-x');
  const paddingX = Number(paddingXString.match(/\d+/)?.[0]) || 0;
  return Math.min(Math.ceil(maxContentWidth + 2 * paddingX), 2 * defaultWidth);
}
