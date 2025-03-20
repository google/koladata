/**
 * @license
 * Copyright 2024 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import {html, NanoElement,} from './nano_element';

/**
 * Interface for the detail of the hover-cell event.
 */
export interface HoverCellDetail {
  row: number;
  col: number;
  content?: HTMLElement;
}

/**
 * Renders a table that maximizes the amount of data in available space.
 * Rows represent different attributes and columns represent different items.
 * This horizontal direction makes it easier to scroll in Colab notebooks.
 * Since some attributes, such as URLs, require more characters to
 * be meaningful than numbers, attributes with long values are tiled into
 * multiple rows using rowspan and colspan. However, each value must stay on
 * a single line.
 *
 * Input cells to be rendered should be passed to this table as densely packed
 * light DOM children in row major order (i.e. all values for an attribute
 * should be adjacent). The following attributes can be configured:
 * - data-headers: A comma separated list of header names.
 * - data-max-folds: The maximum number of times the table will double cell
 *       tiling. A value of 0 means no tiling and a value of 2 means a header
 *       may be split into 4 rows.
 * - data-header-classes: A space-separated list of class associations. A
 *       class association is a header name followed by a colon followed by a
 *       comma separated list of class names:
 *         E.g. "col_0:clickable,deemph col_1:clickable"
 *       There may be multiple class associations for the same header.
 *       These classes are applied on the tr(s) associated with the header.
 */
export class CompactTable extends NanoElement {
  static override get shadowStyle() {
    const cellPaddingY = '2px';
    return `
      :host {
        --background: var(--kd-compact-table-background, white);
        --col-hover-background: color-mix(in srgb, orange 12%, var(--background));
        --deemph-color: color-mix(in srgb, currentcolor 26.6%, var(--background));
        --row-hover-background: color-mix(in srgb, currentcolor 7%, var(--background));
        --stripe-0: color-mix(in srgb, currentcolor 4%, var(--background));
        --stripe-1: var(--background);
        --stripe-background: repeating-linear-gradient(
              45deg,
              var(--stripe-0), var(--stripe-0) 5px,
              var(--stripe-1) 5px, var(--stripe-1) 10px);
        display: inline-block;
      }

      ::slotted(*) {
        overflow: hidden;
        padding: ${cellPaddingY} 8px;
        padding-right: 0;  /* No padding since ellipsis gives enough space. */
        text-overflow: ellipsis;
        white-space: nowrap;
      }

      ::slotted(*.deemph) {
        color: var(--deemph-color);
      }

      table {
        border-collapse: collapse;
        color: inherit;
      }

      th {
        background: var(--background);
        height: 0;
        left: 0;
        padding: 0;
        position: sticky;
        text-align: right;
        z-index: 3;
      }

      /* Note that we style the child of the th since borders on sticky th
         elements apparently do not have sticky behavior. */
      th > * {
        border-right: 2px solid currentcolor;
        height: 100%;
        padding: ${cellPaddingY} 8px;
      }

      td {
        border-right: 1px solid lightgray;
        box-sizing: border-box;
        max-width: var(--kd-compact-table-td-width, 60px);
        min-width: var(--kd-compact-table-td-width, 60px);
        padding: 0;
        position: relative;
      }

      td > div {
        /* Restrict content to one line. */
        max-height: calc(1lh + 2 * ${cellPaddingY});
        overflow: hidden;
      }

      td.four {
        border-left: 3px solid lightgray;
      }

      td:empty {
        background: var(--stripe-background);
      }

      tr.hover td:empty {
        background: var(--stripe-background);
      }

      tr + tr {
        border-top: 1px solid lightgray;
      }

      tr.boundary {
        border-top: 2px solid lightgray;
      }

      th.hover,
      tr.hover,
      tr.hover th {
        background: var(--row-hover-background);
      }

      td.hover {
        background: var(--col-hover-background);
      }

      td.active::before {
        content: '';
        height: 100%;
        outline: 2px solid orange;
        position: absolute;
        width: 100%;
        z-index: 2;
      }

      .header-text {
        /* This class is only for styling and should not be a click target. */
        pointer-events: none;
      }

      tr.clickable {
        th {
          cursor: pointer;
        }
        .header-text {
          text-decoration: underline;
        }
      }

      tr.deemph {
        .header-text {
          color: var(--deemph-color);
        }
      }

      :host(.hover-overflow) td.hover ::slotted(*) {
        background: var(--col-hover-background);
        box-sizing: border-box;
        min-width: calc(100% + 1px);
        padding-right: 8px;
        position: relative;
        z-index: 1;
      }

      :host(.hover-overflow) td.hover > div {
        width: fit-content;
      }
    `;
  }

  static override get shadowInit(): ShadowRootInit {
    return {mode: 'open', slotAssignment: 'manual'};
  }

  static get observedAttributes() {
    return ['data-headers', 'data-max-folds', 'data-header-classes'];
  }

  constructor() {
    super();
    new MutationObserver(this.render.bind(this)).observe(this, {
      childList: true,
    });

    this.addEventListener('click', (e) => {
      if (e.ctrlKey) {
        this.classList.toggle('hover-overflow');
      }
    });

    this.addEventListener('mousemove', (e) => {
      let cell = e
        .composedPath()
        .filter(
          (x): x is HTMLTableCellElement => x instanceof HTMLTableCellElement,
        )[0];
      if (!cell) return;

      // When the column is hovering, the cell contents may extend beyond its
      // bounding box. In this case, move to the next sibling until the mouse
      // is in the cell's bounding box. Otherwise, the user would not be able
      // to hover towards the right.
      while (cell.getBoundingClientRect().right < e.clientX) {
        const next = cell.nextElementSibling as HTMLTableCellElement;
        if (!next) break;
        cell = next;
      }

      const tr = cell.closest<HTMLTableRowElement>('tr');
      const {tableElement: table} = this;
      if (!tr || !table) return;

      // Avoid making any changes if the hover state is the same as before.
      if (tr.classList.contains('hover') && cell.classList.contains('hover')) {
        return;
      }

      if (cell.tagName === 'TD') {
        this.clearHover();

        tr.classList.add('hover');

        const col = Number(cell.dataset['col']);
        const row = Number(tr.dataset['row']);

        // Mark the th element that represents the associated attribute.
        table.querySelector(`tr[data-row="${row}"] th`)?.classList.add('hover');

        // Highlight the cells that belong to the item.
        for (const target of table.querySelectorAll(`td[data-col="${col}"]`)) {
          target.classList.add('hover');
        }

        const content = this.getSlotContent(
          cell.querySelector<HTMLSlotElement>('slot'),
        );
        this.dispatchEvent(
          new CustomEvent<HoverCellDetail>('hover-cell', {
            detail: {
              row,
              col,
              content,
            },
          }),
        );
      } else {
        this.clearHover();

        // Mark all tr elements that represent the same attribute.
        table
          .querySelectorAll<HTMLTableRowElement>(
            `tr[data-row="${tr.dataset['row']}"]`,
          )
          .forEach((tr) => {
            tr.classList.add('hover');
          });
      }
    });

    this.addEventListener('mouseleave', (e) => {
      this.clearHover();
      this.dispatchEvent(new CustomEvent('clear-hover'));
    });
  }

  private tableElement?: HTMLTableElement;

  // Resolves when the last render is complete.
  renderComplete: Promise<void> = Promise.resolve();

  get headerWidth() {
    return (
      this.tableElement?.querySelector('th')?.getBoundingClientRect()?.width ||
      0
    );
  }

  private clearHover() {
    for (const other of this.shadowRoot?.querySelectorAll('.hover') || []) {
      other.classList.remove('hover');
    }
  }

  getCellAssignedNode(row: number, col: number) {
    return this.tableElement
      ?.querySelector<HTMLSlotElement>(
        `tr[data-row="${row}"] td[data-col="${col}"] slot`,
      )
      ?.assignedNodes()?.[0];
  }

  get headers() {
    return this.dataset['headers']?.split(',') || [];
  }

  // Map from header name to the number of rows that the header spans. A
  // header has a span greater than 1 if it has cells that are longer than
  // the max cell width.
  private readonly headerSpanMap = new Map<string, number>();

  private renderCell(
    tr: HTMLElement,
    cellsPerHeader: number,
    i: number,
    j: number,
  ) {
    // We want an extra div to allow limiting the height of the cell to
    // a single line, which doesn't seem possible on a td.
    const td = html.tag('td', html.tag('div', html.tag('slot')));
    const slot = td.children[0].children[0] as HTMLSlotElement;
    tr.appendChild(td);
    slot.assign(this.children[cellsPerHeader * i + j]);
    return td;
  }

  getHeaderSpanCount(header: string) {
    return this.headerSpanMap.get(header) || 1;
  }

  setCellActive(row: number, col: number) {
    this.tableElement
      ?.querySelector(`tr[data-row="${row}"] td[data-col="${col}"]`)
      ?.classList?.add('active');
  }

  clearCellActive() {
    this.tableElement?.querySelectorAll('.active')?.forEach((x) => {
      x.classList.remove('active');
    });
  }

  private renderHeader(header: string) {
    const spanCount = this.getHeaderSpanCount(header);
    const th = html.tag(
      'th',
      // Header is encoded as a data attribute for easier event interpretation
      // by users of this component. The 'header-text' element is necessary
      // for deemph styling such that the border on the div continues to be
      // the currentcolor, but the text is deemphasized.
      html.tag(
        'div',
        html.data('header', header),
        html.tag('span', html.class('header-text'), header),
      ),
    );
    th.setAttribute('rowspan', String(spanCount));
    return th;
  }

  private getHeaderClasses(header: string): string[] {
    return (
      this.dataset['headerClasses']
        ?.split(' ')
        .map((x) => x.split(':'))
        .filter((x) => x[0] === header)
        .flatMap((x) => x[1].split(','))
        .filter((x) => x) || []
    );
  }

  /**
   * Renders all tr elements for a single header/attribute based on the current
   * span for that header.
   *
   * @param table Created rows are appended to this table.
   * @return The rendered rows.
   */
  private renderRows(
    table: HTMLTableElement,
    header: string,
    cellsPerHeader: number,
    headerIndex: number,
  ): HTMLTableRowElement[] {
    const th = this.renderHeader(header);
    const spanCount = this.getHeaderSpanCount(header);
    const customClasses = this.getHeaderClasses(header);

    const rows = [];
    for (let k = 0; k < spanCount; k++) {
      const tr = html.tag<HTMLTableRowElement>(
        'tr',
        html.class(...customClasses),
      );
      tr.dataset['row'] = String(headerIndex);

      // Add class for thicker lines between rows that separate attributes.
      if (headerIndex > 0 && k === 0) {
        tr.classList.add('boundary');
      }

      table.appendChild(tr);
      rows.push(tr);

      // Only the first tr contains the header for the row.
      if (k === 0) {
        tr.append(th);
      }

      // Offset rows to align the start of each cell with the correct column.
      for (let i = 0; i < k; i++) {
        tr.append(document.createElement('td'));
      }

      for (let j = k; j < cellsPerHeader; j += spanCount) {
        const td = this.renderCell(tr, cellsPerHeader, headerIndex, j);
        td.setAttribute('colspan', String(spanCount));
        td.dataset['col'] = String(j);

        // Add class to show thicker border between every 4 cells.
        if (k === 0 && j > 0 && j % 4 === 0) {
          td.classList.add('four');
        }

        tr.append(td);
      }
    }

    return rows;
  }

  private measureCellWidth(table: HTMLTableElement) {
    const td = document.createElement('td');
    table.appendChild(td);
    const width =
      Number(window.getComputedStyle(td).maxWidth.replace('px', '')) ?? 0;
    td.remove();
    return width;
  }

  private getSlotContent(slot: HTMLSlotElement | null) {
    // We only support slot assignment of HTMLElements and not
    // text nodes.
    return (slot?.assignedElements()?.[0] as HTMLElement) ?? slot;
  }

  /**
   * Updates the private map of header span counts based on the rendered
   * dimensions of the input rows.
   *
   * @return Whether the span count for the header was updated.
   */
  private updateSpanCount(
    header: string,
    cellWidth: number,
    rows: HTMLElement[],
  ) {
    const maxFolds = Number(this.dataset['maxFolds'] || 2);
    const impliedSpanCount = rows
      .flatMap((row) =>
        Array.from(row.querySelectorAll<HTMLSlotElement>('slot')),
      )
      .map((x) => this.getSlotContent(x))
      .map((x) => {
        // Slotted content with this class will never attempt to increase the
        // span count of the header. This is useful for numeric content.
        if (x.classList.contains('always-elide')) {
          return 1;
        }

        // We know that the element is too long if its scrollWidth (the total
        // content width) is longer than its offsetWidth (the visible width).
        if (x.scrollWidth <= (x instanceof HTMLElement ? x.offsetWidth : 0)) {
          return 1;
        }

        // We restrict to powers to two because we want to maintain the
        // the positions of cells as much as possible. For example if we double,
        // half of the cells will be in the same position as before.
        const numFolds = Math.ceil(
          Math.log2(Math.max(Math.ceil(x.scrollWidth / cellWidth), 1)),
        );
        return Math.min(2 ** Math.min(numFolds, maxFolds));
      })
      .reduce((x, y) => Math.max(x, y), 1);
    const spanCount = this.getHeaderSpanCount(header);
    if (impliedSpanCount <= spanCount) return false;

    this.headerSpanMap.set(header, impliedSpanCount);
    return true;
  }

  clearSpanCounts() {
    this.headerSpanMap.clear();
  }

  override attributeChangedCallback(
    name: string,
    oldValue: string,
    newValue: string,
  ) {
    if (name === 'data-max-folds') {
      this.clearSpanCounts();
    }
    super.attributeChangedCallback(name, oldValue, newValue);
  }

  override async render() {
    const {shadowRoot} = this;
    if (!shadowRoot) return;
    const {headers, cellWidth, allRows} = this.renderPass(shadowRoot);

    const updateSpanCount = () => {
      // If we updated the span count, re-render the table.
      let needsSecondPass = false;
      for (let i = 0; i < headers.length; i++) {
        if (this.updateSpanCount(headers[i], cellWidth, allRows[i])) {
          needsSecondPass = true;
        }
      }
      if (needsSecondPass) {
        this.renderPass(shadowRoot);
      }
    };

    // Defer the updateSpanCount to avoid measuring before the browser has
    // had a chance to do layout.
    this.renderComplete = new Promise<void>(resolve => setTimeout(() => {
                                              updateSpanCount();
                                              resolve();
                                            }, 0));
    return this.renderComplete;
  }

  private renderPass(shadowRoot: ShadowRoot) {
    shadowRoot.textContent = '';

    this.tableElement = document.createElement('table');
    const table = this.tableElement;
    shadowRoot.appendChild(table);

    const cellWidth = this.measureCellWidth(table);
    const {headers} = this;

    const allRows: HTMLTableRowElement[][] = [];
    const cellsPerHeader = Math.floor(this.children.length / headers.length);
    for (let i = 0; i < headers.length; i++) {
      const rows = this.renderRows(table, headers[i], cellsPerHeader, i);
      allRows.push(rows);
    }

    return {headers, cellWidth, allRows};
  }
}

customElements.define('kd-compact-table', CompactTable);
