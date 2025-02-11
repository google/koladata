/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * @fileoverview Utility functions to render components of a MultiDimTable.
 */

import {CompactTable} from './compact_table';
import {MultiDimNav} from './multi_dim_nav';
import {html} from './nano_element';


/**
 * Renders selector for choosing the cell width. This appears in the view
 * options section.
 */
export function cellWidthSelect(cellWidth: number, dataset: DOMStringMap) {
  const cellWidthSelect = html.tag<HTMLSelectElement>(
      'select',
      html.id('cell-width-select'),
      html.tag('option', '60'),
      html.tag('option', '120'),
      html.tag('option', '240'),
  );

  cellWidthSelect.value = String(cellWidth || 120);
  cellWidthSelect.addEventListener('change', (e) => {
    if (e.target instanceof HTMLSelectElement) {
      dataset['cellWidth'] = String(e.target.value);
    }
  });
  return cellWidthSelect;
}

/**
 * Renders checkbox for whether to allow data in cells to overflow their
 * boundaries. This appears in the view options section.
 */
export function overflowCheckbox() {
  const overflowCheckbox = document.createElement('input');
  overflowCheckbox.type = 'checkbox';
  overflowCheckbox.checked = true;
  return overflowCheckbox;
}

/**
 * Renders selector for choosing how the maximum width of a cell. This appears
 * in the view options section.
 */
export function tilingSelect(maxFolds: number, dataset: DOMStringMap) {
  const tilingSelect = html.tag<HTMLSelectElement>(
      'select',
      html.id('tiling-select'),
      html.tag('option', '1'),
      html.tag('option', '2'),
      html.tag('option', '4'),
  );

  tilingSelect.value = String(2 ** maxFolds);

  tilingSelect.addEventListener('change', (e) => {
    if (e.target instanceof HTMLSelectElement) {
      dataset['maxFolds'] = String(Math.log2(Number(e.target.value)));
    }
  });

  return tilingSelect;
}

/**
 * This section contains both the view options and the message slot.
 */
export function infoBar(
    dataset: DOMStringMap, cellWidth: number, maxFolds: number) {
  const overflowCheckboxElement = overflowCheckbox();
  const viewOptions = html.tag(
      'div',
      html.id('view-options'),
      html.tag('span', ' overflow ', overflowCheckboxElement),
      html.tag('span', ' cell width ', cellWidthSelect(cellWidth, dataset)),
      html.tag('span', ' max tiling ', tilingSelect(maxFolds, dataset)),
  );

  const infoBar = html.tag(
      'div',
      html.tag(
          'div',
          html.id('message-wrapper'),
          html.tag('slot', html.name('message')),
          ),
      html.id('info-bar'),
      viewOptions,
  );

  return {overflowCheckbox: overflowCheckboxElement, infoBar};
}

/**
 * Renders the data region which notably contains the nav bar, the compact
 * table, and the detail pane.
 */
export function dataRegionParts(dataset: DOMStringMap, maxFolds: number) {
  // These are parts of the nav bar.
  const viewElement = html.tag('div', html.id('view'));
  const shadeBefore = html.tag('div', html.class('shade'));
  const shadeAfter = html.tag('div', html.class('shade'));
  const dimNav = html.tag<MultiDimNav>(
      'kd-multi-dim-nav', html.data('sizes', dataset['sizes']),
      html.data('rowHeight', '20'), html.data('layout', 'small'), viewElement,
      shadeBefore, shadeAfter);

  const compactTable = dataCompactTable(
      dataset,
      maxFolds,
      dimNav,
  );
  const main = html.tag(
      'div', html.id('main'), html.tag('div', html.id('nav'), dimNav),
      compactTable);
  const detailPane = html.tag('div', html.id('detail'));
  const dataRegion = html.tag('div', html.id('data-region'), main, detailPane);

  return {
    main,
    dataRegion,
    compactTable,
    dimNav,
    detailPane,
    viewElement,
    shadeBefore,
    shadeAfter,
  };
}

/**
 * Valid values for the content-mode attribute.
 */
export enum ContentMode {
  // Each cell is deep cloned into the compact table.
  CLONE = 'clone',
  // Only the text content of the cell is copied into the compact table.
  TEXT = 'text',
}

/**
 * Renders the main data compact table. This transfers many data attributes from
 * the MultiDimTable to the CompactTable. The dimNav is used add additional
 * rows for the data dimensions.
 */
export function dataCompactTable(
    dataset: DOMStringMap, maxFolds: number, dimNav: MultiDimNav) {
  const allHeaderClasses = [];
  const allHeaders = [];
  const indexBase = Number(dataset['dimIndexBase']) || 0;
  for (let i = 0; i < dimNav.numDims || 0; i++) {
    const dimHeader = `[dim_${i + indexBase}]`;
    allHeaders.push(dimHeader);
    allHeaderClasses.push(`${dimHeader}:deemph`);
  }

  if (dataset['headers']) {
    allHeaders.push(dataset['headers'] || '');
  }
  if (dataset['headerClasses']) {
    allHeaderClasses.push(dataset['headerClasses'] || '');
  }

  return html.tag<CompactTable>(
      'kd-compact-table',
      html.data('headers', allHeaders.join(',')),
      html.data('headerClasses', allHeaderClasses.join(' ')),
      html.data('maxFolds', String(maxFolds)),
      html.class('hover-overflow'),
  );
}

/**
 * Transfers data from the MultiDimTable's children to the CompactTable. The
 * dimNav is used add additional rows for the data dimensions. The contentMode
 * determines how cell content is transferred.
 */
export function cloneToCompactTable(
    children: HTMLCollection, contentMode: string|null,
    loadedRange: [number, number], compactTable: CompactTable,
    dimNav: MultiDimNav|undefined) {
  compactTable.textContent = '';

  if (dimNav) {
    const [begin, end] = loadedRange;
    for (let i = 0; i < dimNav.numDims; i++) {
      let current = begin;
      for (const {index: indexInDim, size} of dimNav.sizeSliceInDim(
               i, begin, end)) {
        for (let j = 0; j < size; j++) {
          // Do not emphasize any dimension label in the last dimension since
          // they always change between each cell.
          const isEmphasized = i + 1 < dimNav.numDims && j === 0;
          compactTable.append(
              html.tag(
                  'div',
                  html.class(isEmphasized ? '' : 'deemph'),
                  html.class('dim-index'),
                  html.data('index', String(current++)),
                  html.data('dimension', String(i)),
                  String(indexInDim),
                  ),
          );
        }
      }
    }
  }

  const dataChildren = Array.from(children).filter(
      (node) => !node.getAttribute('slot'),
  );
  for (const node of dataChildren) {
    let clone = undefined;
    switch (contentMode || ContentMode.CLONE) {
      case ContentMode.TEXT:
        clone = document.createElement('div');
        clone.textContent = node.textContent;
        for (const clz of node.classList) {
          clone.classList.add(clz);
        }
        break;
      default:
        clone = node.cloneNode(true);
        break;
    }
    compactTable.appendChild(clone);
  }
}
