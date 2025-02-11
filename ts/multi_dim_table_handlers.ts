/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * @fileoverview More complex pieces of logic for MultiDimTable listeners.
 */

import {MultiDimNav} from './multi_dim_nav';

/**
 * Parameters necessary to determine whether we need to dispatch a request-load
 * event.
 */
export interface RequestLoadParams {
  loadedRange: [number, number];
  loadedMargin: number;
  visibleRange: [number, number];
}

/**
 * Coordinates that describe the location of a data cell in the compact table
 * that the user is currently hovering over.
 */
export interface HoverCoords {
  row: number;
  col: number;
}

// Callback that dispatches a request-load event on the element.
type DispatchRequestLoad = (centerIndex: number, viewBegin: number) => void;

/**
 * This invokes dispatchRequestLoad when the user changes to an index that
 * triggers the loading of new data.
 */
export function maybeRequestLoadOnChangeCurrent(
    loadParams: RequestLoadParams, index: number,
    dispatchRequestLoad: DispatchRequestLoad) {
  const {visibleRange, loadedRange, loadedMargin} = loadParams;
  const visibleInterval = Math.ceil(visibleRange[1] - visibleRange[0]);
  if (index < loadedRange[0] || index + visibleInterval >= loadedRange[1]) {
    const loadedInterval = loadedRange[1] - loadedRange[0];
    // This handles the case where the view is larger than half the loaded
    // interval. It is computed as though the view is aligned to the
    // right side of the loaded range.
    const centerIndexRight =
        index + visibleInterval + loadedMargin - 0.5 * loadedInterval;
    dispatchRequestLoad(Math.max(index, centerIndexRight), index);
  }
}

/**
 * This invokes dispatchRequestLoad when the user scrolls the compact table
 * in a way that requires loading new data.
 */
export function maybeRequestLoadOnScroll(
    loadParams: RequestLoadParams, viewRange: [number, number],
    dimNav: MultiDimNav|undefined, dispatchRequestLoad: DispatchRequestLoad) {
  const {loadedRange, loadedMargin, visibleRange} = loadParams;
  const [begin, end] = viewRange;
  let margin = loadedMargin;
  let triggerBegin = loadedRange[0] + margin;
  let triggerEnd = loadedRange[1] - margin;

  // If the trigger ranges overlap, keep them separate.
  if (triggerEnd < triggerBegin) {
    const triggerMiddle = (triggerBegin + triggerEnd) / 2;
    triggerBegin = triggerMiddle;
    triggerEnd = triggerMiddle;
    margin = triggerBegin - loadedRange[0];
  }

  // Note that we preserve the current visible range when we dispatch
  // a request-load event in this context.
  const halfLoadedInterval = 0.5 * (loadedRange[1] - loadedRange[0]);
  if (loadedRange[0] > 0 && begin <= triggerBegin) {
    dispatchRequestLoad(
        Math.ceil(visibleRange[1] - halfLoadedInterval + margin),
        visibleRange[0],
    );
  }
  if (loadedRange[1] < (dimNav?.totalSize || 0) && end >= triggerEnd) {
    dispatchRequestLoad(
        Math.floor(visibleRange[0] + halfLoadedInterval - margin),
        visibleRange[0],
    );
  }
}

/**
 * Sets attributes on the detail pane that describe the coordinates of the
 * data it currently contains.
 */
export function setDetailPaneHoverCoords(
    detailPane: HTMLElement, hoverCoords: HoverCoords|undefined) {
  detailPane.dataset['row'] = String(hoverCoords?.row);
  detailPane.dataset['col'] = String(hoverCoords?.col);
}

/**
 * Indicates whether the detail pane shows the data specified by hoverCoords.
 */
export function detailPaneMatchesHoverCoords(
    detailPane: HTMLElement|undefined, hoverCoords: HoverCoords|undefined) {
  return String(hoverCoords?.row) === detailPane?.dataset['row'] &&
      String(hoverCoords?.col) === detailPane?.dataset['col'];
}

/**
 * Updates the contents of the detail pane with the cells along the row that
 * the user is currently hovering over.
 */
export function updateDetailPane(
    children: HTMLCollection, detailPane: HTMLElement,
    hoverCoords: HoverCoords|undefined, numDataRows: number) {
  const row = hoverCoords?.row;
  const col = hoverCoords?.col;

  // Sutract the number of named slots from the number of children. These
  // elements (e.g. message) are laid out differently.
  const numNamedSlot =
      Array.from(children).filter((x) => x.hasAttribute('slot')).length;
  const numCols = (children.length - numNamedSlot) / numDataRows;
  if (detailPane.dataset['row'] !== String(row)) {
    detailPane.textContent = '';

    if (row != null && col != null) {
      // Create slots for each cell in the row.
      for (let i = 0; i < numCols; i++) {
        const content = children[row * numCols + i];
        const slot = document.createElement('slot');
        slot.dataset['col'] = String(i);
        slot.dataset['row'] = String(row);
        detailPane.appendChild(slot);
        if (content) {
          slot.assign(content);
        }
      }
    }
  }
  setDetailPaneHoverCoords(detailPane, hoverCoords);

  // Verify the active element is a slot for typescript.
  const activeElement = detailPane.children[hoverCoords?.col ?? -1];
  const activeSlot =
      activeElement instanceof HTMLSlotElement ? activeElement : null;

  // Make sure only the correct slot is active.
  for (const child of detailPane.children) {
    child.classList.remove('active');
  }
  activeSlot?.classList.add('active');

  // Make sure active slot is in view. We do not use scrollIntoView because
  // it either scrolls the whole window with `block: top` or aligns the bottom
  // of the content with `block: nearest`. The latter is not good when the
  // content is larger than the detail pane.
  if (activeSlot) {
    detailPane.scrollTop = activeSlot.offsetTop;
  }
  return activeSlot;
}
