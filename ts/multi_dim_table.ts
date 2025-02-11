/**
 * @license
 * Copyright 2024 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import './compact_table';
import './multi_dim_nav';

import {CompactTable, HoverCellDetail} from './compact_table';
import {ChangeCurrentDetail, MultiDimNav} from './multi_dim_nav';
import * as multiDimTableHandlers from './multi_dim_table_handlers';
import * as multiDimTableRender from './multi_dim_table_render';
import * as multiDimTableStyle from './multi_dim_table_style';
import {NanoElement} from './nano_element';

declare interface MultiDimTableInterface {
  get visibleRange(): [number, number];
  scrollToIndex: (index: number) => void;
  focusDataCell: (row: number, col: number) => void;
}

declare interface RequestLoadDetail {
  // The index of the center of the range to load.
  centerIndex: number;
  // The index from which we would like to view the new data.
  viewBegin: number;
}

/**
 * Shows the data from a multi-dimensional jagged array in a table. There is a
 * multi-dimensional nav at the top which allows users to see where they are
 * in the data and change their view. The data is rendered in a compact table
 * with some options to configure the cell width, tiling, and overflow.
 *
 * Observed attributes:
 *  - data-cell-width: The width of each cell in the compact table.
 *  - data-headers: The headers for the compact table.
 *  - data-header-classes: The classes for the compact table headers.
 *  - data-loaded-range: A comma separated pair of numbers indicating the
 *      first and last loaded indices.
 *  - data-loaded-margin: The number of items from the boundary of the loaded
 *      range at which the 'request-load' event is fired. More precisely,
 *      it is fired when the user scrolls any item in the margin into view.
 *  - data-max-folds: The maximum number of times rows associated with an
 *      attribute in the compact table should be doubled.
 *  - data-sizes: Describes the shape of the jagged array. See multi_dim_nav.ts
 *      for the format.
 *  - data-dim-index-base: The first index of dimensions -- defaults to 0.
 *  - content-mode:
 *      Determines how cell content is handled in the main table.
 *        - 'clone': [default] The content of the cell is cloned into the cell.
 *        - 'text': Only the text content of the cell is shown.
 */
export class MultiDimTable
  extends NanoElement
  implements MultiDimTableInterface
{
  static override get shadowStyle() {
    return multiDimTableStyle.shadow();
  }

  static get observedAttributes() {
    return [
      'content-mode',
      'data-cell-width',
      'data-headers',
      'data-header-classes',
      'data-loaded-range',
      'data-max-folds',
      'data-sizes',
    ];
  }

  override attributeChangedCallback(
    name: string,
    oldValue: string,
    newValue: string,
  ) {
    if (name === 'data-cell-width') {
      this.style.setProperty('--kd-compact-table-td-width', `${newValue}px`);
      this.compactTable?.clearSpanCounts();
      this.compactTable?.render();
      this.syncViewWithVisibleRange();
    } else if (name === 'data-max-folds') {
      const {compactTable} = this;
      if (compactTable) {
        compactTable.dataset['maxFolds'] = newValue;
      }
    } else if (name === 'data-loaded-range') {
      this.updateShades();
    } else {
      super.attributeChangedCallback(name, oldValue, newValue);
    }
  }

  constructor() {
    super();
    new MutationObserver(this.onMutation.bind(this)).observe(this, {
      childList: true,
    });

    const {shadowRoot} = this;
    shadowRoot?.addEventListener(
      'hover-cell',
      (e) => {
        const cast = e as CustomEvent<HoverCellDetail>;
        this.dimNav?.moveCurrentFlagToIndex(
          cast.detail.col + this.loadedRange[0],
        );
        this.setAttribute('hover-col', '');
      },
      true,
    );

    shadowRoot?.addEventListener(
      'clear-hover',
      () => {
        this.removeAttribute('hover-col');
        if (this.detailPane && !this.detailPane?.classList.contains('pinned')) {
          this.detailPane.textContent = '';
          this.detailPane.dataset['row'] = '';
          this.detailPane.dataset['col'] = '';
        }
      },
      true,
    );
  }

  override connectedCallback() {
    super.connectedCallback();
    const resizeCallback = () => {
      this.syncViewWithVisibleRange();
      this.updateShades();
    };
    new ResizeObserver(resizeCallback).observe(this);
    // Also listen on window resize separately because there is some issue
    // with the default resize in Colab when the resources pane is toggled.
    // Some interaction with the iframe causes the resize to not take effect.
    window.addEventListener('resize', () => {
      setTimeout(resizeCallback, 0);
    });
  }

  get cellWidth() {
    return Number(this.dataset['cellWidth'] || 120);
  }

  get maxFolds() {
    return Number(this.dataset['maxFolds'] || 0);
  }

  // This is element that overlays the nav and indicates the extent of what
  // the user is currently looking at.
  private viewElement?: HTMLElement;

  // These shade elements show the parts of the data that have not been loaded.
  private shadeBefore?: HTMLElement;
  private shadeAfter?: HTMLElement;

  // Renders all loaded data.
  private compactTable?: CompactTable;

  // This pane contains hovered content.
  private detailPane?: HTMLElement;

  // This is the detail of the cell that the user is currently hovering over.
  // This allows a deferred decision about whether to apply the hovered content
  // in the detail pane.
  private hoverCellDetail?: HoverCellDetail;

  // This is the absolute index of the column the user last hovered
  // over. Note that hoverCellDetail is in the coordinates of the currently
  // loaded data.
  private hoverIndex?: number;

  // This checkbox indicates whether the compact table is in overflow mode.
  private overflowCheckbox?: HTMLInputElement;

  // This visualizes the multi-dimensional size information.
  private dimNav?: MultiDimNav;

  // Contains both the nav and the compact table.
  private main?: HTMLElement;

  // scrollLeft from the last time a scroll happened.
  private lastScrollLeft = 0;

  // When greater than zero, avoids dispatching request-load events. This is
  // used to scroll without triggering a load. Since we need to dispatch
  // based on the scroll event, it is hard to avoid triggering the logic.
  private suppressLoadRequests = 0;

  static override get shadowInit(): ShadowRootInit | undefined {
    return {mode: 'open', slotAssignment: 'manual'};
  }

  // Returns the implied range indices of items that are currently visible in
  // the compact table. Note that indices are fractional to indicate the table
  // being partially scrolled into an item.
  get visibleRange(): [number, number] {
    const {compactTable, cellWidth} = this;
    if (!compactTable) return [0, 0];
    const {scrollLeft, headerWidth, offsetWidth} = compactTable;

    const loadedBegin = this.loadedRange[0];
    const begin = scrollLeft / cellWidth + loadedBegin;
    const end = begin + (offsetWidth - headerWidth) / cellWidth;
    return [begin, end];
  }

  // Margin in the loaded region at which we trigger a request-load event.
  get loadedMargin() {
    return Number(this.dataset['loadedMargin'] || 0);
  }

  private syncViewWithVisibleRange(): [number, number] {
    const {visibleRange, viewElement} = this;
    if (viewElement) {
      viewElement.dataset['begin'] = String(visibleRange[0]);
      viewElement.dataset['end'] = String(visibleRange[1]);
    }
    return visibleRange;
  }

  private get loadedRange(): [number, number] {
    const loadedRange = (this.dataset['loadedRange'] || '')
      .split(',')
      .map(Number);
    const begin = loadedRange[0] || 0;
    const end = loadedRange[1] || 0;
    return [begin, end];
  }

  private updateShades() {
    const [begin, end] = this.loadedRange;
    const {shadeBefore, shadeAfter, dimNav} = this;
    if (!shadeBefore || !shadeAfter || !dimNav) return;
    shadeBefore.dataset['begin'] = '0';
    shadeBefore.dataset['end'] = String(begin);
    shadeAfter.dataset['begin'] = String(end);
    shadeAfter.dataset['end'] = String(dimNav.totalSize);
    this.classList.toggle('all-loaded', begin === 0 && end >= dimNav.totalSize);
  }

  scrollToIndex(index: number) {
    const tableIndex = index - this.loadedRange[0];
    this.compactTable?.scrollTo({left: tableIndex * this.cellWidth});
    this.syncViewWithVisibleRange();
  }

  private dispatchRequestLoad(centerIndex: number, viewBegin: number) {
    if (this.suppressLoadRequests > 0) {
      this.suppressLoadRequests--;
      return;
    }
    const oneRequestLoad = this.hasAttribute('one-request-load');
    if (oneRequestLoad && this.classList.contains('loading')) return;

    this.dispatchEvent(
      new CustomEvent<RequestLoadDetail>('request-load', {
        detail: {centerIndex, viewBegin},
      }),
    );
    if (oneRequestLoad) {
      this.classList.add('loading');
    }
  }

  private get requestLoadParams(): multiDimTableHandlers.RequestLoadParams {
    return {
      loadedRange: this.loadedRange,
      loadedMargin: this.loadedMargin,
      visibleRange: this.visibleRange,
    };
  }

  private onCompactTableHoverCell(e: Event) {
    if (e instanceof CustomEvent) {
      const {detailPane} = this;
      this.hoverCellDetail = e.detail;

      if (detailPane && !detailPane.classList.contains('pinned')) {
        this.applyHoveredContent();
      }
    }
  }

  private onCompactTableScroll(e: Event) {
    this.lastScrollLeft = this.compactTable?.scrollLeft || 0;
    // We explicitly set scrollLeft when loading since setting
    // `overflow: hidden` does not prevent momentum scrolling in MacOS.
    const {compactTable} = this;
    if (this.classList.contains('loading') && compactTable) {
      compactTable.scrollLeft = this.lastScrollLeft;
      return;
    }

    multiDimTableHandlers.maybeRequestLoadOnScroll(
        this.requestLoadParams,
        this.syncViewWithVisibleRange(),
        this.dimNav,
        this.dispatchRequestLoad.bind(this),
    );
  }

  private onCompactTableClick(e: MouseEvent) {
    if (!e.ctrlKey) {
      // If we are clicking on the same content as is currently pinned,
      // toggle the pinned state. Otherwise, pin the new content.
      const {detailPane, hoverCoords} = this;
      if (multiDimTableHandlers.detailPaneMatchesHoverCoords(
              detailPane, hoverCoords)) {
        this.detailPane?.classList.toggle('pinned');
      } else {
        this.detailPane?.classList.add('pinned');
        this.applyHoveredContent();
      }
    } else if (e.ctrlKey) {
      this.syncOverflowCheckbox();
    }
  }

  focusDataCell(row: number, col: number) {
    row += this.dimNav?.numDims || 0;
    const content = this.compactTable?.getCellAssignedNode(row, col);
    if (content instanceof HTMLElement) {
      this.hoverCellDetail = {row, col, content};
      this.applyHoveredContent();
    }
  }

  // Data coordinates of the cell the user is currently hovering over. This
  // differs from the hover coordinates in the underlying CompactTable because
  // this component adds rows for the dimension indices.
  private get hoverCoords() {
    const {hoverCellDetail, dimNav} = this;
    if (!hoverCellDetail || !dimNav) return;

    // Subtract the number of dimensions from the number of rows.
    const row = hoverCellDetail.row - dimNav.numDims;
    const col = hoverCellDetail.col;

    // If the row is less than zero, it is not a data cell, but rather a
    // dimension index.
    return row >= 0 ? {row, col} : undefined;
  }

  private updateMainHeightCssVar() {
    this.style.setProperty(
        '--main-height', `${this.main?.offsetHeight ?? 0}px`);
  }

  private applyHoveredContent() {
    const {detailPane, compactTable, hoverCoords, dimNav} = this;
    if (!detailPane || !compactTable || !dimNav) {
      return;
    }

    const numDataRows = compactTable.headers.length - dimNav.numDims;
    const activeSlot = multiDimTableHandlers.updateDetailPane(
        this.children, detailPane, hoverCoords, numDataRows);

    // Update CSS variables necessary to size the detail pane correctly.
    if (activeSlot instanceof HTMLElement) {
      // Add one to avoid numerical issues adding an unnecessary scrollbar.
      detailPane.style.setProperty(
          '--active-cell-height', `${activeSlot.offsetHeight}px`);
    }
    this.updateMainHeightCssVar();

    // Update active cell in compact table.
    compactTable.clearCellActive();

    // Only highlight the cell if the user is hovering over a data cell.
    if (hoverCoords) {
      compactTable.setCellActive(
          this.hoverCellDetail?.row || 0,
          this.hoverCellDetail?.col || 0,
      );
    }
  }

  private syncOverflowCheckbox() {
    const {compactTable, overflowCheckbox} = this;
    if (!compactTable || !overflowCheckbox) return;
    overflowCheckbox.checked =
      compactTable.classList.contains('hover-overflow');
  }

  override render() {
    const {shadowRoot} = this;
    if (!shadowRoot) return;
    const existingScrollLeft = this.dimNav?.scrollLeft || 0;
    shadowRoot.textContent = '';

    // Construct info bar that contains the view options.
    const infoBarParts = multiDimTableRender.infoBar(
        this.dataset, this.cellWidth, this.maxFolds);
    shadowRoot.appendChild(infoBarParts.infoBar);
    infoBarParts.overflowCheckbox.addEventListener(
        'change',
        () => {
          this.compactTable?.classList.toggle('hover-overflow');
          this.syncOverflowCheckbox();
        },
    );

    // Render all main data region components.
    const dataRegionParts =
        multiDimTableRender.dataRegionParts(this.dataset, this.maxFolds);
    shadowRoot.appendChild(dataRegionParts.dataRegion);
    Object.assign(this, dataRegionParts);

    // Add listeners to data region components.
    const {dimNav, compactTable} = dataRegionParts;
    compactTable.addEventListener(
        'hover-cell', this.onCompactTableHoverCell.bind(this));
    compactTable.addEventListener(
        'scroll', this.onCompactTableScroll.bind(this));
    compactTable.addEventListener('click', this.onCompactTableClick.bind(this));

    dimNav.addEventListener('change-current', (e) => {
      const {compactTable} = this;
      if (e instanceof CustomEvent && compactTable) {
        const index = (e.detail as ChangeCurrentDetail).index;
        this.scrollToIndex(index);
        multiDimTableHandlers.maybeRequestLoadOnChangeCurrent(
            this.requestLoadParams, index, this.dispatchRequestLoad.bind(this));
      }
    });
    dimNav.scrollLeft = existingScrollLeft;

    dataRegionParts.detailPane.addEventListener('click', (e: MouseEvent) => {
      const {target} = e;
      if (target instanceof HTMLSlotElement) {
        // Handle a click to scroll to a cell based on the detail pane.
        const row = Number(target.dataset['row']);
        const col = Number(target.dataset['col']);
        this.focusDataCell(row, col);
        this.suppressLoadRequests = 1;
        this.scrollToIndex(col + this.loadedRange[0]);
      }
    });

    // Sync necessary state after rendering.
    this.onMutation();
    this.syncViewWithVisibleRange();
    this.updateShades();
  }

  private onMutation() {
    const {compactTable, dimNav} = this;
    if (compactTable && this.isConnected) {
      multiDimTableRender.cloneToCompactTable(
          this.children, this.getAttribute('content-mode'), this.loadedRange,
          compactTable, dimNav);
    }

    // Manually handle assignment into the message slot. We need manual slot
    // assignment so we can control what is assigned into the detail slot.
    const messageElements = this.querySelectorAll('[slot="message"]');
    this.shadowRoot
      ?.querySelector<HTMLSlotElement>('slot[name="message"]')
      ?.assign(...messageElements);
    this.updateMainHeightCssVar();
  }
}

customElements.define('kd-multi-dim-table', MultiDimTable);
