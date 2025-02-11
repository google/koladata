/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Returns shadow CSS for MultiDimTable.
 */
export function shadow() {
  const detailWidth = 'var(--kd-multi-dim-table-detail-width, 30%)';
  return `
    :host {
      --background: var(--kd-multi-dim-table-background, white);
      /* This is used as a margin to prevent the detail outline from being clipped. */
      --detail-outline-width: var(--kd-multi-dim-table-detail-outline-width, 2px);
      --kd-compact-table-background: var(--background);
      --kd-compact-table-td-width: 120px;
      --kd-multi-dim-nav-background: var(--background);
      --kd-multi-dim-nav-current-visibility: hidden;
      --kd-multi-dim-nav-cursor-background: var(--background);
      --kd-multi-dim-nav-current-background: color-mix(in srgb, var(--background), orange 25%);
      background: var(--background);
      display: inline-block;
      position: relative;
    }

    :host([hover-col]) {
      --kd-multi-dim-nav-current-visibility: visible;
    }

    :host(.loading) {
      filter: brightness(0.9) saturate(0);
      pointer-events: none;
    }

    :host(.all-loaded) kd-multi-dim-nav {
      display: none;
    }

    :host(.clickable-dim-index) .dim-index {
      cursor: pointer;
      text-decoration: underline;
    }

    #data-region {
      display: flex;
      margin-right: var(--detail-outline-width);
      position: relative;
    }

    #main {
      display: var(--kd-multi-dim-table-main-display, flex);
      flex-direction: column;
      position: relative;
      width: calc(100% - ${detailWidth});
    }

    #nav {
      position: sticky;
      top: 0;
      width: 100%;
      z-index: 1;
    }

    #view,
    .shade {
      box-sizing: border-box;
      height: 100%;
      position: absolute;
    }

    #view {
      background: color-mix(in srgb, var(--background) 50%, transparent);
      border: 2px solid var(--background);
    }

    #view-options {
      box-sizing: border-box;
      display: var(--kd-multi-dim-table-view-options-display, flex);
      flex-wrap: wrap;
      gap: 12px;
      justify-content: flex-end;
      /* Keeps view options aligned with the detail pane when possible. */
      min-width: var(--kd-multi-dim-table-detail-width, 30%);
      padding: 4px 8px;
      row-gap: 0;
    }

    #view-options span {
      white-space: nowrap;
    }

    #info-bar {
      align-items: flex-end;
      display: flex;
      justify-content: space-between;
      margin-right: var(--detail-outline-width);
    }

    #message-wrapper {
      color: gray;
      flex: 1;
    }

    .shade {
      background: gray;
      mix-blend-mode: saturation;
    }

    #detail {
      --detail-border-width: 1px;
      border: var(--detail-border-width) solid lightgray;
      box-sizing: border-box;
      flex: 1 1;
      /* Keep height smaller than user-specified height always. When possible,
         use the height of the main element or the active cell. We add double
         the border width to avoid a scrollbar when the height is exactly
         the active cell height. */
      max-height: min(
          max(var(--main-height, 0px),
              calc(var(--active-cell-height, 0px) + 2 * var(--detail-border-width))),
          var(--kd-multi-dim-table-detail-height, 400px));
      overflow: auto;
      position: sticky;
      top: 0;
      white-space: pre-wrap;
      width: ${detailWidth};

      &:empty::before {
        align-items: center;
        box-sizing: border-box;
        color: gray;
        content: "Hover on cell to show contents here and click to toggle pinning.";
        display: flex;
        font-size: 12px;
        height: 100%;
        justify-content: center;
        padding: 16px;
        position: absolute;
        width: 100%;
      }

      slot {
        cursor: pointer;
        display: block;
        padding: 16px;
        position: relative;
      }

      slot::slotted(*) {
        cursor: default;
      }

      /* No pointer when there is only one element. */
      slot:first-child:last-child {
        cursor: default;
      }

      slot + slot {
        border-top: 1px solid lightgray;
      }

      slot.active {
        outline: var(--detail-outline-width) solid orange;
        outline-offset: calc(-1 * var(--detail-outline-width));
        z-index: 1;
      }

      slot::before {
        border-radius: 10px;
        content: '';
        display: block;
        height: 9px;
        left: 4px;
        position: absolute;
        top: 4px;
        width: 9px;
      }

      &.pinned slot.active::before {
        background: orange;
      }
    }

    kd-compact-table {
      box-sizing: border-box;
      flex-grow: 1;
      overflow-x: auto;
      overflow-y: hidden;
      /* Keep the scroll bar away from cells. */
      padding-bottom: var(--kd-compact-table-padding-bottom, 16px);
      position: relative;
      width: 100%;
      z-index: 0;
    }

    select {
      background: var(--background);
      color: currentcolor;
    }

    input {
      margin-right: 0;
      vertical-align: bottom;
    }
  `;
}
