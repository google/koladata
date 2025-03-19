/**
 * @license
 * Copyright 2024 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import {NanoElement} from './nano_element';

/**
 * A flag that visualizes a multi-index vertically. The indcies are configured
 * through the data-index attribute, which should be a comma separate list of
 * indices. Indices are rendered equally spaced from each other.
 */
export class MultiIndexFlag extends NanoElement {
  static override get shadowStyle() {
    const flagColor = 'var(--kd-multi-index-flag-color, white)';
    return `
      :host {
        border-left: 2px solid ${flagColor};
        display: flex;
        flex-direction: column;
        height: 100%;
        justify-content: space-around;
        position: absolute;
        top: 0;
      }

      div {
        background: ${flagColor};
        border-left: 0;
        border-radius: 0 2px 2px 0;
        font-weight: bold;
        min-width: 20px;
        padding: 2px 4px;
        text-align: right;
      }

      :host(.small) div {
        font-size: 10px;
        font-weight: normal;
        line-height: 12px;
        min-width: 10px;
      }

      div:empty {
        visibility: hidden;
      }
    `;
  }

  static get observedAttributes() {
    return ['data-index'];
  }

  get index(): string[] {
    return this.dataset['index']?.split(',') ?? [];
  }

  override render() {
    const {shadowRoot} = this;
    if (shadowRoot) {
      shadowRoot.textContent = '';
      for (const i of this.index) {
        const div = document.createElement('div');
        div.textContent = i;
        shadowRoot.appendChild(div);
      }
    }
  }
}

customElements.define('kd-multi-index-flag', MultiIndexFlag);
