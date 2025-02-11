/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * This custom element makes it easier to directly listen to and interpret
 * events from the DOM in python. The Colab-standard js.AddEventListener only
 * sends over minimal information from events by default (e.g. only
 * the key property for KeyboardEvents). This custom element enables
 * declarative forwarding of additional information so that less JS is
 * necessary.
 *
 * This custom element dispatches CustomEvents for each event registered in
 * the `events` attribute. The `events` attribute is a comma-separated list of
 * standard event names. The new CustomEvent dispatched has an event name
 * that is the original event name prefixed by the value of the `prefix`
 * attribute. `prefix` defaults to `kd-event-reinterpret`. The `detail` is an
 * object that contains the following keys:
 *  - dataset: The dataset property of the composed target.
 *  - class: The class of the composed target.
 *  - tagName: The tag name of the composed target.
 *  - originalDetail: The detail of the reinterpreted event.
 * In the above, "composed target" refers to the first element in the composed
 * path of the original event.
 *
 * Example usage:
 *   <kd-event-reinterpret events="click" prefix="my-handler">
 *     <!-- additional content here -->
 *   </kd-event-reinterpret>
 *   With this usage in HTML, a python handler such as my_handler_py would
 *   have more context when it is called as an event listener:
 *     js.AddEventListener('document', 'my-handler-click', my_handler_py)
 */
export class EventReinterpret extends HTMLElement {
  connectedCallback() {
    const eventNames = this.getAttribute('events')?.split(',') || [];
    const prefix = this.getAttribute('prefix') || 'kd-event-reinterpret';
    for (const eventName of eventNames) {
      this.addEventListener(eventName, (e: Event) => {
        const target = e.composedPath()[0];
        if (!(target instanceof HTMLElement)) {
          return;
        }
        this.dispatchEvent(new CustomEvent(`${prefix}-${eventName}`, {
          // Convert datatypes to something JSON serializable.
          detail: JSON.parse(JSON.stringify({
            'dataset': target.dataset,
            'classList': Array.from(target.classList),
            'tagName': target.tagName,
            'originalDetail': e instanceof CustomEvent ? e.detail : undefined,
          })),
          bubbles: true
        }));
      });
    }
  }
}

customElements.define('kd-event-reinterpret', EventReinterpret);
