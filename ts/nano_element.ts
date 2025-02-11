/**
 * @license
 * Copyright 2024 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Defines a base clase for custom elements that is simpler than LitElement for
 * high frequency use in Colab cells. E.g. We wouldn't want to inline LitElement
 * in each of 100+ cells. This class has the following features:
 * - Automatic shadow root attachment that can be overridden with shadowInit.
 * - Automatic shadow root stylesheet adoption from the shadowStyle property.
 * - Automatic render in connectedCallback and attributeChangedCallback.
 *
 * See https://developer.mozilla.org/en-US/docs/Web/API/Web_components/Using_custom_elements
 * for details on standard custom element lifecycle callbacks.
 */
export class NanoElement extends HTMLElement {
  static get shadowInit(): ShadowRootInit | undefined {
    return {mode: 'open'};
  }

  static get shadowStyle(): string {
    return '';
  }

  // Allows referencing static properties so that they can be overridden
  // by subclasses.
  private get nanoElementRef() {
    return this.constructor as typeof NanoElement;
  }

  constructor() {
    super();
    const {shadowInit} = this.nanoElementRef;
    if (shadowInit) {
      this.attachShadow(shadowInit);
    }
  }

  attributeChangedCallback(name: string, oldValue: string, newValue: string) {
    if (!this.isConnected) return;
    this.render();
  }

  connectedCallback() {
    const style = new CSSStyleSheet();
    style.replaceSync(this.nanoElementRef.shadowStyle);
    const {shadowRoot} = this;
    if (shadowRoot) {
      shadowRoot.adoptedStyleSheets = [style];
    }
    this.render();
  }

  render() {}
}

/**
 * Wrapper around strings that marks them as class names for use in Tag. Strings
 * passed to this wrapper are added to the class attribute of the constructed
 * HTML element.
 */
class ClassList {
  classList: string[];

  constructor(...classList: string[]) {
    this.classList = classList;
  }
}

/**
 * Wrapper around a string to indicate that it is an id.
 */
class Id {
  constructor(public id: string) {}
}

/**
 * Wrapper around a data attribute.
 */
class Data {
  constructor(
    public key: string,
    public value: string | undefined,
  ) {}
}

/**
 * Wrapper around a name attribute.
 */
class Name {
  constructor(public name: string) {}
}

// Union of types that can be used as content for a Tag.
type Content = ClassList | string | Tag | HTMLElement | Id | Data | Name;

/**
 * A simple builder pattern to create DOM elements.
 *
 * Example usage:
 *   new Tag('div', 'cool content',
 *            new Tag('span', 'more content', new
 * ClassList('emphasis'))).render()
 *
 * This renders to the following HTML:
 *   <div>cool content<span class="emphasis">more content</span></div>
 *
 * This would have required the following JS code:
 *   const div = document.createElement('div');
 *   div.appendChild(document.createTextNode('cool content'));
 *   const span = document.createElement('span');
 *   span.appendChild(document.createTextNode('more content'));
 *   span.classList.add('emphasis');
 *   div.appendChild(span);
 */
class Tag {
  content: Content[];

  constructor(
    public tag: string,
    ...content: Content[]
  ) {
    this.content = content;
  }

  render() {
    const result = document.createElement(this.tag);
    for (const child of this.content) {
      if (child instanceof ClassList) {
        for (const name of child.classList) {
          // Adding empty strings to a classList throws an error so we need
          // to filter them out.
          if (!name) continue;
          result.classList.add(name);
        }
      } else if (child instanceof Tag) {
        result.appendChild(child.render());
      } else if (child instanceof HTMLElement) {
        result.appendChild(child);
      } else if (child instanceof Id) {
        result.id = child.id;
      } else if (child instanceof Data) {
        result.dataset[child.key] = child.value;
      } else if (child instanceof Name) {
        result.setAttribute('name', child.name);
      } else {
        result.appendChild(document.createTextNode(child));
      }
    }
    return result;
  }
}

/**
 * Convenience bundle for html construction utilities. The example in the
 * Tag docstring can be written as:
 *   html.tag('div', 'cool content',
 *          html.tag('span', 'more content', html.class('emphasis')))
 */
export const html = {
  tag: <T extends HTMLElement>(tag: string, ...content: Content[]) =>
    new Tag(tag, ...content).render() as T,
  class: (...names: string[]) => new ClassList(...names),
  data: (key: string, value: string | undefined) => new Data(key, value),
  id: (id: string) => new Id(id),
  name: (name: string) => new Name(name),
} as const;
