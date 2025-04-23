/**
 * @license
 * Copyright 2024 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import './multi_index_flag';

import {MultiIndexFlag} from './multi_index_flag';
import {html, NanoElement,} from './nano_element';

/**
 * This should match the standard Koda shape type. That is, the length of the
 * outer array represents the total number of dimensions and each inner array
 * represents the sizes of the partitions in that dimension.
 */
type SizeSpec = number[][];

/**
 * Contains sufficient information to render a rectangle that represents a
 * grouping of items that all have the same multi-index prefix. E.g. If we have
 * sizes [1, 2, 3], the node that represents the whole array would have size 6
 * and depth 1.
 */
interface Node {
  size: number;
  depth: number;
  children?: Node[];
}

/**
 * Converts a SizeSpec to a Node tree. This conversion computes the total size
 * of each node in the tree.
 *
 * @param sizes The SizeSpec to convert.
 * @param dim The current dimension.
 * @param bases Array of the base indices in each dimension. bases[i] gives
 *    the first index in dimension i that we have not yet converted.
 * @param index The index in sizes[dim] to convert.
 */
function sizesToNodes(
  sizes: SizeSpec,
  dim: number,
  bases: number[],
  index: number,
): Node {
  const currentSize = sizes[dim]?.[index] || 0;
  if (dim + 1 >= sizes.length) {
    return {size: currentSize, depth: 0};
  } else {
    const children = [];
    for (let i = 0; i < currentSize; i++) {
      children.push(sizesToNodes(sizes, dim + 1, bases, bases[dim] + i));
    }
    bases[dim] += currentSize;
    return {
      size: children.map((x) => x.size).reduce((x, y) => x + y, 0),
      children,
      depth: Math.max(...children.map(x => x.depth)) + 1,
    };
  }
}

/**
 * Renders nodes recursively on the provided CanvasRenderingContext2D. The
 * rendering is done such that adjacent nodes in the same dimension have
 * alternating, contrasting colors and descendants of a node are influenced by
 * its color.
 *
 * The variables prefixed by "lightness" define the color of a node and its
 * descendants. These variables are in the range [0, 2**numDims-1] and are
 * translated to the HSL lightness of the node.
 *
 * @param lightnessLow The lowest lightness index of any descendant.
 * @param lightnessHigh The highest lightness index of any descendant.
 * @param lightnessMax The maximum lightness index of any node.
 */
function renderNode(
  ctx: CanvasRenderingContext2D,
  node: Node | undefined,
  rowHeight: number,
  bounds: DOMRect,
  lightnessLow: number,
  lightnessHigh: number,
  lightnessMax: number,
) {
  // Performance optimization to avoid rendering nodes outside of the canvas.
  if (bounds.right < 0 || bounds.left >= ctx.canvas.width) return;

  // These constants prevents colors from being too high contrast for small
  // numbers of dimensions.
  const lightnessMargin = 2;
  const lightnessOffset = lightnessMargin / 2;

  const children = node?.children;
  let left = bounds.left;
  const childCount = children?.length || node?.size || 0;
  const lightnessMid = Math.floor(0.5 * (lightnessLow + lightnessHigh));
  for (let i = 0; i < childCount; i++) {
    const childNode = children?.[i];
    const childWidth =
      (bounds.width * (childNode?.size || 1)) / (node?.size || 1);

    const lightnessAlpha =
      (lightnessMid + (i % 2) + lightnessOffset) /
      (lightnessMax + lightnessMargin);
    const fillLightness = 0.8 * lightnessAlpha + 0.1;
    ctx.fillStyle = `hsl(210, 40%, ${100 * fillLightness}%)`;
    ctx.fillRect(left, bounds.top, childWidth, rowHeight);

    const childBounds = new DOMRect(
      left,
      bounds.top + rowHeight,
      childWidth,
      rowHeight,
    );

    const childLightnessLow = i % 2 ? lightnessMid + 1 : lightnessLow;
    const childLightnessHigh = i % 2 ? lightnessHigh : lightnessMid;
    renderNode(
      ctx,
      childNode,
      rowHeight,
      childBounds,
      childLightnessLow,
      childLightnessHigh,
      lightnessMax,
    );
    left += childWidth;
  }
}

/**
 * Determines the multi-index of the item that is currenttly under the cursor
 * position.
 *
 * @param node The node in which to identify the multi-index.
 * @param index The item index if we ignore/flatten all dimensions.
 */
function localizeIndex(node: Node, index: number): number[] {
  const children = node.children;
  if (!children) {
    return [index];
  }

  for (let i = 0; i < children.length; i++) {
    if (index < children[i].size) {
      return [i, ...localizeIndex(children[i], index)];
    } else {
      index -= children[i].size;
    }
  }

  return [];
}

// Used in the return type of enumerateSizesSliceAtDepth. This represents a
// range if items in the flattened space. We expect this range to be a subset
// of the items that belong to some node in the tree.
interface SizeSliceEntry {
  index: number; // Index of the node in its parent.
  size: number; // Number of items in the range.
}

/**
 * Generates the sizes of the children of the specified depth in between
 * begin and end flattened indices. If begin and end overlap with a node,
 * only the size of the overlap is yielded. The begin index is inclusive and
 * the end index is exclusive.
 *
 * @param base The accumulated sizes seen so far at the current depth.
 *    Only the first element is used but this is an array so recursive calls
 *    can modify it.
 */
function* childSizeSliceAtDepth(
  node: Node,
  depth: number,
  base: [number],
  begin: number,
  end: number,
): Generator<SizeSliceEntry> {
  const {children} = node;
  if (node.depth === depth) {
    // Unify the two cases of having child nodes and being at a leaf node.
    const deltas = children
      ? children.map((x) => x.size)
      : Array.from<number>({length: node.size}).fill(1);
    const accumulated = [base[0]];
    for (const increment of deltas) {
      accumulated.push((accumulated.at(-1) || 0) + increment);
    }

    // Yield the intersections with child nodes.
    for (let i = 1; i < accumulated.length; i++) {
      const childBegin = accumulated[i - 1];
      const childEnd = accumulated[i];
      if (childBegin >= end) break;

      if (childEnd > begin) {
        const sliceBegin = Math.max(childBegin, begin);
        const sliceEnd = Math.min(childEnd, end);
        yield {
          index: i - 1,
          size: sliceEnd - sliceBegin,
        };
      }
    }
    base[0] += node.size;
  } else if (node.depth > depth) {
    if (!children) return;
    for (let i = 0; i < children.length; i++) {
      yield* childSizeSliceAtDepth(children[i], depth, base, begin, end);
    }
  }
}

/**
 * Details of the change-current event.
 */
export interface ChangeCurrentDetail {
  position?: number[]; // Multi-index of the item
  index: number; // Flat index of the item
}

/**
 * Visualizes a multi-dimensional jagged array in terms of sizes. The sizes
 * are configured through the data-sizes attribute, which should be a JSON
 * array of numbers. Clicking on the visualization dispatches a
 * change-current event with the multi-index (an array of numbers)
 * of the item that the user clicked on.
 *
 * Child elements that contain the data-begin and data-end attributes are
 * positioned to cover that range. These attributes are interpreted as flat
 * item indices.
 *
 * Additional attributes:
 * - data-layout: If this is set to 'small', flags for current and cursor
 *    positions will be smaller.
 */
export class MultiDimNav extends NanoElement {
  // Element that creates all space necessary for items. Flags and slotted
  // elements are positioned within this element.
  private wrapperElement?: HTMLElement;

  // Element where the visualization is rendered. Since browsers have limits
  // on the maximum dimensions of a canvas around ~32K pixels, this canvas
  // is positioned on each render so that it covers the region of the
  // wrapperElement currently visible to the user. This allows us to handle
  // data much larger than the canvas size limit.
  private canvasElement?: HTMLCanvasElement;

  // Flag that represents the multi-index that the user is currently looking
  // at. This should be synchronized with other components that are coupled
  // with this component.
  private currentFlag?: MultiIndexFlag;

  // Flag that represents the multi-index that the user is currently hovering
  // over.
  private cursorFlag?: MultiIndexFlag;

  // Node that represents the multi-dimensional array parsed from the data-sizes
  // attribute.
  private node: Node = {size: 0, depth: 0};

  // Number of dimensions from the last read of data-sizes.
  private numDimsFromSizes = 0;

  // Width of the cells in the last dimension (i.e. the bottom row)
  // of the visualization.
  private bottomCellWidth = 1;

  static override get shadowStyle() {
    return `
    :host {
      --background: var(--kd-multi-dim-nav-background, white);
      --stripe-background: color-mix(in srgb, currentcolor 4%, var(--background));
      background: repeating-linear-gradient(
        45deg, var(--stripe-background), var(--stripe-background) 5px,
        var(--background) 5px, var(--background) 10px);
      cursor: none;
      display: block;
      overflow-x: auto;
      overflow-y: hidden;
      position: relative;
      user-select: none;
    }

    :host(*:hover),
    :host(*[hover]) {
      --kd-multi-dim-nav-internal-cursor-visibility: visible;
      --kd-multi-dim-nav-internal-current-filter: brightness(0.8);
    }

    kd-multi-index-flag {
      height: 100%;
      left: 0;
      position: absolute;
      top: 0;
    }

    [cursor] {
      --kd-multi-index-flag-color: var(--kd-multi-dim-nav-cursor-background, white);
      visibility: var(--kd-multi-dim-nav-internal-cursor-visibility, hidden);
    }

    [current] {
      --kd-multi-index-flag-color: var(--kd-multi-dim-nav-current-background, white);
      visibility: var(--kd-multi-dim-nav-current-visibility, visible);
      filter: var(--kd-multi-dim-nav-internal-current-filter, none);
    }

    canvas {
      left: 0;
      position: absolute;
      top: 0;
      width: 100%;
    }
  `;
  }

  static get observedAttributes() {
    return ['data-sizes', 'data-layout'];
  }

  get numDims() {
    return this.numDimsFromSizes;
  }

  get totalSize() {
    return this.node?.size || 0;
  }

  get rowHeight() {
    return Number(this.dataset['rowHeight'] ?? 30);
  }

  /**
   * Returns a sequence of sizes over flat item indices in the specified
   * dimension that overlap with begin inclusively and end exclusively.
   */
  *sizeSliceInDim(
    dim: number,
    begin: number,
    end: number,
  ): Generator<SizeSliceEntry> {
    yield* childSizeSliceAtDepth(
      this.node,
      this.numDims - dim - 1,
      [0],
      begin,
      end,
    );
  }

  override connectedCallback() {
    super.connectedCallback();

    const {shadowRoot} = this;
    if (!shadowRoot) return;

    this.canvasElement = document.createElement('canvas');

    const flagClasses = html.class(
      this.dataset['layout'] === 'small' ? 'small' : '',
    );
    this.currentFlag = html.tag('kd-multi-index-flag', flagClasses);
    this.cursorFlag = html.tag('kd-multi-index-flag', flagClasses);

    this.currentFlag.setAttribute('current', '');
    this.cursorFlag.setAttribute('cursor', '');

    const slot: HTMLSlotElement = html.tag('slot');
    this.wrapperElement = html.tag(
      'div',
      html.id('wrapper'),
      slot,
      this.currentFlag,
      this.cursorFlag,
    );
    shadowRoot.append(this.canvasElement, this.wrapperElement);

    const repositionObserver = new MutationObserver((records) => {
      for (const target of new Set(records.map((x) => x.target))) {
        if (target instanceof HTMLElement) {
          this.repositionSlottedElement(target);
        }
      }
    });

    // Register slotted elements with the MutationObserver.
    slot.addEventListener('slotchange', () => {
      for (const child of slot.assignedElements()) {
        if (child instanceof HTMLElement) {
          repositionObserver.observe(child, {attributes: true});
          this.repositionSlottedElement(child);
        }
      }
    });

    this.addEventListener('mousemove', (e) => {
      this.moveFlagToClientX(this.cursorFlag, e);
    });

    this.addEventListener('click', (e) => {
      const position = this.moveFlagToClientX(this.currentFlag, e);
      this.dispatchEvent(
        new CustomEvent<ChangeCurrentDetail>('change-current', {
          detail: {
            position,
            index: Math.floor(
              this.clientXToPosition(e.clientX) / this.bottomCellWidth,
            ),
          },
        }),
      );
    });

    // Re-render on scroll or resize since the canvas must always be updated.
    const boundRender = this.render.bind(this);
    this.addEventListener('scroll', boundRender);
    new ResizeObserver(boundRender).observe(this);

    this.render();
  }

  private repositionSlottedElement(target: HTMLElement) {
    target.style.setProperty('top', '0');
    const begin = Number(target.dataset['begin']) || 0;
    const end = Number(target.dataset['end']) || 0;
    target.style.setProperty('position', 'absolute');
    target.style.setProperty(
      'left',
      `${Math.round(this.bottomCellWidth * begin)}px`,
    );
    target.style.setProperty(
      'width',
      `${Math.round(this.bottomCellWidth * (end - begin))}px`,
    );
  }

  /**
   * Moves the flag to the cursor position and returns the multi-index of the
   * item that the cursor is over.
   *
   * @position The x offset from the left-hand side of the wrapperElement.
   */
  private moveFlagToPosition(
    flag: MultiIndexFlag | undefined,
    position: number,
  ): number[] | undefined {
    if (!this.wrapperElement || !flag) return;
    const left = Math.max(
      0,
      Math.min(
        position,
        (this.wrapperElement.getBoundingClientRect().width || 0) - 1,
      ),
    );
    const cursorPosition = localizeIndex(
      this.node,
      Math.floor(left / this.bottomCellWidth),
    );
    flag.style.setProperty('left', `${left}px`);
    flag.setAttribute('data-index', (cursorPosition ?? []).join(','));
    return cursorPosition;
  }

  private clientXToPosition(clientX: number) {
    return clientX - (this.wrapperElement?.getBoundingClientRect()?.left || 0);
  }

  private moveFlagToClientX(flag: MultiIndexFlag | undefined, e: MouseEvent) {
    return this.moveFlagToPosition(flag, this.clientXToPosition(e.clientX));
  }

  moveCurrentFlagToIndex(index: number) {
    this.moveFlagToPosition(this.currentFlag, index * this.bottomCellWidth);
  }

  override attributeChangedCallback(
    name: string,
    oldValue: string,
    newValue: string,
  ) {
    if (name === 'data-sizes') {
      const sizes = JSON.parse(newValue || '[]') as SizeSpec;
      this.numDimsFromSizes = sizes.length;
      this.node = sizesToNodes(
        sizes,
        0,
        Array.from<number>({length: sizes.length}).fill(0),
        0,
      );
      super.attributeChangedCallback(name, oldValue, newValue);
    }
  }

  override render() {
    const {canvasElement: canvas, node, rowHeight} = this;
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const elementWidth = Math.round(this.getBoundingClientRect().width);
    this.bottomCellWidth = Math.max(
      Math.floor(elementWidth / Math.max(node.size, 1)),
      1,
    );

    const width = this.bottomCellWidth * node.size;
    const height = rowHeight * (node.depth + 1);
    this.wrapperElement?.style?.setProperty('height', `${height}px`);
    this.wrapperElement?.style?.setProperty('width', `${width}px`);

    // In the lines below, we add a margin of the element width on each side
    // to reduce the chance of seeing unrendered areas when srolling.
    // Note that the width and height properties on the canvas are distinct
    // from the CSS width and height and must be set separately.
    const canvasLeft = -elementWidth + this.scrollLeft;
    // If we do not take this min, the absolutely positioned canvas
    // will extend the width of this element and the user would never be able to
    // scroll to the end.
    canvas.width =
      Math.min(2 * elementWidth, width - this.scrollLeft) + elementWidth;
    canvas.height = height;
    canvas.style.setProperty('left', `${canvasLeft}px`);
    canvas.style.setProperty('width', `${canvas.width}px`);
    canvas.style.setProperty('height', `${height}px`);
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const maxLightness = 2 ** this.numDims - 1;

    renderNode(
      ctx,
      node,
      rowHeight,
      new DOMRect(-this.scrollLeft + elementWidth, 0, width, rowHeight),
      0,
      maxLightness,
      maxLightness,
    );

    // Move current flag to beginning if there is a mismatch in the number of
    // dimensions between the current flag and the depth of the root node.
    if (this.currentFlag?.index?.length !== node.depth) {
      this.moveFlagToPosition(this.currentFlag, 0);
    }
  }
}

customElements.define('kd-multi-dim-nav', MultiDimNav);
