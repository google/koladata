# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Koda visualization library."""

import dataclasses
import enum
import functools
import html
import json
import os.path
import sys
import textwrap
from typing import Any as _Any
import uuid

from arolla import arolla
import IPython
from IPython.core import interactiveshell
from koladata import kd


_COLAB_REQUIRED_MSG = (
    'Koda visualization extensions are only supported in Colab.')


@functools.cache
def _colab_frontend():
  return None


def _get_cell_id(info: interactiveshell.ExecutionInfo) -> str:
  if info.cell_id is not None:
    return info.cell_id
  # Note that the current version of IPython does not support cell_id yet,
  # so we fallback to Colab's running cell id internally. Externally, we
  # will just return an empty string, which means any newly outputted
  # visualization will cause all previous ones to no longer be interactive
  # until IPython supports cell_id.
  colab_frontend = _colab_frontend()
  if colab_frontend is None:
    return ''
  return colab_frontend.GetRunningCellId()


@functools.cache
def _colab_output():
  try:
    # pylint: disable=g-import-not-at-top
    from google.colab import output
    # pylint: enable=g-import-not-at-top
  except ImportError as e:
    raise ImportError(_COLAB_REQUIRED_MSG) from e
  return output


@functools.cache
def _colab_js_builder():
  try:
    # pylint: disable=g-import-not-at-top
    from google.colab.output import _js_builder as js_builder
    # pylint: enable=g-import-not-at-top
  except ImportError as e:
    raise ImportError(_COLAB_REQUIRED_MSG) from e
  return js_builder


@functools.cache
def _colab_message():
  try:
    # pylint: disable=g-import-not-at-top
    from google.colab.output import _message as message
    # pylint: enable=g-import-not-at-top
  except ImportError as e:
    raise ImportError(_COLAB_REQUIRED_MSG) from e
  return message


@functools.cache
def _colab_publish():
  try:
    # pylint: disable=g-import-not-at-top
    from google.colab.output import _publish as publish
    # pylint: enable=g-import-not-at-top
  except ImportError as e:
    raise ImportError(_COLAB_REQUIRED_MSG) from e
  return publish


@functools.cache
def _js_eval_global():
  """Empehermal version of js that is not stored in the output cell."""
  return _colab_js_builder().Js(mode=_colab_js_builder().EVAL)


@functools.cache
def _js_global():
  """Persisted version of js that is is saved in the output cell."""
  return _colab_js_builder().js_global


kdi = kd.eager
kde = kd.lazy
DataSliceOrExpr = kd.types.DataSlice | arolla.Expr


class AccessType(enum.Enum):
  """Types of accesses that can appear in an access path."""
  FLAT_SLICE_INDEX = 'flat-slice-index'
  SCHEMA_ATTR = 'schema-attr'
  DICT_KEY_INDEX = 'dict-key-index'
  DICT_VALUE_INDEX = 'dict-value-index'
  LIST_INDEX = 'list-index'
  GET_SCHEMA = 'get-schema'
  ITEM_SCHEMA = 'item-schema'
  KEY_SCHEMA = 'key-schema'
  VALUE_SCHEMA = 'value-schema'
  MULTI_SLICE_INDEX = 'multi-slice-index'


class DescendMode(enum.Enum):
  VALUE = 'value'
  ATTR = 'attr'


def _load_koda_visualization_library() -> str:
  """Loads the full koda webcomponents library."""
  try:
    # Bazel build mode.
    from python.runfiles import runfiles  # pylint: disable=g-import-not-at-top

    js_filename = runfiles.Create().Rlocation(
        '_main/ts/data_slice_webcomponents.js'
    )
  except ImportError:
    # PyPI package mode.
    js_filename = os.path.join(
        os.path.dirname(sys.modules[__name__].__file__),
        'data_slice_webcomponents.js',
    )
  with open(js_filename, 'r') as f:
    return f.read()


def _publish_koda_visualization_library():
  """Publishes the full koda webcomponents library to a colab cell."""
  _colab_publish().javascript(_load_koda_visualization_library())


def _jagged_array_sizes_to_py(
    shape: kd.types.JaggedShape,
) -> list[list[int]]:
  return [
      kd.eval(kde.shapes.dim_sizes(shape, rank)).to_py()
      for rank in range(kd.eval(kde.shapes.rank(shape)))
  ]


_DEFAULT_UNBOUNDED_TYPE_MAX_LEN = 256
_DEFAULT_NUM_ITEMS = 48
_DEFAULT_ATTR_LIMIT = 20


# If this is an int, it is interpreted as pixels. If it is a string, it is
# interpreted as a CSS length and can support percentages (e.g. 40%).
_CssLength = int | str

# Type that describes how to get to a particular value in a DataSlice.
_AccessPath = list[dict[str, str|list[int]]]


@dataclasses.dataclass
class DataSliceVisOptions:
  """Options for visualizing a DataSlice."""
  # Size of the window of data to show.
  num_items: int = _DEFAULT_NUM_ITEMS
  # Maximum length of unbounded types such as strings and bytes.
  unbounded_type_max_len: int = _DEFAULT_UNBOUNDED_TYPE_MAX_LEN
  # Width of the detail pane in pixels.
  detail_width: _CssLength | None = None
  # Height of the detail pane in pixels.
  detail_height: _CssLength | None = 300
  # Maximum number of attributes to show by default.
  attr_limit: int | None = _DEFAULT_ATTR_LIMIT
  # Item limit for nested parts of an item passed to the repr
  item_limit: int | None = 20


def _format_item_html(
    item: kd.types.DataItem,
    truncate_unbounded_types: bool = True,
    options: DataSliceVisOptions | None = None) -> str:
  """Returns an html representation of a DataItem."""
  options = options or DataSliceVisOptions()
  unbounded_type_max_len = options.unbounded_type_max_len
  dtype = item.get_dtype()

  if not dtype.is_empty():
    if dtype == kd.FLOAT32 or dtype == kd.FLOAT64:
      # We can just use str here because we know item is a float.
      return f'<div class="always-elide">{str(item)}</div>'
    elif ((dtype == kd.STRING or dtype == kd.BYTES)
          and not truncate_unbounded_types):
      unbounded_type_max_len = -1

  detail_pane_str = item._repr_with_params(  # pylint: disable=protected-access
      format_html=True,
      item_limit=options.item_limit,
      depth=2,
      unbounded_type_max_len=unbounded_type_max_len)

  # Unescape whitespace for easier viewing in the detail pane.
  if dtype == kd.STRING:
    detail_pane_str = detail_pane_str.replace('\\n', '\n').replace('\\t', '\t')
  return detail_pane_str


def _format_data_item(
    item: kd.types.DataItem,
    additional_access: list[str] | None = None,
    truncate_unbounded_types: bool = True,
    options: DataSliceVisOptions | None = None) -> str:
  """Returns the top-level html representation of a DataItem for use in table.

  Args:
    item: The DataItem to format.
    additional_access: The HTML string from C++ contains all accesses known
      to the DataItem on which it is invoked. However, here in python, we also
      make some accesses. For example, when we show a DataSlice, the HTML for
      each index is generate separately. This argument allows additional
      accesses to be wrapped around the HTML string. These should be a complete
      HTML key-value pair, e.g. 'list-index="1"'.
    truncate_unbounded_types: When True, unbounded types such as strings and
      bytes are truncated to _DEFAULT_UNBOUNDED_TYPE_MAX_LEN. Otherwise, they
      are shown in full.
    options: User-facing view options.
  """
  detail_pane_str = _format_item_html(item, truncate_unbounded_types, options)
  # Wrap with any necessary additional accesses.
  if additional_access:
    for access in additional_access:
      detail_pane_str = f'<div {access}>{detail_pane_str}</div>'
  # The additional div on the outside cleanly represents the slotted element.
  # I.e. the detail pane is content and not the slotted element. This simplifies
  # querySelector on the cell since querySelector ignores the element it is
  # called on.
  return f'<div><div class="detail-pane">{detail_pane_str}</div></div>'


@dataclasses.dataclass
class _DataSliceTableData:
  headers: str
  items_begin: int
  items_end: int
  sizes: list[list[int]]
  data: str
  header_classes: str
  footer: str


def _is_clickable(ds: kd.types.DataSlice) -> bool:
  """Returns True if `ds` has only entities objects or lists."""
  schema = ds.get_schema()
  return (schema.is_entity_schema() or schema.is_list_schema()) or (
      schema == kd.OBJECT
      and not ds.is_empty()
      and kd.full_equal(kd.has(ds), kd.has_entity(ds) | kd.has_list(ds))
  )


def _focus_data_cell(context: _Any, instance_id: str):
  """Focuses the data cell of the table.

  Args:
    context: The Colab JS context. This is Any because we can not directly
        depend on the Colab JS library.
    instance_id: The id of the table.
  """
  table_elem = context.document.querySelector(
      f'#{instance_id} kd-multi-dim-table')
  # This is run in a setTimeout to allow the table to render.
  context.setTimeout(table_elem.focusDataCell.bind(table_elem, 0, 0), 0)


def _create_data_slice_table_data(
    ds: kd.types.DataSlice, items_begin: int = 0,
    options: DataSliceVisOptions | None = None,
) -> _DataSliceTableData:
  """Creates the data for a visualization table."""
  options = options or DataSliceVisOptions()
  # Message containing metadata about the DataSlice, which is generated before
  # we slice the DataSlice to the desired range.
  schema_html = ds.get_schema()._repr_with_params(  # pylint: disable=protected-access
      format_html=True, depth=4,
      unbounded_type_max_len=options.unbounded_type_max_len)
  message = (
      f'size: {ds.get_size()}, ndims: {ds.get_ndim()}, '
      f'schema: <span {AccessType.GET_SCHEMA.value}="">{schema_html}</span>')

  sizes = (
      _jagged_array_sizes_to_py(ds.get_shape()) if ds.get_ndim() != 0 else [[1]]
  )
  num_items = options.num_items
  items_begin = max(min(items_begin, ds.get_size() - num_items), 0)
  items_end = min(items_begin + num_items, ds.get_size())
  flat_slice_access = (
      lambda i: f'{AccessType.FLAT_SLICE_INDEX.value}={i+items_begin}')

  footer = ''
  clickable_headers = []
  headers = '[values]'
  if isinstance(ds, kd.types.DataItem):
    # Allow unbounded types to be shown in full for DataItems.
    joined_data = _format_data_item(
        ds, truncate_unbounded_types=False, options=options)
  else:
    all_data = []
    # Decide attributes to show based on the entire DataSlice rather than items
    # in the selected range.
    all_attrs = (
        sorted(ds.get_attr_names(intersection=False))
        if ds.get_bag() is not None and ds.is_entity()
        else []
    )
    attrs_to_show = all_attrs[0:options.attr_limit]
    ds = kdi.at(ds.flatten(), kdi.range(items_begin, items_end))

    if attrs_to_show:
      headers = ','.join(attrs_to_show)
      for header in attrs_to_show:
        ds_attr = kdi.maybe(ds, header)
        attr_access = f'{AccessType.SCHEMA_ATTR.value}={html.escape(header)}'

        data_items = []
        for i, v in enumerate(ds_attr.L):
          data_items.append(
              _format_data_item(
                  v, [flat_slice_access(i), attr_access], options=options)
          )
        data = ''.join(data_items)

        # Add footer about hidden attrs if applicable.
        if len(all_attrs) > len(attrs_to_show):
          footer = f"""<div>
            Showing first {len(attrs_to_show)} of {len(all_attrs)} attributes.
            <button class="kernel-only load-all-attrs">Load All</button>
          </div>
          """

        # Detect whether the attr is clickable.
        if _is_clickable(ds_attr):
          clickable_headers.append(header)

        all_data.append(data)
    else:
      data_items = []
      for i, v in enumerate(ds.L):
        data_items.append(
            _format_data_item(v, [flat_slice_access(i)], options=options)
        )
      all_data = ''.join(data_items)

    joined_data = ''.join(all_data)

  multiline_attr = 'multi-line' if '\n' in message else ''
  joined_data += (
      f'<div slot="message" {multiline_attr}>{message}</div></div>')
  return _DataSliceTableData(
      headers, items_begin, items_end, sizes, joined_data,
      ' '.join(f'{x}:clickable' for x in clickable_headers), footer)


@dataclasses.dataclass
class _BreadcrumbEntry:
  ds: kd.types.DataSlice
  name: str


class _DataSliceViewState:
  """Kernel-side state for the DataSlice visualization."""

  def __init__(
      self, ds: kd.types.DataSlice, instance_id: str,
      options: DataSliceVisOptions | None = None,
  ):
    self.ds = ds
    self.instance_id = instance_id
    elem = _js_eval_global().document.querySelector(f'#{instance_id}')
    self.footer_elem = elem.querySelector('#footer')
    self.table_elem = elem.querySelector('kd-multi-dim-table')
    self.breadcrumb_elem = elem.querySelector('.breadcrumb')
    self.items_begin = 0
    self.breadcrumb_entries = [_BreadcrumbEntry(self.ds, '[root]')]
    self.options = options or DataSliceVisOptions()

  def load_data(
      self, items_center: int, view_begin: int):
    """Updates a multi-dim-table with new data after a request-load event."""
    # Note that we may reload half the items, but this is simpler for now.
    # We currently do not expect the frontend to cache multiple blocks
    # (which may not be contiguous). We want the loaded window to contain half
    # of the original data because we don't want the user to need to wait for
    # a load if they want to scroll back a little bit.
    self.items_begin = max(items_center - self.options.num_items // 2, 0)
    self.render_table(view_begin)

  def render_table(self, view_begin: int|None = None):
    """Updates data in the kd-multi-dim-table based on current state."""
    table_elem = self.table_elem
    new_data = _create_data_slice_table_data(
        self.ds, items_begin=self.items_begin,
        options=self.options,
    )

    # Sync the data-item attribute with whether the DataSlice is a DataItem.
    if isinstance(self.ds, kd.types.DataItem):
      table_elem.setAttribute('data-item', '')
    else:
      table_elem.removeAttribute('data-item')

    table_elem.innerHTML = ''
    table_elem.setAttribute('data-loaded-range',
                            f'{new_data.items_begin},{new_data.items_end}')
    table_elem.setAttribute('data-headers', new_data.headers)
    table_elem.setAttribute('data-sizes', json.dumps(new_data.sizes))
    table_elem.setAttribute('data-header-classes', new_data.header_classes)
    table_elem.innerHTML = new_data.data
    self.footer_elem.innerHTML = new_data.footer

    # We defer this invocation of scrollToIndex to the next JS event loop.
    # This is because the table may not be rendered yet.
    scroll_to_index = view_begin or self.items_begin
    _js_eval_global().setTimeout(
        table_elem.scrollToIndex.bind(table_elem, scroll_to_index), 0)

    if isinstance(self.ds, kd.types.DataItem):
      _focus_data_cell(_js_eval_global(), self.instance_id)

    table_elem.classList.remove('loading')

  def render_breadcrumb(self) -> None:
    """Renders the breadcrumb for a DataSlice."""
    breadcrumb_entries = self.breadcrumb_entries
    parts = []
    for i, entry in enumerate(breadcrumb_entries):
      parts.append(
          f'<span class="crumb" data-ds-index="{i}">{entry.name}</span>')
    self.breadcrumb_elem.innerHTML = ''.join(parts) if len(parts) > 1 else ''

  def _update_ds(self, next_ds: kd.types.DataSlice):
    if self.ds.get_ndim() != next_ds.get_ndim():
      self.items_begin = 0
    self.ds = next_ds

  def _start_loading(self):
    self.table_elem.classList.add('loading')

  def _end_loading(self):
    self.table_elem.classList.remove('loading')

  def refresh(self):
    self._start_loading()
    self.render_breadcrumb()
    self.render_table()
    self._end_loading()

  def descend_into_attr(self, header: str):
    """Handles a click on a header that represents an expandable attr."""
    if kdi.is_primitive(self.ds):
      return
    next_ds = kdi.maybe(self.ds, header)

    # Need to check clickability here because all header clicks will call
    # this method.
    if not _is_clickable(next_ds):
      return

    crumb_name = header
    if next_ds.is_list():
      next_ds = next_ds[:]
      crumb_name += '[:]'

    self._update_ds(next_ds)
    self.breadcrumb_entries.append(_BreadcrumbEntry(next_ds, '.' + crumb_name))
    self.refresh()

  def _apply_access_path(self, access_path: list[dict[str, str|list[int]]]):
    """Executes the access path and returns resulting DataSlice and crumbs."""
    next_ds = self.ds
    crumbs = []
    for entry in access_path:
      value = entry['value']
      match entry['type']:
        case AccessType.FLAT_SLICE_INDEX.value:
          if not isinstance(next_ds, kd.types.DataItem):
            next_ds = next_ds.take(int(value))
            crumbs.append(f'.flatten().S[{value}]')
        case AccessType.SCHEMA_ATTR.value:
          next_ds = next_ds.get_attr(value)
          crumbs.append(f'.{value}')
        case AccessType.DICT_KEY_INDEX.value:
          next_ds = next_ds.get_keys().take(int(value))
          crumbs.append(f'.get_keys().S[{value}]')
        case AccessType.DICT_VALUE_INDEX.value:
          next_ds = next_ds.get_values().take(int(value))
          crumbs.append(f'.get_values().S[{value}]')
        case AccessType.LIST_INDEX.value:
          next_ds = next_ds[int(value)]
          crumbs.append(f'[{value}]')
        case AccessType.GET_SCHEMA.value:
          next_ds = next_ds.get_schema()
          crumbs.append('.get_schema()')
        case AccessType.ITEM_SCHEMA.value:
          next_ds = next_ds.get_item_schema()
          crumbs.append('.get_item_schema()')
        case AccessType.KEY_SCHEMA.value:
          next_ds = next_ds.get_key_schema()
          crumbs.append('.get_key_schema()')
        case AccessType.VALUE_SCHEMA.value:
          next_ds = next_ds.get_value_schema()
          crumbs.append('.get_value_schema()')
        case AccessType.MULTI_SLICE_INDEX.value:
          joined_multi_index = ','.join(
              str(value[i]) if i < len(value) else ':'
              for i in range(next_ds.get_ndim()))
          next_ds = kd.subslice(next_ds, *value, ...)
          crumbs.append(f'.S[{joined_multi_index}]')
    return next_ds, crumbs

  def descend_into_access_path(
      self, access_path: list[dict[str, str|list[int]]], mode: DescendMode):
    """Handles a click on a header that represents an expandable attr.

    Args:
      access_path: A list of dicts that encode each DataSlice access. It has
          the shape {'type': '<access-type>', 'value': '<access-value>'}.
          'access-type' is one of the possible string values in AccessType.
      mode: Determines additional actions after the data slice is accessed.
          If this is ATTR and the DataSlice is a list, then the list is
          exploded. Otherwise, the DataSlice is rendered as is.
    """
    if not access_path:
      return
    next_ds, crumbs = self._apply_access_path(access_path)

    if mode == DescendMode.ATTR and next_ds.is_list():
      crumbs.append('[:]')
      next_ds = next_ds[:]

    self._update_ds(next_ds)
    self.breadcrumb_entries.append(_BreadcrumbEntry(next_ds, ''.join(crumbs)))
    self.refresh()

  def return_to_crumb(self, crumb_index: int):
    self._update_ds(self.breadcrumb_entries[crumb_index].ds)
    self.breadcrumb_entries = self.breadcrumb_entries[:crumb_index+1]
    self.refresh()

  # Interprets the request-load event from the frontend.
  def _handle_request_load(self, detail):
    self.load_data(
        items_center=int(detail['centerIndex']),
        view_begin=int(detail['viewBegin']),
    )

  def _handle_ds_vis_click(self, detail):
    """Interprets the ds-vis-click event from the frontend."""
    # Note that dataset converts dash separated names to camel case by
    # browser convention.
    dataset = detail.get('dataset', {})
    # Handle click on a header.
    if 'header' in dataset:
      self.descend_into_attr(dataset['header'])
    # Handle a click on the breadcrumb.
    elif 'dsIndex' in dataset:
      self.return_to_crumb(int(dataset['dsIndex']))
    # Handle click on load all attrs button.
    elif 'load-all-attrs' in detail.get('classList', []):
      self.options.attr_limit = None
      self.refresh()
    # Click on dim index.
    elif 'dim-index' in detail.get('classList', []):
      self._handle_dim_index_click(
          int(dataset['dimension']), int(dataset['index']))

  def _handle_expand_detail(self, detail):
    """Replaces an element with fully expanded version."""
    self._start_loading()

    original_detail = detail.get('originalDetail', {})
    cell_index = int(original_detail['cellIndex'])
    access_path = original_detail['accessPath']
    access_id = original_detail['accessId']
    whitespace = original_detail['whitespace']

    target_elem = self.table_elem.children[cell_index].querySelector(
        f'[data-access-id="{access_id}"]'
    )
    target_ds, _ = self._apply_access_path(access_path)
    options = dataclasses.replace(self.options, item_limit=-1)
    item_html = _format_item_html(target_ds, options=options)
    # lstrip since the previousSibling of elem already has the whitespace to
    # indent the first line.
    target_elem.innerHTML = textwrap.indent(item_html, whitespace).lstrip()
    self._end_loading()

  def _handle_dim_index_click(self, dimension: int, flat_index: int):
    """Handles a click on a dim index in the main data table."""
    # Convert the flat index into a multi-index.
    multi_index = []
    for i in range(dimension+1):
      multi_index.append(kd.index(self.ds, dim=i).flatten().take(flat_index))
    self.descend_into_access_path(
        [{'type': AccessType.MULTI_SLICE_INDEX.value, 'value': multi_index}],
        DescendMode.VALUE)

  def _handle_show_access_path(self, detail):
    """Interprets the show-access-path event from the frontend."""
    original_detail = detail.get('originalDetail', [])
    self.descend_into_access_path(
        original_detail.get('accessPath', []),
        DescendMode(original_detail.get('mode', ''))
    )

  def _get_listener_id(self, event_name: str) -> str:
    return f'{self.instance_id}:{event_name}'

  def _get_listener_invocations(self):
    return [
        ('request-load', self._handle_request_load),
        ('ds-vis-click', self._handle_ds_vis_click),
        ('ds-vis-expand-detail', self._handle_expand_detail),
        ('ds-vis-show-access-path', self._handle_show_access_path),
    ]

  def add_listeners(self):
    """Registers event listeners for the visualization."""
    # Emit makeDsListener helper in JS to construct event listeners below.
    _colab_publish().javascript("""
      window.makeDsListener = (listenerId) => {
        return e => {
          google.colab.kernel.invokeFunction(listenerId, [e.detail]);
        };
      };
    """)

    for (event_name, handler) in self._get_listener_invocations():
      listener_id = self._get_listener_id(event_name)
      _colab_output().register_callback(listener_id, handler)
      _js_eval_global().document.addEventListener(
          event_name, _js_eval_global().makeDsListener(listener_id), True)

  def remove_listeners(self):
    for (event_name, _) in self._get_listener_invocations():
      _colab_output().unregister_callback(self._get_listener_id(event_name))
      # No need to unregister listeners in the output cell since they are
      # not saved.


def _css_length_to_string(length: _CssLength) -> str:
  if isinstance(length, int):
    return f'{length}px'
  else:
    return length


def visualize_slice(
    ds: kd.types.DataSlice,
    options: DataSliceVisOptions | None = None,
) -> _DataSliceViewState:
  """Visualizes a DataSlice as a html widget."""
  options = options or DataSliceVisOptions()
  instance_id = 'id_' + str(uuid.uuid4())
  data = _create_data_slice_table_data(ds, items_begin=0, options=options)

  # Populate style dict with optional CSS properties.
  style_dict = {}
  if options.detail_width is not None:
    style_dict['--kd-multi-dim-table-detail-width'] = _css_length_to_string(
        options.detail_width)
  if options.detail_height is not None:
    style_dict['--kd-multi-dim-table-detail-height'] = _css_length_to_string(
        options.detail_height)
  style_string = ';'.join(
      f'{key}:{value}' for key, value in style_dict.items()
  )

  # Publish custom element definitions.
  _publish_koda_visualization_library()

  # Global CSS primarily for breadcrumb and darkmode
  dark_mode_css = """
    kd-multi-dim-table {
      --kd-ds-vis-message-color: lightgray;
      --kd-ds-vis-shadow-color: #181818;
      --kd-multi-dim-table-background: #383838;
      color: white;
    }
  """
  _colab_publish().css("""
    .breadcrumb {
      font-weight: bold;
    }

    .crumb[data-ds-index]:not(:last-child) {
      cursor: pointer;
      text-decoration: underline;
    }

    .crumb:last-child {
      pointer-events: none;
    }

    .crumb:not(:first-child) {
      margin-left: 4px;
    }

    kd-event-reinterpret {
      display: block;
      --kd-ds-vis-message-height: 100px;
      /*
        Extra 20px is to avoid having an extra scrollbar when we have a data
        item and a large message.
      */
      max-height: calc(var(--kd-multi-dim-table-detail-height, 300px)
          + var(--kd-ds-vis-message-height) + 20px);
      /*
        This line-height prevents bits of the top of detail content from showing
        up inline when the Roboto font is used in Colab. These bits shows up
        as small pixels at the bottom of table cells. It is not possible to
        hide the detail content inline since we clone the inline content into
        a shadow root.
      */
      line-height: calc(1lh + 2px);
      overflow-y: auto;
    }

    /* Configures the table to only show the detail section for DataItems. */
    kd-multi-dim-table[data-item] {
      --kd-compact-table-padding-bottom: 0;
      --kd-multi-dim-table-detail-outline-width: 0;
      --kd-multi-dim-table-detail-width: 100%;
      --kd-multi-dim-table-main-display: none;
      --kd-multi-dim-table-view-options-display: none;
    }

    .detail-pane {
      white-space: pre-wrap;
      word-break: break-word;
    }

    .attr {
      color: darkorange;
    }
    .object-id {
      color: cornflowerblue;
    }

    .kernel-only {
      visibility: hidden;
    }

    [kernel-available] {
      .attr,
      .object-id,
      .truncated,
      .limited {
        cursor: pointer;
        text-decoration: underline;
      }
      .attr:active,
      .object-id:active,
      .truncated:active,
      .limited:active {
        font-weight: bold;
        user-select: none;
      }

      .kernel-only {
        visibility: visible;
      }
    }

    [slot="message"] {
      color: var(--kd-ds-vis-message-color, unset);
      container-type: inline-size;
      filter: opacity(0.96);
      max-height: var(--kd-ds-vis-message-height);
      overflow-y: auto;
      padding: 4px 0;
      white-space: pre-wrap;
    }

    [slot="message"][multi-line] {
      border-radius: 10px 10px 0 0;
      box-shadow: 0 0 20px 0 var(--kd-ds-vis-shadow-color, lightgray) inset;
      padding: 10px;
    }

    kd-multi-dim-table {
      width: 100%;
    }

    /* Disable clicking on object ids when there is no bag. */
    kd-multi-dim-table[no-bag] .object-id:hover {
      pointer-events: none;
      cursor: default;
      text-decoration: none;
    }

    [theme="dark"] {
      """ + dark_mode_css + """
    }

    @media (prefers-color-scheme: dark) {
      """ + dark_mode_css + """
    }
  """)

  # Main HTML output that persists in the notebook even if the kernel is
  # disconnected.
  is_data_item = isinstance(ds, kd.types.DataItem)
  data_item_attr = 'data-item' if is_data_item else ''
  no_bag_attr = 'no-bag' if ds.get_bag() is None else ''
  _colab_publish().html(f"""
    <kd-event-reinterpret
        id="{instance_id}"
        style="{style_string}"
        events="click,show-access-path,expand-detail" prefix="ds-vis">
      <div class="breadcrumb"></div>
      <kd-multi-dim-table
          data-headers="{data.headers}"
          data-loaded-range="{data.items_begin},{data.items_end}"
          data-sizes="{data.sizes}"
          {data_item_attr} {no_bag_attr} content-mode="text">
        {data.data}
      </kd-multi-dim-table>
      <div id="footer">{data.footer}</div>
    </kd-event-reinterpret>
  """)

  # The JS in this block is not saved with the notebook. The one-request-load
  # attribute freezes the view when the viewer scrolls into the load margin.
  # When not connected to a kernel, we don't want to freeze since no load
  # can happen.
  elem = _js_eval_global().document.querySelector(
      f'#{instance_id}')
  table_elem = elem.querySelector('kd-multi-dim-table')
  table_elem.setAttribute('one-request-load', '')
  table_elem.setAttribute('data-header-classes', data.header_classes)
  table_elem.classList.add('clickable-dim-index')
  elem.setAttribute('kernel-available', '')

  # Save a script that focuses the single cell in the table.
  if is_data_item:
    _focus_data_cell(_js_global(), instance_id)

  # Script that interprets clicks as a show-access-path event that
  # contains the access path to the clicked element. Attributes and class names
  # assumed in this script should be populated by the C++ code that generates
  # the HTML string.
  access_type_strings = ', '.join(f"'{access.value}'" for access in AccessType)
  _colab_publish().javascript("""
    const ACCESS_KEYS = new Set([
        """ + access_type_strings + """
    ]);

    function getAccessPath(e) {
      return e.composedPath()
        .filter(node => node instanceof HTMLElement)
        .flatMap(node => Array.from(node.attributes)
            .filter(attr => ACCESS_KEYS.has(attr.name))
            .map(attr => ({type: attr.name, value: attr.value})))
        .reverse();
    }

    document.addEventListener('click', (e) => {
      const target = e.composedPath()[0];

      // In this case, the user wants to see more data in the detail pane.
      // We dispatch a new click event on the cell that includes the unique
      // cell index in the table.
      if (target.matches('.limited')) {
        const cell = target.closest('kd-multi-dim-table > *');
        const cellIndex = Array.prototype.indexOf.call(
            cell.parentElement.children, cell);

        // Find whitespace needed to indent the target element.
        const whitespace = target.parentElement
            .previousSibling?.textContent?.match(/ *$/)?.[0] || '';

        // Set an access id so we can find the target's parent in python.
        const accessPath = getAccessPath(e);
        const accessId = accessPath.map(x => `${x.type}=${x.value}`).join(':');
        target.parentElement.dataset['accessId'] = accessId;

        cell.dispatchEvent(new CustomEvent('expand-detail', {
          detail: { accessPath, cellIndex, whitespace, accessId },
          bubbles: true,
          composed: true,
        }));
        return;
      }

      let mode = null;
      if (target.matches('.object-id') || target.matches('.truncated')) {
    """ + f"""mode = '{DescendMode.VALUE.value}';""" + """
      } else if (target.matches('.attr')) {
    """ + f"""mode = '{DescendMode.ATTR.value}';""" + """
      }
      if (!mode) return;

      target.dispatchEvent(new CustomEvent('show-access-path', {
        detail: {accessPath: getAccessPath(e), mode},
        bubbles: true,
        composed: true,
      }));
    });
  """)

  view_state = _DataSliceViewState(ds, instance_id, options=options)
  view_state.add_listeners()
  return view_state


class _Watcher:
  """Responsible for releasing references to visualized DataSlices in Colab.

  Without this class, Colab would continue to reference DataSlices through
  handlers passed to js.AddEventListener calls. This class keeps track of
  which _DataSliceViewState objects are associated with each cell and removes
  event listeners of the previous execution when the cell is executed again.
  """

  def __init__(self, shell: interactiveshell.InteractiveShell):
    self._cell_id_to_state = {}
    self._shell = shell

  def post_run_cell(self, exec_result: interactiveshell.ExecutionResult):
    if isinstance(exec_result.result, kd.types.DataSlice):
      state = visualize_slice(exec_result.result)
      cell_id = _get_cell_id(exec_result.info)

      # Remove listeners for any existing state.
      existing = self._cell_id_to_state.get(cell_id, None)
      if existing is not None:
        existing.remove_listeners()

      # Remember state associated with this cell.
      self._cell_id_to_state[cell_id] = state


# Keep private global reference to last watcher for easier testing/debugging.
_WATCHER = None


def register_formatters():
  """Register DataSlice visualization in IPython."""
  # Avoid showing the default repr of the DataSlice.
  shell = IPython.get_ipython()
  formatters = shell.display_formatter.formatters
  formatters['text/html'].for_type(kd.types.DataSlice, lambda *args: '')

  # Remove any existing post_run_cell back from this module.
  post_run_cell_callbacks = shell.events.callbacks['post_run_cell']
  for callback in post_run_cell_callbacks:
    if (hasattr(callback, '__self__')
        and callback.__self__.__class__.__module__ == __name__):
      post_run_cell_callbacks.remove(callback)

  # Register a new post_run_cell back.
  global _WATCHER
  _WATCHER = _Watcher(shell)
  shell.events.register('post_run_cell', _WATCHER.post_run_cell)
