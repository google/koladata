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

"""Koda View class."""

from __future__ import annotations
import copy
import functools
import itertools
import pprint
import textwrap
from typing import Any, Callable
from koladata.ext.view import clib_py_ext as view_clib
from koladata.ext.view import mask_constants
from optree import pytree

_cc_map_structure = view_clib.map_structures
_INTERNAL_CALL = object()


# We need a custom repr so that the generated documentation is stable.
class _NoDefault:

  def __repr__(self) -> str:
    return 'NO_DEFAULT'


NO_DEFAULT = _NoDefault()


def _get_item_impl(x: Any, key_or_index: Any):
  try:
    return x[key_or_index]
  except (IndexError, KeyError):
    return None


def _set_item_impl(x: Any, key_or_index: Any, value: Any):
  if x is None or key_or_index is None:
    return
  try:
    x[key_or_index] = value
  except IndexError:
    pass


def _group_by_impl(
    sort: bool, x: tuple[Any, ...], *args: tuple[Any, ...]
) -> tuple[tuple[Any, ...], ...]:
  """Implements group_by for a single item."""
  grouped = {}
  for one_x, *one_args in zip(x, *args, strict=True):
    if not one_args:
      one_args = (one_x,)
    if any(arg is None for arg in one_args):
      continue
    grouped.setdefault(tuple(one_args), []).append(one_x)
  if sort:
    return tuple([tuple(values) for _, values in sorted(grouped.items())])
  else:
    return tuple([tuple(values) for values in grouped.values()])


def _collapse_impl(x: tuple[Any, ...]) -> Any:
  """Implements collapse for a single item."""
  res = None
  for item in x:
    if item is None:
      continue
    if res is None:
      res = item
    elif res != item:
      return None
  return res


def _presence_and_impl(x: Any, y: Any) -> Any:
  """Implements presence_and for a single item."""
  # This is called only when x and y are not None, so we do not need to handle
  # the case when y is None.
  if y is mask_constants.present:
    return x
  raise ValueError(
      'the second argument of & must have only kv.present and None values,'
      f' got: {y}'
  )


def _select_impl(x: tuple[Any, ...], fltr: tuple[Any, ...]) -> tuple[Any, ...]:
  """Implements select for a single item."""
  res = []
  for a, b in zip(x, fltr, strict=True):
    if b is mask_constants.present:
      res.append(a)
    elif b is not None:
      raise ValueError(
          f'the filter must have only kv.present and None values, got: {b}'
      )
  return tuple(res)


def _select_constant_fltr_impl(
    x: tuple[Any, ...], fltr: Any
) -> tuple[Any, ...]:
  """Implements select for a single item where the filter is a constant."""
  if fltr is mask_constants.present:
    return x
  if fltr is None:
    return ()
  raise ValueError(
      f'the filter must have only kv.present and None values, got: {fltr}'
  )


def _inverse_select_impl(
    x: tuple[Any, ...], fltr: tuple[Any, ...]
) -> tuple[Any, ...]:
  """Implements inverse_select for a single item."""
  x_iter = iter(x)
  length_mismatch_error = (
      'the number of items in the data does not match the number of present'
      ' values in the filter'
  )

  def _get_next(b: Any) -> Any:
    if b is mask_constants.present:
      try:
        return next(x_iter)
      except StopIteration:
        raise ValueError(length_mismatch_error) from None
    if b is None:
      return None
    raise ValueError(
        f'the filter must have only kv.present and None values, got: {b}'
    )

  res = [_get_next(b) for b in fltr]
  try:
    next(x_iter)
    raise ValueError(length_mismatch_error)
  except StopIteration:
    pass
  return tuple(res)


# Most methods of this class are also available as operators in the `kv` module.
# Please consider adding an operator when adding a new method here.
class View:
  """A view on a particular path inside an object.

  See the docstring for view() method for more details.
  """

  __slots__ = ['_obj', '_depth']

  _HAS_DYNAMIC_ATTRIBUTES = True
  _obj: Any
  _depth: int

  # TODO: Consider either implementing parts in C++ to improve
  # initialization performance, or to e.g. inherit from `tuple` (~2x faster
  # construction). Note that since we override `__getitem__`, we need to
  # implement a CPython accessor for the attributes since using
  # `tuple.__getitem__` kills all gains due to the additional indirection.
  def __init__(self, obj: Any, depth: int, internal_call: object, /):
    """Internal constructor. Please use kv.view() instead."""
    if internal_call is not _INTERNAL_CALL:
      raise ValueError(
          'Please do not call the View constructor directly, use view()'
          ' instead.'
      )
    # This is noticably faster than isinstance(obj, View). Since we don't expect
    # to have child classes of View, this is a cheaper but equivalent version.
    if type(obj) == View:  # pylint: disable=unidiomatic-typecheck
      raise TypeError('cannot wrap a View into another View')
    # This class represents a jagged array of objects, in other words a
    # list-of-lists-...-of-lists with a constant level of nesting, for example
    # [[A, B], [], [C]].
    #
    # It is internally represented as an object with a depth that describes how
    # deep the nesting goes. The assumption is that for depth > 0, `obj` is a
    # tuple.
    #
    # Note that some of the objects A, B, C may be tuples themselves, we do not
    # look into them and just treat them as arbitrary Python objects unless
    # the user explicitly calls explode().
    #
    # Since we override __setattr__, we use a lower-level API here to quickly
    # initialize the attributes.
    _VIEW_OBJ_SETTER(self, obj)
    _VIEW_DEPTH_SETTER(self, depth)

  def __repr__(self) -> str:
    obj_prefix = '  obj='
    obj_pp = pprint.pformat(
        self._obj, sort_dicts=False, width=80 - len(obj_prefix), compact=True
    )
    obj_pp = textwrap.indent(obj_pp, ' ' * len(obj_prefix))[len(obj_prefix):]
    return f'<View(\n{obj_prefix}{obj_pp},\n  depth={self._depth!r},\n)>'

  def get_attr(self, attr_name: str, default: Any = NO_DEFAULT) -> View:
    """Returns a new view with the given attribute of each item."""
    if default is NO_DEFAULT:
      attr_getter = lambda x: getattr(x, attr_name)
    else:
      if isinstance(default, View):
        if default.get_depth():
          raise ValueError(
              'Default value for get_attr must be a scalar, got a view with'
              f' depth {default.get_depth()}'
          )
        default = default.get()

      def attr_getter(x):
        try:
          res = getattr(x, attr_name)
          return res if res is not None else default
        except AttributeError:
          return default

    return _map1(attr_getter, self)

  def __getattr__(self, attr_name: str) -> View:
    """Returns a new view with the given attribute of each item.

    This is a convenience method for `get_attr`. It allows to write `view.a`
    instead of `view.get_attr('a')`. Attributes starting with `_` are not
    supported to avoid conflicts with private attributes of `View` class.

    Example:
      x = types.SimpleNamespace(a=1)
      view(x).a.get()
      # 1

    Args:
      attr_name: The name of the attribute to get.
    """
    if attr_name.startswith('_'):
      raise AttributeError(attr_name)
    attr_getter = lambda x: getattr(x, attr_name)
    return _map1(attr_getter, self)

  def set_attrs(self, /, **attrs: ViewOrAutoBoxType) -> None:
    """Sets the given attributes of each item."""

    # NOTE: For most cases, this impl faster than changing attr_setter to take
    # `**attrs` which requires repeated handling of dicts.
    for attr_name, value in attrs.items():
      if isinstance(value, View) and value.get_depth() > self._depth:
        raise ValueError(
            f'The value being set as attribute {attr_name!r} must have same or'
            ' lower depth than the object itself. Maybe you forgot to call'
            ' .implode()?'
        )

      def attr_setter(x: Any, v: Any):
        if x is not None:
          setattr(x, attr_name, v)  # pylint: disable=cell-var-from-loop

      _map2(attr_setter, self, value, include_missing=True)

  def __setattr__(self, attr_name: str, value: ViewOrAutoBoxType):
    """Sets the given attribute of each item."""
    if attr_name.startswith('_'):
      raise AttributeError(attr_name)
    return self.set_attrs(**{attr_name: value})

  # TODO: In this and other places, make sure (and test) that
  # the API aligns with the corresponding Koda API.
  def explode(self, ndim: int = 1) -> View:
    """Unnests iterable elements, increasing rank by `ndim`."""
    if ndim < 0:
      raise ValueError(
          'the number of dimensions to explode must be non-negative, got'
          f' {ndim}'
      )
    # TODO: For dicts, this does not align with Koda
    # (.explode() raises, [:] raises in tracing mode and returns values in
    # eager mode), while we return keys here.
    explode_fn = lambda x: () if x is None else tuple(x)
    res = self
    for _ in range(ndim):
      res = _map1(explode_fn, res, include_missing=True)
      res = View(res.get(), res.get_depth() + 1, _INTERNAL_CALL)
    return res

  def get_item(self, key_or_index: ViewOrAutoBoxType | slice) -> View:
    """Returns an item or items from the given view containing containers."""
    if isinstance(key_or_index, slice):
      start = box_and_unbox_scalar(key_or_index.start)
      stop = box_and_unbox_scalar(key_or_index.stop)
      step = box_and_unbox_scalar(key_or_index.step)
      if step is not None:
        # We could easily support this, but then we'd have more discrepancy
        # with Koda.
        raise ValueError('slice step is not supported in View.__getitem__')
      res = _map1(
          lambda x: () if x is None else tuple(x[start:stop]),
          self,
          include_missing=True,
      )
      return View(res.get(), res.get_depth() + 1, _INTERNAL_CALL)
    else:
      return _map2(_get_item_impl, self, key_or_index)

  def __getitem__(self, key_or_index: ViewOrAutoBoxType | slice) -> View:
    """Returns an item or items from the given view containing containers."""
    return self.get_item(key_or_index)

  def __iter__(self):
    # We need this, otherwise thanks to __getitem__ never raising KeyError
    # Python iterates infinitely over the view.
    raise ValueError('iteration over a view is not supported yet')

  def set_item(self, key_or_index: ViewOrAutoBoxType, value: ViewOrAutoBoxType):
    """Sets an item or items for all containers in the view."""
    if isinstance(key_or_index, slice):
      raise ValueError('slice is not yet supported in View.__setitem__')
    if (
        isinstance(value, View)
        and value.get_depth() > self._depth
        and value.get_depth()
        > (key_or_index.get_depth() if isinstance(key_or_index, View) else 0)
    ):
      raise ValueError(
          'The value being set must have same or lower depth than the objects'
          ' or the keys. Maybe you forgot to call .implode()?'
      )
    return map_(_set_item_impl, self, key_or_index, value, include_missing=True)

  def __setitem__(
      self, key_or_index: ViewOrAutoBoxType, value: ViewOrAutoBoxType
  ):
    """Sets an item or items for all containers in the view."""
    return self.set_item(key_or_index, value)

  def append(self, value: ViewOrAutoBoxType):
    """Appends an item or items to all containers in the view."""
    _map2(
        lambda x, y: x.append(y) if x is not None else None,
        self,
        value,
        include_missing=True,
    )

  def take(self, index: ViewOrAutoBoxType) -> View:
    """Returns a view with the given index in the last dimension."""
    return self.implode()[index]

  def implode(self, ndim: int = 1) -> View:
    """Reduces view dimension by grouping items into tuples."""
    depth = self._depth
    if ndim < 0:
      depth = 0
    elif ndim <= depth:
      depth -= ndim
    else:
      raise ValueError(
          f'Cannot implode by {ndim} dimensions, the shape has only'
          f' {depth} dimensions.'
      )
    return View(self._obj, depth, _INTERNAL_CALL)

  # TODO: Once View also stores the root and the path from root,
  # we might want to make this method return a subset of original data
  # structure, instead of just a tuple of tuples, for example:
  #
  # class Foo:
  #   a: int | None
  #   b: int | None
  #
  # class Bar:
  #   f: Foo | None
  #   g: int | None
  #
  # Bar(Foo(1, 2), 3).f.a.get() --> returns Bar(Foo(1, None), None)
  #
  # But it is not clear what should happen to dicts in that world.
  def get(self) -> Any:
    """Returns an object represented by the view.

    Example:
      view('foo').get()
      # 'foo'
      view([[1,2],[3]])[:].get()
      # ([1,2],[3]).
      view([[1,2],[3]])[:][:].get()
      # ((1,2),(3,)).
    """
    return self._obj

  def flatten(self, from_dim: int = 0, to_dim: int | None = None) -> View:
    """Flattens the specified dimensions of the view."""
    if from_dim < 0:
      from_dim += self._depth
    from_dim = max(0, min(from_dim, self._depth))
    if to_dim is None:
      to_dim = self._depth
    else:
      if to_dim < 0:
        to_dim += self._depth
      to_dim = max(from_dim, min(to_dim, self._depth))
    if from_dim + 1 == to_dim:
      return self
    inner = self.implode(self._depth - from_dim)
    outer = self.implode(self._depth - to_dim)
    res = _map1(lambda x: [], inner, include_missing=True)
    _map2(lambda x, y: x.append(y), res, outer, include_missing=True)
    # We need to run explode() once to convert lists to tuples, but for the
    # remaining levels we can just set the depth to avoid additional traversal,
    # since they already have tuples.
    return View(
        res.explode().get(),
        self._depth - (to_dim - from_dim) + 1,
        _INTERNAL_CALL,
    )

  def expand_to(self, other: ViewOrAutoBoxType, ndim: int = 0) -> View:
    """Expands the view to the shape of other view."""
    if ndim:
      if ndim < 0:
        raise ValueError(
            'the number of dimensions to expand must be non-negative, got'
            f' {ndim}'
        )
      res = self.implode(ndim)
    else:
      if self is other:
        return self
      res = self
    other = box(other)
    if res._depth <= other._depth:  # pylint: disable=protected-access
      res = _map2(lambda x, y: x, res, other, include_missing=True)
    else:
      raise ValueError(
          f'a View with depth {res._depth} cannot be broadcasted to a View'  # pylint: disable=protected-access
          f' with depth {other._depth}'  # pylint: disable=protected-access
      )
    if ndim:
      # We already have tuples so we can just increase the depth.
      return View(res.get(), res.get_depth() + ndim, _INTERNAL_CALL)
    return res

  def map(
      self,
      f: Callable[[Any], Any],
      *,
      ndim: int = 0,
      include_missing: bool | None = None,
  ) -> View:
    """Applies a function to every item in the view.

    If `ndim=0`, then the function is applied to the items of the view.
    If `ndim=1`, then the function is applied to tuples of items of the
    view corresponding to the last dimension. If `ndim=2`, then the function
    is applied to tuples of tuples, and so on. The depth of the result is
    therefore decreased by `ndim` compared to the depth of `self`.

    Example:
      view([1, None, 2])[:].map(lambda x: x * 2).get()
      # (2, None, 4)
      view([1, None, 2]).map(lambda x: x * 2).get()
      # [1, None, 2, 1, None, 2]
      # We have used "*" operator on the list [1, None, 2] and the integer 2.
      view([1, None, 2])[:].map(lambda x: x * 2, ndim=1).get()
      # (1, None, 2, 1, None, 2)
      # Here we have used "*" operator on the tuple (1, None, 2) and the integer
      # 2.

    Args:
      f: The function to apply.
      ndim: Dimensionality of items to pass to `f`, must be less or equal to the
        depth of the view.
      include_missing: Specifies whether `f` applies to all items (`=True`) or
        only to present items (`=False`, valid only when `ndim=0`); defaults to
        `False` when `ndim=0`.

    Returns:
      A new view with the function applied to every item.
    """
    return map_(f, self, ndim=ndim, include_missing=include_missing)

  def deep_map(
      self,
      f: Callable[[Any], Any],
      *,
      include_missing: bool = False,
      namespace: str = '',
  ) -> View:
    """Applies a function to every nested primitive value in the view.

    Unlike `map`, which applies the function to each value at the current depth,
    `deep_map` traverses nested structures indiscriminately using
    `optree.tree_map` while keeping structures intact. See
    https://optree.readthedocs.io for more details on how to register handlers
    for custom types.

    Example:
      view([1, None, 2]).deep_map(lambda x: x * 2).get()
      # [2, None, 4]
      view([1, None, 2])[:].deep_map(lambda x: x * 2).get()
      # (2, None, 4)
      view([{'x': 1, 'y': 2, 'z': None}]).deep_map(lambda x: x * 2).get()
      # [{'x': 2, 'y': 4, 'z': None}]

    Args:
      f: The function to apply.
      include_missing: Specifies whether `f` applies to all items (`=True`) or
        only to present items (`=False`).
      namespace: The namespace to use for the custom type handler.

    Returns:
      A new view with the function applied to every nested primitive value.
    """
    return View(
        pytree.map(
            f, self._obj, none_is_leaf=include_missing, namespace=namespace
        ),
        self._depth,
        _INTERNAL_CALL,
    )

  def group_by(self, *args: ViewOrAutoBoxType, sort: bool = False) -> View:
    """Groups items by the values of the given args."""
    if not self._depth:
      raise ValueError(
          'the argument being grouped must have at least one dimension'
      )
    args = [box(arg) for arg in args]
    for arg in args:
      if arg.get_depth() != self._depth:
        raise ValueError('all arguments must have the same shape')
    res = map_(
        functools.partial(_group_by_impl, sort),
        self.implode(),
        *[arg.implode() for arg in args],
    )
    # We already have tuples so we can just increase the depth.
    return View(res.get(), res.get_depth() + 2, _INTERNAL_CALL)

  def collapse(self, ndim: int = 1) -> View:
    """Collapses equal items along the specified number dimensions of the view."""
    if ndim < 0 or ndim > self._depth:
      raise ValueError(
          'the number of dimensions to collapse must be in range [0,'
          f' {self._depth}], got {ndim}'
      )
    return self.flatten(self._depth - ndim).map(_collapse_impl, ndim=1)

  def select(self, fltr: ViewOrAutoBoxType, expand_filter: bool = True) -> View:
    """Keeps only items in the view where the filter is present."""
    fltr = box(fltr)
    if fltr.get_depth() > self._depth:
      raise ValueError(
          'the filter cannot have a higher depth than the data being selected'
      )
    if expand_filter:
      select_dim = self._depth - 1
    else:
      select_dim = fltr.get_depth() - 1
    if select_dim < 0:
      if not self._depth:
        raise ValueError(
            'cannot select from a scalar view, maybe use .flatten() first?'
        )
      raise ValueError(
          'the filter must have at least one dimension when expand_filter=False'
      )
    data = self.implode(self._depth - select_dim)
    if fltr.get_depth() > select_dim:
      assert fltr.get_depth() == select_dim + 1
      res = _map2(_select_impl, data, fltr.implode())
    else:
      res = _map2(_select_constant_fltr_impl, data, fltr, include_missing=True)
    # We already have tuples so we can just increase the depth.
    return View(res.get(), self._depth, _INTERNAL_CALL)

  def inverse_select(self, fltr: ViewOrAutoBoxType) -> View:
    """Restores the original shape that was reduced by select."""
    fltr = box(fltr)
    if fltr.get_depth() != self._depth:
      raise ValueError(
          'the filter must have the same depth as the data being selected'
      )
    if not self._depth:
      raise ValueError(
          'cannot select from a scalar view, maybe use .flatten() first?'
      )
    res = _map2(_inverse_select_impl, self.implode(), fltr.implode())
    # We already have tuples so we can just increase the depth.
    return View(res.get(), self._depth, _INTERNAL_CALL)

  def deep_clone(self) -> View:
    """Returns a deep copy of the view."""
    return View(copy.deepcopy(self._obj), self._depth, _INTERNAL_CALL)

  def get_depth(self) -> int:
    """Returns the depth of the view."""
    return self._depth

  def __bool__(self) -> bool:
    if self._depth:
      raise ValueError(
          'can only use views with depth=0 in if-statements, got'
          f' depth={self._depth}'
      )
    if self._obj is mask_constants.present:
      return True
    if self._obj is None:
      return False
    raise ValueError(
        'can only use views with kv.present or None in if-statements, got'
        f' {self._obj}'
    )

  def __and__(self, other: ViewOrAutoBoxType) -> View:
    return _map2(_presence_and_impl, self, other)

  def __rand__(self, other: ViewOrAutoBoxType) -> View:
    return map_(_presence_and_impl, other, self)

  def __or__(self, other: ViewOrAutoBoxType) -> View:
    return _map2(
        lambda x, y: y if x is None else x, self, other, include_missing=True
    )

  def __ror__(self, other: ViewOrAutoBoxType) -> View:
    return map_(
        lambda x, y: y if x is None else x, other, self, include_missing=True
    )

  def __eq__(self, other: ViewOrAutoBoxType) -> View:
    return _map2(
        lambda x, y: mask_constants.present if x == y else None, self, other
    )

  def __ne__(self, other: ViewOrAutoBoxType) -> View:
    return _map2(
        lambda x, y: mask_constants.present if x != y else None, self, other
    )

  def __lt__(self, other: ViewOrAutoBoxType) -> View:
    return _map2(
        lambda x, y: mask_constants.present if x < y else None, self, other
    )

  def __le__(self, other: ViewOrAutoBoxType) -> View:
    return _map2(
        lambda x, y: mask_constants.present if x <= y else None, self, other
    )

  def __gt__(self, other: ViewOrAutoBoxType) -> View:
    return _map2(
        lambda x, y: mask_constants.present if x > y else None, self, other
    )

  def __ge__(self, other: ViewOrAutoBoxType) -> View:
    return _map2(
        lambda x, y: mask_constants.present if x >= y else None, self, other
    )


def view(obj: Any) -> View:
  """Creates a view on an object that can be used for vectorized access.

  A view represents traversing a particular path in a tree represented
  by the object, with the leaves of that path being the items in the view,
  and the structure of that path being the shape of the view.

  Note that when we traverse a path, sometimes we branch when there are several
  uniform edges, such as when using the `view[:]` API. So in formal terms what
  we call a `path` is actually a subtree of the tree.

  For example, consider the following set of objects:

  x = Obj(d=3)
  y = Obj(d=4)
  z = [x, y]
  w = Obj(b=1, c=z)

  Object w can be represented as the following tree:

  w --b--> 1
    --c--> z --item0--> x --d--> 3
             --item1--> y --d--> 4

  Now view(w) corresponds to just the root of this tree. view(w).c corresponds
  to traversing edge labeled with c to z. view(w).c[:] corresponds to traversing
  the edges labeled with item0 and item1 to x and y respectively. view(w).c[:].d
  corresponds to traversing the edges labeled with d to 3 and 4.

  We call the leaf nodes of this path traversal the items of the view, and
  the number of branches used to get to them the depth of the view.

  For example, for view(w).c[:].d, its items are 3 and 4, and its depth is 1.

  Example:
    view([1, 2])[:].map(lambda x: x + 1).get()
    # (2, 3)
    view([[1, 2], [3]])[:].map(lambda x: len(x)).get()
    # (2, 1)

  Args:
    obj: An arbitrary object to create a view for.

  Returns:
    A scalar view on the object.
  """
  return View(obj, 0, _INTERNAL_CALL)


_VIEW_OBJ_SETTER = View.__dict__['_obj'].__set__  # pytype: disable=attribute-error
_VIEW_DEPTH_SETTER = View.__dict__['_depth'].__set__  # pytype: disable=attribute-error

AutoBoxType = (
    int | float | str | bytes | bool | type(mask_constants.present) | None
)
ViewOrAutoBoxType = View | AutoBoxType


def box(obj: ViewOrAutoBoxType) -> View:
  """Wraps the given object into a view.

  Unlike view(), this method only works for a predefined set of types,
  so that we can use it for implicit boxing in various APIs.

  Currently we auto-box Python primitive types only.

  Args:
    obj: The object to box.

  Returns:
    A view on the object, or raises a ValueError if the object cannot be
    automatically boxed.
  """
  if isinstance(obj, View):
    return obj
  if isinstance(obj, AutoBoxType):
    return view(obj)
  raise ValueError(
      f'Cannot automatically box {obj} of type {type(obj)} to a view. Use'
      ' kv.view() explicitly if you want to construct a view from it.'
  )


def box_and_unbox_scalar(obj: ViewOrAutoBoxType) -> Any:
  """Unboxes the given object if it is a view, and asserts it is a scalar.

  This method is equivalent to:
  v = box(obj)
  assert v.get_depth() == 0
  return v.get()

  But it allows to avoid creating temporary views in the common case.

  Args:
    obj: The object to box and unbox as scalar.

  Returns:
    The unboxed object.
  """
  if isinstance(obj, View):
    if obj.get_depth():
      raise ValueError(f'expected a scalar, got depth {obj.get_depth()}')
    return obj.get()
  if isinstance(obj, AutoBoxType):
    return obj
  raise ValueError(
      f'Cannot automatically box {obj} of type {type(obj)} to a view. Use'
      ' kv.view() explicitly if you want to construct a view from it.'
  )


# This method is in view.py since it is used in the map_ method.
def align(*args: ViewOrAutoBoxType) -> tuple[View, ...]:
  """Aligns the views to a common shape.

  We will also apply auto-boxing if some inputs are not views but can be
  automatically boxed into one.

  Args:
    *args: The views to align, or values that can be automatically boxed into
      views.

  Returns:
    A tuple of aligned views, of size len(others) + 1.
  """
  if not args:
    return ()
  if len(args) == 1:
    return (box(args[0]),)
  args = [box(o) for o in args]
  ref_view = max(args, key=lambda v: v.get_depth())
  return tuple(v.expand_to(ref_view) for v in args)


def _map1(
    f: Callable[..., Any], arg: View, *, include_missing: bool = False
) -> View:
  """Unary specialization of map_. `arg` _must_ be a View."""
  depth = arg._depth  # pylint: disable=protected-access
  obj = _cc_map_structure(f, (depth,), include_missing, (arg._obj,))  # pylint: disable=protected-access
  return View(obj, depth, _INTERNAL_CALL)


def _map2(
    f: Callable[..., Any],
    arg1: View,
    arg2: ViewOrAutoBoxType,
    *,
    include_missing: bool = False,
) -> View:
  """Binary specialization of map_. `arg1` _must_ be a View."""
  # pylint: disable=protected-access
  depth1 = arg1._depth
  arg2_is_view = isinstance(arg2, View)
  unboxed_arg2 = arg2._obj if arg2_is_view else arg2
  depth2 = arg2._depth if arg2_is_view else 0
  obj = _cc_map_structure(
      f, (depth1, depth2), include_missing, (arg1._obj, unboxed_arg2)
  )
  return View(obj, max(depth1, depth2), _INTERNAL_CALL)
  # pylint: enable=protected-access


# This method is in view.py since we expect to use it from implementations
# of methods of View class.
def map_(
    f: Callable[..., Any],
    *args: ViewOrAutoBoxType,
    ndim: int = 0,
    include_missing: bool | None = None,
    **kwargs: ViewOrAutoBoxType,
) -> View:
  """Applies a function to corresponding items in the args/kwargs view.

  Arguments will be broadcasted to a common shape. There must be at least one
  argument or keyword argument.

  The `ndim` argument controls how many dimensions should be passed to `f` in
  each call. If `ndim = 0` then the items of the corresponding view will be
  passed, if `ndim = 1` then python tuples of items corresponding
  to the last dimension will be passed, if `ndim = 2` then tuples of tuples,
  and so on.

  Example:
    x = types.SimpleNamespace(
        a=[types.SimpleNamespace(b=1), types.SimpleNamespace(b=2)]
    )
    kv.map(lambda i: i + 1, kv.view(x).a[:].b).get()
    # (2, 3)
    kv.map(lambda x: x + y, kv.view(x).a[:].b, kv.view(1)).get()
    # (2, 3)
    kv.map(lambda i: i + i, kv.view(x).a[:].b, ndim=1).get()
    # (1, 2, 1, 2)

  Args:
    f: The function to apply.
    *args: The positional arguments to pass to the function. They must all be
      views or auto-boxable into views.
    ndim: Dimensionality of items to pass to `f`.
    include_missing: Specifies whether `f` applies to all items (`=True`) or
      only to items present in all `args` and `kwargs` (`=False`, valid only
      when `ndim=0`); defaults to `False` when `ndim=0`.
    **kwargs: The keyword arguments to pass to the function. They must all be
      views or auto-boxable into views.

  Returns:
    A new view with the function applied to the corresponding items.
  """
  if include_missing is None:
    include_missing = bool(ndim)

  all_args = itertools.chain(args, kwargs.values())
  if ndim:
    if not include_missing:
      raise ValueError('include_missing=False can only be used with ndim=0')
    if ndim < 0:
      raise ValueError(f'invalid argument {ndim=}, must be non-negative')
    # When ndim>0, we actually need tuples of the same value for arguments
    # that are expanded, so we call align explicitly.
    all_args = align(*all_args)

  unboxed_args = []
  depths = []
  append_arg = unboxed_args.append
  append_depth = depths.append

  for arg in all_args:
    if isinstance(arg, View):
      append_depth(arg._depth)  # pylint: disable=protected-access
      append_arg(arg._obj)  # pylint: disable=protected-access
    else:
      if not isinstance(arg, AutoBoxType):
        raise ValueError(
            f'expected a View or a boxable typle, got {arg}. Use kv.view()'
            ' explicitly if you want to construct a view from it.'
        )
      append_depth(0)
      append_arg(arg)

  res_depth = max(depths)
  if ndim:
    assert all(depth == res_depth for depth in depths)
    if ndim > res_depth:
      raise ValueError(
          f'invalid argument {ndim=}, only values smaller or equal to'
          f' {res_depth} are supported for the given'
          f' view{"s" if len(depths) > 1 else ""}'
      )
    res_depth -= ndim
    depths = [res_depth] * len(depths)

  obj = _cc_map_structure(
      f, depths, include_missing, unboxed_args, tuple(kwargs)
  )
  return View(obj, res_depth, _INTERNAL_CALL)
