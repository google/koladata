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
import itertools
from typing import Any, Callable
from koladata.ext.view import clib_py_ext as view_clib

_cc_map_structure = view_clib.map_structures
_INTERNAL_CALL = object()


# Most methods of this class are also available as operators in the `kv` module.
# Please consider adding an operator when adding a new method here.
class View:
  """A view on a particular path inside an object.

  See the docstring for view() method for more details.
  """

  __slots__ = ['_obj', '_depth']

  # TODO: Consider either implementing parts in C++ to improve
  # initialization performance, or to e.g. inherit from `tuple` (~2x faster
  # construction). Note that since we override `__getitem__`, we need to
  # implement a CPython accessor for the attributes since using
  # `tuple.__getitem__` kills all gains due to the additional indirection.
  def __init__(self, obj: Any, depth: int, internal_call: object, /):
    if internal_call is not _INTERNAL_CALL:
      raise ValueError(
          'Please do not call the View constructor directly, use view()'
          ' instead.'
      )
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
    self._obj = obj
    self._depth = depth

  def __repr__(self) -> str:
    return f'<View(\n  obj={self._obj!r},\n  depth={self._depth!r},\n)>'

  def get_attr(self, attr_name: str) -> View:
    """Returns a new view with the given attribute of each item."""
    attrgetter = lambda x: getattr(x, attr_name)
    return _map1(attrgetter, self)

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
    return self.get_attr(attr_name)

  # TODO: In this and other places, make sure (and test) that
  # the API aligns with the corresponding Koda API.
  def explode(self, ndim: int = 1) -> View:
    """Unnests iterable elements, increasing rank by `ndim`."""
    if ndim < 0:
      raise ValueError(
          'the number of dimensions to explode must be non-negative, got'
          f' {ndim}'
      )
    explode_fn = lambda x: () if x is None else tuple(x)
    res = self
    for _ in range(ndim):
      # TODO: For dicts, this does not align with Koda ([:] returns
      # values there, while we return keys here).
      res = _map1(explode_fn, res, include_missing=True)
      res = View(res.get(), res.get_depth() + 1, _INTERNAL_CALL)
    return res

  def __getitem__(self, key: slice) -> View:
    """Provides `view[:]` syntax as a shortcut for `view.explode()`.

    Example:
      x = types.SimpleNamespace(
          a=[types.SimpleNamespace(b=1), types.SimpleNamespace(b=2)]
      )
      view(x).a[:].b.get()
      # [1, 2]

    Args:
      key: The key to use for getitem. Only slice() (usually obtained via [:])
        is supported for now.

    Returns:
      The result of `view.explode()` if `key` is `[:]`, otherwise raises a
      ValueError.
    """
    if isinstance(key, slice):
      if key.start is None and key.stop is None and key.step is None:
        return self.explode()
    raise ValueError(
        'Only everything slice [:] is supported in View.__getitem__ yet.'
    )

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
    return res.explode(self._depth - to_dim + 1)

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
      res = res.explode(ndim)
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

  def get_depth(self) -> int:
    """Returns the depth of the view."""
    return self._depth


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


AutoBoxType = int | float | str | bytes | bool | None
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
  elif isinstance(obj, AutoBoxType):
    return view(obj)
  else:
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
      assert isinstance(arg, AutoBoxType)
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
