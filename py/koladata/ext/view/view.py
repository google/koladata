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

  def get_attr(self, attr_name: str) -> View:
    """Returns a new view with the given attribute of each item.

    If one of the items is None, the corresponding value will be None as well,
    instead of raising an error that getattr() would raise.

    Example:
      x = types.SimpleNamespace(_b=6)
      view(x).get_attr('_b').get()
      # 6

    Args:
      attr_name: The name of the attribute to get.
    """
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
  def explode(self) -> View:
    """Unnests iterable elements by one level, increasing rank by 1.

    If a view contains iterable elements, `explode` creates a new view
    containing elements from those iterables, and increases view rank by 1.
    This is useful for "diving" into lists within your data structure.
    Usually used via `[:]`.

    It is user's responsibility to ensure that all items are iterable and
    have `len`.

    If one of the items is None, it will be treated as an empty iterable,
    instead of raising an error that len() would raise.

    Example:
      x = types.SimpleNamespace(a=[1, 2])
      view(x).a.explode().map(lambda i: i + 1).get()
      # (2, 3)

    Returns:
      A new view with one more dimension.
    """
    # TODO: For dicts, this does not align with Koda ([:] returns
    # values there, while we return keys here).
    explode_fn = lambda x: () if x is None else tuple(x)
    res = _map1(explode_fn, self, include_missing=True)
    return View(res.get(), self._depth + 1, _INTERNAL_CALL)

  def __getitem__(self, key: Any) -> View:
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
    """Reduces view dimension by grouping items into tuples.

    This is an inverse operation to `explode`. It groups items into tuples
    according to the shape of topmost `ndim` dimensions. If `ndim` is negative,
    will implode all the way to a scalar.

    Example:
      view_2d = view([[1,2],[3]])[:][:]
      view_2d.implode()
      # The same structure as view([(1,2),(3,)])[:].
      view_2d.implode(ndim=2)
      view_2d.implode(ndim=-1)
      # The same structure as view(((1,2),(3,))).

    Args:
      ndim: The number of dimensions to implode.

    Returns:
      A new view with `ndim` less dimensions.
    """
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

  def flatten(self) -> View:
    """Flattens all dimensions of the view.

    The result is always a view of depth 1 containing all items in order. Note
    that this does not look into the objects stored at the leaf level,
    so even if they are tuples themselves, they will not be flattened.

    Example:
      x = [[1, 2], [3]]
      view(x)[:][:].flatten().get()
      # (1, 2, 3)
      view(x)[:].flatten().get()
      # ([1, 2], [3])
      view(x).flatten().get()
      # ([[1, 2], [3]],)

    Returns:
      A new view with rank 1.
    """
    res = []
    _map1(res.append, self, include_missing=True)
    return View(tuple(res), 1, _INTERNAL_CALL)

  def expand_to(self, other: View) -> View:
    """Expands the view to the shape of other view."""
    if self is other:
      return self
    if self._depth <= other._depth:  # pylint: disable=protected-access
      return _map2(lambda x, y: x, self, other, include_missing=True)
    else:
      raise ValueError(
          f'a View with depth {self._depth} cannot be broadcasted to a View'  # pylint: disable=protected-access
          f' with depth {other._depth}'  # pylint: disable=protected-access
      )

  def map(
      self, f: Callable[[Any], Any], *, include_missing: bool = False
  ) -> View:
    """Applies a function to every item in the view.

    Example:
      view([1, None, 2])[:].map(lambda x: x * 2).get()
      # (2, None, 4)
      view([1, 2]).map(lambda x: x * 2).get()
      # [1, 2, 1, 2]

    Args:
      f: The function to apply.
      include_missing: Whether to call f when the corresponding item is None. If
        False, f will not be called and the result will preserve the None.

    Returns:
      A new view with the function applied to every item.
    """
    return _map1(f, self, include_missing=include_missing)

  def agg_map(self, f: Callable[[tuple[Any, ...]], Any]) -> View:
    """Applies a function to tuples from the last dimension of the view.

    This is a shortcut for .implode().map(). Every invocation of `f` will be
    passed a tuple of values corresponding to all items within the last (most
    nested) dimension of the view, including Nones for missing values.

    Example:
      view([[None, 1, 2], []])[:][:].agg_map(len).get()
      # (3, 0)
      view([[None, 1, 2], []])[:][:].agg_map(print).get()
      # (None, None)
      # Prints:
      # (None, 1, 2)
      # ()

    Args:
      f: The function to apply.

    Returns:
      A new view with the function applied to every tuple of items, with one
      fewer dimension.
    """
    return _map1(f, self.implode())

  def get_depth(self) -> int:
    """Returns the depth of the view."""
    return self._depth


def view(obj: Any) -> View:
  """Creates a view on an object that can be used for vectorized access.

  A view represents traversing a particular path in a tree represented
  by the object, with the leaves of that path being the items in the view,
  and the structure of that path being the shape of the view.

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


_AUTO_BOX_TYPES = (int, float, str, bytes, bool, type(None))


def box(obj: Any) -> View:
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
  elif isinstance(obj, _AUTO_BOX_TYPES):
    return view(obj)
  else:
    raise ValueError(
        f'Cannot automatically box {obj} of type {type(obj)} to a view. Use'
        ' kv.view() explicitly if you want to construct a view from it.'
    )


# This method is in view.py since we expect to use it from implementations
# of methods of View class.
def align(first: Any, *others: Any) -> tuple[View, ...]:
  """Aligns the views to a common shape.

  We will also apply auto-boxing if some inputs are not views but can be
  automatically boxed into one.

  Args:
    first: The first argument to align.
    *others: The remaining arguments to align.

  Returns:
    A tuple of aligned views, of size len(others) + 1.
  """
  first = box(first)
  if not others:
    return (first,)
  others = tuple(box(o) for o in others)
  ref_view = max((first, *others), key=lambda l: l.get_depth())
  return (
      first.expand_to(ref_view),
      *(l.expand_to(ref_view) for l in others),
  )


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
    arg2: Any,
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
    *args: Any,
    include_missing: bool = False,
    **kwargs: Any,
) -> View:
  """Applies a function to corresponding items in the args/kwargs view.

  Arguments will be broadcasted to a common shape. There must be at least one
  argument or keyword argument.

  Example:
    x = types.SimpleNamespace(
        a=[types.SimpleNamespace(b=1), types.SimpleNamespace(b=2)]
    )
    kv.map(lambda i: i + 1, kv.view(x).a[:].b).get()
    # [2, 3]
    kv.map(lambda x: x + y, kv.view(x).a[:].b, kv.view(1)).get()
    # [2, 3]

  Args:
    f: The function to apply.
    *args: The positional arguments to pass to the function. They must all be
      views or auto-boxable into views.
    include_missing: Whether to call fn when one of the args is None. If False,
      the result will be None if any of the args is None.
    **kwargs: The keyword arguments to pass to the function. They must all be
      views or auto-boxable into views.

  Returns:
    A new view with the function applied to the corresponding items.
  """
  unboxed_args = []
  depths = []
  append_arg = unboxed_args.append
  append_depth = depths.append

  for arg in itertools.chain(args, kwargs.values()):
    if isinstance(arg, View):
      append_depth(arg._depth)  # pylint: disable=protected-access
      append_arg(arg._obj)  # pylint: disable=protected-access
    else:
      assert isinstance(arg, _AUTO_BOX_TYPES)
      append_depth(0)
      append_arg(arg)

  obj = _cc_map_structure(
      f, depths, include_missing, unboxed_args, tuple(kwargs)
  )
  return View(obj, max(depths), _INTERNAL_CALL)
