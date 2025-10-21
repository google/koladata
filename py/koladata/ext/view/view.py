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

from arolla import arolla
from koladata import kd


class View:
  """A view on a particular path inside an object.

  See the docstring for view() method for more details.
  """

  __slots__ = ['_flat_items', '_shape']

  def __init__(
      self,
      flat_items: list[Any],
      shape: kd.types.JaggedShape,
      *,
      is_internal_call: bool = False,
  ):
    if not is_internal_call:
      raise ValueError(
          'Please do not call the View constructor directly, use view()'
          ' instead.'
      )
    # This class represents a jagged array of objects, in other words a
    # list-of-lists-...-of-lists with a constant level of nesting, for example
    # [[A, B], [], [C]].
    # It is internally represented as a flat list of items at the lowest level
    # of nesting, in this example [A, B, C], plus a shape that describes the
    # nesting, in this example kd.shapes.new(3, [2, 0, 1]).
    # Note that some of the objects A, B, C may be lists themselves, we do not
    # look into them and just treat them as arbitrary Python objects unless
    # the user explicitly calls explode().
    self._flat_items = flat_items
    self._shape = shape

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
    new_flat_items = [
        None if x is None else getattr(x, attr_name) for x in self._flat_items
    ]
    return View(new_flat_items, self._shape, is_internal_call=True)

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
      # [2, 3]

    Returns:
      A new view with one more dimension.
    """
    new_edge = arolla.types.DenseArrayEdge.from_sizes(
        [0 if x is None else len(x) for x in self._flat_items]
    )
    arolla_shape = arolla.abc.invoke_op(
        'koda_internal.to_arolla_jagged_shape', (self._shape,)
    )
    new_arolla_shape = arolla.abc.invoke_op(
        'jagged.add_dims', (arolla_shape, new_edge)
    )
    new_shape = arolla.abc.invoke_op(
        'koda_internal.from_arolla_jagged_shape', (new_arolla_shape,)
    )
    # TODO: For dicts, this does not align with Koda ([:] returns
    # values there, while we return keys here).
    new_flat_items = list(
        itertools.chain.from_iterable(
            x for x in self._flat_items if x is not None
        )
    )
    return View(new_flat_items, new_shape, is_internal_call=True)

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
    """Reduces view dimension by grouping items into lists.

    This is an inverse operation to `explode`. It groups items into lists
    according to the shape of topmost `ndim` dimensions. If `ndim` is negative,
    will implode all the way to a scalar.

    Example:
      view_2d = view([[1,2],[3]])[:][:]
      view_2d.implode()
      # The same structure as view([[1,2],[3]])[:], but different list
      # pointers.
      view_2d.implode(ndim=2)
      view_2d.implode(ndim=-1)
      # The same structure as view([[1,2],[3]]), but different list pointers.

    Args:
      ndim: The number of dimensions to implode.

    Returns:
      A new view with `ndim` less dimensions.
    """
    rank = self._shape.rank()
    if ndim < 0:
      ndim = rank
    elif ndim > rank:
      raise ValueError(
          f'Cannot implode by {ndim} dimensions, the shape has only'
          f' {rank} dimensions.'
      )
    if ndim == 0:
      return self
    flat_items = self._flat_items
    for i in range(rank - 1, rank - ndim - 1, -1):
      edge = self._shape[i]
      dim_sizes = arolla.abc.invoke_op('edge.sizes', (edge,))
      new_flat_items = []
      ptr = 0
      for size in dim_sizes.py_value():
        new_ptr = ptr + size
        new_flat_items.append(flat_items[ptr:new_ptr])
        ptr = new_ptr
      assert ptr == len(flat_items)
      flat_items = new_flat_items
    new_shape = self._shape[: rank - ndim]
    return View(flat_items, new_shape, is_internal_call=True)

  # TODO: Once View also stores the root and the path from root,
  # we might want to make this method return a subset of original data
  # structure, instead of just a list of lists, for example:
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

    In case the view has a non-scalar shape, this method creates new
    (potentially nested) Python lists to represent the shape of the view.

    Example:
      view('foo').get()
      # 'foo'
      view([[1,2],[3]])[:].get()
      # [[1,2],[3]], but a different list pointer for the outer list.
      view([[1,2],[3]])[:][:].get()
      # [[1,2],[3]], but all different list pointers.
    """
    return self.implode(ndim=-1)._flat_items[0]  # pylint: disable=protected-access

  def flatten(self) -> View:
    """Flattens all dimensions of the view.

    The result is always a view of rank 1 containing all items in order. Note
    that this does not look into the objects stored at the leaf level,
    so even if they are lists themselves, they will not be flattened.

    Example:
      x = [[1, 2], [3]]
      view(x)[:][:].flatten().get()
      # [1, 2, 3]
      view(x)[:].flatten().get()
      # [[1, 2], [3]]
      view(x).flatten().get()
      # [[[1, 2], [3]]]

    Returns:
      A new view with rank 1.
    """
    arolla_shape = arolla.abc.invoke_op(
        'koda_internal.to_arolla_jagged_shape', (self._shape,)
    )
    new_arolla_shape = arolla.abc.invoke_op(
        'jagged.flatten', (arolla_shape, arolla.int64(0), arolla.unspecified())
    )
    new_shape = arolla.abc.invoke_op(
        'koda_internal.from_arolla_jagged_shape', (new_arolla_shape,)
    )
    return View(self._flat_items, new_shape, is_internal_call=True)

  def expand_to_shape(self, shape: kd.types.JaggedShape) -> View:
    """Expands the view to the given shape.

    The shape of this view must be a prefix of the given shape.

    Args:
      shape: The shape to expand to.

    Returns:
      A new view with the given shape, with the items of this view repeated
      as necessary.
    """
    arolla_self_shape = arolla.abc.invoke_op(
        'koda_internal.to_arolla_jagged_shape', (self._shape,)
    )
    arolla_shape = arolla.abc.invoke_op(
        'koda_internal.to_arolla_jagged_shape', (shape,)
    )
    if not arolla.abc.invoke_op(
        'jagged.is_broadcastable_to', (arolla_self_shape, arolla_shape)
    ):
      raise ValueError(
          'Views do not have a common shape. Shapes: '
          f'{self._shape} and {shape}.'
      )
    # TODO: Move some of this logic to JaggedShape.
    our_rank = self._shape.rank()
    new_rank = shape.rank()
    if our_rank == new_rank:
      return self
    flat_shape = arolla.abc.invoke_op(
        'jagged.flatten',
        (arolla_shape, arolla.int64(our_rank), arolla.unspecified()),
    )
    expand_edge = flat_shape[our_rank]
    sizes = arolla.abc.invoke_op('edge.sizes', (expand_edge,)).py_value()
    new_flat_items = list(
        itertools.chain.from_iterable(
            map(itertools.repeat, self._flat_items, sizes)
        )
    )
    return View(new_flat_items, shape, is_internal_call=True)

  def expand_to(self, other: View) -> View:
    """Expands the view to the shape of other view."""
    return self.expand_to_shape(other.get_shape())

  def internal_get_flat_items(self) -> list[Any]:
    """Returns the flat items of the view.

    This returns a pointer to an internal mutable list, so the caller needs
    to make sure it does not modify it, hence the internal_ prefix.

    We cannot change the storage to use tuple instead of list, as it seems to be
    significantly slower to create tuples than lists in operations.
    """
    return self._flat_items

  def get_shape(self) -> kd.types.JaggedShape:
    """Returns the shape of the view."""
    return self._shape


_SCALAR_SHAPE = kd.shapes.new()


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
    # [2, 3]
    view([[1, 2], [3]])[:].map(lambda x: len(x)).get()
    # [2, 1]

  Args:
    obj: An arbitrary object to create a view for.

  Returns:
    A scalar view on the object.
  """
  return View([obj], _SCALAR_SHAPE, is_internal_call=True)


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
  # TODO: Move some of this logic to JaggedShape.
  shape = max((first, *others), key=lambda l: l.get_shape().rank()).get_shape()
  return (
      first.expand_to_shape(shape),
      *(l.expand_to_shape(shape) for l in others),
  )


# This method is in view.py since we expect to use it from implementations
# of methods of View class.
def map_(f: Callable[..., Any], *args: Any, **kwargs: Any) -> View:
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
    **kwargs: The keyword arguments to pass to the function. They must all be
      views or auto-boxable into views.

  Returns:
    A new view with the function applied to the corresponding items.
  """
  aligned_args = align(*args, *kwargs.values())
  kwnames = tuple(kwargs)
  vcall = arolla.abc.vectorcall
  new_flat_items = map(
      vcall,
      itertools.repeat(f),
      *(arg.internal_get_flat_items() for arg in aligned_args),
      itertools.repeat(kwnames),
  )
  return View(
      list(new_flat_items), aligned_args[0].get_shape(), is_internal_call=True
  )
