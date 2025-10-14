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

"""Konstucts Lens API."""

from __future__ import annotations

import itertools
from typing import Any, Callable

from arolla import arolla
from koladata import kd


class Lens:
  """A Lens is a view on a particular path inside an object.

  See the docstring for lens() method for more details.
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
          'Please do not call the Lens constructor directly, use lens()'
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

  def get_attr(self, attr_name: str) -> Lens:
    """Returns a new lens with the given attribute of each item.

    Example:
      x = types.SimpleNamespace(_b=6)
      lens(x).get_attr('_b').get()
      # 6

    Args:
      attr_name: The name of the attribute to get.
    """
    # TODO: In this and similar places, skip over Nones, to support
    # sparse workflows similar to Koda.
    new_flat_items = [getattr(x, attr_name) for x in self._flat_items]
    return Lens(new_flat_items, self._shape, is_internal_call=True)

  def __getattr__(self, attr_name: str) -> Lens:
    """Returns a new lens with the given attribute of each item.

    This is a convenience method for `get_attr`. It allows to write `lens.a`
    instead of `lens.get_attr('a')`. Attributes starting with `_` are not
    supported to avoid conflicts with private attributes of `Lens` class.

    Example:
      x = types.SimpleNamespace(a=1)
      lens(x).a.get()
      # 1

    Args:
      attr_name: The name of the attribute to get.
    """
    if attr_name.startswith('_'):
      raise AttributeError(attr_name)
    return self.get_attr(attr_name)

  # TODO: In this and other places, make sure (and test) that
  # the API aligns with the corresponding Koda API.
  def explode(self) -> Lens:
    """Unnests iterable elements by one level, increasing rank by 1.

    If a lens contains iterable elements, `explode` creates a new lens
    containing elements from those iterables, and increases lens rank by 1.
    This is useful for "diving" into lists within your data structure.
    Usually used via `[:]`.

    It is user's responsibility to ensure that all items are iterable and
    have `len`.

    Example:
      x = types.SimpleNamespace(a=[1, 2])
      lens(x).a.explode().map(lambda i: i + 1).get()
      # [2, 3]

    Returns:
      A new lens with one more dimension.
    """
    new_edge = arolla.types.DenseArrayEdge.from_sizes(
        [len(x) for x in self._flat_items]
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
    new_flat_items = list(itertools.chain.from_iterable(self._flat_items))
    return Lens(new_flat_items, new_shape, is_internal_call=True)

  def __getitem__(self, key: Any) -> Lens:
    """Provides `lens[:]` syntax as a shortcut for `lens.explode()`.

    Example:
      x = types.SimpleNamespace(
          a=[types.SimpleNamespace(b=1), types.SimpleNamespace(b=2)]
      )
      lens(x).a[:].b.get()
      # [1, 2]

    Args:
      key: The key to use for getitem. Only slice() (usually obtained via [:])
        is supported for now.

    Returns:
      The result of `lens.explode()` if `key` is `[:]`, otherwise raises a
      ValueError.
    """
    if isinstance(key, slice):
      if key.start is None and key.stop is None and key.step is None:
        return self.explode()
    raise ValueError(
        'Only everything slice [:] is supported in Lens.__getitem__ yet.'
    )

  def map(self, f: Callable[[Any], Any]) -> Lens:
    """Applies a function to each item in the lens.

    Example:
      x = types.SimpleNamespace(
          a=[types.SimpleNamespace(b=1), types.SimpleNamespace(b=2)]
      )
      lens(x).a[:].b.map(lambda i: i + 1).get()
      # [2, 3]

    Args:
      f: The function to apply.

    Returns:
      A new lens with the function applied to each item.
    """
    new_flat_items = [f(x) for x in self._flat_items]
    return Lens(new_flat_items, self._shape, is_internal_call=True)

  def implode(self, ndim: int = 1) -> Lens:
    """Reduces lens dimension by grouping items into lists.

    This is an inverse operation to `explode`. It groups items into lists
    according to the shape of topmost `ndim` dimensions. If `ndim` is negative,
    will implode all the way to a scalar.

    Example:
      lens_2d = lens([[1,2],[3]])[:][:]
      lens_2d.implode()
      # The same structure as lens([[1,2],[3]])[:], but different list
      # pointers.
      lens_2d.implode(ndim=2)
      lens_2d.implode(ndim=-1)
      # The same structure as lens([[1,2],[3]]), but different list pointers.

    Args:
      ndim: The number of dimensions to implode.

    Returns:
      A new lens with `ndim` less dimensions.
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
    return Lens(flat_items, new_shape, is_internal_call=True)

  # TODO: Once Lens also stores the root and the path from root,
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
    """Returns an object represented by the lens.

    In case the lens has a non-scalar shape, this method creates new
    (potentially nested) Python lists to represent the shape of the lens.

    Example:
      lens('foo').get()
      # 'foo'
      lens([[1,2],[3]])[:].get()
      # [[1,2],[3]], but a different list pointer for the outer list.
      lens([[1,2],[3]])[:][:].get()
      # [[1,2],[3]], but all different list pointers.
    """
    return self.implode(ndim=-1)._flat_items[0]  # pylint: disable=protected-access

  def flatten(self) -> Lens:
    """Flattens all dimensions of the lens.

    The result is always a lens of rank 1 containing all items in order. Note
    that this does not look into the objects stored at the leaf level,
    so even if they are lists themselves, they will not be flattened.

    Example:
      x = [[1, 2], [3]]
      lens(x)[:][:].flatten().get()
      # [1, 2, 3]
      lens(x)[:].flatten().get()
      # [[1, 2], [3]]
      lens(x).flatten().get()
      # [[[1, 2], [3]]]

    Returns:
      A new lens with rank 1.
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
    return Lens(self._flat_items, new_shape, is_internal_call=True)

  def get_shape(self) -> kd.types.JaggedShape:
    """Returns the shape of the lens."""
    return self._shape


_SCALAR_SHAPE = kd.shapes.new()


def lens(obj: Any) -> Lens:
  """Creates a view on an object that can be used for vectorized access.

  A lens represents traversing a particular path in a tree represented
  by the object, with the leaves of that path being the items in the lens,
  and the structure of that path being the shape of the lens.

  For example, consider the following set of objects:

  x = Obj(d=3)
  y = Obj(d=4)
  z = [x, y]
  w = Obj(b=1, c=z)

  Object w can be represented as the following tree:

  w --b--> 1
    --c--> z --item0--> x --d--> 3
             --item1--> y --d--> 4

  Now lens(w) corresponds to just the root of this tree. lens(w).c corresponds
  to traversing edge labeled with c to z. lens(w).c[:] corresponds to traversing
  the edges labeled with item0 and item1 to x and y respectively. lens(w).c[:].d
  corresponds to traversing the edges labeled with d to 3 and 4.

  Example:
    lens([1, 2])[:].map(lambda x: x + 1).get()
    # [2, 3]
    lens([[1, 2], [3]])[:].map(lambda x: len(x)).get()
    # [2, 1]

  Args:
    obj: An arbitrary object to create a lens for.

  Returns:
    A scalar lens view on the object.
  """
  return Lens([obj], _SCALAR_SHAPE, is_internal_call=True)
