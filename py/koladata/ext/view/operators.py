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

"""Operators that work on views.

For operators that are also available as a View method, we just call it here
after auto-boxing the first argument. We cannot do the reverse and call an
operator from the implementation of View, as that would create a dependency
cycle. Note that we don't need to auto-box other arguments, as they should
be auto-boxed by the View method that we call.

The tests are in operator_tests/ folder.
"""

from typing import Any, Callable
from koladata.ext.view import view as view_lib
from optree import pytree


def get_attr(
    v: view_lib.ViewOrAutoBoxType,
    attr_name: str,
    default: Any = view_lib.NO_DEFAULT,
) -> view_lib.View:
  """Returns a new view with the given attribute of each item.

  If one of the items is None, the corresponding value will be None as well,
  instead of raising an error that Python's built-in getattr() would raise.

  Example:
    x = kv.view(types.SimpleNamespace(_b=6))
    kv.get_attr(x, '_b').get()
    # 6

  Args:
    v: The view to get the attribute from.
    attr_name: The name of the attribute to get.
    default: When specified, if the attribute value is None or getting the
      attribute raises AttributeError, this value will be used instead.
  """
  return view_lib.box(v).get_attr(attr_name, default)


def set_attrs(
    v: view_lib.ViewOrAutoBoxType, /, **attrs: view_lib.ViewOrAutoBoxType
):
  """Sets the given attributes of each item.

  If one of the items in `v` is None, the corresponding value will be ignored.
  If one of the items in `attrs` is None, the attribute of the corresponding
  item will be set to None.

  If the same object has multiple references in `v`, we will process the
  set_attrs in order, so the attribute will have the last assigned value.

  Example:
    o = kv.view([types.SimpleNamespace(), types.SimpleNamespace()])[:]
    kv.set_attrs(o, a=1, _b=kv.view([None, 2])[:])
    o.get()
    # (namespace(a=1, _b=None), namespace(a=1, _b=2))

  Args:
    v: The view to set the attribute for.
    **attrs: The values to set the attributes to. Can also be a Python
      primitive, which will be automatically boxed into a view.
  """
  view_lib.box(v).set_attrs(**attrs)


def set_item(
    v: view_lib.ViewOrAutoBoxType,
    key_or_index: view_lib.ViewOrAutoBoxType,
    value: view_lib.ViewOrAutoBoxType,
):
  """Sets an item or items for all containers in the view.

  This essentially calls `(x[y] = z) for x, y, z in zip(v, key_or_index,
  value)`, but with additions:
  - when `key_or_index` or `value` are views or auto-boxable into a view, we
    first align all arguments.
  - if `x` is None or `y` is None, we skip setting the item.
  - if `x[y] = z` raises IndexError, we catch it and ignore it.

  If the same (object, key) pair appears multiple times, we will process the
  assignments in order, so the attribute will have the last assigned value.

  Example:
    x = [{}, {'a': 1}]
    kv.set_item(kv.view(x)[:], 'a', kv.view([10, 20])[:])
    # x is now [{'a': 10}, {'a': 20}]

  Args:
    v: The view containing the collections to set items in.
    key_or_index: The key or index to set.
    value: The value to set.
  """
  view_lib.box(v).set_item(key_or_index, value)


def append(
    v: view_lib.ViewOrAutoBoxType,
    value: view_lib.ViewOrAutoBoxType,
):
  """Appends an item or items to all containers in the view.

  This essentially calls `x.append(y) for x, y in `zip(v, value)`, but with
  additions:
  - when `value` is a view or auto-boxable into a view, we first align all
    arguments.
  - if `x` is None, we skip appending the item.

  If the same list object appears multiple times in `v`, or `v` has lower depth
  than `value`, we will append all corresponding values in order. Where in
  Python one would call `list1.extend(list2)`, we can achieve the same effect
  with `kv.append(kv.view(list1), kv.view(list2)[:])`.

  Example:
    x = [[], [1]]
    kv.append(kv.view(x)[:], kv.view([10, 20])[:])
    # x is now [[10], [1, 20]]
    kv.append(kv.view(x)[:], kv.view([[30, 40], [50]])[:][:])
    # x is now [[10, 30, 40], [1, 20, 50]]
    kv.append(kv.view(x)[:], kv.view(None))
    # x is now [[10, 30, 40, None], [1, 20, 50, None]]

  Args:
    v: The view containing the lists to append items to.
    value: The value to append.
  """
  view_lib.box(v).append(value)


def explode(v: view_lib.ViewOrAutoBoxType, ndim: int = 1) -> view_lib.View:
  """Unnests iterable elements, increasing rank by `ndim`.

  If a view contains iterable elements, `explode` with `ndim=1` creates a new
  view containing elements from those iterables, and increases view rank by 1.
  This is useful for "diving" into lists within your data structure.
  Usually used via `[:]`.

  `ndim=2` applies the same transformation twice, and so on.

  It is user's responsibility to ensure that all items are iterable and
  have `len`.

  If one of the items is None, it will be treated as an empty iterable,
  instead of raising an error that len() would raise.

  Example:
    x = kv.view(types.SimpleNamespace(a=[1, 2]))
    kv.explode(x).map(lambda i: i + 1).get()
    # (2, 3)

  Args:
    v: The view to explode.
    ndim: The number of dimensions to explode. Must be non-negative.

  Returns:
    A new view with `ndim` more dimensions.
  """
  return view_lib.box(v).explode(ndim=ndim)


def implode(v: view_lib.ViewOrAutoBoxType, ndim: int = 1) -> view_lib.View:
  """Reduces view dimension by grouping items into tuples.

  This is an inverse operation to `explode`. It groups items into tuples
  according to the shape of topmost `ndim` dimensions. If `ndim` is negative,
  will implode all the way to a scalar.

  Example:
    view_2d = kv.view([[1,2],[3]])[:][:]
    kv.implode(view_2d)
    # The same structure as view([(1,2),(3,)])[:].
    kv.implode(view_2d, ndim=2)
    kd.implode(view_2d, ndim=-1)
    # The same structure as view(((1,2),(3,))).

  Args:
    v: The view to implode.
    ndim: The number of dimensions to implode.

  Returns:
    A new view with `ndim` fewer dimensions.
  """
  return view_lib.box(v).implode(ndim=ndim)


def flatten(
    v: view_lib.ViewOrAutoBoxType, from_dim: int = 0, to_dim: int | None = None
) -> view_lib.View:
  """Flattens the specified dimensions of the view.

  Indexing works as in Python:
  * If `to_dim` is unspecified, `to_dim = get_depth()` is used.
  * If `to_dim < from_dim`, `to_dim = from_dim` is used.
  * If `to_dim < 0`, `max(0, to_dim + get_depth())` is used. The same goes for
    `from_dim`.
  * If `to_dim > get_depth()`, `get_depth()` is used. The same goes for
  `from_dim`.

  The above-mentioned adjustments places both `from_dim` and `to_dim` in the
  range `[0, get_depth()]`. After adjustments, the new View has `get_depth() ==
  old_rank - (to_dim - from_dim) + 1`. Note that if `from_dim == to_dim`, a
  "unit" dimension is inserted at `from_dim`.

  Note that this does not look into the objects stored at the leaf level,
  so even if they are tuples or lists themselves, they will not be flattened.

  Example:
    x = kv.view([[1, 2], [3]])
    kv.flatten(x[:][:]).get()
    # (1, 2, 3)
    kv.flatten(x[:]).get()
    # ([1, 2], [3])
    kv.flatten(x).get()
    # ([[1, 2], [3]],)
    kv.flatten(x[:][:], 1).get()
    # ((1, 2), (3,))
    kv.flatten(x[:][:], -1).get()
    # ((1, 2), (3,))
    kv.flatten(x[:][:], 2).get()
    # (((1,), (2,)), ((3,),))
    kv.flatten(x[:][:], 1, 1).get()
    # (((1,), (2,)), ((3,),))

  Args:
    v: The view to flatten. Can also be a Python primitive, which will be
      automatically boxed into a view.
    from_dim: The dimension to start flattening from.
    to_dim: The dimension to end flattening at, or None to flatten until the
      last dimension.

  Returns:
    A new view with the specified dimensions flattened.
  """
  return view_lib.box(v).flatten(from_dim, to_dim)


def expand_to(
    v: view_lib.ViewOrAutoBoxType,
    other: view_lib.ViewOrAutoBoxType,
    ndim: int = 0,
) -> view_lib.View:
  """Returns the view expanded to the shape of other view.

  The view must have dimensions that match a prefix of the other view's
  dimensions. The corresponding items then will be repeated among the additional
  dimensions.

  When `ndim` is set, the expansion is performed in 3 steps:
  1) the last N dimensions of `v` are first imploded into tuples
  2) the expansion operation is performed on the View of those tuples
  3) the tuples in the expanded View are exploded

  Example:
    x = kv.view([1, None, 2])[:]
    y = kv.view([[], [1, None], [3, 4, 5]])[:][:]
    kv.expand_to(x, y).get()
    # ((), (None, None), (2, 2, 2))
    kv.expand_to(x, y, ndim=1).get()
    # ((), ((1, None, 2), (1, None, 2)), ((1, None, 2), (1, None, 2), (1, None,
    # 2)))

  Args:
    v: The view to expand.
    other: The view to expand to.
    ndim: the number of dimensions to implode before expansion and explode back
      afterwards.
  """
  return view_lib.box(v).expand_to(other, ndim)


def get_item(
    v: view_lib.ViewOrAutoBoxType,
    key_or_index: view_lib.ViewOrAutoBoxType | slice,
) -> view_lib.View:
  """Returns an item or items from the given view containing containers.

  This essentially calls `[x[y] for x, y in zip(v, key_or_index)]`, but
  with some additions:
  - when `key_or_index` is a slice (`v[a:b]` syntax), we add a new
    dimension to the resulting view that corresponds to iterating over the
    requested range of indices.
  - when `key_or_index` is a view or auto-boxable into a view, we first align
    it with `v`. See the examples below for more details.
  - if x[y] raises IndexError or KeyError, we catch it and return None for
    that item instead.

  Example:
    x = [
        types.SimpleNamespace(
          a=[types.SimpleNamespace(b=1), types.SimpleNamespace(b=2)]
        ),
        types.SimpleNamespace(
          a=[types.SimpleNamespace(b=3)]
        ),
    ]
    kv.get_item(kv.get_item(kv.view(x), slice(None)).a, slice(None)).b.get()
    # ((1, 2), (3,))
    # Shorter syntax for the same result:
    kv.view(x)[:].a[:].b.get()
    # ((1, 2), (3,))
    kv.view(x)[:].a[:-1].b.get()
    # ((1,), ())
    # Get the second element from each list (`key_or_index` is expanded to `v`):
    kv.view(x)[:].a[2].b.get()
    # (2, None)

    y = [{'a': 1, 'b': 2}, {'a': 3, 'c': 4}]
    # Get the value for 'a' from each dict (`key_or_index` is expanded to `v`):
    kv.get_item(kv.view(y)[:], 'a').get()
    # (1, 3)
    kv.get_item(kv.view(y)[:], 'c').get()
    # (None, 4)
    # Get the value for the corresponding key from each dict (`key_or_index` has
    # same shape as `v`):
    kv.get_item(kv.view(y)[:], kv.view(['b', 'c'])[:]).get()
    # (2, 4)
    # Get the value for multiple keys from each dict (`v` is expanded to
    # `key_or_index`):
    kv.get_item(kv.view(y)[:],
                kv.view([['b', 'a'], ['a', 'b', 'c']])[:][:]).get()
    # ((2, 1), (3, None, 4))

  Args:
    v: The view containing the collections to get items from.
    key_or_index: The key or index or a slice or indices to get.
  """
  return view_lib.box(v).get_item(key_or_index)


def take(
    v: view_lib.ViewOrAutoBoxType,
    index: view_lib.ViewOrAutoBoxType,
) -> view_lib.View:
  """Returns a view with the items at the given index in the last dimension.

  This is a shortcut for `kv.get_item(kv.implode(v), index)`. This also implies
  the broadcasting behavior, for example `index` must have compatible shape with
  `kv.implode(v)`.

  Example:
    x = kv.view([1, 2, 3])[:]
    kv.take(x, 1).get()
    # 2
    kv.take(x, -1).get()
    # 3
    kv.take(x, kv.view([1, 2, 3, 4])[:]).get()
    # (2, 3, None, None)

  Args:
    v: The view to take the index from. It must have at least one dimension.
    index: The index in the last dimension of `v` to take the item from.
  """
  return view_lib.box(v).take(index)


def group_by(
    v: view_lib.ViewOrAutoBoxType,
    *keys: view_lib.ViewOrAutoBoxType,
    sort: bool = False,
) -> view_lib.View:
  """Returns `v` with values in last dimension grouped using a new dimension.

  The resulting View has depth increased by 1. The first `v.get_depth() - 1`
  dimensions are unchanged. The last two dimensions correspond to the groups
  and the items within the groups. Elements within the same group are ordered by
  the appearance order in `v`.

  `keys` are used for the grouping keys. If length of `keys` is greater than 1,
  the key is a tuple. If `keys` is empty, the key is `v`.

  If sort=True groups are ordered by the grouping key, otherwise groups are
  ordered by the appearance of the first object in the group.

  Example 1:
    v: kv.view([1, 3, 2, 1, 2, 3, 1, 3])[:]
    result: kv.view([[1, 1, 1], [3, 3, 3], [2, 2]])[:][:]

  Example 2:
    v: kv.view([1, 3, 2, 1, 2, 3, 1, 3])[:], sort=True
    result: kv.view([[1, 1, 1], [2, 2], [3, 3, 3]])[:][:]

  Example 3:
    v: kv.view([[1, 2, 1, 3, 1, 3], [1, 3, 1]])[:][:]
    result: kv.view([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]])[:][:][:]

  Example 4:
    v: kv.view([1, 3, 2, 1, None, 3, 1, None])[:]
    result: kv.view([[1, 1, 1], [3, 3], [2]])[:][:]

    Missing values are not listed in the result.

  Example 5:
    v:    kv.view([1, 2, 3, 4, 5, 6, 7, 8])[:],
    key1: kv.view([7, 4, 0, 9, 4, 0, 7, 0])[:],
    result: kv.view([[1, 7], [2, 5], [3, 6, 8], [4]])[:][:]

    When *keys is present, `v` is not used for the key.

  Example 6:
    v:    kv.view([1, 2, 3, 4, None, 6, 7, 8])[:],
    key1: kv.view([7, 4, 0, 9, 4,    0, 7, None])[:],
    result: kv.view([[1, 7], [2, None], [3, 6], [4]])[:][:]

    Items with missing key are not listed in the result.
    Missing `v` values are missing in the result.

  Example 7:
    v:    kv.view([ 1,   2,   3,   4,   5,   6,   7,   8])[:],
    key1: kv.view([ 7,   4,   0,   9,   4,   0,   7,   0])[:],
    key2: kv.view(['A', 'D', 'B', 'A', 'D', 'C', 'A', 'B'])[:],
    result: kv.view([[1, 7], [2, 5], [3, 8], [4], [6]])[:][:]

    When *keys has two or more values, the key is a tuple.
    In this example we have the following groups:
    (7, 'A'), (4, 'D'), (0, 'B'), (9, 'A'), (0, 'C')

  Args:
    v: the view to group.
    *keys: the keys to group by. All views must have the same shape as `v`.
      Scalar views are not supported. If not present, `v` is used as the key.
    sort: Whether groups in the result should be ordered by the grouping key.

  Returns:
    A view with items within the last dimension reordered into groups and
    injected grouped by dimension.
  """
  return view_lib.box(v).group_by(*keys, sort=sort)


def collapse(v: view_lib.ViewOrAutoBoxType, ndim: int = 1) -> view_lib.View:
  """Collapses equal items along the specified number dimensions of the view.

  Example:
    x = kv.view([[1, 1, None, 1], [2, 3], []])[:]
    kv.collapse(x).get()
    # (1, None, None)
    kv.collapse(x, ndim=2).get()
    # None

  Args:
    v: The view to collapse.
    ndim: The number of dimensions to collapse.

  Returns:
    A new view with `ndim` fewer dimensions. The value of each item is equal
    to the value of its uncollapsed items if they are the same, or None
    otherwise.
  """
  return view_lib.box(v).collapse(ndim)


def select(
    v: view_lib.ViewOrAutoBoxType,
    fltr: view_lib.ViewOrAutoBoxType,
    expand_filter: bool = True,
) -> view_lib.View:
  """Creates a new view by filtering out items where filter is not present.

  The dimensions of `fltr` needs to be compatible with the dimensions of `v`.
  By default, `fltr` is expanded to 'v' and items in `v` corresponding to
  missing items in `fltr` are removed. The last dimension of the resulting
  view is changed while the first N-1 dimensions are the same as those in
  `v`.

  Example:
    val = kv.view([[1, None, 4], [None], [2, 8]])[:][:]
    kv.select(val, val > 3).get()
    # ((4,), (), (8,))
    fltr = kv.view(
        [[None, kv.present, kv.present], [kv.present], [kv.present, None]]
    )[:][:]
    kv.select(val, fltr).get()
    # ((None, 4), (None,), (2))

    fltr = kv.view([kv.present, kv.present, None])[:]
    kv.select(val, fltr)
    # ((1, None, 4), (None,), ())
    kv.select(val, fltr, expand_filter=False)
    # ((1, None, 4), (None,))

  Args:
    v: View with depth > 0 to be filtered.
    fltr: filter view with values kv.present or None.
    expand_filter: flag indicating if the 'fltr' should be expanded to 'v'. When
      False, we will remove items at the level of `fltr`.

  Returns:
    Filtered view.
  """
  return view_lib.box(v).select(fltr, expand_filter)


def inverse_select(
    v: view_lib.ViewOrAutoBoxType, fltr: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """Creates a view by putting items in `v` to present positions in `fltr`.

  The depth of `v` and `fltr` must be the same.
  The number of items in `v` must be equal to the number of present items in
  `fltr`.

  Example:
    v = kv.view([[1, None], [2]])[:][:]
    fltr = kv.view([[None, kv.present, kv.present], [kv.present, None]])[:][:]
    kv.inverse_select(v, fltr).get()
    # ((None, 1, None), (2, None))

  The most common use case of inverse_select is to restore the shape of the
  original view after applying select and performing some operations on
  the subset of items in the original view. E.g.
    a = kv.view(...)
    fltr = a > 0
    filtered_v = kv.select(a, fltr)
    # do something on filtered_v
    a = kv.inverse_select(filtered_v, fltr) | a

  Args:
    v: view to be inverse filtered.
    fltr: filter view with values kv.present or None.

  Returns:
    Inverse filtered view.
  """
  return view_lib.box(v).inverse_select(fltr)


def apply_mask(
    a: view_lib.ViewOrAutoBoxType, b: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """An alias for `a & b`.

  Returns the values from `a` where `b` is present, and None otherwise.

  Args:
    a: The view to apply the mask to.
    b: The mask to apply. Must only have `kv.present` and `None` values.

  Returns:
    A new view with only the requested values from `a`.
  """
  return view_lib.box(a) & b


def coalesce(
    a: view_lib.ViewOrAutoBoxType, b: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """An alias for `a | b`.

  Returns the values from `a` where they are present, or the values from `b`
  otherwise.

  Args:
    a: The view to coalesce.
    b: The view to coalesce with.

  Returns:
    A new view with the values from `a` and `b` combined.
  """
  return view_lib.box(a) | b


def equal(
    a: view_lib.ViewOrAutoBoxType, b: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """An alias for `a == b`.

  Compares the items of `a` and `b` and returns a view with `kv.present` if the
  corresponding items are equal and `None` otherwise.

  Args:
    a: The view to compare.
    b: The view to compare with.

  Returns:
    A new view with the comparison result.
  """
  return view_lib.box(a) == b


def not_equal(
    a: view_lib.ViewOrAutoBoxType, b: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """An alias for `a != b`.

  Compares the items of `a` and `b` and returns a view with `kv.present` if the
  corresponding items are not equal and `None` otherwise.

  Args:
    a: The view to compare.
    b: The view to compare with.

  Returns:
    A new view with the comparison result.
  """
  return view_lib.box(a) != b


def less(
    a: view_lib.ViewOrAutoBoxType, b: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """An alias for `a < b`.

  Compares the items of `a` and `b` and returns a view with `kv.present` if the
  corresponding items are less and `None` otherwise.

  Args:
    a: The view to compare.
    b: The view to compare with.

  Returns:
    A new view with the comparison result.
  """
  return view_lib.box(a) < b


def less_equal(
    a: view_lib.ViewOrAutoBoxType, b: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """An alias for `a <= b`.

  Compares the items of `a` and `b` and returns a view with `kv.present` if the
  corresponding items are less or equal and `None` otherwise.

  Args:
    a: The view to compare.
    b: The view to compare with.

  Returns:
    A new view with the comparison result.
  """
  return view_lib.box(a) <= b


def greater(
    a: view_lib.ViewOrAutoBoxType, b: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """An alias for `a > b`.

  Compares the items of `a` and `b` and returns a view with `kv.present` if the
  corresponding items are greater and `None` otherwise.

  Args:
    a: The view to compare.
    b: The view to compare with.

  Returns:
    A new view with the comparison result.
  """
  return view_lib.box(a) > b


def greater_equal(
    a: view_lib.ViewOrAutoBoxType, b: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """An alias for `a >= b`.

  Compares the items of `a` and `b` and returns a view with `kv.present` if the
  corresponding items are greater or equal and `None` otherwise.

  Args:
    a: The view to compare.
    b: The view to compare with.

  Returns:
    A new view with the comparison result.
  """
  return view_lib.box(a) >= b


def deep_clone(v: view_lib.ViewOrAutoBoxType) -> view_lib.View:
  """Returns a deep copy of the given view.

  Utilizes `copy.deepcopy` in the implementation. See its documentation for how
  to customize the copying behavior.

  Args:
    v: The view to deep copy.
  """
  return view_lib.box(v).deep_clone()


# TODO: Implement an equivalent to deep_map in Koda.
def deep_map(
    f: Callable[..., Any],
    *args: view_lib.ViewOrAutoBoxType,
    include_missing: bool = False,
    namespace: str = '',
) -> view_lib.View:
  """Applies a function to every nested primitive value in the args views.

  All arguments will be broadcasted to a common shape based on the depth of the
  views. There must be at least one argument.

  Unlike `map`, which applies the function to each value at the current depth,
  `deep_map` traverses nested structures indiscriminately using
  `optree.tree_map` keeping structures intact. See https://optree.readthedocs.io
  for more details on how to register handlers for custom types.

  Example:
    kv.deep_map(lambda x: x * 2, kv.view([1, None, 2])).get()
    # [2, None, 4]
    kv.deep_map(lambda x: x * 2, kv.view([{'x': 1, 'y': 2, 'z': None}])).get()
    # [{'x': 2, 'y': 4, 'z': None}]
    kv.deep_map(
        lambda x, y: x + y,
        kv.view([[1, 2], [3, 4]])[:],
        kv.view([5, 6])
    ).get()
    # [[6, 8], [8, 10]]
    # `[5, 6]` is broadcasted to ([5, 6], [5, 6]) before mapped.

  Args:
    f: The function to apply.
    *args: The views to apply the function to.
    include_missing: Specifies whether `f` applies to all items (`=True`) or
      only to present items (`=False`).
    namespace: The namespace to use for the custom type handler.

  Returns:
    A new view with the function applied to every nested primitive value.
  """
  if include_missing:
    fn = f
  else:
    fn = lambda *args: None if any(arg is None for arg in args) else f(*args)
  # NOTE: `none_is_leaf=False` raises for `pytree.map(fn, [None], [1])`, so we
  # avoid this altogether.
  map_fn = lambda *args: pytree.map(
      fn, *args, none_is_leaf=True, namespace=namespace
  )
  # NOTE: `include_missing=True` to force structural equality even in case of
  # missing data.
  return view_lib.map_(map_fn, *args, include_missing=True)
