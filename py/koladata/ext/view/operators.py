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

from koladata.ext.view import view as view_lib


def align(*args: view_lib.ViewOrAutoBoxType) -> tuple[view_lib.View, ...]:
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
    return (view_lib.box(args[0]),)
  args = [view_lib.box(o) for o in args]
  ref_view = max(args, key=lambda v: v.get_depth())
  return tuple(v.expand_to(ref_view) for v in args)


def get_attr(v: view_lib.ViewOrAutoBoxType, attr_name: str) -> view_lib.View:
  """Returns a new view with the given attribute of each item.

  If one of the items is None, the corresponding value will be None as well,
  instead of raising an error that Python's built-in getattr() would raise.

  Example:
    x = kv.view(types.SimpleNamespace(_b=6))
    kv.get_attr(x, '_b').get()
    # 6

  Args:
    v: The view to get the attribute from. Can also be a Python primitive, which
      will be automatically boxed into a view, but most likely raise an
      exception afterwards, unless it is None.
    attr_name: The name of the attribute to get.
  """
  return view_lib.box(v).get_attr(attr_name)


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
    v: The view to explode. Can also be a Python primitive, which will be
      automatically boxed into a view, but most likely raise an exception
      afterwards, unless it is None.
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
    v: view_lib.ViewOrAutoBoxType, other: view_lib.ViewOrAutoBoxType
) -> view_lib.View:
  """Returns the view expanded to the shape of other view.

  The view must have dimensions that match a prefix of the other view's
  dimensions. The corresponding items then will be repeated among the additional
  dimensions.

  Example:
    x = kv.view([1, None, 2])[:]
    y = kv.view([[], [1, None], [3, 4, 5]])[:][:]
    kv.expand_to(x, y).get()
    # ((), (None, None), (2, 2, 2))

  Args:
    v: The view to expand.
    other: The view to expand to.
  """
  return view_lib.box(v).expand_to(other)
