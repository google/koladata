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


def align(
    first: view_lib.ViewOrAutoBoxType, *others: view_lib.ViewOrAutoBoxType
) -> tuple[view_lib.View, ...]:
  """Aligns the views to a common shape.

  We will also apply auto-boxing if some inputs are not views but can be
  automatically boxed into one.

  Args:
    first: The first argument to align.
    *others: The remaining arguments to align.

  Returns:
    A tuple of aligned views, of size len(others) + 1.
  """
  first = view_lib.box(first)
  if not others:
    return (first,)
  others = tuple(view_lib.box(o) for o in others)
  ref_view = max((first, *others), key=lambda l: l.get_depth())
  return (
      first.expand_to(ref_view),
      *(l.expand_to(ref_view) for l in others),
  )


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


def flatten(v: view_lib.ViewOrAutoBoxType) -> view_lib.View:
  """Flattens all dimensions of the view.

  The result is always a view of depth 1 containing all items in order. Note
  that this does not look into the objects stored at the leaf level,
  so even if they are tuples themselves, they will not be flattened.

  Example:
    x = kv.view([[1, 2], [3]])
    kv.flatten(x[:][:]).get()
    # (1, 2, 3)
    kv.flatten(x[:]).get()
    # ([1, 2], [3])
    kv.flatten(x).get()
    # ([[1, 2], [3]],)

  Args:
    v: The view to flatten. Can also be a Python primitive, which will be
      automatically boxed into a view.

  Returns:
    A new view with rank 1.
  """
  return view_lib.box(v).flatten()


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
