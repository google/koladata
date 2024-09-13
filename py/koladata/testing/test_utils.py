# Copyright 2024 Google LLC
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

"""Testing utilities."""

from typing import Any

from arolla import arolla as _arolla
from koladata.exceptions import exceptions as _
from koladata.types import data_bag as _data_bag
from koladata.types import data_slice as _data_slice
from koladata.types import dict_item as _  # pylint: disable=unused-import
from koladata.types import ellipsis as _ellipsis
from koladata.types import jagged_shape as _jagged_shape

_KodaVal = (
    _data_bag.DataBag
    | _data_slice.DataSlice
    | _jagged_shape.JaggedShape
    | type(_ellipsis.ellipsis())
    | _arolla.types.Slice
    | _arolla.Expr
    | None
)


def _expect_data_slice(ds: Any):
  if not isinstance(ds, _data_slice.DataSlice):
    raise TypeError('expected DataSlice, got ', type(ds))


def _assert_expr_equal_by_fingerprint(
    actual_value: _arolla.Expr,
    expected_value: _arolla.Expr,
    *,
    msg: str | None = None,
) -> None:
  """Koda specific expr equality check."""
  # This implementation uses the prettified expr representation rather than the
  # raw string representation to make Koda constructs such as kd.literal()
  # easier to understand.
  if actual_value.fingerprint == expected_value.fingerprint:
    return
  if not msg:
    msg = (
        'Exprs not equal by fingerprint:\n'
        + f'  actual_fingerprint={actual_value.fingerprint}, '
        + f'expected_fingerprint={expected_value.fingerprint}\n'
        + '  actual:\n    '
        + '\n    '.join(f'{actual_value!r}'.split('\n'))
        + '\n  expected:\n    '
        + '\n    '.join(f'{expected_value!r}'.split('\n'))
    )
  raise AssertionError(msg)


def assert_equal(
    actual_value: _KodaVal, expected_value: _KodaVal, *, msg: str | None = None
) -> None:
  """Koda equality check.

  Compares the argument by their fingerprint:
  * 2 DataSlice(s) are equal if their contents and JaggedShape(s) are
    equal / equivalent and they reference the same DataBag instance.
  * 2 DataBag(s) are equal if they are the same DataBag instance.
  * 2 JaggedShape(s) are equal if they have the same number of dimensions and
    all "sizes" in each dimension are equal.

  NOTE: For JaggedShape equality and equivalence are the same thing.

  Args:
    actual_value: DataSlice, DataBag or JaggedShape.
    expected_value: DataSlice, DataBag or JaggedShape.
    msg: A custom error message.

  Raises:
    AssertionError: If actual_qvalue and expected_qvalue are not equal.
  """
  # NOTE: None occurs frequently when comparing DataBag(s) from DataSlice(s).
  if actual_value is None and expected_value is None:
    return
  if isinstance(actual_value, _arolla.Expr) and isinstance(
      expected_value, _arolla.Expr
  ):
    _assert_expr_equal_by_fingerprint(actual_value, expected_value, msg=msg)
    return
  _arolla.testing.assert_qvalue_equal_by_fingerprint(
      actual_value, expected_value, msg=msg
  )


def _assert_equivalent_bags(
    actual_value: _data_bag.DataBag,
    expected_value: _data_bag.DataBag,
    *,
    msg: str | None = None,
):
  """Asserts that DataBags are equivalent."""
  if actual_value is None and expected_value is None:
    return
  if (
      actual_value is not None
      and expected_value is not None
      and (
          actual_value._exactly_equal(expected_value)  # pylint: disable=protected-access
      )
  ):
    return
  raise AssertionError(
      msg
      or (
          f'DataBags are not equivalent\n\n{actual_value.contents_repr()!r} !='
          f' {expected_value.contents_repr()!r}'
      )
  )


def assert_equivalent(
    actual_value: _KodaVal, expected_value: _KodaVal, *, msg: str | None = None
):
  """Koda equivalency check.

  * 2 DataSlice(s) are equivalent if their contents and JaggedShape(s) are
    equivalent and their DataBag(s) have the same contents (including the
    distribution of data in fallback DataBag(s)).
  * 2 DataBag(s) are equivalent if their contents are the same (including the
    distribution of data in fallback DataBag(s).
  * 2 JaggedShape(s) are equivalent if they are equal, i.e. if sizes / edges
    across all their dimensions are the same.

  Args:
    actual_value: DataSlice, DataBag or JaggedShape.
    expected_value: DataSlice, DataBag or JaggedShape.
    msg: A custom error message.

  Raises:
    AssertionError: If actual_value.fingerprint and expected_value.fingerprint
      are not equal.
  """
  # NOTE: None occurs frequently when comparing DataBag(s) from DataSlice(s).
  if actual_value is None and expected_value is None:
    return
  if isinstance(actual_value, _data_bag.DataBag) and isinstance(
      expected_value, _data_bag.DataBag
  ):
    _assert_equivalent_bags(actual_value, expected_value, msg=msg)
    return
  if isinstance(actual_value, _data_slice.DataSlice) and isinstance(
      expected_value, _data_slice.DataSlice
  ):
    _arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_value.with_db(None), expected_value.with_db(None), msg=msg
    )
    try:
      _assert_equivalent_bags(actual_value.db, expected_value.db)
    except AssertionError:
      raise AssertionError(
          msg
          or (
              'DataSlices are not equivalent, because their DataBags are not'
              f' equivalent\n\n  {actual_value.db.contents_repr()!r} !='
              f' {expected_value.db.contents_repr()!r}'
          )
      ) from None
    return
  _arolla.testing.assert_qvalue_equal_by_fingerprint(
      actual_value, expected_value, msg=msg
  )


def _as_arolla_value(ds: _data_slice.DataSlice) -> _arolla.QValue:
  try:
    return ds.as_arolla_value()
  except ValueError:
    raise AssertionError(
        f'{ds!r} cannot be converted to Arolla value'
    ) from None


def _assert_equal_shape(
    actual_value: _data_slice.DataSlice,
    expected_value: _data_slice.DataSlice,
):
  assert_equal(
      actual_value.get_shape(),
      expected_value.get_shape(),
      msg=(
          f'{actual_value!r} and {expected_value!r} have different shapes\n\n'
          f'  {actual_value.get_shape()} != {expected_value.get_shape()}'
      ),
  )


def _assert_equal_schema(
    actual_value: _data_slice.DataSlice,
    expected_value: _data_slice.DataSlice,
):
  assert_equal(
      actual_value.get_schema().with_db(None),
      expected_value.get_schema().with_db(None),
      msg=(
          f'{actual_value!r} and {expected_value!r} have different schemas\n\n'
          f'  {actual_value.get_schema()} != {expected_value.get_schema()}'
      ),
  )


def assert_allclose(
    actual_value: _data_slice.DataSlice,
    expected_value: _data_slice.DataSlice,
    *,
    rtol: float | None = None,
    atol: float = 0.0,
):
  """Koda variant of NumPy's allclose predicate.

  See the NumPy documentation for numpy.testing.assert_allclose.

  The main difference from the numpy is that assert_allclose works with Koda
  DataSlice(s) and checks that actual_value and expected_value have close values
  under the hood.

  It also supports sparse array types.

  Args:
    actual_value: DataSlice.
    expected_value: DataSlice.
    rtol: Relative tolerance.
    atol: Absolute tolerance.

  Raises:
    AssertionError: If actual_value and expected_value values are not close up
      to the given tolerance or shape and DataBag are not equivalent and their
      check was requested.
  """
  _expect_data_slice(actual_value)
  _expect_data_slice(expected_value)
  _assert_equal_shape(actual_value, expected_value)
  _assert_equal_schema(actual_value, expected_value)
  _arolla.testing.assert_qvalue_allclose(
      _as_arolla_value(actual_value),
      _as_arolla_value(expected_value),
      rtol=rtol,
      atol=atol,
  )
  assert_equal(
      actual_value.db, expected_value.db, msg='inputs have different DataBags'
  )


def _expect_dicts(dicts: _data_slice.DataSlice):
  try:
    _expect_data_slice(dicts)
    _ = dicts.get_keys()
  except ValueError:
    raise AssertionError(f'expected Koda Dicts, got {dicts!r}') from None


def assert_unordered_equal(
    actual_value: _data_slice.DataSlice,
    expected_value: _data_slice.DataSlice,
):
  """Checks DataSlices are equal ignoring the ordering in the last dimension.

  This assertion verifies actual_value and expected_value have the same
  shapes, schemas, dbs and that their items in the last dimensions are equal
  ignoring the order.

  Args:
    actual_value: DataSlice.
    expected_value: DataSlice.

  Raises:
    AssertionError: If DataSlices are not equal.
  """
  _expect_data_slice(actual_value)
  _expect_data_slice(expected_value)
  _assert_equal_shape(actual_value, expected_value)
  _assert_equal_schema(actual_value, expected_value)
  assert_equal(
      actual_value.db, expected_value.db, msg='inputs have different DataBags'
  )
  # Checking from the last dimension.
  actual_val = actual_value.flatten(0, -1)
  expected_val = expected_value.flatten(0, -1)
  for actual, expected in zip(
      actual_val.internal_as_py(), expected_val.internal_as_py()
  ):
    is_list = isinstance(actual, list)
    if (is_list and set(actual) != set(expected)) or (
        not is_list and actual != expected
    ):
      raise AssertionError(
          f'Unordered DataSlice {actual_value!r} != {expected_value!r}'
      )


def assert_dicts_keys_equal(
    dicts: _data_slice.DataSlice,
    expected_keys: _data_slice.DataSlice,
):
  """Koda check for Dict keys equality.

  Koda Dict keys are stored and returned in arbitrary order. When they are also
  not-flat, it is difficult to compare them using other assertion primitives.

  This assertion verifies dicts.get_keys() and expected_keys have the same
  shapes, schemas and that their contents have the same values and their count.

  NOTE: This assertion method ignores DataBag(s) associated with the inputs.

  Args:
    dicts: DataSlice.
    expected_keys: DataSlice.

  Raises:
    AssertionError: If dicts.get_keys() and expected_keys cannot represent the
      keys of the same dict.
  """
  _expect_dicts(dicts)
  assert_unordered_equal(dicts.get_keys().no_db(), expected_keys.no_db())


def assert_dicts_values_equal(
    dicts: _data_slice.DataSlice,
    expected_values: _data_slice.DataSlice,
):
  """Koda check for Dict values equality.

  Koda Dict values are stored and returned in arbitrary order. When they are
  also not-flat, it is difficult to compare them using other assertion
  primitives.

  This assertion verifies dicts.get_values() and expected_values have the same
  shapes, schemas and that their contents have the same values and their count.

  NOTE: This assertion method ignores DataBag(s) associated with the inputs.

  Args:
    dicts: DataSlice.
    expected_values: DataSlice.

  Raises:
    AssertionError: If dicts.get_values() and expected_values cannot represent
      the values of the same dict.
  """
  _expect_dicts(dicts)
  assert_unordered_equal(dicts.get_values().no_db(), expected_values.no_db())


def assert_dicts_equal(
    actual_dict: _data_slice.DataSlice,
    expected_dict: _data_slice.DataSlice,
):
  """Koda check for Dict equality.

  Koda Dict equality check includes checking the DataSlice(s) have the same
  shape and schema. In addition, it verifies that the keys fetched from their
  corresponding DataBag(s) are the same (regardless of their order in the last
  dimension) and that the returned Dict values for those keys are the same.

  NOTE: This assertion method checks for DataBag equality referenced by inputs.

  NOTE: It also does not verify that ItemId(s) inside `actual_dict` and
  `expected_dict` DataSlices are the same.

  Args:
    actual_dict: DataSlice.
    expected_dict: DataSlice.

  Raises:
    AssertionError: If actual_dict and expected_dict do not represent equal Koda
      Dicts.
  """
  _expect_dicts(actual_dict)
  _expect_dicts(expected_dict)
  _assert_equal_shape(actual_dict, expected_dict)
  assert_dicts_keys_equal(actual_dict, expected_dict.get_keys())
  same_order_keys = actual_dict.get_keys()
  assert_equivalent(
      # We need to skip checking the DataBags, as dict ItemId(s) are usually
      # different.
      actual_dict[same_order_keys].with_db(None),
      expected_dict[same_order_keys].with_db(None),
  )


def assert_nested_lists_equal(
    actual_list: _data_slice.DataSlice,
    expected_list: _data_slice.DataSlice,
):
  """Koda check for nested List equality.

  This checks that the DataSlices have the same shape and schema, and that the
  corresponding values are either the same, or are nested Lists with the same
  structure containing equivalent non-List values.

  Args:
    actual_list: DataSlice.
    expected_list: DataSlice.

  Raises:
    AssertionError: If actual_dict and expected_dict do not represent equal Koda
      nested Lists.
  """
  _expect_data_slice(actual_list)
  _expect_data_slice(expected_list)
  _assert_equal_schema(actual_list, expected_list)
  _assert_equal_shape(actual_list, expected_list)

  # Explode as many times as both actual and expected will allow, except if
  # both are empty, which would allow infinite explosion.
  while not (actual_list.is_empty() or expected_list.is_empty()):
    try:
      actual_list = actual_list[:]
      expected_list = expected_list[:]
    except ValueError:
      break

  assert_equivalent(actual_list.no_db(), expected_list.no_db())
