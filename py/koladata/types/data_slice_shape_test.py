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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import dict_item as _
from koladata.types import jagged_shape
from koladata.types import list_item as _
from koladata.types import mask_constants
from koladata.types import schema_constants


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals

present = mask_constants.present


class DataSliceShapeTest(parameterized.TestCase):

  def test_internal_as_arolla_value(self):
    x = ds([1, 2, 3], schema_constants.FLOAT32)
    arolla.testing.assert_qvalue_allclose(
        x.internal_as_arolla_value(),
        arolla.dense_array([1.0, 2, 3], arolla.FLOAT32),
    )
    x = ds([1, 'abc', 3.14])
    with self.assertRaisesRegex(
        ValueError,
        'only DataSlices with primitive values of the same type can be '
        'converted to Arolla value, got: MIXED',
    ):
      x.internal_as_arolla_value()

  def test_internal_as_dense_array(self):
    x = ds([1, 2, 3], schema_constants.FLOAT32)
    arolla.testing.assert_qvalue_allclose(
        x.internal_as_dense_array(),
        arolla.dense_array([1.0, 2, 3], arolla.FLOAT32),
    )
    x = ds([1, 'abc', 3.14])
    with self.assertRaisesRegex(
        ValueError,
        'only DataSlices with primitive values of the same type can be '
        'converted to Arolla value, got: MIXED',
    ):
      x.internal_as_dense_array()

  @parameterized.parameters(
      (ds([1, 2, 3]), jagged_shape.create_shape([3])),
      (ds([[1, 2], [3]]), jagged_shape.create_shape([2], [2, 1])),
  )
  def test_get_shape(self, x, expected_shape):
    testing.assert_equal(x.get_shape(), expected_shape)

  @parameterized.parameters(
      (ds(1), jagged_shape.create_shape([1]), ds([1])),
      (ds(1), jagged_shape.create_shape([1], [1], [1]), ds([[[1]]])),
      (ds([1]), jagged_shape.create_shape(), ds(1)),
      (ds([[[1]]]), jagged_shape.create_shape([1]), ds([1])),
      (ds([[[1]]]), jagged_shape.create_shape(), ds(1)),
      (ds([[1, 2], [3]]), jagged_shape.create_shape([3]), ds([1, 2, 3])),
      (
          ds([1, 2, 3]),
          jagged_shape.create_shape([2], [2, 1]),
          ds([[1, 2], [3]]),
      ),
      (
          ds([1, 2, 3]),
          (2, ds([2, 1])),
          ds([[1, 2], [3]]),
      ),
      (
          ds([[1, 2], [3]]),
          (-1,),
          ds([1, 2, 3]),
      ),
  )
  def test_reshape(self, x, shape, expected_output):
    new_x = x.reshape(shape)
    testing.assert_equal(new_x, expected_output)

  def test_reshape_incompatible_shape_exception(self):
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        'shape size must be compatible with number of items: shape_size=2 !='
        ' items_size=3',
    ):
      x.reshape(jagged_shape.create_shape([2]))

  @parameterized.parameters(1, arolla.int32(1))
  def test_reshape_non_shape(self, non_shape):
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'expected a tuple'):
      x.reshape(non_shape)

  @parameterized.parameters(
      (ds([1, 2, 3]),),
      (ds([[1, 2], [3]]),),
      (ds([[[1], [2]], [[3]]]),),
      (ds([[[1], [2]], [[], [3]]]),),
  )
  def test_reshape_as(self, shape_from):
    x = ds(['a', 'b', 'c'])
    res = x.reshape_as(shape_from)
    testing.assert_equal(res.flatten(), x)
    testing.assert_equal(res.get_shape(), shape_from.get_shape())

  def test_reshape_as_errors(self):
    with self.assertRaisesRegex(
        ValueError, 'shape size must be compatible with number of items'
    ):
      ds(1).reshape_as(ds([1, 2]))
    with self.assertRaisesRegex(TypeError, '`shape_from` must be a DataSlice'):
      ds(1).reshape_as([])  # pyrefly: ignore[bad-argument-type]

  def test_pipe(self):
    x = ds([1, 2, 3])
    testing.assert_equal(x.pipe(lambda s: s + 1), ds([2, 3, 4]))
    testing.assert_equal(x.pipe(lambda s: s * 2 + 5), ds([7, 9, 11]))

  @parameterized.parameters(
      (ds(1), ds([1])),
      (ds([[1, 2], [3, 4]]), ds([1, 2, 3, 4])),
      (ds([[[1], [2]], [[3], [4]]]), 1, ds([[1, 2], [3, 4]])),
      (ds([[[1], [2]], [[3], [4]]]), -2, ds([[1, 2], [3, 4]])),
      (ds([[[1, 2], [3]], [[4, 5]]]), 0, 2, ds([[1, 2], [3], [4, 5]])),
  )
  def test_flatten(self, *inputs_and_expected):
    args, expected = inputs_and_expected[:-1], inputs_and_expected[-1]
    flattened = args[0].flatten(*args[1:])
    testing.assert_equal(flattened, expected)

  @parameterized.parameters(
      (ds([[1, 2], [3, 4]]), ds([1, 2, 3, 4])),
      (ds([[[1], [2]], [[3], [4]]]), 1, ds([[1, 2], [3, 4]])),
      (ds([[[1], [2]], [[3], [4]]]), 2, ds([1, 2, 3, 4])),
  )
  def test_flatten_end(self, *inputs_and_expected):
    args, expected = inputs_and_expected[:-1], inputs_and_expected[-1]
    flattened = args[0].flatten_end(*args[1:])
    testing.assert_equal(flattened, expected)

  @parameterized.parameters(
      (ds(1), 2, ds([1, 1])),
      (ds([1, 2]), 2, ds([[1, 1], [2, 2]])),
      (ds([[1, 2], [3]]), ds([2, 3]), ds([[[1, 1], [2, 2]], [[3, 3, 3]]])),
      (ds([[1, 2], [3]]), ds([[0, 1], [2]]), ds([[[], [2]], [[3, 3]]])),
  )
  def test_repeat(self, x, sizes, expected):
    testing.assert_equal(x.repeat(sizes), expected)

  def test_resize(self):
    x = ds([[1, 2, 3], [4, 5], [6]])
    shape = jagged_shape.create_shape([3], [2, 3, 1])
    testing.assert_equal(
        x.resize(shape),
        ds([[1, 2], [4, 5, None], [6]]),
    )

  def test_resize_as(self):
    x = ds([[1, 2], [3, 4, 5]])
    target = ds([[0, 0, 0], [0, 0, 0], [0, 0, 0]])
    testing.assert_equal(
        x.resize_as(target),
        ds([[1, 2, None], [3, 4, 5], [None, None, None]]),
    )

  @parameterized.parameters(
      (
          ds([1, 2, 3]),
          ds([arolla.missing(), arolla.present(), arolla.missing()]),
          ds([2]),
      ),
      (
          ds([[1], [2], [3]]),
          ds([[arolla.missing()], [arolla.present()], [arolla.missing()]]),
          ds([[], [2], []]),
      ),
      (
          ds([[1], [None], [3]]),
          ds([[arolla.present()], [arolla.present()], [arolla.present()]]),
          ds([[1], [None], [3]]),
      ),
      (
          ds(['a', 1, None, 1.5]),
          ds([
              arolla.missing(),
              arolla.missing(),
              arolla.missing(),
              arolla.present(),
          ]),
          ds([1.5], schema_constants.OBJECT),
      ),
      # Test case for kd.present.
      (ds([1]), ds(arolla.present()), ds([1])),
      # Test case for kd.missing.
      (ds([1]), ds(arolla.missing()), ds([], schema_constants.INT32)),
  )
  def test_select(self, x, filter_input, expected_output):
    testing.assert_equal(x.select(filter_input), expected_output)

  def test_select_filter_error(self):
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.select: the schema of the `fltr` DataSlice should only'
            ' be OBJECT or MASK or can be evaluated to such DataSlice'
            ' (i.e. Python function or Koda Functor)'
        ),
    ):
      x.select(ds([1, 2, 3]))

    with self.subTest('Shape mismatch'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape('kd.slices.select: failed to broadcast `fltr` to `ds`'),
      ):
        x = ds([[1, 2], [None, None], [7, 8, 9]])
        y = ds([arolla.present(), arolla.present(), None, arolla.present()])
        x.select(y)

  @parameterized.parameters(
      (
          ds([[1, 2, None, 4], [None, None], [7, 8, 9]]),
          ds([[1, 2, 4], [], [7, 8, 9]]),
      ),
      (ds([1]), ds([1])),
      (ds([arolla.missing()]), ds([], schema_constants.MASK)),
  )
  def test_select_present(self, x, expected_output):
    testing.assert_equal(x.select_present(), expected_output)

  # More comprehensive tests are in the core_select_items_test.py.
  @parameterized.parameters(
      (
          bag().list([1, 2, 3]),
          ds([None, present, present]),
          ds([2, 3]),
      ),
      (
          bag().list([1, 2, 3]),
          functor_factories.expr_fn(I.self >= 2),
          ds([2, 3]),
      ),
      (
          ds([bag().list([1, 2, 3]), bag().list([2, 3, 4])]),
          ds([None, present]),
          ds([[], [2, 3, 4]]),
      ),
      (
          bag().list([1, 2, 3]),
          lambda x: x >= 2,
          ds([2, 3]),
      ),
  )
  def test_select_items(self, x, filter_input, expected_output):
    testing.assert_equal(x.select_items(filter_input).no_bag(), expected_output)

  # More comprehensive tests are in the core_select_keys_test.py.
  @parameterized.parameters(
      (
          ds([bag().dict({1: 1}), bag().dict({2: 2}), bag().dict({3: 3})]),
          ds([present, None, None]),
          ds([[1], [], []]),
      ),
      (
          ds([bag().dict({1: 1}), bag().dict({2: 2}), bag().dict({3: 3})]),
          functor_factories.expr_fn(I.self == 1),
          ds([[1], [], []]),
      ),
      (
          ds([[bag().dict({1: 1})], [bag().dict({2: 2}), bag().dict({3: 3})]]),
          ds([present, None]),
          ds([[[1]], [[], []]]),
      ),
      (
          bag().dict({1: 3, 2: 4, 3: 5}),
          lambda x: x == 2,
          ds([2]),
      ),
  )
  def test_select_keys(self, x, filter_input, expected_output):
    testing.assert_equal(x.select_keys(filter_input).no_bag(), expected_output)

  # More comprehensive tests are in the core_select_values_test.py.
  @parameterized.parameters(
      (
          ds([bag().dict({1: 1}), bag().dict({2: 2}), bag().dict({3: 3})]),
          ds([present, None, None]),
          ds([[1], [], []]),
      ),
      (
          ds([bag().dict({4: 1}), bag().dict({5: 2}), bag().dict({6: 3})]),
          functor_factories.expr_fn(I.self == 1),
          ds([[1], [], []]),
      ),
      (
          ds([[bag().dict({1: 1})], [bag().dict({2: 2}), bag().dict({3: 3})]]),
          ds([present, None]),
          ds([[[1]], [[], []]]),
      ),
      (
          bag().dict({3: 1, 4: 2, 5: 3}),
          lambda x: x == 2,
          ds([2]),
      ),
  )
  def test_select_values(self, x, filter_input, expected_output):
    testing.assert_equal(
        x.select_values(filter_input).no_bag(), expected_output
    )

  @parameterized.parameters(
      # ndim=0
      (ds(1), ds([1, 2, 3]), 0, ds([1, 1, 1])),
      (ds(1), ds([[1, 2], [3]]), 0, ds([[1, 1], [1]])),
      (ds([1, 2]), ds([[1, 2], [3]]), 0, ds([[1, 1], [2]])),
      # ndim=1
      (ds([1, 2]), ds([1, 2, 3]), 1, ds([[1, 2], [1, 2], [1, 2]])),
  )
  def test_expand_to(self, source, target, ndim, expected_output):
    testing.assert_equal(source.expand_to(target, ndim), expected_output)
    if ndim == 0:
      testing.assert_equal(source.expand_to(target), expected_output)


if __name__ == '__main__':
  absltest.main()
