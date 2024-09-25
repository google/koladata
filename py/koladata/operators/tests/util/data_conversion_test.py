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

"""Tests for data_conversion."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.operators.tests.util import data_conversion
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import schema_constants


class DataConversionTest(parameterized.TestCase):

  @parameterized.named_parameters(
      (
          'all_missing_dense_array_floats',
          arolla.dense_array_float32([None]),
          data_slice.DataSlice.from_vals(arolla.dense_array_float32([None])),
      ),
      (
          'all_missing_dense_array_int',
          arolla.dense_array_int32([None]),
          data_slice.DataSlice.from_vals(arolla.dense_array_int32([None])),
      ),
      (
          'dense_array_int32',
          arolla.dense_array_int32([1]),
          data_slice.DataSlice.from_vals(arolla.dense_array_int32([1])),
      ),
      (
          'dense_array_int64',
          arolla.dense_array_int64([1]),
          data_slice.DataSlice.from_vals(arolla.dense_array_int64([1])),
      ),
      (
          'dense_array_float32',
          arolla.dense_array_float32([1.0]),
          data_slice.DataSlice.from_vals(arolla.dense_array_float32([1.0])),
      ),
      (
          'dense_array_float64',
          arolla.dense_array_float64([1.0]),
          data_slice.DataSlice.from_vals(arolla.dense_array_float64([1.0])),
      ),
      (
          'dense_array_unit',
          arolla.dense_array_unit([arolla.present()]),
          data_slice.DataSlice.from_vals(
              arolla.dense_array_unit([arolla.present()])
          ),
      ),
      (
          'dense_array_text',
          arolla.dense_array_text(['a']),
          data_slice.DataSlice.from_vals(arolla.dense_array_text(['a'])),
      ),
      (
          'dense_array_bytes',
          arolla.dense_array_bytes([b'a']),
          data_slice.DataSlice.from_vals(arolla.dense_array_bytes([b'a'])),
      ),
      (
          'scalar_int32',
          arolla.int32(1),
          data_slice.DataSlice.from_vals(1),
      ),
      (
          'optional_scalar_int32',
          arolla.optional_int32(1),
          data_slice.DataSlice.from_vals(1),
      ),
      (
          'optional_scalar_float64_missing',
          arolla.optional_float64(None),
          data_slice.DataSlice.from_vals(None, schema_constants.FLOAT64),
      ),
      (
          'array_float32',
          arolla.array_float32([1.0]),
          data_slice.DataSlice.from_vals(arolla.dense_array_float32([1.0])),
      ),
      ('unspecified_passthrough', arolla.unspecified(), arolla.unspecified()),
  )
  def test_explicit_conversion(self, arolla_value, koda_value):
    """Tests Arolla <-> Koda conversion."""
    converted_koda_value = data_conversion.koda_from_arolla(arolla_value)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        converted_koda_value, koda_value
    )
    converted_arolla_value = data_conversion.arolla_from_koda(
        koda_value, output_qtype=arolla_value.qtype
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        converted_arolla_value, arolla_value
    )

  @parameterized.named_parameters(
      (
          'dense_array_to_dense_array',
          arolla.dense_array_float32([1.0]),
          arolla.dense_array_float32([1.0]),
      ),
      (
          'array_to_dense_array',
          arolla.array_float32([1.0]),
          arolla.dense_array_float32([1.0]),
      ),
      ('optional_scalar_to_scalar', arolla.optional_int32(1), arolla.int32(1)),
      ('scalar_to_scalar', arolla.int32(1), arolla.int32(1)),
      (
          'optional_scalar_to_optional_scalar',
          arolla.optional_int32(None),
          arolla.optional_int32(None),
      ),
  )
  def test_implicit_conversion(
      self, arolla_in_value, expected_arolla_out_value
  ):
    converted_koda_value = data_conversion.koda_from_arolla(arolla_in_value)
    arolla_out_value = data_conversion.arolla_from_koda(converted_koda_value)
    arolla.testing.assert_qvalue_allequal(
        expected_arolla_out_value, arolla_out_value
    )

  @parameterized.named_parameters(
      (
          'array_edge',
          arolla.types.ArrayEdge.from_sizes([2, 1]),
          jagged_shape.create_shape([2], [2, 1]),
      ),
      (
          'dense_array_edge',
          arolla.types.DenseArrayEdge.from_sizes([2, 1]),
          jagged_shape.create_shape([2], [2, 1]),
      ),
      (
          'array_to_scalar_edge',
          arolla.types.ArrayToScalarEdge(2),
          jagged_shape.create_shape([2]),
      ),
      (
          'dense_array_to_scalar_edge',
          arolla.types.DenseArrayToScalarEdge(2),
          jagged_shape.create_shape([2]),
      ),
      (
          'scalar_to_scalar_edge',
          arolla.types.ScalarToScalarEdge(),
          jagged_shape.create_shape(),
      ),
  )
  def test_edge_to_shape_conversion(self, arolla_value, koda_value):
    """Tests edge -> shape conversion."""
    converted_koda_value = data_conversion.koda_from_arolla(arolla_value)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        converted_koda_value, koda_value
    )
    converted_arolla_value = data_conversion.arolla_from_koda(
        koda_value, output_qtype=koda_value.qtype
    )
    # Kept as JaggedShape since it's already a proper arolla type.
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        converted_arolla_value, koda_value
    )

  def test_koda_from_arolla_unsupported_qvalues(self):
    with self.assertRaisesRegex(ValueError, r'unsupported qtype: QTYPE'):
      data_conversion.koda_from_arolla(arolla.NOTHING)

  def test_arolla_from_koda_unsupported_rank(self):
    with self.assertRaisesWithLiteralMatch(ValueError, 'rank=2 not in (0, 1)'):
      data_conversion.arolla_from_koda(
          data_slice.DataSlice.from_vals([[1, 2], [3]])
      )

  def test_arolla_from_koda_unsupported_output_qtype(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'unsupported output_qtype: ARRAY_EDGE'
    ):
      data_conversion.arolla_from_koda(
          data_slice.DataSlice.from_vals([1, 2, 3]),
          output_qtype=arolla.ARRAY_EDGE,
      )

  def test_arolla_from_koda_failed_cast(self):
    with self.assertRaisesRegex(
        ValueError, 'expected a scalar or optional scalar type'
    ):
      data_conversion.arolla_from_koda(
          data_slice.DataSlice.from_vals([1, 2, 3]), output_qtype=arolla.INT32
      )

  def test_arolla_from_koda_unsafe_cast(self):
    with self.assertRaisesRegex(
        ValueError,
        'could not safely cast DENSE_ARRAY_INT32 to DENSE_ARRAY_INT64',
    ):
      data_conversion.arolla_from_koda(
          data_slice.DataSlice.from_vals([1, 2, 3]),
          output_qtype=arolla.DENSE_ARRAY_INT64,
      )


if __name__ == '__main__':
  absltest.main()
