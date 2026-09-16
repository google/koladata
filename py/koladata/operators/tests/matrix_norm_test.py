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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants
import numpy as np

I = input_container.InputContainer('I')
kde = kde_operators.kde
kd = kde_operators.kd
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    # (x,) -> result:
    (DATA_SLICE, DATA_SLICE),
    # (x, ord) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])

# All supported ord values for parametric tests.
_ALL_ORDS = ['fro', 'nuc', float('inf'), -float('inf'), 1, -1, 2, -2]


class MatrixMatrixNormTest(parameterized.TestCase):

  def test_frobenius_norm_default(self):
    # sqrt(1 + 4 + 9 + 16) = sqrt(30)
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.matrix_norm(x)
    testing.assert_allclose(result, ds(30.0**0.5), atol=1e-5)

  def test_frobenius_default_matches_none_and_fro(self):
    # Default, ord=None (missing), and ord='fro' all compute Frobenius.
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    result_default = kd.matrix.matrix_norm(x)
    result_none = kd.matrix.matrix_norm(
        x, ord=ds(None, schema_constants.FLOAT32)
    )
    result_fro = kd.matrix.matrix_norm(x, ord='fro')
    result_py_none = kd.matrix.matrix_norm(x, ord=None)
    testing.assert_allclose(result_default, result_none)
    testing.assert_allclose(result_default, result_fro)
    testing.assert_allclose(result_default, result_py_none)

  def test_inf_norm(self):
    # Max row sum: max(|1|+|2|, |3|+|4|) = max(3, 7) = 7.
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.matrix_norm(x, ord=float('inf'))
    testing.assert_allclose(result, ds(7.0))

  def test_inf_norm_negative_values(self):
    # max(|-1|+|2|, |3|+|-4|) = max(3, 7) = 7.
    x = ds([[-1.0, 2.0], [3.0, -4.0]])
    result = kd.matrix.matrix_norm(x, ord=float('inf'))
    testing.assert_allclose(result, ds(7.0))

  def test_neg_inf_norm(self):
    # Min row sum: min(|1|+|2|, |3|+|4|) = min(3, 7) = 3.
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.matrix_norm(x, ord=-float('inf'))
    testing.assert_allclose(result, ds(3.0))

  def test_1_norm(self):
    # Max column sum: max(|1|+|3|, |2|+|4|) = max(4, 6) = 6.
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.matrix_norm(x, ord=1)
    testing.assert_allclose(result, ds(6.0))

  def test_neg_1_norm(self):
    # Min column sum: min(|1|+|3|, |2|+|4|) = min(4, 6) = 4.
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.matrix_norm(x, ord=-1)
    testing.assert_allclose(result, ds(4.0))

  def test_2_norm_spectral(self):
    # 2-norm is the largest singular value.
    # For [[1, 0], [0, 2]], singular values are {2, 1}, so 2-norm = 2.
    x = ds([[1.0, 0.0], [0.0, 2.0]])
    result = kd.matrix.matrix_norm(x, ord=2)
    testing.assert_allclose(result, ds(2.0), atol=1e-5)

  def test_neg_2_norm(self):
    # -2-norm is the smallest singular value.
    # For [[1, 0], [0, 2]], singular values are {2, 1}, so -2-norm = 1.
    x = ds([[1.0, 0.0], [0.0, 2.0]])
    result = kd.matrix.matrix_norm(x, ord=-2)
    testing.assert_allclose(result, ds(1.0), atol=1e-5)

  def test_non_square_matrix(self):
    # (2, 3) matrix.
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    x_np = np.array([[1, 2, 3], [4, 5, 6.0]])
    expected_results = {
        'fro': float(np.linalg.norm(x_np, ord='fro')),
        'nuc': float(np.linalg.norm(x_np, ord='nuc')),
        float('inf'): float(np.linalg.norm(x_np, ord=np.inf)),
        -float('inf'): float(np.linalg.norm(x_np, ord=-np.inf)),
        1: float(np.linalg.norm(x_np, ord=1)),
        -1: float(np.linalg.norm(x_np, ord=-1)),
        2: float(np.linalg.norm(x_np, ord=2)),
        -2: float(np.linalg.norm(x_np, ord=-2)),
    }
    for ord_val, expected in expected_results.items():
      with self.subTest(ord=ord_val):
        result = kd.matrix.matrix_norm(x, ord=ord_val)
        testing.assert_allclose(result, ds(expected), atol=1e-5)

  def test_1x1_matrix(self):
    # All norms of [[5]] should be 5.
    x = ds([[5.0]])
    for ord_val in _ALL_ORDS:
      with self.subTest(ord=ord_val):
        testing.assert_allclose(
            kd.matrix.matrix_norm(x, ord=ord_val), ds(5.0), atol=1e-5
        )

  def test_identity_matrix(self):
    # 3x3 identity: fro=sqrt(3), nuc=3, all others=1.
    x = ds([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]])
    expected = {
        'fro': 3.0**0.5,
        'nuc': 3.0,
        float('inf'): 1.0,
        -float('inf'): 1.0,
        1: 1.0,
        -1: 1.0,
        2: 1.0,
        -2: 1.0,
    }
    for ord_val, exp in expected.items():
      with self.subTest(ord=ord_val):
        testing.assert_allclose(
            kd.matrix.matrix_norm(x, ord=ord_val), ds(exp), atol=1e-5
        )

  def test_zero_matrix(self):
    # All-zero populated matrix: all norms should be 0.
    x = ds([[0.0, 0.0], [0.0, 0.0]])
    for ord_val in _ALL_ORDS:
      with self.subTest(ord=ord_val):
        testing.assert_allclose(
            kd.matrix.matrix_norm(x, ord=ord_val), ds(0.0), atol=1e-6
        )

  def test_empty_zero_by_zero(self):
    # 0x0 matrix: all norms should be 0.
    x = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.NONE)
    for ord_val in _ALL_ORDS:
      with self.subTest(ord=ord_val):
        testing.assert_allclose(
            kd.matrix.matrix_norm(x, ord=ord_val), ds(0.0), atol=1e-6
        )

  def test_empty_three_by_zero(self):
    # 3x0 matrix (3 rows, 0 columns): all norms should be 0.
    x = kd.empty_shaped(kd.shapes.new(3, 0), schema_constants.FLOAT32)
    for ord_val in _ALL_ORDS:
      with self.subTest(ord=ord_val):
        testing.assert_allclose(
            kd.matrix.matrix_norm(x, ord=ord_val), ds(0.0), atol=1e-6
        )

  def test_empty_zero_by_zero_float64(self):
    x = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.FLOAT64)
    testing.assert_allclose(
        kd.matrix.matrix_norm(x),
        ds(0.0, schema_constants.FLOAT64),
        atol=1e-6,
    )

  def test_none_schema(self):
    # All-None matrix is treated as all-zeros.
    x = ds([[None, None], [None, None]], schema_constants.NONE)
    for ord_val in _ALL_ORDS:
      with self.subTest(ord=ord_val):
        testing.assert_allclose(
            kd.matrix.matrix_norm(x, ord=ord_val), ds(0.0), atol=1e-6
        )

  def test_rank_deficient_2_norm(self):
    # Rank-1 matrix: smallest singular value is 0 -> ord=-2 returns 0.
    x = ds([[1.0, 2.0], [2.0, 4.0]])
    testing.assert_allclose(
        kd.matrix.matrix_norm(x, ord=-2), ds(0.0), atol=1e-5
    )
    # Largest singular value should be nonzero.
    self.assertGreater(float(kd.matrix.matrix_norm(x, ord=2)), 0)

  def test_batched_with_one_rank_deficient(self):
    # Batch: first is full-rank, second is rank-deficient.
    a1 = [[1.0, 0.0], [0.0, 1.0]]  # identity
    a2 = [[1.0, 2.0], [2.0, 4.0]]  # rank 1
    x = ds([a1, a2])
    result = kd.matrix.matrix_norm(x, ord=-2)
    testing.assert_allclose(
        result, ds([1.0, 0.0], schema_constants.FLOAT32), atol=1e-5
    )

  def test_int32_input(self):
    x = ds([[1, 2], [3, 4]])
    result = kd.matrix.matrix_norm(x)
    self.assertEqual(result.get_schema(), schema_constants.FLOAT32)
    testing.assert_allclose(result, ds(30.0**0.5), atol=1e-5)

  def test_int64_input(self):
    x = ds([[1, 2], [3, 4]], schema_constants.INT64)
    result = kd.matrix.matrix_norm(x)
    self.assertEqual(result.get_schema(), schema_constants.FLOAT32)
    testing.assert_allclose(result, ds(30.0**0.5), atol=1e-5)

  def test_float64_input(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]], schema_constants.FLOAT64)
    result = kd.matrix.matrix_norm(x)
    self.assertEqual(result.get_schema(), schema_constants.FLOAT64)
    testing.assert_allclose(
        result, ds(30.0**0.5, schema_constants.FLOAT64), atol=1e-10
    )

  def test_sparse_missing_as_zero(self):
    # Missing treated as 0. Matrix is [[1, 0], [0, 4]].
    x = ds([[1.0, None], [None, 4.0]])
    # Frobenius: sqrt(1 + 16) = sqrt(17).
    testing.assert_allclose(kd.matrix.matrix_norm(x), ds(17.0**0.5), atol=1e-5)
    # Inf: max(1, 4) = 4.
    testing.assert_allclose(kd.matrix.matrix_norm(x, ord=float('inf')), ds(4.0))
    # 1-norm: max(1, 4) = 4.
    testing.assert_allclose(kd.matrix.matrix_norm(x, ord=1), ds(4.0))

  def test_batched_sparse(self):
    # Batch of 2 matrices with missing values.
    x = ds([
        [[1.0, None], [None, 2.0]],  # diag(1, 2), fro = sqrt(5)
        [[None, 3.0], [None, None]],  # [[0, 3], [0, 0]], fro = 3
    ])
    result = kd.matrix.matrix_norm(x)
    testing.assert_allclose(
        result, ds([5.0**0.5, 3.0], schema_constants.FLOAT32), atol=1e-5
    )

  @parameterized.parameters(
      (schema_constants.FLOAT32, schema_constants.FLOAT32),
      (schema_constants.FLOAT64, schema_constants.FLOAT64),
      (schema_constants.INT32, schema_constants.FLOAT32),
  )
  def test_object_schema(self, inner_schema, expected_obj_schema):
    x = kd.obj(ds([[1.0, 2.0], [3.0, 4.0]], inner_schema))
    result = kd.matrix.matrix_norm(x)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        expected_obj_schema,
    )
    testing.assert_allclose(
        kd.cast_to(result, expected_obj_schema),
        ds(30.0**0.5, expected_obj_schema),
        atol=1e-5,
    )

  def test_object_schema_mixed_numeric_types(self):
    # Mixed numeric types wrapped in OBJECT: INT32 and INT64 -> FLOAT32.
    x = ds([[kd.int64(1), kd.obj(kd.int32(0))], [0, 1]])
    result = kd.matrix.matrix_norm(x)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        schema_constants.FLOAT32,
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32), ds(2.0**0.5), atol=1e-5
    )

  def test_batched_3d(self):
    # (2, 2, 2) -> (2,) output.
    x = ds([[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]])
    result = kd.matrix.matrix_norm(x)
    testing.assert_allclose(
        result,
        ds([30.0**0.5, 174.0**0.5], schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_batched_4d(self):
    # (2, 1, 2, 2) -> (2, 1) output.
    x = ds([[[[1.0, 0.0], [0.0, 1.0]]], [[[2.0, 0.0], [0.0, 2.0]]]])
    result = kd.matrix.matrix_norm(x)
    # Identity: sqrt(2), 2*Identity: sqrt(8) = 2*sqrt(2).
    testing.assert_allclose(
        result,
        ds([[2.0**0.5], [8.0**0.5]], schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_jagged_matrix_dims(self):
    # Batch of matrices with different sizes: 2x2 and 1x1.
    a1 = [[1.0, 2.0], [3.0, 4.0]]
    a2 = [[5.0]]
    x = ds([a1, a2])
    result = kd.matrix.matrix_norm(x)
    testing.assert_allclose(
        result,
        ds([30.0**0.5, 5.0], schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_jagged_matrix_dims_non_square(self):
    # Batch of non-square matrices with different shapes: 2x3 and 3x2.
    a1 = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]  # 2x3
    a2 = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]  # 3x2
    x = ds([a1, a2])
    result = kd.matrix.matrix_norm(x)
    expected = ds([
        float(np.linalg.norm(np.array(a1), ord='fro')),
        float(np.linalg.norm(np.array(a2), ord='fro')),
    ])
    testing.assert_allclose(result, expected, atol=1e-4)

  def test_jagged_batch_dimensions(self):
    # 4D input where batch dim 1 is jagged. Each matrix is 2x2.
    # Shape: dim0=2, dim1=[2, 1], dim2=2, dim3=2. Total 3 matrices.
    x = ds([
        [[[1.0, 0.0], [0.0, 1.0]], [[2.0, 0.0], [0.0, 2.0]]],
        [[[3.0, 0.0], [0.0, 3.0]]],
    ])
    result = kd.matrix.matrix_norm(x)
    testing.assert_allclose(
        result,
        ds(
            [
                [2.0**0.5, 8.0**0.5],
                [18.0**0.5],
            ],
            schema_constants.FLOAT32,
        ),
        atol=1e-5,
    )

  def test_ord_per_batch_element(self):
    m = [[1.0, 2.0], [3.0, 4.0]]
    x = ds([m, m, m, m, m, m, m])
    ords = ds([float('inf'), -float('inf'), 1, -1, None, 2, -2])
    result = kd.matrix.matrix_norm(x, ord=ords)
    m_np = np.array(m)
    expected = ds([
        float(np.linalg.norm(m_np, ord=np.inf)),
        float(np.linalg.norm(m_np, ord=-np.inf)),
        float(np.linalg.norm(m_np, ord=1)),
        float(np.linalg.norm(m_np, ord=-1)),
        float(np.linalg.norm(m_np, ord='fro')),
        float(np.linalg.norm(m_np, ord=2)),
        float(np.linalg.norm(m_np, ord=-2)),
    ])
    testing.assert_allclose(result, expected, atol=1e-5)

  def test_ord_per_batch_element_mixed_string_numeric(self):
    # Batch mixing string and numeric ord values (OBJECT schema for ord).
    m = [[1.0, 2.0], [3.0, 4.0]]
    x = ds([m, m, m, m])
    ords = ds(['fro', 'nuc', 2, -2])
    result = kd.matrix.matrix_norm(x, ord=ords)
    m_np = np.array(m)
    expected = ds([
        float(np.linalg.norm(m_np, ord='fro')),
        float(np.linalg.norm(m_np, ord='nuc')),
        float(np.linalg.norm(m_np, ord=2)),
        float(np.linalg.norm(m_np, ord=-2)),
    ])
    testing.assert_allclose(result, expected, atol=1e-5)

  def test_ord_all_missing_defaults_to_fro(self):
    # Batch of all-missing ords defaults each element to Frobenius.
    x = ds([[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]])
    result_missing = kd.matrix.matrix_norm(
        x, ord=ds([None, None], schema_constants.FLOAT32)
    )
    result_default = kd.matrix.matrix_norm(x)
    testing.assert_allclose(result_missing, result_default)

  def test_ord_scalar_broadcast(self):
    x = ds([[[1.0, 0.0], [0.0, 1.0]], [[2.0, 0.0], [0.0, 2.0]]])
    result_scalar = kd.matrix.matrix_norm(x, ord=float('inf'))
    result_vector = kd.matrix.matrix_norm(
        x, ord=ds([float('inf'), float('inf')])
    )
    testing.assert_allclose(result_scalar, result_vector)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.matrix_norm,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.matrix_norm(I.x)))
    self.assertTrue(view.has_koda_view(kde.matrix.matrix_norm(I.x, ord=I.ord)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify conceptual equivalence."""

  @parameterized.parameters(
      ('fro', 'fro'),
      ('nuc', 'nuc'),
      (float('inf'), np.inf),
      (-float('inf'), -np.inf),
      (1, 1),
      (-1, -1),
      (2, 2),
      (-2, -2),
  )
  def test_single_matrix_vs_numpy(self, kd_ord, np_ord):
    x_np = np.array([[1.0, -2.0], [3.0, 4.0], [-5.0, 6.0]])
    expected = float(np.linalg.norm(x_np, ord=np_ord))
    result = kd.matrix.matrix_norm(ds(x_np.tolist()), ord=kd_ord)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_large_matrix_vs_numpy(self):
    # Large 10x10 matrix cross-validated against NumPy for all ord values.
    rng = np.random.default_rng(123)
    x_np = rng.standard_normal((10, 10))
    x_kd = ds(x_np.tolist())
    expected_results = {
        'fro': float(np.linalg.norm(x_np, ord='fro')),
        'nuc': float(np.linalg.norm(x_np, ord='nuc')),
        float('inf'): float(np.linalg.norm(x_np, ord=np.inf)),
        -float('inf'): float(np.linalg.norm(x_np, ord=-np.inf)),
        1: float(np.linalg.norm(x_np, ord=1)),
        -1: float(np.linalg.norm(x_np, ord=-1)),
        2: float(np.linalg.norm(x_np, ord=2)),
        -2: float(np.linalg.norm(x_np, ord=-2)),
    }
    for kd_ord, expected in expected_results.items():
      with self.subTest(ord=kd_ord):
        result = kd.matrix.matrix_norm(x_kd, ord=kd_ord)
        testing.assert_allclose(
            result, ds(expected, schema_constants.FLOAT32), atol=1e-4
        )

  def test_batched_frobenius_vs_numpy(self):
    rng = np.random.default_rng(42)
    x_np = rng.standard_normal((5, 3, 4))
    result = kd.matrix.matrix_norm(ds(x_np.tolist()))
    expected = [float(np.linalg.norm(x_np[i], ord='fro')) for i in range(5)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_batched_all_ords_vs_numpy(self):
    rng = np.random.default_rng(99)
    x_np = rng.standard_normal((4, 3, 5))
    for kd_ord, np_ord in [
        (float('inf'), np.inf),
        (-float('inf'), -np.inf),
        (1, 1),
        (-1, -1),
        (2, 2),
        (-2, -2),
    ]:
      with self.subTest(ord=kd_ord):
        result = kd.matrix.matrix_norm(ds(x_np.tolist()), ord=kd_ord)
        expected = [
            float(np.linalg.norm(x_np[i], ord=np_ord)) for i in range(4)
        ]
        testing.assert_allclose(
            result, ds(expected, schema_constants.FLOAT32), atol=1e-5
        )


class ErrorTest(parameterized.TestCase):
  """Tests for error messages."""

  def test_1d_input(self):
    x = ds([1.0, 2.0, 3.0])
    with self.assertRaisesRegex(ValueError, r'expected at least 2D, got 1D'):
      kd.matrix.matrix_norm(x)

  def test_0d_input(self):
    x = ds(3.0)
    with self.assertRaisesRegex(ValueError, r'expected at least 2D, got 0D'):
      kd.matrix.matrix_norm(x)

  def test_string_schema_fails(self):
    x = ds([['a', 'b'], ['c', 'd']])
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowed schema: STRING'
    ):
      kd.matrix.matrix_norm(x)

  def test_strings_with_object_schema_fails(self):
    x = kd.obj(ds([['a', 'b'], ['c', 'd']]))
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowed schema: STRING'
    ):
      kd.matrix.matrix_norm(x)

  def test_non_uniform_rows_fails(self):
    # Jagged matrix: rows have different lengths.
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    with self.assertRaisesRegex(ValueError, r'non-uniform row sizes'):
      kd.matrix.matrix_norm(x)

  @parameterized.parameters(
      (0, 'unsupported matrix norm ord=0'),
      (3, 'unsupported matrix norm ord=3'),
      (0.5, 'unsupported matrix norm ord=0.5'),
  )
  def test_unsupported_numeric_ord(self, ord_val, error_regex):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(ValueError, error_regex):
      kd.matrix.matrix_norm(x, ord=ord_val)

  def test_unsupported_string_ord(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError,
        "unsupported matrix norm ord='xyz'",
    ):
      kd.matrix.matrix_norm(x, ord='xyz')

  def test_ord_same_rank_as_x_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, '`ord` must have at least 2 fewer dimensions than `x`'
    ):
      kd.matrix.matrix_norm(x, ord=ds([[1, 2], [3, 4]]))

  def test_ord_rank_one_less_than_rank_of_x_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, '`ord` must have at least 2 fewer dimensions than `x`'
    ):
      kd.matrix.matrix_norm(x, ord=ds([1, 2]))

  def test_ord_broadcast_mismatch(self):
    x = ds([[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]])
    with self.assertRaisesRegex(ValueError, 'cannot be expanded to'):
      kd.matrix.matrix_norm(x, ord=ds([1, 2, 3]))


if __name__ == '__main__':
  absltest.main()
