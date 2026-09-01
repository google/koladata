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


class MatrixVectorNormTest(parameterized.TestCase):

  def test_l2_norm(self):
    x = ds([3.0, 4.0])
    result = kd.matrix.vector_norm(x)
    testing.assert_allclose(result, ds(5.0), atol=1e-5)

  def test_l1_norm(self):
    x = ds([-1.0, 2.0, -3.0])
    result = kd.matrix.vector_norm(x, ord=1)
    testing.assert_allclose(result, ds(6.0), atol=1e-5)

  def test_l3_norm(self):
    x = ds([1.0, 2.0, 3.0])
    expected = (1.0**3 + 2.0**3 + 3.0**3) ** (1.0 / 3.0)
    result = kd.matrix.vector_norm(x, ord=3)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_l3_norm_negative(self):
    # Verify abs(x) is applied before pow for general p-norms.
    x = ds([-1.0, 2.0, -3.0])
    expected = (1.0**3 + 2.0**3 + 3.0**3) ** (1.0 / 3.0)
    result = kd.matrix.vector_norm(x, ord=3)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_l0_norm(self):
    x = ds([0.0, 3.0, -4.0, 0.0, 5.0])
    result = kd.matrix.vector_norm(x, ord=0)
    testing.assert_allclose(result, ds(3.0))  # 3 non-zero elements

  def test_l0_norm_all_zero(self):
    x = ds([0.0, 0.0, 0.0])
    result = kd.matrix.vector_norm(x, ord=0)
    testing.assert_allclose(result, ds(0.0))

  def test_neg_inf_norm(self):
    x = ds([3.0, -1.0, 4.0])
    result = kd.matrix.vector_norm(x, ord=-float('inf'))
    testing.assert_allclose(result, ds(1.0))  # min(|3|, |-1|, |4|)

  def test_neg_inf_norm_with_zero(self):
    x = ds([3.0, 0.0, 4.0])
    result = kd.matrix.vector_norm(x, ord=-float('inf'))
    testing.assert_allclose(result, ds(0.0))  # min(|3|, |0|, |4|)

  def test_negative_ord(self):
    x = ds([1.0, 2.0, 3.0])
    # p=-1 norm: (sum(|x_i|^-1))^(-1) = (1 + 0.5 + 1/3)^(-1) = 6/11
    result = kd.matrix.vector_norm(x, ord=-1)
    testing.assert_allclose(result, ds(6.0 / 11.0))

  def test_float64_input(self):
    x = ds([3.0, 4.0], schema_constants.FLOAT64)
    result = kd.matrix.vector_norm(x)
    testing.assert_allclose(result, ds(5.0, schema_constants.FLOAT64))

  def test_batched_2d(self):
    # (2, 3) input -> (2,) output. L2 norm of each row.
    x = ds([[3.0, 4.0, 0.0], [0.0, 5.0, 12.0]])
    result = kd.matrix.vector_norm(x)
    testing.assert_allclose(result, ds([5.0, 13.0]))

  def test_batched_3d(self):
    # (2, 2, 2) input -> (2, 2) output.
    x = ds([[[3.0, 4.0], [5.0, 12.0]], [[1.0, 0.0], [0.0, 1.0]]])
    result = kd.matrix.vector_norm(x)
    testing.assert_allclose(
        result, ds([[5.0, 13.0], [1.0, 1.0]], schema_constants.FLOAT32)
    )

  def test_batched_sparse(self):
    # (2, 3) sparse. None treated as 0.
    x = ds([[3.0, None, 4.0], [None, 6.0, None]])

    # L2:
    # norm([3, 0, 4]) = sqrt(9 + 16) = 5
    # norm([0, 6, 0]) = 6
    result = kd.matrix.vector_norm(x)
    testing.assert_allclose(result, ds([5.0, 6.0], schema_constants.FLOAT32))

    # L0: count non-zero. [3, 0, 4] -> 2, [0, 6, 0] -> 1.
    result = kd.matrix.vector_norm(x, ord=0)
    testing.assert_allclose(result, ds([2.0, 1.0], schema_constants.FLOAT32))

    # -inf: min(|x_i|). [3, 0, 4] -> 0, [0, 6, 0] -> 0.
    result = kd.matrix.vector_norm(x, ord=-float('inf'))
    testing.assert_allclose(result, ds([0.0, 0.0], schema_constants.FLOAT32))

  def test_int32_input(self):
    x = ds([3, 4])

    result = kd.matrix.vector_norm(x)
    testing.assert_allclose(
        result, ds(5.0, schema_constants.FLOAT32), atol=1e-5
    )

    result = kd.matrix.vector_norm(x, ord=1)
    testing.assert_allclose(
        result, ds(7.0, schema_constants.FLOAT32), atol=1e-5
    )

    result = kd.matrix.vector_norm(x, ord=float('inf'))
    testing.assert_allclose(
        result, ds(4.0, schema_constants.FLOAT32), atol=1e-5
    )

    result = kd.matrix.vector_norm(x, ord=0)
    testing.assert_allclose(
        result, ds(2.0, schema_constants.FLOAT32), atol=1e-5
    )

    result = kd.matrix.vector_norm(x, ord=-float('inf'))
    testing.assert_allclose(
        result, ds(3.0, schema_constants.FLOAT32), atol=1e-5
    )

    # p=-1: (1/3 + 1/4)^(-1) = 12/7
    result = kd.matrix.vector_norm(x, ord=-1)
    testing.assert_allclose(
        result, ds(12.0 / 7.0, schema_constants.FLOAT32), atol=1e-5
    )

  def test_int64_input(self):
    x = ds([3, 4], schema_constants.INT64)
    result = kd.matrix.vector_norm(x)
    testing.assert_allclose(
        result, ds(5.0, schema_constants.FLOAT32), atol=1e-5
    )

  def test_inf_norm(self):
    x = ds([3.0, -4.0, 2.0])
    result = kd.matrix.vector_norm(x, ord=float('inf'))
    testing.assert_allclose(result, ds(4.0))

  def test_inf_norm_all_negative(self):
    x = ds([-5.0, -1.0, -3.0])
    result = kd.matrix.vector_norm(x, ord=float('inf'))
    testing.assert_allclose(result, ds(5.0))

  def test_inf_norm_batched(self):
    x = ds([[3.0, -4.0], [1.0, 2.0], [-7.0, 0.0]])
    result = kd.matrix.vector_norm(x, ord=float('inf'))
    testing.assert_allclose(result, ds([4.0, 2.0, 7.0]))

  def test_inf_norm_sparse(self):
    # None values in `x` are treated as 0; max(|3|, |0|, |-4|) = 4.
    x = ds([3.0, None, -4.0])
    result = kd.matrix.vector_norm(x, ord=float('inf'))
    testing.assert_allclose(result, ds(4.0))

  def test_inf_norm_float64(self):
    x = ds([3.0, -4.0], schema_constants.FLOAT64)
    result = kd.matrix.vector_norm(x, ord=float('inf'))
    testing.assert_allclose(result, ds(4.0, schema_constants.FLOAT64))

  def test_norm_none_schema(self):
    # All-None vector is treated as all-zeros.
    x = ds([None, None], schema_constants.NONE)

    result = kd.matrix.vector_norm(x, ord=float('inf'))
    testing.assert_allclose(result, ds(0.0))

    result = kd.matrix.vector_norm(x, ord=2)
    testing.assert_allclose(result, ds(0.0))

    result = kd.matrix.vector_norm(x, ord=0)
    testing.assert_allclose(result, ds(0.0))

    result = kd.matrix.vector_norm(x, ord=-float('inf'))
    testing.assert_allclose(result, ds(0.0))

    result = kd.matrix.vector_norm(x, ord=-1)
    testing.assert_allclose(result, ds(0.0))

  def test_ord_per_batch_element(self):
    x = ds([
        [3.0, 4.0],
        [-1.0, 2.0, -3.0],
        [3.0, -4.0, 2.0],
        [0.0, 3.0, -4.0, 0.0, 5.0],
        [3.0, -1.0, 4.0],
        [1.0, 2.0, 3.0],
        [1.0, 2.0, 4.0],
    ])
    result = kd.matrix.vector_norm(
        x, ord=ds([2, 1, float('inf'), 0, -float('inf'), -1, -2])
    )
    # ord=2: sqrt(9+16) = 5
    # ord=1: 1+2+3 = 6
    # ord=inf: max(3, 4, 2) = 4
    # ord=0: count(!=0) = 3
    # ord=-inf: min(|3|, |-1|, |4|) = 1
    # ord=-1: (1 + 1/2 + 1/3)^(-1) = 6/11
    # ord=-2: (1 + 1/4 + 1/16)^(-1/2) = (21/16)^(-1/2) = 4/sqrt(21)
    testing.assert_allclose(
        result,
        ds([5.0, 6.0, 4.0, 3.0, 1.0, 6.0 / 11.0, 4.0 / 21.0**0.5]),
        atol=1e-5,
    )

  def test_ord_per_batch_element_vs_numpy(self):
    rng = np.random.default_rng(404)
    ords = [2, 1, 3, np.inf, 0, -np.inf, -1, -2]
    # All positive, no zeros: negative ord values produce trivial results
    # when any element is zero (see test_norm_negative_ord_batched_vs_numpy).
    x_np = np.abs(rng.standard_normal((len(ords), 6))) + 0.1
    result = kd.matrix.vector_norm(
        ds(x_np.tolist()),
        ord=ds([float(o) for o in ords]),
    )
    expected = [
        float(np.linalg.norm(x_np[i], ord=ords[i])) for i in range(len(ords))
    ]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_ord_scalar_broadcast_matches_vector(self):
    x = ds([[3.0, 4.0], [5.0, 12.0]])
    result_scalar = kd.matrix.vector_norm(x, ord=2)
    result_vector = kd.matrix.vector_norm(x, ord=ds([2, 2]))
    testing.assert_allclose(result_scalar, result_vector)

  def test_ord_scalar_broadcast_inf(self):
    x = ds([[3.0, -4.0], [5.0, 12.0]])
    result_scalar = kd.matrix.vector_norm(x, ord=float('inf'))
    result_vector = kd.matrix.vector_norm(
        x, ord=ds([float('inf'), float('inf')])
    )
    testing.assert_allclose(result_scalar, result_vector)

  def test_ord_mixed_finite_and_inf(self):
    x = ds([[3.0, 4.0], [-7.0, 1.0]])
    result = kd.matrix.vector_norm(x, ord=ds([2, float('inf')]))
    # ord=2: sqrt(9+16) = 5
    # ord=inf: max(7, 1) = 7
    testing.assert_allclose(result, ds([5.0, 7.0]), atol=1e-5)

  def test_ord_batched_3d(self):
    # (2, 2, 3) input -> (2, 2) output. ord is scalar, broadcasts to all.
    x = ds([
        [[3.0, 4.0, 0.0], [0.0, 5.0, 12.0]],
        [[1.0, 0.0, 0.0], [0.0, 0.0, 2.0]],
    ])
    result = kd.matrix.vector_norm(x, ord=1)
    testing.assert_allclose(
        result, ds([[7.0, 17.0], [1.0, 2.0]], schema_constants.FLOAT32)
    )

  def test_ord_missing_defaults_to_l2(self):
    # Scalar missing ord -> defaults to 2 (L2 norm).
    x = ds([3.0, 4.0])
    result = kd.matrix.vector_norm(x, ord=ds(None, schema_constants.INT32))
    testing.assert_allclose(result, ds(5.0), atol=1e-5)

  def test_ord_per_batch_with_missing(self):
    # Batch of 3 vectors: ord=[1, None, inf].
    # None defaults to L2.
    x = ds([[-1.0, 2.0, -3.0], [3.0, 4.0, 0.0], [3.0, -4.0, 2.0]])
    result = kd.matrix.vector_norm(x, ord=ds([1, None, float('inf')]))
    # ord=1: 1+2+3 = 6
    # ord=None -> 2: sqrt(9+16+0) = 5
    # ord=inf: max(3, 4, 2) = 4
    testing.assert_allclose(result, ds([6.0, 5.0, 4.0]), atol=1e-5)

  def test_ord_all_missing_defaults_to_l2(self):
    # All missing ord -> all default to L2.
    x = ds([[3.0, 4.0], [5.0, 12.0]])
    result = kd.matrix.vector_norm(
        x, ord=ds([None, None], schema_constants.INT32)
    )
    expected = kd.matrix.vector_norm(x)  # default ord=2
    testing.assert_allclose(result, expected)

  def test_single_element_vector(self):
    x = ds([5.0])
    testing.assert_allclose(kd.matrix.vector_norm(x), ds(5.0))
    testing.assert_allclose(kd.matrix.vector_norm(x, ord=1), ds(5.0))
    testing.assert_allclose(kd.matrix.vector_norm(x, ord=float('inf')), ds(5.0))
    testing.assert_allclose(kd.matrix.vector_norm(x, ord=0), ds(1.0))
    testing.assert_allclose(
        kd.matrix.vector_norm(x, ord=-float('inf')), ds(5.0)
    )
    testing.assert_allclose(kd.matrix.vector_norm(x, ord=-1), ds(5.0))

  def test_zero_vector(self):
    x = ds([0.0, 0.0, 0.0])
    testing.assert_allclose(kd.matrix.vector_norm(x), ds(0.0))
    testing.assert_allclose(kd.matrix.vector_norm(x, ord=1), ds(0.0))
    testing.assert_allclose(kd.matrix.vector_norm(x, ord=float('inf')), ds(0.0))
    testing.assert_allclose(kd.matrix.vector_norm(x, ord=0), ds(0.0))
    testing.assert_allclose(
        kd.matrix.vector_norm(x, ord=-float('inf')), ds(0.0)
    )
    # p=-1 of zero vector: sum(|0|^-1)^(-1) = sum(inf)^(-1) = 0.
    testing.assert_allclose(kd.matrix.vector_norm(x, ord=-1), ds(0.0))

  def test_jagged_batch_dims(self):
    # Jagged: vectors of different lengths.
    x = ds([[3.0, 4.0], [3.0, 3.0, 3.0, 3.0]])
    result = kd.matrix.vector_norm(x)
    testing.assert_allclose(
        result,
        ds([5.0, 6.0], schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_object_schema(self):
    x = ds([[3.0, 4.0], [5.0, 12.0]], schema_constants.OBJECT)
    result = kd.matrix.vector_norm(x)
    testing.assert_allclose(
        result, ds([5.0, 13.0], schema_constants.OBJECT), atol=1e-5
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.vector_norm,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.vector_norm(I.x)))
    self.assertTrue(view.has_koda_view(kde.matrix.vector_norm(I.x, ord=I.ord)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_norm_l2_vs_numpy(self):
    x_np = np.array([3.0, 4.0])
    expected = float(np.linalg.norm(x_np))
    result = kd.matrix.vector_norm(ds(x_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_norm_l1_vs_numpy(self):
    x_np = np.array([-1.0, 2.0, -3.0])
    expected = float(np.linalg.norm(x_np, ord=1))
    result = kd.matrix.vector_norm(ds(x_np.tolist()), ord=1)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_norm_l3_vs_numpy(self):
    x_np = np.array([1.0, 2.0, 3.0])
    expected = float(np.linalg.norm(x_np, ord=3))
    result = kd.matrix.vector_norm(ds(x_np.tolist()), ord=3)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_norm_batched_vs_numpy(self):
    rng = np.random.default_rng(303)
    x_np = rng.standard_normal((6, 8))
    result = kd.matrix.vector_norm(ds(x_np.tolist()))
    expected = [float(np.linalg.norm(x_np[i])) for i in range(6)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_norm_inf_vs_numpy(self):
    x_np = np.array([3.0, -4.0, 2.0])
    expected = float(np.linalg.norm(x_np, ord=np.inf))
    result = kd.matrix.vector_norm(ds(x_np.tolist()), ord=float('inf'))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_norm_inf_batched_vs_numpy(self):
    rng = np.random.default_rng(303)
    x_np = rng.standard_normal((6, 8))
    result = kd.matrix.vector_norm(ds(x_np.tolist()), ord=float('inf'))
    expected = [float(np.linalg.norm(x_np[i], ord=np.inf)) for i in range(6)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_norm_neg_inf_vs_numpy(self):
    x_np = np.array([3.0, -1.0, 4.0])
    expected = float(np.linalg.norm(x_np, ord=-np.inf))
    result = kd.matrix.vector_norm(ds(x_np.tolist()), ord=-float('inf'))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_norm_l0_vs_numpy(self):
    x_np = np.array([0.0, 3.0, -4.0, 0.0, 5.0])
    expected = float(np.linalg.norm(x_np, ord=0))
    result = kd.matrix.vector_norm(ds(x_np.tolist()), ord=0)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_norm_negative_finite_ord_vs_numpy(self):
    x_np = np.array([1.0, 2.0, 3.0])
    expected = float(np.linalg.norm(x_np, ord=-1))
    result = kd.matrix.vector_norm(ds(x_np.tolist()), ord=-1)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_norm_negative_ord_batched_vs_numpy(self):
    rng = np.random.default_rng(505)
    # All positive, no zeros: for negative p, 0^p = +inf which makes the
    # entire norm collapse to 0, giving a trivial result.
    x_np = np.abs(rng.standard_normal((6, 8))) + 0.1
    for p in [-0.5, -1, -2, -3]:
      result = kd.matrix.vector_norm(ds(x_np.tolist()), ord=p)
      expected = [float(np.linalg.norm(x_np[i], ord=p)) for i in range(6)]
      testing.assert_allclose(
          result, ds(expected, schema_constants.FLOAT32), atol=1e-5
      )


class ErrorTest(parameterized.TestCase):
  """Tests for error messages."""

  def test_norm_0d_input(self):
    # norm of a scalar is undefined.
    x = ds(3.0)
    with self.assertRaisesRegex(ValueError, r'expected rank\(x\) \> 0'):
      kd.matrix.vector_norm(x)

  def test_1d_x_1d_ord_same_size_fails(self):
    # 1D vector of size 3, `ord` also 1D size 3 -> would silently compute
    # element-wise pow without the ndim guard.
    with self.assertRaisesRegex(
        ValueError, '`ord` must have fewer dimensions than `x`'
    ):
      kd.matrix.vector_norm(ds([3.0, 4.0, 5.0]), ord=ds([2, 1, 3]))

  def test_1d_x_1d_ord_diff_size_fails(self):
    x = ds([3.0, 4.0, 5.0])
    with self.assertRaisesRegex(
        ValueError, '`ord` must have fewer dimensions than `x`'
    ):
      kd.matrix.vector_norm(x, ord=ds([2, 1]))

  def test_ord_higher_rank_than_batch_fails(self):
    # 2D `x` input (2, 2) has 1D batch shape (2,).
    # 2D `ord` (2, 2) has rank >= rank(x), so the ndim guard catches it.
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, '`ord` must have fewer dimensions than `x`'
    ):
      kd.matrix.vector_norm(x, ord=ds([[2, 1], [1, 2]]))

  def test_2d_x_1d_ord_matching_n_not_batch_fails(self):
    # 2D input (2,3), ord=(3,) matches n dim, not batch dim.
    x = ds([[3.0, 4.0, 5.0], [1.0, 2.0, 3.0]])
    with self.assertRaisesRegex(ValueError, 'cannot be expanded to'):
      kd.matrix.vector_norm(x, ord=ds([2, 1, 3]))

  def test_string_schema_fails(self):
    x = ds(['a', 'b', 'c'])
    with self.assertRaisesRegex(
        ValueError,
        'unsupported narrowed schema: STRING',
    ):
      kd.matrix.vector_norm(x)

  def test_object_schema_with_strings_inside_fails(self):
    x = kd.obj(ds(['a', 'b', 'c']))
    with self.assertRaisesRegex(
        ValueError,
        'unsupported narrowed schema: STRING',
    ):
      kd.matrix.vector_norm(x)

  def test_ord_string_schema_fails(self):
    x = ds([3.0, 4.0])
    with self.assertRaisesRegex(
        ValueError,
        'argument `ord` must be a slice of numeric values, got a slice of'
        ' STRING',
    ):
      kd.matrix.vector_norm(x, ord='fro')


if __name__ == '__main__':
  absltest.main()
