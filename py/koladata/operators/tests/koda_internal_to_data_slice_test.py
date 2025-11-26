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

"""Tests for koda_to_data_slice."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import literal_operator


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde_internal = kde_operators.internal


def gen_testcases():
  # DataSlice fallthrough
  yield ds(1), ds(1)
  yield ds([1, 2, 3]), ds([1, 2, 3])
  # Standard arolla types.
  values = (None, 1.5, 0, 1, 'a', b'a', True, False)
  qtypes = pointwise_test_utils.lift_qtypes(
      arolla.INT32,
      arolla.INT64,
      arolla.FLOAT32,
      arolla.FLOAT64,
      arolla.BOOLEAN,
      arolla.UNIT,
      arolla.TEXT,
      arolla.BYTES,
  )
  for x, res in pointwise_test_utils.gen_cases(
      tuple((v, v) for v in values), *((qtype, qtype) for qtype in qtypes)
  ):
    if arolla.types.is_array_qtype(x.qtype):
      ds_res = ds(res)
      yield x, ds_res
    else:
      ds_res = ds(res)
      yield x, ds_res
  # We also test fully missing values explicitly.
  yield arolla.array_int32([None]), ds(arolla.array_int32([None]))
  yield arolla.dense_array_int32([None]), ds(arolla.dense_array_int32([None]))
  yield arolla.optional_int32(None), ds(
      arolla.optional_int32(None), ds(arolla.INT32)
  )


TEST_CASES = tuple(gen_testcases())
QTYPES = frozenset(tuple(arg.qtype for arg in args) for args in TEST_CASES)


class KodaToDataSliceTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde_internal.to_data_slice(x))
    testing.assert_equal(res, expected)

  def test_unsupported_value(self):
    with self.assertRaisesRegex(
        ValueError,
        r'expected the scalar qtype to be one of \[BOOLEAN,.*\], got x: UINT64',
    ):
      kde_internal.to_data_slice(arolla.types.uint64(1))

  def test_boxing_qvalue(self):
    arolla.testing.assert_expr_equal_by_fingerprint(
        kde_internal.to_data_slice(arolla.int64(1)),
        arolla.abc.bind_op(
            kde_internal.to_data_slice,
            literal_operator.literal(arolla.int64(1)),
        ),
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde_internal.to_data_slice,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde_internal.to_data_slice(I.x)))


if __name__ == '__main__':
  absltest.main()
