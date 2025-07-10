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

"""Base class for testing common properties for all curves."""

from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer("I")
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


SUPPORTED_INPUT_SCHEMAS = (
    schema_constants.FLOAT32,
    schema_constants.FLOAT64,
    schema_constants.INT32,
    schema_constants.INT64,
    schema_constants.OBJECT,
)


DEFAULT_CURVE = ds([[1, 5], [2, 10], [3, 20]])


class BaseCurveTestcase(parameterized.TestCase):
  """Base class for testing common attributes of all curve operators."""

  @property
  def operator(self):
    raise NotImplementedError("Overloaded in derived classes")

  @parameterized.parameters(
      *[
          (
              ds(inp),
              DEFAULT_CURVE,
              ds(exp, schema_constants.FLOAT64),
          )
          for inp, exp in [
              (1, 5.0),
              (1.0, 5.0),
              (2, 10.0),
              (2.0, 10.0),
              (3, 20.0),
              (3.0, 20.0),
          ]
      ],
      *[
          (
              ds(1, schema),
              DEFAULT_CURVE,
              ds(5.0, schema_constants.FLOAT64),
          )
          for schema in SUPPORTED_INPUT_SCHEMAS
      ],
      (
          ds([1, 3, 2]),
          DEFAULT_CURVE,
          ds([5.0, 20.0, 10.0], schema_constants.FLOAT64),
      ),
      (
          ds([[1, 3], [2]]),
          DEFAULT_CURVE,
          ds([[5.0, 20.0], [10.0]], schema_constants.FLOAT64),
      ),
      (  # mixed
          ds([[1, None, 3, None], [2.0]]),
          DEFAULT_CURVE,
          ds([[5.0, None, 20.0, None], [10.0]], schema_constants.FLOAT64),
      ),
  )
  def test_eval_exact_points(self, p, adjustments, expected):
    """Testing curve in the exact points."""
    result = expr_eval.eval(
        self.operator(I.p, I.adjustments), p=p, adjustments=adjustments
    )
    testing.assert_allclose(result, expected)

  @parameterized.parameters(
      (
          ds(None),
          DEFAULT_CURVE,
          ds(None, schema_constants.FLOAT64),
      ),
      *[
          (
              ds(None, schema),
              DEFAULT_CURVE,
              ds(None, schema_constants.FLOAT64),
          )
          for schema in SUPPORTED_INPUT_SCHEMAS + (schema_constants.NONE,)
      ],
      (
          ds([]),
          DEFAULT_CURVE,
          ds([], schema_constants.FLOAT64),
      ),
      (
          ds([None, None]),
          DEFAULT_CURVE,
          ds([None, None], schema_constants.FLOAT64),
      ),
      *[
          (
              ds([None], schema),
              DEFAULT_CURVE,
              ds([None], schema_constants.FLOAT64),
          )
          for schema in SUPPORTED_INPUT_SCHEMAS + (schema_constants.NONE,)
      ],
  )
  def test_empty_and_unknown_eval(self, p, adjustments, expected):
    """Testing curve on empty inputs."""
    result = expr_eval.eval(
        self.operator(I.p, I.adjustments), p=p, adjustments=adjustments
    )
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (
          ds([[1, 5], [9, 10], [3, 20]]),
          "X_VALUES_NOT_STRICTLY_MONOTONICALLY_INCREASING",
      ),
      (
          ds([]),
          "curve adjustments must be non empty",
      ),
      (
          ds([[None, None]]),
          "curve adjustments must be non empty",
      ),
      (
          ds([[1, 5], [2, None], [3, 20]]),
          "curve adjustments must be all present",
      ),
      (
          ds([[1, 5], [None, 3], [3, 20]]),
          "curve adjustments must be all present",
      ),
      (
          ds([[1, 5.0], [2, None], [3, 20]]),
          "curve adjustments must be all present",
      ),
      (
          ds([[1, 5], [None, 3], [3.0, 20]]),
          "curve adjustments must be all present",
      ),
      (
          ds([1, 5]),
          "rank=2 found: 1",
      ),
      (
          ds([[[1, 5]]]),
          "rank=2 found: 3",
      ),
      (
          ds([[1], [5], [2], [10], [3, 20]]),
          "regular size=2, found: 1",
      ),
      (
          ds([[1, 5, 2], [10, 3, 20]]),
          "regular size=2, found: 3",
      ),
      (
          ds([[], [1, 5], [2, 10], [3, 20]]),
          "regular size=2, found: 0",
      ),
  )
  def test_incorrect_curve(self, adjustments, error_regexp):
    """Test common case incorrect curves."""
    with self.assertRaisesRegex(ValueError, error_regexp):
      expr_eval.eval(
          self.operator(I.p, I.adjustments), p=0, adjustments=adjustments
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            self.operator,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(self.operator(I.x, I.y)))
