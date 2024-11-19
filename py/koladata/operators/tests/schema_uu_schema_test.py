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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
M = arolla.M
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde


class KodaUuSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          '',
          dict(a=schema_constants.INT32, b=schema_constants.FLOAT32),
          '',
          dict(b=schema_constants.FLOAT32, a=schema_constants.INT32),
      ),
      (
          'specified_seed',
          dict(a=schema_constants.INT32, b=schema_constants.FLOAT32),
          'specified_seed',
          dict(b=schema_constants.FLOAT32, a=schema_constants.INT32),
      ),
      (
          ds('specified_seed'),
          dict(a=schema_constants.INT32, b=schema_constants.FLOAT32),
          ds('specified_seed'),
          dict(b=schema_constants.FLOAT32, a=schema_constants.INT32),
      )
  )
  def test_equal(self, lhs_seed, lhs_kwargs, rhs_seed, rhs_kwargs):
    lhs = expr_eval.eval(kde.schema.uu_schema(lhs_seed, **lhs_kwargs))
    rhs = expr_eval.eval(kde.schema.uu_schema(rhs_seed, **rhs_kwargs))
    # Check that required attributes are present.
    for attr_name, val in lhs_kwargs.items():
      testing.assert_equal(
          getattr(lhs, attr_name), ds(val).with_bag(lhs.get_bag())
      )
    for attr_name, val in rhs_kwargs.items():
      testing.assert_equal(
          getattr(rhs, attr_name), ds(val).with_bag(rhs.get_bag())
      )
    testing.assert_equal(lhs, rhs.with_bag(lhs.get_bag()))
    self.assertFalse(lhs.is_mutable())

  @parameterized.parameters(
      (
          '',
          dict(a=schema_constants.INT32, b=schema_constants.FLOAT32),
          '',
          dict(a=schema_constants.INT32, c=schema_constants.FLOAT32)
      ),
      (
          '',
          dict(a=schema_constants.INT32, b=schema_constants.FLOAT32),
          '',
          dict(a=schema_constants.FLOAT32, b=schema_constants.INT32),
      ),
      (
          'seed1',
          dict(a=schema_constants.INT32, b=schema_constants.FLOAT32),
          'seed2',
          dict(a=schema_constants.INT32, b=schema_constants.FLOAT32),
      ),
  )
  def test_not_equal(self, lhs_seed, lhs_kwargs, rhs_seed, rhs_kwargs):
    lhs = expr_eval.eval(kde.schema.uu_schema(lhs_seed, **lhs_kwargs))
    rhs = expr_eval.eval(kde.schema.uu_schema(rhs_seed, **rhs_kwargs))
    self.assertNotEqual(
        lhs.fingerprint, rhs.with_bag(lhs.get_bag()).fingerprint
    )

  def test_default_seed(self):
    lhs = expr_eval.eval(
        kde.schema.uu_schema(
            a=schema_constants.INT32, b=schema_constants.FLOAT32
        )
    )
    rhs = expr_eval.eval(
        kde.schema.uu_schema(
            '', a=schema_constants.INT32, b=schema_constants.FLOAT32
        )
    )
    testing.assert_equal(lhs, rhs.with_bag(lhs.get_bag()))

  def test_no_args(self):
    lhs = expr_eval.eval(kde.schema.uu_schema())
    rhs = expr_eval.eval(kde.schema.uu_schema(''))
    testing.assert_equal(lhs, rhs.with_bag(lhs.get_bag()))

  def test_seed_works_as_kwarg(self):
    lhs = expr_eval.eval(
        kde.schema.uu_schema(
            ds('seed'), a=schema_constants.INT32, b=schema_constants.FLOAT32
        )
    )
    rhs = expr_eval.eval(
        kde.schema.uu_schema(
            a=schema_constants.INT32,
            b=schema_constants.FLOAT32,
            seed=ds('seed'),
        )
    )
    testing.assert_equal(lhs, rhs.with_bag(lhs.get_bag()))

  def test_bag_adoption(self):
    a = expr_eval.eval(kde.schema.uu_schema(a=schema_constants.INT32))
    b = expr_eval.eval(kde.schema.uu_schema(a=a))
    testing.assert_equal(
        b.a.a, ds(schema_constants.INT32).with_bag(b.get_bag())
    )

  @parameterized.parameters(
      (
          ds(['seed1', 'seed2']),
          dict(a=schema_constants.INT32, b=schema_constants.FLOAT32),
          'requires `seed` to be DataItem holding string, got DataSlice',
      ),
      (
          0,
          dict(a=schema_constants.INT32, b=schema_constants.FLOAT32),
          (
              r'requires `seed` to be DataItem holding string, got DataItem\(0'
              r', schema: INT32\)'
          ),
      ),
      (
          ds('seed1'),
          dict(a=ds(0), b=schema_constants.FLOAT32),
          'schema\'s schema must be SCHEMA, got: INT32',
      )
  )
  def test_error(self, seed, kwargs, err_regex):
    with self.assertRaisesRegex(
        ValueError,
        err_regex,
    ):
      _ = expr_eval.eval(kde.schema.uu_schema(seed, **kwargs))

  def test_non_data_slice_binding(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected all arguments to be DATA_SLICE, got kwargs:'
        ' namedtuple<a=DATA_SLICE,b=UNSPECIFIED>',
    ):
      _ = expr_eval.eval(
          kde.schema.uu_schema(
              a=schema_constants.INT32,
              b=arolla.unspecified(),
          )
      )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.uu_schema(I.seed)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.uu_schema, kde.uu_schema))

  def test_repr(self):
    self.assertEqual(
        repr(kde.schema.uu_schema(a=I.a)),
        "kde.schema.uu_schema(DataItem('', schema: STRING), a=I.a)",
    )


if __name__ == '__main__':
  absltest.main()
