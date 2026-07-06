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
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


class EntitiesUuTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          '',
          dict(a=ds(1), b=ds(2)),
          '',
          dict(b=ds(2), a=ds(1)),
      ),
      (
          'specified_seed',
          dict(a=ds(1), b=ds(2)),
          'specified_seed',
          dict(b=ds(2), a=ds(1)),
      ),
      (
          ds('specified_seed'),
          dict(a=ds(1), b=ds(2)),
          ds('specified_seed'),
          dict(b=ds(2), a=ds(1)),
      ),
      (
          '',
          dict(a=ds([1, 2, 3]), b=ds(2)),
          '',
          dict(b=ds(2), a=ds([1, 2, 3])),
      ),
      (
          '',
          dict(a=ds([1, None, 3]), b=ds(2)),
          '',
          dict(b=ds(2), a=ds([1, None, 3])),
      ),
      (
          '',
          dict(a=ds([1, 2, 3]), b=2),
          '',
          dict(b=2, a=ds([1, 2, 3])),
      ),
  )
  def test_equal(self, lhs_seed, lhs_kwargs, rhs_seed, rhs_kwargs):
    lhs = kd.entities.uu(seed=lhs_seed, **lhs_kwargs)
    rhs = kd.entities.uu(seed=rhs_seed, **rhs_kwargs)
    # Check that required attributes are present.
    for attr_name, val in lhs_kwargs.items():
      testing.assert_equal(
          getattr(lhs, attr_name),
          ds(val).expand_to(lhs).with_bag(lhs.get_bag()),
      )
    for attr_name, val in rhs_kwargs.items():
      testing.assert_equal(
          getattr(rhs, attr_name),
          ds(val).expand_to(rhs).with_bag(rhs.get_bag()),
      )
    testing.assert_equal(lhs.no_bag(), rhs.no_bag())
    self.assertFalse(lhs.is_mutable())
    self.assertFalse(rhs.is_mutable())

  @parameterized.parameters(
      ('', dict(a=ds(1), b=ds(2)), '', dict(a=ds(1), c=ds(2))),
      (
          '',
          dict(a=ds(1), b=ds(2)),
          '',
          dict(a=ds(2), b=ds(1)),
      ),
      (
          'seed1',
          dict(a=ds(1), b=ds(2)),
          'seed2',
          dict(a=ds(1), b=ds(2)),
      ),
  )
  def test_not_equal(self, lhs_seed, lhs_kwargs, rhs_seed, rhs_kwargs):
    lhs = kd.entities.uu(seed=lhs_seed, **lhs_kwargs)
    rhs = kd.entities.uu(seed=rhs_seed, **rhs_kwargs)
    self.assertNotEqual(
        lhs.fingerprint, rhs.with_bag(lhs.get_bag()).fingerprint
    )

  def test_schema_arg(self):
    db = data_bag.DataBag.empty_mutable()
    uu = kd.entities.uu(
        seed='',
        a=ds([3.14], schema_constants.FLOAT32),
        schema=db.uu_schema(a=schema_constants.FLOAT64),
    )
    testing.assert_equal(
        uu.get_schema(),
        db.uu_schema(a=schema_constants.FLOAT64).with_bag(uu.get_bag()),
    )
    testing.assert_equal(
        uu.a.get_schema(), schema_constants.FLOAT64.with_bag(uu.get_bag())
    )
    testing.assert_allclose(
        uu.a,
        ds([3.14], schema_constants.FLOAT64).with_bag(uu.get_bag()),
        atol=1e-6,
    )

  def test_str_as_schema_arg(self):
    db = data_bag.DataBag.empty_mutable()
    uu = kd.entities.uu(
        seed='anything', a=ds([3.14], schema_constants.FLOAT32), schema='a'
    )
    testing.assert_equal(
        uu.get_schema(), db.named_schema('a').with_bag(uu.get_bag())
    )
    testing.assert_equal(
        uu.a.get_schema(), schema_constants.FLOAT32.with_bag(uu.get_bag())
    )
    testing.assert_allclose(
        uu.a,
        ds([3.14], schema_constants.FLOAT32).with_bag(uu.get_bag()),
        atol=1e-6,
    )

  def test_overwrite_schema_arg(self):
    db = data_bag.DataBag.empty_mutable()
    uu = kd.entities.uu(
        seed='',
        a=ds([3.14], schema_constants.FLOAT32),
        schema=db.uu_schema(a=schema_constants.FLOAT64),
        overwrite_schema=True,
    )
    testing.assert_equal(
        uu.a.get_schema(), schema_constants.FLOAT32.with_bag(uu.get_bag())
    )
    testing.assert_allclose(
        uu.a, ds([3.14], schema_constants.FLOAT32).with_bag(uu.get_bag())
    )

  def test_default_seed(self):
    lhs = kd.entities.uu(a=ds(1), b=ds(2))
    rhs = kd.entities.uu('', a=ds(1), b=ds(2))
    testing.assert_equal(lhs.no_bag(), rhs.no_bag())

  def test_no_args(self):
    lhs = kd.entities.uu()
    rhs = kd.entities.uu('')
    testing.assert_equal(lhs.no_bag(), rhs.no_bag())

  def test_keywod_only_args(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'takes from 0 to 1 positional arguments but 2 were given'
    ):
      _ = kd.entities.uu(ds('1'), ds('a'))

  def test_bag_adoption(self):
    a = kd.entities.uu(a=1)
    b = kd.entities.uu(a=a)
    testing.assert_equal(b.a.a, ds(1).with_bag(b.get_bag()))

  @parameterized.parameters(
      (
          '',
          arolla.unspecified(),
          False,
          dict(a=ds([1, 2, 3]), b=ds([1, 2])),
          (
              'kd.entities.uu: cannot align shapes due to a shape not being'
              ' broadcastable to the common shape candidate.'
          ),
      ),
      (
          ds(['seed1', 'seed2']),
          arolla.unspecified(),
          False,
          dict(a=ds([1, 2, 3]), b=ds([1, 2, 3])),
          (
              'kd.entities.uu: argument `seed` must be an item holding STRING,'
              ' got a slice of rank 1 > 0'
          ),
      ),
      (
          0,
          arolla.unspecified(),
          False,
          dict(a=ds([1, 2, 3]), b=ds([1, 2, 3])),
          (
              'kd.entities.uu: argument `seed` must be an item holding STRING,'
              ' got an item of INT32'
          ),
      ),
      (
          '',
          0,
          False,
          dict(a=ds([1, 2, 3]), b=ds([1, 2, 3])),
          r'schema\'s schema must be SCHEMA, got: INT32',
      ),
      (
          '',
          arolla.unspecified(),
          0,
          dict(a=ds([1, 2, 3]), b=ds([1, 2, 3])),
          (
              'kd.entities.uu: argument `overwrite_schema` must be an item '
              'holding BOOLEAN, got an item of INT32'
          ),
      ),
  )
  def test_error(self, seed, schema, overwrite_schema, kwargs, err_regex):
    with self.assertRaisesRegex(ValueError, err_regex):
      _ = kd.entities.uu(
          seed=seed, schema=schema, overwrite_schema=overwrite_schema, **kwargs
      )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.entities.uu(seed=I.seed)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.entities.uu, kde.uu))

  def test_repr(self):
    self.assertEqual(
        repr(kde.entities.uu(a=I.z, seed=I.seed)),
        'kd.entities.uu(I.seed, schema=unspecified,'
        ' overwrite_schema=DataItem(False, schema: BOOLEAN), a=I.z)',
    )
    self.assertEqual(
        repr(kde.entities.uu(I.seed, a=I.z)),
        'kd.entities.uu(I.seed, schema=unspecified,'
        ' overwrite_schema=DataItem(False, schema: BOOLEAN), a=I.z)',
    )


if __name__ == '__main__':
  absltest.main()
