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
from koladata.expr import input_container
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


class DictsUuTest(parameterized.TestCase):

  def test_basics(self):
    keys = ds([1, 2])
    values = ds([3, 4])
    res = kd.dicts.uu(keys, values)

    self.assertFalse(res.is_mutable())
    self.assertTrue(res.get_schema().is_dict_schema())
    testing.assert_equivalent(kd.sort(res.get_keys()), kd.sort(keys))
    testing.assert_equivalent(kd.sort(res.get_values()), kd.sort(values))
    testing.assert_equivalent(res[1], ds(3))
    testing.assert_equivalent(res[2], ds(4))

  @parameterized.parameters(
      (ds([1, 2]), ds([3, 4]), ''),
      (ds([1, 2]), ds([3, 4]), 'specified_seed'),
  )
  def test_uuid_equals(self, keys, values, seed):
    lhs = kd.dicts.uu(keys, values, seed=seed)
    rhs = kd.dicts.uu(keys, values, seed=seed)
    testing.assert_equal(lhs.no_bag(), rhs.no_bag())

  @parameterized.parameters(
      (ds([1, 2]), ds([3, 4]), '', ds([1, 2]), ds([3, 5]), ''),
      (ds([1, 2]), ds([3, 4]), '', ds([1, 3]), ds([3, 4]), ''),
      (ds([1, 2]), ds([3, 4]), 'seed1', ds([1, 2]), ds([3, 4]), 'seed2'),
  )
  def test_uuid_not_equals(
      self, lhs_keys, lhs_values, lhs_seed, rhs_keys, rhs_values, rhs_seed
  ):
    lhs = kd.dicts.uu(lhs_keys, lhs_values, seed=lhs_seed)
    rhs = kd.dicts.uu(rhs_keys, rhs_values, seed=rhs_seed)
    self.assertNotEqual(
        lhs.fingerprint, rhs.with_bag(lhs.get_bag()).fingerprint
    )

  def test_default_seed(self):
    lhs = kd.dicts.uu(ds([1, 2]), ds([3, 4]))
    rhs = kd.dicts.uu(ds([1, 2]), ds([3, 4]), seed='')
    testing.assert_equal(lhs.no_bag(), rhs.no_bag())

  def test_python_dict(self):
    d = kd.uudict({1: 2, 3: 4})
    testing.assert_equal(d.no_bag(), kd.uudict(ds([1, 3]), ds([2, 4])).no_bag())
    testing.assert_equal(d[ds(1)], ds(2).with_bag(d.get_bag()))
    testing.assert_equal(d[ds(3)], ds(4).with_bag(d.get_bag()))

  def test_python_dict_and_values_error(self):
    with self.assertRaisesRegex(
        ValueError, 'if items_or_keys is a dict, values must be unspecified'
    ):
      kd.uudict({1: 2}, ds([3, 4]))

  def test_key_schema(self):
    d = kd.dicts.uu(
        ds([1, 2]),
        ds([3, 4]),
        key_schema=schema_constants.INT64,
    )
    testing.assert_equal(
        d[ds(1, schema_constants.INT64)],
        ds(3, schema_constants.INT32).with_bag(d.get_bag()),
    )

  def test_value_schema(self):
    d = kd.dicts.uu(
        ds([1, 2]),
        ds([3, 4]),
        value_schema=schema_constants.INT64,
    )
    testing.assert_equal(
        d[ds(1)],
        ds(3, schema_constants.INT64).with_bag(d.get_bag()),
    )

  def test_schema(self):
    dict_schema = kd.dict_schema(
        schema_constants.INT64, schema_constants.OBJECT
    )
    d = kd.dicts.uu(
        ds([1, 2]),
        ds([3, 4]),
        schema=dict_schema,
    )
    testing.assert_equal(
        d[ds(1, schema_constants.INT64)],
        ds(3, schema_constants.OBJECT).with_bag(d.get_bag()),
    )

  def test_determinism(self):
    keys = ds([2, 3]).freeze_bag()
    values = ds([3, 7]).freeze_bag()
    expr = kde.dicts.uu(I.keys, I.values)
    res_1 = expr.eval(keys=keys, values=values)
    res_2 = expr.eval(keys=keys, values=values)
    testing.assert_equivalent(res_1, res_2, ids_equality=True)

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.dicts.uu, kde.uudict))


if __name__ == '__main__':
  absltest.main()
