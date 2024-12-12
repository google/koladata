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

I = input_container.InputContainer('I')
M = arolla.M
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde


class KodaUuObjTest(parameterized.TestCase):

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
    lhs = expr_eval.eval(kde.core.uuobj(seed=lhs_seed, **lhs_kwargs))
    rhs = expr_eval.eval(kde.core.uuobj(seed=rhs_seed, **rhs_kwargs))
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
    testing.assert_equal(lhs, rhs.with_bag(lhs.get_bag()))
    self.assertFalse(lhs.is_mutable())
    self.assertFalse(rhs.is_mutable())

  @parameterized.parameters(
      (
          '',
          dict(a=ds(1), b=ds(2)),
          '',
          dict(a=ds(1), c=ds(2)),
      ),
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
    lhs = expr_eval.eval(kde.core.uuobj(seed=lhs_seed, **lhs_kwargs))
    rhs = expr_eval.eval(kde.core.uuobj(seed=rhs_seed, **rhs_kwargs))
    self.assertNotEqual(
        lhs.fingerprint, rhs.with_bag(lhs.get_bag()).fingerprint
    )

  def test_default_seed(self):
    lhs = expr_eval.eval(kde.core.uuobj(a=ds(1), b=ds(2)))
    rhs = expr_eval.eval(kde.core.uuobj('', a=ds(1), b=ds(2)))
    testing.assert_equal(lhs, rhs.with_bag(lhs.get_bag()))

  def test_no_args(self):
    lhs = expr_eval.eval(kde.core.uuobj())
    rhs = expr_eval.eval(kde.core.uuobj(''))
    testing.assert_equal(lhs, rhs.with_bag(lhs.get_bag()))

  def test_keywod_only_args(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'takes from 0 to 1 positional arguments but 2 were given'
    ):
      _ = expr_eval.eval(kde.core.uuobj(ds('1'), ds('a')))

  def test_bag_adoption(self):
    a = expr_eval.eval(kde.core.uuobj(a=1))
    b = expr_eval.eval(kde.core.uuobj(a=a))
    testing.assert_equal(b.a.a, ds(1).with_bag(b.get_bag()))

  @parameterized.parameters(
      (
          '',
          dict(a=ds([1, 2, 3]), b=ds([1, 2])),
          'shapes are not compatible',
      ),
      (
          ds(['seed1', 'seed2']),
          dict(a=ds([1, 2, 3]), b=ds([1, 2, 3])),
          (
              'argument `seed` must be an item holding STRING, got a slice of'
              ' rank 1 > 0'
          ),
      ),
      (
          0,
          dict(a=ds([1, 2, 3]), b=ds([1, 2, 3])),
          (
              'argument `seed` must be an item holding STRING, got an item of'
              ' INT32'
          ),
      ),
  )
  def test_error(self, seed, kwargs, err_regex):
    with self.assertRaisesRegex(
        ValueError,
        err_regex,
    ):
      _ = expr_eval.eval(kde.core.uuobj(seed=seed, **kwargs))

  def test_non_data_slice_binding(self):
    with self.assertRaisesRegex(
        ValueError, 'expected all arguments to be DATA_SLICE'
    ):
      _ = kde.core.uuobj(
          a=ds(1),
          b=arolla.unspecified(),
      )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.uuobj(seed=I.seed)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.uuobj, kde.uuobj))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.uuobj(a=I.z, seed=I.seed)),
        'kde.core.uuobj(I.seed, a=I.z)',
    )
    self.assertEqual(
        repr(kde.core.uuobj(I.seed, a=I.z)),
        'kde.core.uuobj(I.seed, a=I.z)',
    )


if __name__ == '__main__':
  absltest.main()
