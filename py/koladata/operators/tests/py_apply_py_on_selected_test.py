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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde

DATA_BAG = qtypes.DATA_BAG
DATA_SLICE = qtypes.DATA_SLICE
PY_OBJECT = arolla.abc.PY_OBJECT
UNSPECIFIED = arolla.UNSPECIFIED


def gen_possible_qtypes():
  # We limit the tested types to ensure the qtype_signature test
  # runs in a reasonable time.
  base_qtypes = (PY_OBJECT, UNSPECIFIED, DATA_BAG, DATA_SLICE)
  yield from base_qtypes
  for q in base_qtypes:
    for n in range(3):
      yield arolla.types.make_tuple_qtype(*([q] * n))
      yield arolla.types.make_namedtuple_qtype(**dict(zip(['a', 'b'], [q] * n)))


def gen_qtype_signatures():
  yield (PY_OBJECT, DATA_SLICE, DATA_SLICE)
  for n in range(3):
    args_qtype = arolla.types.make_tuple_qtype(*([DATA_SLICE] * n))
    yield (PY_OBJECT, DATA_SLICE, args_qtype, DATA_SLICE)
    for m in range(3):
      kwargs_qtype = arolla.types.make_namedtuple_qtype(
          **dict(zip(['a', 'b'], [DATA_SLICE] * m))
      )
      yield (PY_OBJECT, DATA_SLICE, args_qtype, kwargs_qtype, DATA_SLICE)


class PyApplyPyOnSelectedTest(parameterized.TestCase):

  def test_1_dim(self):
    x = ds([1, 2, 3])
    y = ds([4, 5, 6])
    f = ds(
        [mask_constants.present, mask_constants.missing, mask_constants.present]
    )
    testing.assert_equal(
        expr_eval.eval(
            kde.py.apply_py_on_selected(lambda x, y: x + y, f, x, y)
        ),
        ds([5, None, 9]),
    )

  def test_0_dim(self):
    x = ds(1)
    y = ds(2)
    f = mask_constants.present
    testing.assert_equal(
        expr_eval.eval(
            kde.py.apply_py_on_selected(lambda x, y: x + y, f, x, y)
        ),
        ds(3),
    )
    f = mask_constants.missing
    testing.assert_equal(
        expr_eval.eval(
            kde.py.apply_py_on_selected(lambda x, y: x + y, f, x, y)
        ),
        ds(None, schema_constants.INT32),
    )

  def test_0_1_dim(self):
    x = ds(1)
    y = ds([1, 2, None])
    f = ds(
        [mask_constants.present, mask_constants.missing, mask_constants.present]
    )
    testing.assert_equal(
        expr_eval.eval(
            kde.py.apply_py_on_selected(lambda x, y: x + y, f, x, y)
        ),
        ds([2, None, None]),
    )

  def test_with_kwargs(self):
    x = ds([1, 2, 3])
    y = ds([4, 5, 6])
    f = ds(
        [mask_constants.present, mask_constants.missing, mask_constants.present]
    )
    testing.assert_equal(
        expr_eval.eval(
            kde.py.apply_py_on_selected(lambda x, y: x + y, f, y=y, x=x)
        ),
        ds([5, None, 9]),
    )

  def test_return_python_type(self):
    f = ds(
        [mask_constants.present, mask_constants.missing, mask_constants.present]
    )
    testing.assert_equal(
        expr_eval.eval(kde.py.apply_py_on_selected(lambda: 1, f)),
        ds(1),
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.py.apply_py_on_selected,
        gen_qtype_signatures(),
        possible_qtypes=gen_possible_qtypes(),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.py.apply_py_on_selected(I.fn, I.cond))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.py.apply_py_on_selected, kde.apply_py_on_selected
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.py.apply_py_on_selected(I.fn, I.cond, I.x, a=I.a)),
        'kd.py.apply_py_on_selected(I.fn, I.cond, I.x, a=I.a)',
    )


if __name__ == '__main__':
  absltest.main()
