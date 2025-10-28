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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import bootstrap
from koladata.operators import kde_operators
from koladata.types import data_bag
from koladata.types import iterable_qvalue
from koladata.types import qtypes


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty_mutable
koda_internal_iterables = kde_operators.internal.iterables


_ALL_POSSIBLE_QTYPES = list(arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES)
_ALL_POSSIBLE_QTYPES.append(arolla.types.make_sequence_qtype(arolla.INT32))
_ALL_POSSIBLE_QTYPES.append(arolla.types.make_sequence_qtype(qtypes.DATA_SLICE))
_ALL_POSSIBLE_QTYPES.append(arolla.types.make_sequence_qtype(qtypes.DATA_BAG))
_ALL_POSSIBLE_QTYPES.append(
    arolla.eval(koda_internal_iterables.get_iterable_qtype(arolla.INT32))
)
_ALL_POSSIBLE_QTYPES.append(
    arolla.eval(koda_internal_iterables.get_iterable_qtype(qtypes.DATA_SLICE))
)
_ALL_POSSIBLE_QTYPES.append(
    arolla.eval(koda_internal_iterables.get_iterable_qtype(qtypes.DATA_BAG))
)
_ALL_POSSIBLE_QTYPES.extend([qtypes.DATA_SLICE, qtypes.DATA_BAG])


_QTYPE_SIGNATURES = tuple(
    (arg_qtype, arg_qtype)
    for arg_qtype in _ALL_POSSIBLE_QTYPES
    if arolla.eval(bootstrap.is_iterable_qtype(arg_qtype))
)


class IterablesInternalEmptyAsTest(absltest.TestCase):

  def test_return_value(self):
    iterable = iterable_qvalue.Iterable(bag())
    res = expr_eval.eval(koda_internal_iterables.empty_as(I.x), x=iterable)
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.name, 'ITERABLE[DATA_BAG]')
    self.assertEmpty(list(res))

  def test_empty_input(self):
    iterable = iterable_qvalue.Iterable(value_type_as=bag())
    res = expr_eval.eval(koda_internal_iterables.empty_as(I.x), x=iterable)
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.name, 'ITERABLE[DATA_BAG]')
    self.assertEmpty(list(res))

  def test_qtype_signatures(self):
    self.assertEqual(
        frozenset(
            arolla.testing.detect_qtype_signatures(
                koda_internal_iterables.empty_as,
                possible_qtypes=_ALL_POSSIBLE_QTYPES,
            )
        ),
        frozenset(_QTYPE_SIGNATURES),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(koda_internal_iterables.empty_as(I.x)))


if __name__ == '__main__':
  absltest.main()
