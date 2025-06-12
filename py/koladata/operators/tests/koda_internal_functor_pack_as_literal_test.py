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
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view
from koladata.operators import koda_internal_functor
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes

ds = data_slice.DataSlice.from_vals
I = input_container.InputContainer('I')


class KodaInternalFunctorPackAsLiteralTest(absltest.TestCase):

  def test_eval(self):
    res = koda_internal_functor.pack_as_literal(arolla.int32(10)).eval()
    testing.assert_equal(
        res, introspection.pack_expr(py_boxing.as_expr(arolla.int32(10)))
    )
    res = koda_internal_functor.pack_as_literal(ds(10)).eval()
    testing.assert_equal(
        res, introspection.pack_expr(py_boxing.as_expr(ds(10)))
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        koda_internal_functor.pack_as_literal,
        [
            (arolla.INT32, qtypes.DATA_SLICE),
            (qtypes.DATA_SLICE, qtypes.DATA_SLICE),
            (arolla.UNSPECIFIED, qtypes.DATA_SLICE),
        ],
        possible_qtypes=(arolla.INT32, arolla.UNSPECIFIED, qtypes.DATA_SLICE),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_functor.pack_as_literal(I.x))
    )


if __name__ == '__main__':
  absltest.main()
