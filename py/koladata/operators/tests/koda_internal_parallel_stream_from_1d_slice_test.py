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
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators
from koladata.operators import koda_internal_parallel
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_slice


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class KodaInternalParallelStreamFrom1DSliceTest(absltest.TestCase):

  def test_basic(self):
    a = fns.new(x=ds([1, 2, 3]))
    res = koda_internal_parallel.stream_from_1d_slice(I.arg).eval(arg=a)
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = res.read_all(timeout=0)
    self.assertLen(res_list, 3)
    testing.assert_equal(res_list[0], a.S[0])
    testing.assert_equal(res_list[1], a.S[1])
    testing.assert_equal(res_list[2], a.S[2])

  def test_mixed_values(self):
    a = ds([1, '2', fns.obj(x=3)])
    res = koda_internal_parallel.stream_from_1d_slice(I.arg).eval(arg=a)
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = res.read_all(timeout=0)
    self.assertLen(res_list, 3)
    testing.assert_equal(res_list[0], a.S[0])
    testing.assert_equal(res_list[1], a.S[1])
    testing.assert_equal(res_list[2], a.S[2])

  def test_empty(self):
    res = koda_internal_parallel.stream_from_1d_slice(I.arg).eval(arg=ds([]))
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = res.read_all(timeout=0)
    self.assertEmpty(res_list)

  def test_non_1d(self):
    with self.assertRaisesRegex(
        ValueError, 'expected a 1D data slice, got 0 dimensions'
    ):
      _ = koda_internal_parallel.stream_from_1d_slice(I.arg).eval(arg=ds(5))

  def test_qtype_signatures(self):
    stream_of_slice = stream_clib.Stream.new(qtypes.DATA_SLICE)[0].qtype
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.stream_from_1d_slice,
        [(qtypes.DATA_SLICE, stream_of_slice)],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.stream_from_1d_slice(I.arg))
    )
    self.assertTrue(view.has_koda_view(kde.streams.from_1d_slice(I.arg)))


if __name__ == '__main__':
  absltest.main()
