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

import re
from absl.testing import absltest
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor.parallel import clib
from koladata.operators import koda_internal_parallel
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


def _make_stream(*items: arolla.QValue) -> clib.Stream:
  assert items
  stream, writer = clib.make_stream(items[0].qtype)
  for item in items:
    writer.write(item)
  writer.close()
  return stream


class KodaInternalParallelStreamChainTest(absltest.TestCase):

  def _read_full_stream(self, res: clib.Stream) -> list[arolla.QValue]:
    stream_reader = res.make_reader()
    res = stream_reader.read_available() or []
    self.assertIsNone(stream_reader.read_available())
    return res

  def test_chain(self):
    res = expr_eval.eval(
        koda_internal_parallel.stream_chain(
            _make_stream(ds(1), ds(2)), _make_stream(ds(3))
        )
    )
    res_list = self._read_full_stream(res)
    self.assertLen(res_list, 3)
    testing.assert_equal(res_list[0], ds(1))
    testing.assert_equal(res_list[1], ds(2))
    testing.assert_equal(res_list[2], ds(3))

  def test_does_maximum_possible_progress(self):
    stream1, stream1_writer = clib.make_stream(arolla.INT32)
    stream2, stream2_writer = clib.make_stream(arolla.INT32)
    stream3, stream3_writer = clib.make_stream(arolla.INT32)
    chained_stream = expr_eval.eval(
        koda_internal_parallel.stream_chain(I.stream1, I.stream2, I.stream3),
        stream1=stream1,
        stream2=stream2,
        stream3=stream3,
    )
    stream1_writer.write(arolla.int32(0))
    stream2_writer.write(arolla.int32(2))
    stream1_writer.write(arolla.int32(1))
    stream2_writer.write(arolla.int32(3))
    stream3_writer.write(arolla.int32(4))
    stream2_writer.close()
    stream_reader = chained_stream.make_reader()
    self.assertEqual(
        stream_reader.read_available(),
        [arolla.int32(0), arolla.int32(1)],
    )
    stream1_writer.close()
    self.assertEqual(
        stream_reader.read_available(),
        [arolla.int32(2), arolla.int32(3), arolla.int32(4)],
    )
    self.assertEqual(stream_reader.read_available(), [])
    stream3_writer.close()
    self.assertIsNone(stream_reader.read_available())

  def test_error_in_one_stream(self):
    stream1, stream1_writer = clib.make_stream(arolla.INT32)
    stream2, stream2_writer = clib.make_stream(arolla.INT32)
    stream1_writer.close(AssertionError('test error'))
    stream2_writer.write(arolla.int32(1))
    stream2_writer.close()
    chained_stream = expr_eval.eval(
        koda_internal_parallel.stream_chain(I.stream1, I.stream2),
        stream1=stream1,
        stream2=stream2,
    )
    stream_reader = chained_stream.make_reader()
    with self.assertRaisesRegex(AssertionError, 'test error'):
      _ = stream_reader.read_available()

  def test_chain_with_bags(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    res = expr_eval.eval(
        koda_internal_parallel.stream_chain(
            _make_stream(db1), _make_stream(db2)
        )
    )
    res_list = self._read_full_stream(res)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], db1)
    testing.assert_equal(res_list[1], db2)

  def test_chain_with_bags_explicit_value_type_as(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    res = expr_eval.eval(
        koda_internal_parallel.stream_chain(
            _make_stream(db1),
            _make_stream(db2),
            value_type_as=data_bag.DataBag,
        )
    )
    res_list = self._read_full_stream(res)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], db1)
    testing.assert_equal(res_list[1], db2)

  def test_chain_with_bags_wrong_value_type_as(self):
    db1 = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        ValueError,
        'input streams must be compatible with value_type_as',
    ):
      _ = expr_eval.eval(
          koda_internal_parallel.stream_chain(
              _make_stream(db1), value_type_as=ds(1)
          )
      )

  def test_chain_with_empty_bags_wrong_value_type_as(self):
    with self.assertRaisesRegex(
        ValueError,
        'input streams must be compatible with value_type_as',
    ):
      _ = expr_eval.eval(
          koda_internal_parallel.stream_chain(
              koda_internal_parallel.stream_chain(
                  value_type_as=data_bag.DataBag
              ),
              value_type_as=ds(1),
          )
      )

  def test_chain_mixed_types(self):
    with self.assertRaisesRegex(ValueError, 'must have the same value type'):
      _ = expr_eval.eval(
          koda_internal_parallel.stream_chain(
              _make_stream(ds(1)),
              _make_stream(data_bag.DataBag.empty()),
          )
      )

  def test_chain_empty(self):
    res = expr_eval.eval(koda_internal_parallel.stream_chain())
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_SLICE)
    self.assertEmpty(self._read_full_stream(res))

  def test_chain_empty_with_value_type_as(self):
    res = expr_eval.eval(
        koda_internal_parallel.stream_chain(value_type_as=data_bag.DataBag)
    )
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_BAG)
    self.assertEmpty(self._read_full_stream(res))

  def test_chain_with_only_empty_stream(self):
    res = expr_eval.eval(
        koda_internal_parallel.stream_chain(
            koda_internal_parallel.stream_chain(value_type_as=data_bag.DataBag)
        )
    )
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_BAG)
    self.assertEmpty(self._read_full_stream(res))

  def test_non_stream_arg(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('all inputs must be streams'),
    ):
      _ = expr_eval.eval(koda_internal_parallel.stream_chain(ds(1)))

  def test_non_determinism(self):
    res = expr_eval.eval(
        arolla.M.core.make_tuple(
            koda_internal_parallel.stream_chain(),
            koda_internal_parallel.stream_chain(),
        )
    )
    self.assertNotEqual(res[0].fingerprint, res[1].fingerprint)

  def test_view(self):
    self.assertTrue(view.has_koda_view(koda_internal_parallel.stream_chain()))


if __name__ == '__main__':
  absltest.main()
