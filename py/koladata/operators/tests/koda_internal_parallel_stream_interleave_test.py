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

import random
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor.parallel import clib
from koladata.operators import kde_operators as _
from koladata.operators import koda_internal_parallel
from koladata.types import qtypes

I = input_container.InputContainer('I')
i32 = arolla.int32


class KodaInternalParallelStreamInterleaveTest(parameterized.TestCase):

  def test_no_input_streams(self):
    res = expr_eval.eval(koda_internal_parallel.stream_interleave())
    self.assertIsInstance(res, clib.Stream)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    self.assertIsNone(res.make_reader().read_available())

  def test_no_input_streams_with_explicit_value_type_as(self):
    res = expr_eval.eval(
        koda_internal_parallel.stream_interleave(value_type_as=i32(0))
    )
    self.assertIsInstance(res, clib.Stream)
    self.assertEqual(res.qtype.value_qtype, arolla.INT32)
    self.assertIsNone(res.make_reader().read_available())

  @parameterized.parameters(*range(1, 5))
  def test_n_input_streams(self, n):
    streams = []
    writers = []
    for _ in range(n):
      stream, writer = clib.make_stream(arolla.INT32)
      streams.append(stream)
      writers.append(writer)
    res = expr_eval.eval(koda_internal_parallel.stream_interleave(*streams))
    self.assertIsInstance(res, clib.Stream)
    self.assertEqual(res.qtype.value_qtype, arolla.INT32)
    reader = res.make_reader()
    for i in range(64):
      writers[random.randrange(n)].write(i32(i))
      self.assertEqual(reader.read_available(), [i32(i)])
    for writer in writers:
      self.assertEqual(reader.read_available(), [])
      writer.close()
    self.assertIsNone(reader.read_available())

  def test_with_explicit_value_type_as(self):
    stream, writer = clib.make_stream(arolla.INT32)
    res = expr_eval.eval(
        koda_internal_parallel.stream_interleave(stream, value_type_as=i32(0))
    )
    self.assertIsInstance(res, clib.Stream)
    self.assertEqual(res.qtype.value_qtype, arolla.INT32)
    writer.write(i32(1))
    writer.close()
    self.assertEqual(res.make_reader().read_available(), [i32(1)])

  def test_error_wrong_value_type_as(self):
    stream, writer = clib.make_stream(arolla.INT32)
    writer.close()
    with self.assertRaisesRegex(
        ValueError,
        re.escape('input streams must be compatible with value_type_as'),
    ):
      _ = expr_eval.eval(
          koda_internal_parallel.stream_interleave(
              stream, value_type_as=arolla.FLOAT32
          )
      )

  def test_error_mixed_stream_types(self):
    stream_1, writer_1 = clib.make_stream(arolla.INT32)
    stream_2, writer_2 = clib.make_stream(arolla.FLOAT32)
    writer_1.close()
    writer_2.close()
    with self.assertRaisesRegex(
        ValueError, re.escape('must have the same value type')
    ):
      _ = expr_eval.eval(
          koda_internal_parallel.stream_interleave(stream_1, stream_2)
      )

  def test_error_non_iterable_arg(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('all inputs must be streams')
    ):
      _ = expr_eval.eval(koda_internal_parallel.stream_interleave(i32(1)))

  def test_empty_input_streams(self):
    streams = []
    writers = []
    for _ in range(5):
      stream, writer = clib.make_stream(arolla.INT32)
      streams.append(stream)
      writers.append(writer)
    reader = expr_eval.eval(
        koda_internal_parallel.stream_interleave(*streams)
    ).make_reader()
    for writer in writers:
      self.assertEqual(reader.read_available(), [])
      writer.close()
    self.assertIsNone(reader.read_available())

  def test_non_determinism(self):
    stream_1, stream_2 = expr_eval.eval((
        koda_internal_parallel.stream_interleave(),
        koda_internal_parallel.stream_interleave(),
    ))
    self.assertNotEqual(stream_1.fingerprint, stream_2.fingerprint)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.stream_interleave())
    )

  def test_repr(self):
    self.assertEqual(
        repr(koda_internal_parallel.stream_interleave(I.a, I.b)),
        'koda_internal.parallel.stream_interleave(I.a, I.b,'
        ' value_type_as=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
