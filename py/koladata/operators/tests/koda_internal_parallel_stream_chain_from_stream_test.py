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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators as _
from koladata.operators import koda_internal_parallel


I = input_container.InputContainer('I')
i32 = arolla.int32

STREAM_OF_INT32 = stream_clib.make_stream(arolla.INT32)[0].qtype


class KodaInternalParallelStreamChainFromStreamTest(parameterized.TestCase):

  def test_basic(self):
    sstream, swriter = stream_clib.make_stream(STREAM_OF_INT32)
    res = expr_eval.eval(
        koda_internal_parallel.stream_chain_from_stream(sstream)
    )
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    reader = res.make_reader()
    stream_1, writer_1 = stream_clib.make_stream(arolla.INT32)
    stream_2, writer_2 = stream_clib.make_stream(arolla.INT32)
    swriter.write(stream_1)
    swriter.write(stream_2)
    writer_1.write(i32(1))
    self.assertEqual(reader.read_available(), [i32(1)])
    writer_2.write(i32(2))
    self.assertEqual(reader.read_available(), [])
    writer_1.write(i32(3))
    self.assertEqual(reader.read_available(), [i32(3)])
    writer_2.write(i32(4))
    self.assertEqual(reader.read_available(), [])
    writer_1.close()
    self.assertEqual(reader.read_available(), [i32(2), i32(4)])
    writer_2.close()
    stream_3, writer_3 = stream_clib.make_stream(arolla.INT32)
    swriter.write(stream_3)
    swriter.close()
    writer_3.write(i32(5))
    self.assertEqual(reader.read_available(), [i32(5)])
    writer_3.close()
    self.assertIsNone(reader.read_available())

  def test_empty_input_stream(self):
    sstream, swriter = stream_clib.make_stream(STREAM_OF_INT32)
    res = expr_eval.eval(
        koda_internal_parallel.stream_chain_from_stream(sstream)
    )
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    reader = res.make_reader()
    writers = []
    for _ in range(5):
      stream, writer = stream_clib.make_stream(arolla.INT32)
      writers.append(writer)
      swriter.write(stream)
      self.assertEqual(reader.read_available(), [])
    swriter.close()
    for writer in writers:
      self.assertEqual(reader.read_available(), [])
      writer.close()
    self.assertIsNone(reader.read_available())

  def test_no_input_streams(self):
    sstreams, swriter = stream_clib.make_stream(STREAM_OF_INT32)
    swriter.close()
    res = expr_eval.eval(
        koda_internal_parallel.stream_chain_from_stream(sstreams)
    )
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    self.assertIsNone(res.make_reader().read_available())

  def test_error_on_stream(self):
    sstream, swriter = stream_clib.make_stream(STREAM_OF_INT32)
    res = expr_eval.eval(
        koda_internal_parallel.stream_chain_from_stream(sstream)
    )
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    reader = res.make_reader()
    writers = []
    for _ in range(3):
      stream, writer = stream_clib.make_stream(arolla.INT32)
      writers.append(writer)
      swriter.write(stream)
      self.assertEqual(reader.read_available(), [])
    writers[1].close(ValueError('Boom!'))
    self.assertEqual(reader.read_available(), [])
    writers[0].close()
    with self.assertRaisesRegex(ValueError, re.escape('Boom!')):
      reader.read_available()

  def test_error_non_stream_of_streams(self):
    stream, writer = stream_clib.make_stream(arolla.INT32)
    writer.close()
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a stream of streams, got sstream: STREAM[INT32]'),
    ):
      koda_internal_parallel.stream_chain_from_stream(stream)

  def test_non_determinism(self):
    sstream, swriter = stream_clib.make_stream(STREAM_OF_INT32)
    swriter.close()
    stream_1, stream_2 = expr_eval.eval((
        koda_internal_parallel.stream_chain_from_stream(sstream),
        koda_internal_parallel.stream_chain_from_stream(sstream),
    ))
    self.assertNotEqual(stream_1.fingerprint, stream_2.fingerprint)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.stream_chain_from_stream(I.sstream)
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(koda_internal_parallel.stream_chain_from_stream(I.a)),
        'koda_internal.parallel.stream_chain_from_stream(I.a)',
    )


if __name__ == '__main__':
  absltest.main()
