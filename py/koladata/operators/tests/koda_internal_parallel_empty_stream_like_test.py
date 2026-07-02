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
from koladata.functor.parallel import clib as _
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes

I = input_container.InputContainer('I')
kde_internal = kde_operators.internal


class KodaInternalParallelEmptyStreamLikeTest(absltest.TestCase):

  def test_eval(self):
    expr = kde_internal.parallel.empty_stream_like(  # pyrefly: ignore[missing-attribute]
        kde_internal.parallel.stream_make(I.x)  # pyrefly: ignore[missing-attribute]
    )
    res = expr_eval.eval(expr, x=arolla.int32(10))
    self.assertEqual(
        res.qtype,
        expr_eval.eval(kde_internal.parallel.get_stream_qtype(arolla.INT32)),  # pyrefly: ignore[missing-attribute]
    )
    self.assertEqual(res.read_all(timeout=5.0), [])

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(arolla.INT32)  # pyrefly: ignore[missing-attribute]
    )
    stream_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_stream_qtype(arolla.INT32)  # pyrefly: ignore[missing-attribute]
    )
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.empty_stream_like,  # pyrefly: ignore[missing-attribute]
        [
            (stream_int32_qtype, stream_int32_qtype),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES  # pyrefly: ignore[bad-argument-type]
        + (arolla.INT32, future_int32_qtype, stream_int32_qtype),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde_internal.parallel.empty_stream_like(I.x))  # pyrefly: ignore[missing-attribute]
    )


if __name__ == '__main__':
  absltest.main()
