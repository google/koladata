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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import parallel as parallel_fns
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kd = eager_op_utils.operators_container(top_level_arolla_container=kde)
kd_internal = eager_op_utils.operators_container(
    top_level_arolla_container=kde_operators.internal
)
DATA_SLICE = qtypes.DATA_SLICE


# The majority of the logic tests are in optools/json_stream_test.cc. These
# tests only cover the bindings.
class JsonStreamUnquoteTest(parameterized.TestCase):

  def test_basic(self):
    result = kd.json_stream.unquote(kd.iterables.make('"hello"'))
    testing.assert_equal(result, kd.iterables.make('hello'))

  def test_parallel_transform(self):
    executor = kd_internal.parallel.get_default_executor()
    result = parallel_fns.transform(
        functor_factories.expr_fn(returns=kde.json_stream.unquote(I.x)),
    )(
        executor,
        x=kd.streams.make('"hello"'),
        return_type_as=kd.streams.make(),
    ).yield_all(
        timeout=1
    )
    testing.assert_equal(kd.stack(*result), ds(['hello']))

  def test_bad_arguments(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected STRING DataItem from input stream, got slice with schema'
            ' INT32 and ndim=0'
        ),
    ):
      kd.json_stream.unquote(kd.iterables.make(123))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected STRING DataItem from input stream, got slice with schema'
            ' STRING and ndim=1'
        ),
    ):
      kd.json_stream.unquote(kd.iterables.make(ds(['x', 'y'])))

  @arolla.abc.add_default_cancellation_context
  def test_cancellation(self):
    stream, _ = stream_clib.Stream.new(DATA_SLICE)
    result_stream = parallel_fns.transform(
        functor_factories.expr_fn(returns=kde.json_stream.unquote(I.x)),
    )(
        kd_internal.parallel.get_default_executor(),
        x=stream,
        return_type_as=kd.streams.make(),
    )

    cancellation_context = arolla.abc.current_cancellation_context()
    assert cancellation_context is not None
    cancellation_context.cancel('Boom!')
    with self.assertRaisesRegex(ValueError, r'\[CANCELLED\].*Boom!'):
      result_stream.read_all(timeout=1)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.json_stream.unquote(I.x)))


if __name__ == '__main__':
  absltest.main()
