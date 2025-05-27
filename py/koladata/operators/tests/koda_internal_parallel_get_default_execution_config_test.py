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

import threading

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import tracing_decorator
from koladata.operators import koda_internal_parallel
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

from koladata.functor.parallel import execution_config_pb2

ds = data_slice.DataSlice.from_vals
I = input_container.InputContainer('I')


class KodaInternalParallelGetDefaultExecutionConfigTest(parameterized.TestCase):

  def test_format_matches_proto(self):
    config = koda_internal_parallel.get_default_execution_config().eval()
    self.assertEqual(config.qtype, qtypes.DATA_SLICE)
    config_proto = fns.to_proto(config, execution_config_pb2.ExecutionConfig)
    reconstructed_config = fns.from_proto(config_proto)
    self.assertEqual(
        config.to_py(max_depth=-1), reconstructed_config.to_py(max_depth=-1)
    )

  def test_works(self):
    # What we would like to test is that the config matches the default
    # execution context, but we cannot extract the config back from the
    # context in Python now. So we do an imperfect proxy test here by just
    # doing one test for the execution context created from the default config.
    # Most of the testing is done in the get_default_execution_context tests.
    context = koda_internal_parallel.create_execution_context(
        koda_internal_parallel.get_default_executor(),
        koda_internal_parallel.get_default_execution_config(),
    ).eval()

    barrier = threading.Barrier(2)

    @tracing_decorator.TraceAsFnDecorator(
        py_fn=True, return_type_as=data_bag.DataBag
    )
    def wait_and_return(x):
      barrier.wait(timeout=5.0)
      return x

    @tracing_decorator.TraceAsFnDecorator(
        return_type_as=(data_bag.DataBag, data_bag.DataBag)
    )
    def f(x, y, z):
      return wait_and_return(user_facing_kd.attrs(x, foo=y)), wait_and_return(
          user_facing_kd.attrs(x, bar=z)
      )

    def g(x):
      parts = f(x, 'foo', 'bar')
      return x.updated(parts[0], parts[1])

    obj = fns.new()
    res_future = koda_internal_parallel.parallel_call(
        I.context, I.fn, I.x
    ).eval(
        context=context,
        fn=koda_internal_parallel.as_future(g).eval(),
        x=koda_internal_parallel.as_future(obj).eval(),
    )
    res = (
        koda_internal_parallel.stream_from_future(res_future)
        .eval()
        .read_all(timeout=5.0)[0]
    )
    testing.assert_equal(res.no_bag(), obj.no_bag())
    testing.assert_equal(res.foo.no_bag(), ds('foo'))
    testing.assert_equal(res.bar.no_bag(), ds('bar'))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.get_default_execution_config,
        [
            (qtypes.DATA_SLICE,),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.get_default_execution_config()
        )
    )


if __name__ == '__main__':
  absltest.main()
