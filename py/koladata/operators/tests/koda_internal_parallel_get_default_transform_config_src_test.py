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

import threading

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functions import proto_conversions
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

from koladata.functor.parallel import transform_config_pb2

ds = data_slice.DataSlice.from_vals
I = input_container.InputContainer('I')
koda_internal_parallel = kde_operators.internal.parallel


class KodaInternalParallelGetDefaultTransformConfigSrcTest(
    parameterized.TestCase
):

  def test_format_matches_proto(self):
    config = koda_internal_parallel.get_default_transform_config_src().eval()
    self.assertEqual(config.qtype, qtypes.DATA_SLICE)
    config_proto = proto_conversions.to_proto(
        config, transform_config_pb2.ParallelTransformConfigProto
    )
    reconstructed_config = proto_conversions.from_proto(config_proto)
    self.assertEqual(
        config.to_py(max_depth=-1), reconstructed_config.to_py(max_depth=-1)
    )

  def test_works(self):
    # What we would like to test is that the config_src matches the default
    # parallel transform config, but we cannot extract the config back from the
    # context in Python now. So we do an imperfect proxy test here by just
    # doing one test for the config created from the default_config_src.
    # Most of the testing is done in the get_default_transform_config tests.
    context = koda_internal_parallel.create_transform_config(
        koda_internal_parallel.get_default_transform_config_src(),
    ).eval()

    barrier = threading.Barrier(2)

    @tracing_decorator.TraceAsFnDecorator(
        functor_factory=functor_factories.py_fn, return_type_as=data_bag.DataBag
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
    res_future = koda_internal_parallel.transform(context, g)(
        I.executor,
        I.x,
        return_type_as=koda_internal_parallel.as_future(data_slice.DataSlice),
    ).eval(
        executor=expr_eval.eval(koda_internal_parallel.get_default_executor()),
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
        koda_internal_parallel.get_default_transform_config_src,
        [
            (qtypes.DATA_SLICE,),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.get_default_transform_config_src()
        )
    )


if __name__ == '__main__':
  absltest.main()
