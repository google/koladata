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
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_slice


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
koda_internal_parallel = kde_operators.internal.parallel


class KodaInternalParallelTransformManyTest(absltest.TestCase):

  def test_item(self):
    fn = functor_factories.trace_py_fn(lambda x, y: x + y)
    config = koda_internal_parallel.create_transform_config(None)
    transformed_fn = koda_internal_parallel.transform_many(config, fn).eval()
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(
            transformed_fn(
                koda_internal_parallel.get_eager_executor(),
                koda_internal_parallel.as_future(1),
                koda_internal_parallel.as_future(2),
                return_type_as=koda_internal_parallel.as_future(
                    data_slice.DataSlice
                ),
            )
        ).eval(),
        ds(3),
    )

  def test_missing_item(self):
    fn = fns.obj(None)
    config = koda_internal_parallel.create_transform_config(None)
    transformed_fn = koda_internal_parallel.transform_many(config, fn).eval()
    # Note that here we get schema NONE, not schema OBJECT. It's probably OK.
    testing.assert_equal(transformed_fn, ds(None))

  def test_unspecified(self):
    fn = arolla.unspecified()
    config = koda_internal_parallel.create_transform_config(None)
    transformed_fn = koda_internal_parallel.transform_many(config, fn).eval()
    testing.assert_equal(transformed_fn, fn)

  def test_slice(self):
    fn1 = functor_factories.trace_py_fn(lambda x, y: x + y)
    fn2 = functor_factories.trace_py_fn(lambda x, y: x - y)
    fn = fns.slice([[fn1, None, fn2], [fn1, fn1, None, None]])
    config = koda_internal_parallel.create_transform_config(None)
    transformed_fn = koda_internal_parallel.transform_many(config, fn).eval()
    testing.assert_equal(transformed_fn.get_shape(), fn.get_shape())
    transformed_fn1 = transformed_fn.S[0, 0]
    transformed_fn2 = transformed_fn.S[0, 2]
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(
            transformed_fn1(
                koda_internal_parallel.get_eager_executor(),
                koda_internal_parallel.as_future(1),
                koda_internal_parallel.as_future(2),
                return_type_as=koda_internal_parallel.as_future(
                    data_slice.DataSlice
                ),
            )
        ).eval(),
        ds(3),
    )
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(
            transformed_fn2(
                koda_internal_parallel.get_eager_executor(),
                koda_internal_parallel.as_future(1),
                koda_internal_parallel.as_future(2),
                return_type_as=koda_internal_parallel.as_future(
                    data_slice.DataSlice
                ),
            )
        ).eval(),
        ds(-1),
    )
    # Make sure we have multiple copies of the same transformed functor.
    testing.assert_equal(
        transformed_fn,
        fns.slice([
            [transformed_fn1, None, transformed_fn2],
            [transformed_fn1, transformed_fn1, None, None],
        ]),
    )

  def test_qtype_signatures(self):
    parallel_transform_config_qtype = expr_eval.eval(
        koda_internal_parallel.get_transform_config_qtype()
    )
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.transform_many,
        [
            (
                parallel_transform_config_qtype,
                qtypes.DATA_SLICE,
                qtypes.DATA_SLICE,
            ),
            (
                parallel_transform_config_qtype,
                arolla.UNSPECIFIED,
                arolla.UNSPECIFIED,
            ),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES
        + (parallel_transform_config_qtype,),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.transform(I.config, I.fn))
    )


if __name__ == '__main__':
  absltest.main()
