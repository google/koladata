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
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as _
from koladata.operators import kde_operators
from koladata.operators import koda_internal_parallel
from koladata.operators import optools

py_fn = functor_factories.py_fn

eager_executor = expr_eval.eval(koda_internal_parallel.get_eager_executor())
default_executor = expr_eval.eval(koda_internal_parallel.get_default_executor())
kde = kde_operators.kde


class KodaInternalParallelCurrentExecutorTest(absltest.TestCase):

  def test_default(self):
    result = koda_internal_parallel.current_executor().eval()
    arolla.testing.assert_qvalue_equal_by_fingerprint(result, default_executor)

  def test_nested_py_fn(self):
    [result] = (
        koda_internal_parallel.stream_call(
            eager_executor,
            py_fn(
                lambda: koda_internal_parallel.current_executor().eval(),  # pylint: disable=unnecessary-lambda
                return_type_as=default_executor,
            ),
            return_type_as=default_executor,
        )
        .eval()
        .read_all(timeout=1)
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(result, eager_executor)

  def test_non_determinism(self):
    [result_1, async_result_2] = expr_eval.eval((
        koda_internal_parallel.current_executor(),
        koda_internal_parallel.stream_call(
            eager_executor,
            lambda: koda_internal_parallel.current_executor(),  # pylint: disable=unnecessary-lambda
            return_type_as=default_executor,
        ),
    ))
    [result_2] = async_result_2.read_all(timeout=1)
    self.assertNotEqual(result_1.fingerprint, result_2.fingerprint)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        result_1, default_executor
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(result_2, eager_executor)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.current_executor())
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            koda_internal_parallel.current_executor,
            kde.streams.current_executor,
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(koda_internal_parallel.current_executor()),
        'koda_internal.parallel.current_executor()',
    )
    self.assertEqual(
        repr(kde.streams.current_executor()),
        'kd.streams.current_executor()',
    )


if __name__ == '__main__':
  absltest.main()
