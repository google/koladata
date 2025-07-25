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

from __future__ import annotations  # MUST BE IMPORTED.

from absl.testing import absltest
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.testing import testing
from koladata.types import data_slice


ds = data_slice.DataSlice.from_vals


class SimpleTracingConfig(tracing_decorator.TypeTracingConfig):

  def return_type_as(self, annotation):
    return data_slice.DataSlice

  def to_kd(self, annotation, value):
    return value.x

  def from_kd(self, annotation, value):
    return annotation(x=value)


class SimpleClass:

  _koladata_type_tracing_config_ = SimpleTracingConfig

  def __init__(self, x):
    self.x = x

  def get_x(self):
    return self.x


class TracingDecoratorWithFutureAnnotationsTest(absltest.TestCase):

  def test_tracing(self):

    @tracing_decorator.TraceAsFnDecorator()
    def inner_fn(c: SimpleClass):
      return c.get_x()

    def outer_fn(x):
      c = SimpleClass(x)
      return inner_fn(c)

    with self.subTest('direct_eval'):
      testing.assert_equal(outer_fn(ds(1)), ds(1))

    with self.subTest('functor_eval'):
      functor = functor_factories.trace_py_fn(outer_fn)
      testing.assert_equal(functor(ds(1)), ds(1))

  def test_forward_declaration(self):
    # pytype: disable=name-error
    def fn(c: ForwardDeclarationClass):
      return c.x

    with self.assertRaisesRegex(
        NameError, "name 'ForwardDeclarationClass' is not defined"
    ):
      _ = tracing_decorator.TraceAsFnDecorator()(fn)

    class ForwardDeclarationClass:
      _koladata_type_tracing_config_ = SimpleTracingConfig

      def __init__(self, x):
        self.x = x

    # pytype: enable=name-error


if __name__ == '__main__':
  absltest.main()
