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
from koladata.expr import tracing
from koladata.extension_types import extension_types
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals


@extension_types.extension_type()
class SimpleClass:
  x: schema_constants.INT32


class TracingWithFutureAnnotationsTest(absltest.TestCase):

  def test_tracing(self):

    def fn(c: SimpleClass):
      return c.x + 1

    c = SimpleClass(x=ds(1))

    with self.subTest('direct_eval'):
      testing.assert_equal(fn(c), ds(2))

    with self.subTest('functor_eval'):
      functor = tracing.trace(fn)
      testing.assert_equal(functor.eval(c=c), ds(2))

  def test_forward_declaration(self):
    # pytype: disable=name-error
    def fn(c: ForwardDeclarationClass):
      return c.x

    with self.assertRaisesRegex(
        NameError, "name 'ForwardDeclarationClass' is not defined"
    ):
      _ = tracing.trace(fn)

    @extension_types.extension_type()
    class ForwardDeclarationClass:
      x: schema_constants.INT32

    # pytype: enable=name-error


if __name__ == '__main__':
  absltest.main()
