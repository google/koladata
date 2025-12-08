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
from absl.testing import parameterized
from arolla import arolla
from koladata import kd
from koladata.serving import simplify_functor


class SimplifyFunctorTest(parameterized.TestCase):

  def test_strip_source_locations(self):
    fn1 = kd.fn(lambda x: x - 1)
    fn2 = kd.fn(lambda x: x + 1)
    d = kd.dict(dict(fn1=fn1, fn2=fn2))
    fn3 = kd.fn(lambda fn, x: kd.get_item(d, fn)(x))

    kd.testing.assert_equal(
        kd.expr.unpack_expr(fn3.returns).op,
        arolla.abc.lookup_operator('kd.annotation.source_location'),
    )
    traced_d = fn3.get_attr('_aux_0')
    kd.testing.assert_equal(
        kd.expr.unpack_expr(traced_d['fn1'].returns).op,
        arolla.abc.lookup_operator('kd.annotation.source_location'),
    )

    stripped_fn3 = simplify_functor.strip_source_locations(fn3)

    kd.testing.assert_equal(
        kd.expr.unpack_expr(stripped_fn3.returns).op,
        arolla.abc.lookup_operator('kd.call'),
    )
    striped_traced_d = stripped_fn3.get_attr('_aux_0')
    kd.testing.assert_equal(
        kd.expr.unpack_expr(striped_traced_d['fn1'].returns).op,
        arolla.abc.lookup_operator('kd.math.subtract'),
    )

if __name__ == '__main__':
  absltest.main()
