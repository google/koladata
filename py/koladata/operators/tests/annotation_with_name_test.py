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

"""Tests for kde.annotation.with_name."""

from absl.testing import absltest
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde


class AnnotationWithNameTest(absltest.TestCase):

  def test_basic(self):
    expr = kde.annotation.with_name(I.x, 'foo')
    self.assertEqual(expr.eval(x=1), 1)
    self.assertIn('foo =', str(expr))
    self.assertEqual(introspection.get_name(expr), 'foo')

  def test_proper_boxing(self):
    expr = kde.annotation.with_name(1, 'foo')
    self.assertEqual(expr.eval().qtype, qtypes.DATA_SLICE)
    self.assertEqual(expr.eval(), 1)

  def test_error_name_type(self):
    with self.assertRaisesWithLiteralMatch(ValueError, 'Name must be a string'):
      kde.annotation.with_name(1, b'bar')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.annotation.with_name(I.x, 'foo')))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.annotation.with_name, kde.with_name)
    )


if __name__ == '__main__':
  absltest.main()
