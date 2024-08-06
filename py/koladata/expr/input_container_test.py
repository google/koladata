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

"""Tests for input_container."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import literal_operator

ds = data_slice.DataSlice.from_vals


# pylint: disable=invalid-name
class InputContainerTest(parameterized.TestCase):

  def test_qtype(self):
    I = input_container.InputContainer('I')
    self.assertIsNone(I.x.qtype)

  def test_view(self):
    I = input_container.InputContainer('I')
    self.assertTrue(view.has_data_slice_view(I.x))

  def test_repr(self):
    I = input_container.InputContainer('I')
    self.assertEqual(repr(I.x), 'I.x')
    self.assertEqual(repr(I['-x']), "I['-x']")
    self.assertEqual(repr((I.x + I.y).attr), '(I.x + I.y).attr')
    self.assertEqual(repr(I.self), 'S')
    self.assertEqual(repr(I.self.x), 'S.x')

  def test_arolla_eval(self):
    I = input_container.InputContainer('I')
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'I.x cannot be evaluated - please provide data to `I` inputs and'
            ' substitute all `V` variables'
        ),
    ):
      arolla.eval(I.x, x=ds([1, 2, 3]))

  def test_kd_eval(self):
    I = input_container.InputContainer('I')
    testing.assert_equal(expr_eval.eval(I.x, x=ds([1, 2, 3])), ds([1, 2, 3]))  # pytype: disable=attribute-error

  def test_relationship_with_arolla_leaves(self):
    op = arolla.abc.lookup_operator('koda_internal.input')
    I = input_container.InputContainer('I')
    arolla.testing.assert_expr_equal_by_fingerprint(
        I.x,
        op(
            literal_operator.literal(arolla.text('I')),
            literal_operator.literal(arolla.text('x')),
        ),
    )
    self.assertEqual(arolla.get_leaf_keys(I.x + I.y), [])
    self.assertEqual(arolla.abc.get_placeholder_keys(I.x + I.y), [])

  def test_get_input_names(self):
    decayed_input_op = arolla.abc.decay_registered_operator(
        'koda_internal.input'
    )
    I = input_container.InputContainer('I')
    V = input_container.InputContainer('V')
    expr = I.x + I.y + I.z.val + V.w + decayed_input_op('V', 'v')
    self.assertEqual(input_container.get_input_names(expr, I), ['x', 'y', 'z'])
    # Note: decayed inputs are _not_ supported.
    self.assertEqual(input_container.get_input_names(expr, V), ['w'])

  def test_sub_inputs(self):
    decayed_input_op = arolla.abc.decay_registered_operator(
        'koda_internal.input'
    )
    I = input_container.InputContainer('I')
    V = input_container.InputContainer('V')
    expr = I.x + I.y + I.z.val + V.x + decayed_input_op('V', 'v')
    arolla.testing.assert_expr_equal_by_fingerprint(
        input_container.sub_inputs(expr, I, x=I.a, z=I.o),
        I.a + I.y + I.o.val + V.x + decayed_input_op('V', 'v'),
    )
    # Note: decayed inputs are _not_ supported.
    arolla.testing.assert_expr_equal_by_fingerprint(
        input_container.sub_inputs(expr, V, x=I.a, y=I.o, v=I.z),
        I.x + I.y + I.z.val + I.a + decayed_input_op('V', 'v'),
    )

  def test_get_input_names_in_lambda(self):
    I = input_container.InputContainer('I')

    @arolla.optools.as_lambda_operator('foo.bar')
    def foo_bar():
      return I.x

    self.assertEmpty(input_container.get_input_names(foo_bar(), I))
    self.assertEqual(
        input_container.get_input_names(arolla.abc.to_lowest(foo_bar()), I),
        ['x'],
    )

  def test_sub_inputs_in_lambda(self):
    I = input_container.InputContainer('I')

    @arolla.optools.as_lambda_operator('foo.bar')
    def foo_bar():
      return I.x

    arolla.testing.assert_expr_equal_by_fingerprint(
        input_container.sub_inputs(foo_bar(), I, x=arolla.L.x), foo_bar()
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        input_container.sub_inputs(
            arolla.abc.to_lowest(foo_bar()), I, x=arolla.L.x
        ),
        arolla.L.x,
    )


# pylint: enable=invalid-name


if __name__ == '__main__':
  absltest.main()
