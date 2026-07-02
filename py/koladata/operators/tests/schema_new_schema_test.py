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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
M = arolla.M
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde


class KodaNewSchemaTest(parameterized.TestCase):

  def test_get_attr(self):
    kwargs = {
        'a': schema_constants.INT32,
        'b': schema_constants.FLOAT32,
    }
    schema = expr_eval.eval(kde.schema.new_schema(**kwargs))  # pyrefly: ignore[missing-attribute]
    self.assertFalse(schema.is_mutable())
    for attr_name, val in kwargs.items():
      testing.assert_equal(
          getattr(schema, attr_name), ds(val).with_bag(schema.get_bag())
      )

  def test_non_determinism(self):
    kwargs = {
        'a': schema_constants.INT32,
        'b': schema_constants.FLOAT32,
    }
    expr = kde.schema.new_schema(**kwargs)  # pyrefly: ignore[missing-attribute]
    with self.subTest('multiple eval'):
      schema_first_eval = expr_eval.eval(expr)
      schema_second_eval = expr_eval.eval(expr)
      self.assertNotEqual(schema_first_eval, schema_second_eval)
      self.assertNotEqual(
          schema_first_eval.get_bag().fingerprint,
          schema_second_eval.get_bag().fingerprint,
      )
    with self.subTest('same expr within larger expr'):
      res = expr_eval.eval(kde.schema.new_schema(x=expr, y=expr))  # pyrefly: ignore[missing-attribute]
      testing.assert_equal(res.x, res.y)

    with self.subTest('new expr new fingerprint'):
      expr_1 = kde.schema.new_schema(**kwargs)  # pyrefly: ignore[missing-attribute]
      expr_2 = kde.schema.new_schema(**kwargs)  # pyrefly: ignore[missing-attribute]
      self.assertNotEqual(expr_1.fingerprint, expr_2.fingerprint)

  def test_bag_adoption(self):
    a = expr_eval.eval(kde.schema.new_schema(a=schema_constants.INT32))  # pyrefly: ignore[missing-attribute]
    b = expr_eval.eval(kde.schema.new_schema(a=a))  # pyrefly: ignore[missing-attribute]
    testing.assert_equal(
        b.a.a, ds(schema_constants.INT32).with_bag(b.get_bag())
    )

  def test_invalid_arguments(self):
    with self.assertRaisesRegex(
        ValueError,
        "kd.schema.new_schema: schema's schema must be SCHEMA, got: INT32",
    ):
      _ = expr_eval.eval(
          kde.schema.new_schema(  # pyrefly: ignore[missing-attribute]
              a=schema_constants.INT32,
              b=ds(1),
          )
      )

  def test_non_data_slice_binding(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected all arguments to be DATA_SLICEs, got'
            ' **kwargs: {a: DATA_SLICE, b: UNSPECIFIED}'
        ),
    ):
      _ = expr_eval.eval(
          kde.schema.new_schema(  # pyrefly: ignore[missing-attribute]
              a=schema_constants.INT32, b=arolla.unspecified()
          )
      )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.new_schema()))  # pyrefly: ignore[missing-attribute]

  def test_repr(self):
    # This has a hidden seed which is stripped...
    self.assertEqual(
        repr(kde.schema.new_schema(a=I.a, b=I.b)),  # pyrefly: ignore[missing-attribute]
        'kd.schema.new_schema(a=I.a, b=I.b)',
    )


if __name__ == '__main__':
  absltest.main()
