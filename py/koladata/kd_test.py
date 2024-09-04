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

"""Tests for kd."""

import inspect
import types

from absl.testing import absltest
from arolla import arolla
from koladata import kd
from koladata.functor import signature_utils
from koladata.types import jagged_shape
from koladata.types import schema_constants

kde = kd.kde
I = kd.I
V = kd.V
S = kd.S
kdf = kd.kdf


class KdTest(absltest.TestCase):

  def test_types(self):
    self.assertIsInstance(kd.types, types.ModuleType)
    self.assertIsInstance(kd.bag(), kd.types.DataBag)
    self.assertIsInstance(kd.slice([1, 2, 3]), kd.types.DataSlice)
    self.assertIsInstance(kd.item(5), kd.types.DataItem)
    self.assertIsInstance(kd.bag().list([1, 2]), kd.types.ListItem)
    self.assertIsInstance(kd.bag().dict({'a': 42}), kd.types.DictItem)
    self.assertIsInstance(kd.INT32, kd.types.SchemaItem)

  def test_type_annotations(self):
    def f(
        bag: kd.types.DataBag, item: kd.types.DataItem, sl: kd.types.DataSlice  # pylint: disable=unused-argument
    ):
      pass

    sig = inspect.signature(f)
    self.assertIs(sig.parameters['bag'].annotation, kd.types.DataBag)
    self.assertIs(sig.parameters['item'].annotation, kd.types.DataItem)
    self.assertIs(sig.parameters['sl'].annotation, kd.types.DataSlice)

  def test_bag_returns_new_instance(self):
    db1 = kd.bag()
    db2 = kd.bag()
    kd.testing.assert_equivalent(db1, db2)
    with self.assertRaises(AssertionError):
      kd.testing.assert_equal(db1, db2)

  def test_data_slice_and_data_item_magic_methods(self):
    kd.testing.assert_equal(
        kd.slice([1, 2, 3]) + kd.slice([4, 5, 6]), kd.slice([5, 7, 9])
    )
    kd.testing.assert_equal(kd.item(1) + kd.item(4), kd.item(5))

  def test_schema_constants(self):
    for const in dir(schema_constants):
      if isinstance(getattr(schema_constants, const), arolla.QValue):
        kd.testing.assert_equal(
            getattr(schema_constants, const), getattr(kd, const)
        )

  def test_mask_constants(self):
    self.assertEqual(kd.present.get_schema(), kd.MASK)
    self.assertEqual(kd.missing.get_schema(), kd.MASK)
    self.assertEqual(kd.has(kd.item(1)), kd.present)
    # NOTE: `==` on missing items returns missing and bool(missing) is False.
    kd.testing.assert_equal(kd.has(kd.item(None)), kd.missing)

  def test_ops(self):
    kd.testing.assert_equal(
        kd.add(kd.slice([1, 2]), kd.slice([3, 4])), kd.slice([4, 6])
    )
    kd.testing.assert_equal(
        kd.has(kd.slice([1, None])), kd.slice([arolla.present(), None])
    )
    kd.testing.assert_equal(
        kd.shapes.create(kd.slice([1])), jagged_shape.create_shape([1])
    )
    kd.testing.assert_equal(
        kd.strings.agg_join(kd.slice(['ab', 'd'])), kd.item('abd')
    )

  def test_entities(self):
    x = kd.new(a=1, b='abc')
    y = kd.new(a=1, b='abc')
    kd.testing.assert_equal(x.get_schema().a, kd.INT32.with_db(x.db))
    kd.testing.assert_equal(x.get_schema().b, kd.TEXT.with_db(x.db))
    with self.assertRaises(AssertionError):
      kd.testing.assert_equal(x, y)
    kd.testing.assert_equal(x.a.with_db(None), y.a.with_db(None))

  def test_objects(self):
    x = kd.obj(a=1, b='abc')
    y = kd.obj(a=1, b='abc')
    kd.testing.assert_equal(x.get_schema(), kd.OBJECT.with_db(x.db))
    kd.testing.assert_equal(y.get_schema(), kd.OBJECT.with_db(y.db))
    with self.assertRaises(AssertionError):
      kd.testing.assert_equal(x, y)
    kd.testing.assert_equal(x.a.with_db(None), y.a.with_db(None))

  def test_expr(self):
    kd.testing.assert_equal(
        kd.eval(
            kde.add(S.x, I.y), kd.new(x=kd.slice([1, 2])), y=kd.slice([3, 4])
        ),
        kd.slice([4, 6]),
    )

  def test_literal(self):
    expr = kd.literal(kd.item(1))
    self.assertIsInstance(expr, arolla.Expr)
    kd.testing.assert_equal(arolla.eval(expr), kd.item(1))

  def test_dir(self):
    for api_name in dir(kd):
      self.assertFalse(api_name.startswith('_'))

  def test_docstring(self):
    self.assertIn('Koda API', kd.__doc__)

  def test_exception_type(self):
    self.assertIsInstance(kd.exceptions, types.ModuleType)

    def f(e: kd.exceptions.KodaError):  # pylint: disable=unused-argument
      pass

    sig = inspect.signature(f)
    self.assertIs(sig.parameters['e'].annotation, kd.exceptions.KodaError)

  def test_kdf(self):
    fn = kdf.fn(
        returns=I.x + V.foo,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
            signature_utils.parameter(
                'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
        foo=I.y,
    )
    self.assertEqual(kdf.call(fn, 1, 2), 3)

  def test_with_name(self):
    x = kd.slice([1, 2, 3])
    y = kd.with_name(x, 'foo')
    self.assertIs(y, x)

  def test_get_name(self):
    expr = kde.with_name(I.x + I.y, 'foo')
    self.assertEqual(kd.get_name(expr), 'foo')
    self.assertIsNone(kd.get_name(expr + I.z))

  def test_unwrap_named(self):
    expr = kde.with_name(I.x + I.y, 'foo')
    kd.testing.assert_equal(kd.unwrap_named(expr), I.x + I.y)
    with self.assertRaisesRegex(ValueError, 'non-named'):
      _ = kd.unwrap_named(expr + I.z)


if __name__ == '__main__':
  absltest.main()
