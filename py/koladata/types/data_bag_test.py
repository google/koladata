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

import gc
import inspect
import re
import sys

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape as arolla_jagged_shape
from koladata.exceptions import exceptions
from koladata.operators import comparison as _  # pylint: disable=unused-import
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import schema_constants


kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class DataBagTest(parameterized.TestCase):

  def test_ref_count(self):
    gc.collect()
    diff_count = 10
    base_count = sys.getrefcount(data_bag.DataBag)
    dbs = []
    for _ in range(diff_count):
      dbs.append(bag())

    self.assertEqual(sys.getrefcount(data_bag.DataBag), base_count + diff_count)

    del dbs
    gc.collect()
    # NOTE: bag() invokes `PyDataBag_Type()` C Python function multiple times
    # and this verifies there are no leaking references.
    self.assertEqual(sys.getrefcount(data_bag.DataBag), base_count)

    # NOTE: _exactly_equal() invokes `PyDataBag_Type()` C Python function
    # and this verifies there are no leaking references.
    bag()._exactly_equal(bag())
    self.assertEqual(sys.getrefcount(data_bag.DataBag), base_count)

    x = bag().obj()
    self.assertEqual(sys.getrefcount(data_bag.DataBag), base_count)
    db = x.get_bag()
    self.assertEqual(sys.getrefcount(data_bag.DataBag), base_count + 1)
    del x
    gc.collect()
    self.assertEqual(sys.getrefcount(data_bag.DataBag), base_count + 1)
    del db
    gc.collect()
    self.assertEqual(sys.getrefcount(data_bag.DataBag), base_count)

  def test_qvalue(self):
    self.assertIsInstance(bag(), arolla.QValue)

  def test_fingerprint(self):
    db1 = bag()
    fp1 = db1.fingerprint
    del db1
    db2 = bag()
    self.assertNotEqual(db2.fingerprint, fp1)

    x = db2.obj()
    self.assertIsNot(x.get_bag(), db2)
    self.assertEqual(x.get_bag().fingerprint, db2.fingerprint)

  def test_getitem(self):
    db = bag()
    x = db[ds([1, 2, 3])]
    testing.assert_equal(x.get_bag(), db)

    x = db[ds(42)]
    testing.assert_equal(x.get_bag(), db)

    with self.assertRaisesRegex(TypeError, 'expected DataSlice, got list'):
      _ = db[[1, 2, 3]]

  @parameterized.named_parameters(
      (
          'empty',
          bag(),
          (
              r'^DataBag \$[0-9a-f]{4} with 0 values in 0 attrs, plus 0 schema'
              r' values and 0 fallbacks. Top attrs:\n'
              r'Use db.contents_repr\(\) to see the actual values\.$'
          ),
      ),
      (
          'attributes',
          bag().new(a=1, b='a').get_bag(),
          (
              r'^DataBag \$[0-9a-f]{4} with 2 values in 2 attrs, plus 2 schema'
              r' values and 0 fallbacks. Top attrs:\n'
              r'  b: 1 values\n'
              r'  a: 1 values\n'
              r'Use db.contents_repr\(\) to see the actual values\.$'
          ),
      ),
      (
          'lists',
          bag().list([[1, 2], [3]]).get_bag(),
          (
              r'DataBag \$[0-9a-f]{4} with 5 values in 1 attrs, plus 2 schema'
              r' values and 0 fallbacks. Top attrs:\n'
              r'  <list items>: 5 values\n'
              r'Use db.contents_repr\(\) to see the actual values\.$'
          ),
      ),
      (
          'dicts',
          bag().dict({'a': {'inner': 42}}).get_bag(),
          (
              r'^DataBag \$[0-9a-f]{4} with 2 values in 2 attrs, plus 4 schema'
              r' values and 0 fallbacks. Top attrs:\n'
              r'  <dict value>: 1 values\n'
              r'  <dict value>: 1 values\n'
              r'Use db.contents_repr\(\) to see the actual values\.$'
          ),
      ),
      (
          'fallback',
          bag().new(a=1).enriched(bag().list([1, 2]).get_bag()).get_bag(),
          (
              r'^DataBag \$[0-9a-f]{4} with 0 values in 0 attrs, plus 0 schema'
              r' values and 2 fallbacks. Top attrs:\n'
              r'Use db.contents_repr\(\) to see the actual values.$'
          ),
      ),
      (
          'two_fallbacks',
          bag()
          .new(a=1)
          .enriched(bag().list([1, 2]).get_bag())
          .enriched(bag().dict({'a': 42}).get_bag())
          .get_bag(),
          (
              r'^DataBag \$[0-9a-f]{4} with 0 values in 0 attrs, plus 0 schema'
              r' values and 2 fallbacks. Top attrs:\n'
              r'Use db.contents_repr\(\) to see the actual values\.$'
          ),
      ),
  )
  def test_repr(self, db, expected_repr):
    # Repr may not be deterministic, so we save it into a variable in order to
    # include it in the error message.
    db_repr = repr(db)
    self.assertRegex(
        db_repr,
        expected_repr,
        msg=f'\n\nregex={expected_repr}\n\ndb={db_repr}',
    )

  @parameterized.named_parameters(
      (
          'empty',
          bag(),
          r"""DataBag \$[0-9a-f]{4}:

SchemaBag:
""",
      ),
      (
          'lists',
          bag().list([1, 2, 3]).get_bag(),
          r"""DataBag \$[0-9a-f]{4}:
\$[0-9a-zA-Z]{22}\[:\] => \[1, 2, 3\]

SchemaBag:
""",
      ),
      (
          'dicts',
          bag().dict({'a': 1, 2: 'b'}).get_bag(),
          r"""DataBag \$[0-9a-f]{4}:
\$[0-9a-zA-Z]{22}\[2\] => 'b'
\$[0-9a-zA-Z]{22}\['a'\] => 1

SchemaBag:
""",
      ),
      (
          'entity',
          bag().new(a=1, b='a').get_bag(),
          r"""DataBag \$[0-9a-f]{4}:
\$[0-9a-zA-Z]{22}\.a => 1
\$[0-9a-zA-Z]{22}\.b => a

SchemaBag:
\$[0-9a-zA-Z]{22}\.a => INT32
\$[0-9a-zA-Z]{22}\.b => STRING
""",
      ),
      (
          'object',
          bag().obj(a=1, b='a').get_bag(),
          r"""DataBag \$[0-9a-f]{4}:
\$[0-9a-zA-Z]{22}\.get_obj_schema\(\) => #[0-9a-zA-Z]{22}
\$[0-9a-zA-Z]{22}\.a => 1
\$[0-9a-zA-Z]{22}\.b => a

SchemaBag:
#[0-9a-zA-Z]{22}\.a => INT32
#[0-9a-zA-Z]{22}\.b => STRING
""",
      ),
  )
  def test_contents_repr(self, db, expected_repr_regex):
    db_repr = repr(db.contents_repr())
    self.assertRegex(
        db_repr,
        expected_repr_regex,
        msg=f'\n\nregex={expected_repr_regex}\n\ndb={db_repr}',
    )

  def test_contents_repr_schema(self):
    db = bag()
    db.new(a=db.new(b=1))
    self.assertRegex(
        repr(db.contents_repr()),
        r"""DataBag \$[0-9a-f]{4}:
\$[0-9a-zA-Z]{22}\.b => 1
\$[0-9a-zA-Z]{22}\.a => \$[0-9a-zA-Z]{22}

SchemaBag:
\$[0-9a-zA-Z]{22}\.b => INT32
\$[0-9a-zA-Z]{22}\.a => \$[0-9a-zA-Z]{22}
""",
    )

    db = bag()
    db.obj(a=db.obj(b=1))
    self.assertRegex(
        repr(db.contents_repr()),
        r"""DataBag \$[0-9a-f]{4}:
\$[0-9a-zA-Z]{22}\.get_obj_schema\(\) => #[0-9a-zA-Z]{22}
\$[0-9a-zA-Z]{22}\.b => 1
\$[0-9a-zA-Z]{22}\.get_obj_schema\(\) => #[0-9a-zA-Z]{22}
\$[0-9a-zA-Z]{22}\.a => \$[0-9a-zA-Z]{22}

SchemaBag:
#[0-9a-zA-Z]{22}\.(b|a) => (INT32|OBJECT)
#[0-9a-zA-Z]{22}\.(b|a) => (OBJECT|INT32)
""",
    )

  def test_contents_repr_fallback(self):
    db = bag()
    entity = db.new(x=1)
    ds2 = entity.with_bag(bag()).enriched(db)
    db_repr = repr(ds2.get_bag().contents_repr())
    expected_repr = r"""DataBag \$[0-9a-f]{4}:

SchemaBag:

2 fallback DataBag\(s\):
  fallback #0 \$[0-9a-f]{4}:
  DataBag:

  SchemaBag:

  fallback #1 \$[0-9a-f]{4}:
  DataBag:
  \$[0-9a-zA-Z]{22}\.x => 1

  SchemaBag:
  \$[0-9a-zA-Z]{22}\.x => INT32
"""
    self.assertRegex(
        db_repr, expected_repr, msg=f'\n\nregex={expected_repr}\n\ndb={db_repr}'
    )
    with self.subTest('nested-fallbacks'):
      db = bag()
      entity = db.new(x=1)
      db2 = bag()
      db2.new(y=2)
      entity2 = entity.with_bag(db2).enriched(db)
      entity3 = entity2.with_bag(bag()).enriched(entity2.get_bag())
      db_repr = repr(entity3.get_bag().contents_repr())
      expected_repr = r"""DataBag \$[0-9a-f]{4}:

SchemaBag:

2 fallback DataBag\(s\):
  fallback #0 \$[0-9a-f]{4}:
  DataBag:

  SchemaBag:

  fallback #1 \$[0-9a-f]{4}:
  DataBag:

  SchemaBag:

  2 fallback DataBag\(s\):
    fallback #0 \$[0-9a-f]{4}:
    DataBag:
    \$[0-9a-zA-Z]{22}\.y => 2

    SchemaBag:
    \$[0-9a-zA-Z]{22}\.y => INT32

    fallback #1 \$[0-9a-f]{4}:
    DataBag:
    \$[0-9a-zA-Z]{22}\.x => 1

    SchemaBag:
    \$[0-9a-zA-Z]{22}\.x => INT32
"""
      self.assertRegex(
          db_repr,
          expected_repr,
          msg=f'\n\nregex={expected_repr}\n\ndb={db_repr}',
      )

  def test_contents_repr_triple_limit(self):
    db = bag()
    db.obj(a=1, b='a')
    self.assertRegex(
        repr(db.contents_repr(triple_limit=4)),
        r"""DataBag \$[0-9a-f]{4}:
\$[0-9a-zA-Z]{22}\.get_obj_schema\(\) => #[0-9a-zA-Z]{22}
\$[0-9a-zA-Z]{22}\.a => 1
\$[0-9a-zA-Z]{22}\.b => a

SchemaBag:
#[0-9a-zA-Z]{22}\.a => INT32
\.\.\.

Showing only the first 4 triples. Use 'triple_limit' parameter of 'db\.contents_repr\(\)' to adjust this""",
    )
    with self.subTest('invalid-type'):
      db = bag()
      with self.assertRaisesRegex(
          TypeError,
          "'str' object cannot be interpreted as an integer",
      ):
        _ = repr(db.contents_repr(triple_limit='one thousand'))
      with self.subTest('positional-argument'):
        with self.assertRaisesRegex(
            TypeError,
            r'_contents_repr\(\) takes 1 positional argument but 2 were given',
        ):
          _ = repr(db.contents_repr(1000))
      with self.subTest('negative-limit'):
        with self.assertRaisesRegex(
            ValueError,
            'triple_limit must be a positive integer',
        ):
          _ = repr(db.contents_repr(triple_limit=-1))

  def test_contents_repr_refreshed_content(self):
    db = bag()
    db.obj(a=1, b='a')
    repr_1 = repr(db.contents_repr())
    repr_2 = repr(db.contents_repr())
    self.assertEqual(repr_1, repr_2)

    db.new(a=42)
    repr_3 = repr(db.contents_repr())
    self.assertNotEqual(repr_3, repr_2)

  def test_contents_repr_no_refs_to_data_bag(self):
    gc.collect()
    db = bag()
    self.assertEqual(sys.getrefcount(db), 2)
    _ = db.new(a=db.new(x=42))
    self.assertEqual(sys.getrefcount(db), 2)

    r = repr(db)
    self.assertEqual(sys.getrefcount(db), 2)

    r = db.contents_repr()
    self.assertEqual(sys.getrefcount(db), 2)

    r = repr(db.contents_repr())
    self.assertEqual(sys.getrefcount(db), 2)

    del r

  def test_is_mutable(self):
    db = bag()
    self.assertTrue(db.is_mutable())
    self.assertTrue(db.fork().is_mutable())
    self.assertTrue(db.fork(mutable=True).is_mutable())
    self.assertFalse(db.fork(mutable=False).is_mutable())

  def test_new(self):
    db = bag()
    x = db.new(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    y = db.new(x=x)
    testing.assert_allclose(
        y.x.a, ds([3.14], schema_constants.FLOAT64).with_bag(db)
    )
    testing.assert_equal(y.x.b, ds(['abc']).with_bag(db))
    testing.assert_equal(x.get_schema(), y.get_schema().x)
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(db)
    )
    testing.assert_equal(x.b.get_schema(), schema_constants.STRING.with_bag(db))

  def test_new_str_as_schema_arg(self):
    db = bag()
    x = db.new(schema='name', a=42)
    expected_schema = db.named_schema('name')
    testing.assert_equal(x.get_schema(), expected_schema)
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_new_str_slice_as_schema_arg(self):
    db = bag()
    x = db.new(schema=ds('name'), a=42)
    expected_schema = db.named_schema('name')
    testing.assert_equal(x.get_schema(), expected_schema)
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_new_schema_arg_errors(self):
    db = bag()
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: STRING"
    ):
      _ = db.new(schema=ds(['name']), a=42)

  def test_obj(self):
    db = bag()
    x = db.obj(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    y = db.obj(x=x)
    testing.assert_equal(y.get_schema(), schema_constants.OBJECT.with_bag(db))
    testing.assert_equal(y.x.get_schema(), schema_constants.OBJECT.with_bag(db))
    testing.assert_equal(
        y.get_attr('__schema__').x,
        ds([schema_constants.OBJECT]).with_bag(db),
    )
    testing.assert_allclose(
        y.x.a, ds([3.14], schema_constants.FLOAT64).with_bag(db)
    )
    testing.assert_equal(y.x.b, ds(['abc']).with_bag(db))
    testing.assert_equal(x.get_schema(), schema_constants.OBJECT.with_bag(db))
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(db)
    )
    testing.assert_equal(x.b.get_schema(), schema_constants.STRING.with_bag(db))
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT64).with_bag(db)
    )
    testing.assert_equal(x.b, ds(['abc']).with_bag(db))

    with self.assertRaises(AttributeError):
      # NOTE: Not possible through __getattr__.
      _ = x.__schema__

    testing.assert_equal(
        x.get_attr('__schema__').a, ds([schema_constants.FLOAT64]).with_bag(db)
    )
    testing.assert_equal(
        x.get_attr('__schema__').b, ds([schema_constants.STRING]).with_bag(db)
    )

  def test_uu(self):
    db = bag()
    x = db.uu(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    testing.assert_equal(
        x.get_schema(),
        db.uu_schema(a=schema_constants.FLOAT64, b=schema_constants.STRING),
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(db)
    )
    testing.assert_equal(x.b.get_schema(), schema_constants.STRING.with_bag(db))
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT64).with_bag(db)
    )
    testing.assert_equal(x.b, ds(['abc']).with_bag(db))

    # Uuids are the same.
    z = db.uu(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    testing.assert_equal(x, z)

    # Seed arg.
    u = db.uu(
        'seed',
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    self.assertNotEqual(x.fingerprint, u.fingerprint)

    v = db.uu(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
        seed='seed',
    )
    testing.assert_equal(u, v)
    with self.assertRaises(ValueError):
      # seed is not an attribute.
      _ = v.seed

    with self.assertRaisesWithLiteralMatch(
        TypeError, 'seed must be a utf8 string, got bytes'
    ):
      _ = db.uu(
          a=ds([3.14], schema_constants.FLOAT64),
          b=ds(['abc'], schema_constants.STRING),
          seed=b'seed',
      )

    # schema arg
    x = db.uu(
        a=ds([3.14], schema_constants.FLOAT32),
        schema=db.uu_schema(a=schema_constants.FLOAT64),
    )
    testing.assert_equal(
        x.get_schema(),
        db.uu_schema(a=schema_constants.FLOAT64),
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(db)
    )
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT64).with_bag(db), atol=1e-6
    )

    # update_schema arg
    x = db.uu(
        a=ds([3.14], schema_constants.FLOAT32),
        schema=db.uu_schema(a=schema_constants.FLOAT64),
        update_schema=True,
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT32.with_bag(db)
    )
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT32).with_bag(db)
    )

    # no args
    _ = db.uu()

    # incompatible schema error message
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for attribute 'a' is incompatible.

Expected schema for 'a': SCHEMA(b=INT32)
Assigned schema for 'a': SCHEMA(b=STRING)"""),
    ):
      schema = db.uu_schema(a=db.uu_schema(b=schema_constants.INT32))
      _ = db.uu(a=bag().uu(b='dudulu'), schema=schema)

  def test_uuobj(self):
    db = bag()
    x = db.uuobj(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    testing.assert_equal(x.get_schema(), schema_constants.OBJECT.with_bag(db))
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(db)
    )
    testing.assert_equal(x.b.get_schema(), schema_constants.STRING.with_bag(db))
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT64).with_bag(db)
    )
    testing.assert_equal(x.b, ds(['abc']).with_bag(db))

    testing.assert_equal(
        x.get_attr('__schema__').a, ds([schema_constants.FLOAT64]).with_bag(db)
    )
    testing.assert_equal(
        x.get_attr('__schema__').b, ds([schema_constants.STRING]).with_bag(db)
    )

    z = db.uuobj(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    testing.assert_equal(x, z)
    u = db.uuobj(
        'seed',
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    self.assertNotEqual(x.fingerprint, u.fingerprint)

    v = db.uuobj(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
        seed='seed',
    )
    testing.assert_equal(u, v)
    with self.assertRaises(ValueError):
      # seed is not an attribute.
      _ = v.seed

    with self.assertRaisesWithLiteralMatch(
        TypeError, 'seed must be a utf8 string, got bytes'
    ):
      _ = db.uuobj(
          a=ds([3.14], schema_constants.FLOAT64),
          b=ds(['abc'], schema_constants.STRING),
          seed=b'seed',
      )

    # no args
    _ = db.uuobj()

  def test_uu_schema(self):
    db = bag()
    x = db.uu_schema(a=schema_constants.INT32, b=schema_constants.STRING)

    testing.assert_equal(x.a, schema_constants.INT32.with_bag(db))
    testing.assert_equal(x.b, schema_constants.STRING.with_bag(db))

    y = db.uu_schema(
        a=schema_constants.FLOAT32,
        b=schema_constants.STRING,
    )
    self.assertNotEqual(x.fingerprint, y.fingerprint)

    z = db.uu_schema(
        a=schema_constants.INT32,
        b=schema_constants.STRING,
    )
    testing.assert_equal(x, z)
    u = db.uu_schema(
        'seed',
        a=schema_constants.INT32,
        b=schema_constants.STRING,
    )
    self.assertNotEqual(x.fingerprint, u.fingerprint)

    v = db.uu_schema(
        a=schema_constants.INT32, b=schema_constants.STRING, seed='seed'
    )
    testing.assert_equal(u, v)
    with self.assertRaises(ValueError):
      # seed is not an attribute.
      _ = v.seed

    with self.assertRaisesWithLiteralMatch(
        TypeError, 'seed must be a utf8 string, got bytes'
    ):
      _ = db.uu_schema(
          a=schema_constants.INT32,
          b=schema_constants.STRING,
          seed=b'seed',
      )

    # no args
    _ = db.uu_schema()

  def test_named_schema(self):
    db = bag()
    x = db.named_schema('name')
    self.assertCountEqual(dir(x), [])
    self.assertEqual(x.get_schema(), schema_constants.SCHEMA)

    y = db.named_schema('other name')
    self.assertNotEqual(x.fingerprint, y.fingerprint)

    z = db.named_schema('name')
    testing.assert_equal(x, z)

    # This is not perfect as the operator allows passing it as a keyword
    # argument, but it seems minor enough to bother complicating the
    # implementation.
    with self.assertRaisesRegex(TypeError, 'no keyword arguments'):
      _ = db.named_schema(name='name')

    with self.assertRaisesRegex(
        ValueError, 'requires name to be DataItem holding Text'
    ):
      _ = db.named_schema(b'name')

  def test_new_schema(self):
    db = bag()
    db2 = bag()
    x = db.new_schema(a=schema_constants.INT32, b=schema_constants.STRING)

    testing.assert_equal(x.a, schema_constants.INT32.with_bag(db))
    testing.assert_equal(x.b, schema_constants.STRING.with_bag(db))

    y = db.new_schema(
        a=schema_constants.INT32,
        b=schema_constants.STRING,
    )
    self.assertNotEqual(x, y)

    # Testing DataBag adoption.
    z = db.new_schema(
        a=schema_constants.INT32,
        b=db2.new_schema(a=schema_constants.INT32),
    )
    testing.assert_equal(z.a, schema_constants.INT32.with_bag(db))
    testing.assert_equal(z.b.a, schema_constants.INT32.with_bag(db))

    db = bag()
    with self.assertRaisesRegex(
        ValueError,
        'expected DataSlice argument, got list',
    ):
      _ = db.new_schema(
          a=schema_constants.INT32,
          b=[1, 2, 3],
      )
    testing.assert_equivalent(db, bag())

    db = bag()
    with self.assertRaisesRegex(
        ValueError,
        'expected DataSlice argument, got dict',
    ):
      _ = db.new_schema(
          a=schema_constants.INT32,
          b={'a': 1},
      )
    testing.assert_equivalent(db, bag())

    db = bag()
    with self.assertRaisesRegex(
        ValueError,
        'expected DataSlice argument, got Text',
    ):
      _ = db.new_schema(
          a=schema_constants.INT32,
          b=arolla.text('hello'),
      )
    testing.assert_equivalent(db, bag())

    db = bag()
    with self.assertRaisesRegex(
        ValueError,
        'schema\'s schema must be SCHEMA, got: OBJECT',
    ):
      db2 = bag()
      _ = db.new_schema(
          a=schema_constants.INT32,
          b=db2.obj(a=schema_constants.INT32),
      )
    testing.assert_equivalent(db, bag())

    db = bag()
    schema = db.new_schema(a=db.new_schema(b=schema_constants.INT32))
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for attribute 'a' is incompatible.

Expected schema for 'a': SCHEMA(b=INT32)
Assigned schema for 'a': SCHEMA(c=STRING)"""),
    ):
      _ = schema(a=bag().new(c='dudulu'))

  def test_list_schema(self):
    db = bag()
    schema = db.list_schema(schema_constants.INT32)
    testing.assert_equal(schema, db.list([1, 2, 3]).get_schema())
    testing.assert_equal(
        schema.get_attr('__items__'), schema_constants.INT32.with_bag(db)
    )

    # Keyword works.
    schema = db.list_schema(item_schema=schema_constants.INT32)
    testing.assert_equal(schema, db.list([1, 2, 3]).get_schema())
    testing.assert_equal(
        schema.get_attr('__items__'), schema_constants.INT32.with_bag(db)
    )

    # Nested schema with databag adoption.
    db2 = bag()
    schema = db.list_schema(
        db2.uu_schema(a=schema_constants.INT32, b=schema_constants.STRING)
    )
    testing.assert_equal(
        schema.get_attr('__items__').a, schema_constants.INT32.with_bag(db)
    )

    with self.assertRaisesWithLiteralMatch(
        TypeError, "got an unexpected keyword 'seed'"
    ):
      _ = db.list_schema(seed='')

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'missing required argument to DataBag._list_schema: `item_schema`',
    ):
      _ = db.list_schema()

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'accepts 0 to 1 positional arguments but 2 were given',
    ):
      _ = db.list_schema(1, 2)

    with self.assertRaisesRegex(
        TypeError,
        'expecting item_schema to be a DataSlice, got NoneType',
    ):
      _ = db.list_schema(item_schema=None)

  def test_list_schema_errors(self):
    db = bag()
    schema = db.list_schema(schema_constants.INT32)
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for List item is incompatible.

Expected schema for List item: INT32
Assigned schema for List item: STRING""",
    ):
      _ = schema(['a'])

  def test_dict_schema(self):
    db = bag()
    schema = db.dict_schema(schema_constants.STRING, schema_constants.INT32)
    testing.assert_equal(schema, db.dict({'a': 1}).get_schema())
    testing.assert_equal(
        schema.get_attr('__keys__'), schema_constants.STRING.with_bag(db)
    )
    testing.assert_equal(
        schema.get_attr('__values__'), schema_constants.INT32.with_bag(db)
    )

    # Keywords work.
    schema = db.dict_schema(
        key_schema=schema_constants.STRING, value_schema=schema_constants.INT32
    )
    testing.assert_equal(schema, db.dict({'a': 1}).get_schema())
    testing.assert_equal(
        schema.get_attr('__keys__'), schema_constants.STRING.with_bag(db)
    )
    testing.assert_equal(
        schema.get_attr('__values__'), schema_constants.INT32.with_bag(db)
    )

    # Nested schema with databag adoption.
    db2 = bag()
    schema = db.dict_schema(
        db2.uu_schema(a=schema_constants.INT32),
        db2.uu_schema(a=schema_constants.FLOAT32),
    )
    testing.assert_equal(
        schema.get_attr('__keys__').a, schema_constants.INT32.with_bag(db)
    )
    testing.assert_equal(
        schema.get_attr('__values__').a, schema_constants.FLOAT32.with_bag(db)
    )

    with self.assertRaisesWithLiteralMatch(
        TypeError, "got an unexpected keyword 'seed'"
    ):
      _ = db.dict_schema(seed='')

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'missing required argument to DataBag._dict_schema: `key_schema`',
    ):
      _ = db.dict_schema()

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'missing required argument to DataBag._dict_schema: `value_schema`',
    ):
      _ = db.dict_schema(schema_constants.INT32)

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'accepts 0 to 2 positional arguments but 3 were given',
    ):
      _ = db.dict_schema(1, 2, 3)

    with self.assertRaisesRegex(
        TypeError,
        'expecting key_schema to be a DataSlice, got NoneType',
    ):
      _ = db.dict_schema(key_schema=None, value_schema=None)

    with self.assertRaisesRegex(
        TypeError,
        'expecting value_schema to be a DataSlice, got NoneType',
    ):
      _ = db.dict_schema(key_schema=schema_constants.INT32, value_schema=None)

    with self.assertRaisesRegex(
        ValueError,
        'dict keys cannot be FLOAT32',
    ):
      _ = db.dict_schema(schema_constants.FLOAT32, schema_constants.FLOAT32)

  def test_dict_schema_errors(self):
    db = bag()
    schema = db.dict_schema(schema_constants.STRING, schema_constants.INT32)
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for Dict value is incompatible.

Expected schema for Dict value: INT32
Assigned schema for Dict value: STRING""",
    ):
      _ = schema({'a': 'steins;gate'})

    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for Dict key is incompatible.

Expected schema for Dict key: STRING
Assigned schema for Dict key: INT32""",
    ):
      _ = schema({1: 'steins;gate'})

  def test_new_auto_broadcasting(self):
    db = bag()
    x = db.new(a=ds(12), b=ds([[1, None, 6], [None], [123]]))
    testing.assert_equal(x.a, ds([[12, 12, 12], [12], [12]]).with_bag(db))
    testing.assert_equal(x.b, ds([[1, None, 6], [None], [123]]).with_bag(db))

    with self.assertRaisesRegex(
        exceptions.KodaError, 'shapes are not compatible'
    ):
      db.new(a=ds([1, 2, 3]), b=ds([3.14, 3.14]))

  def test_obj_auto_broadcasting(self):
    db = bag()
    x = db.obj(a=ds(b'a'), b=ds([[1, None, 6], [None], [123]]))
    testing.assert_equal(
        x.a, ds([[b'a', b'a', b'a'], [b'a'], [b'a']]).with_bag(db)
    )
    testing.assert_equal(x.b, ds([[1, None, 6], [None], [123]]).with_bag(db))
    testing.assert_equal(x.a.get_schema(), schema_constants.BYTES.with_bag(db))
    testing.assert_equal(x.b.get_schema(), schema_constants.INT32.with_bag(db))

    with self.assertRaisesRegex(
        exceptions.KodaError, 'shapes are not compatible'
    ):
      db.obj(a=ds([1, 2, 3]), b=ds([3.14, 3.14]))
    testing.assert_equal(
        x.get_attr('__schema__').a,
        ds([
            [schema_constants.BYTES] * 3,
            [schema_constants.BYTES],
            [schema_constants.BYTES],
        ]).with_bag(db),
    )
    testing.assert_equal(
        x.get_attr('__schema__').b,
        ds([
            [schema_constants.INT32] * 3,
            [schema_constants.INT32],
            [schema_constants.INT32],
        ]).with_bag(db),
    )

  def test_new_shaped(self):
    db = bag()
    shape = jagged_shape.create_shape([3])
    x = db.new_shaped(shape)
    self.assertIsInstance(x, data_slice.DataSlice)
    x.a = ds([1, 2, 3])
    testing.assert_equal(x.a, ds([1, 2, 3]).with_bag(db))

    # Rank 0.
    shape = jagged_shape.create_shape()
    self.assertIsInstance(db.new_shaped(shape), data_item.DataItem)

    with self.assertRaisesRegex(
        TypeError, r'expected mandatory \'shape\' argument'
    ):
      db.new_shaped()
    with self.assertRaisesRegex(
        TypeError, 'expecting shape to be a JaggedShape, got int'
    ):
      db.new_shaped(4)
    with self.assertRaisesRegex(
        TypeError, 'expecting shape to be a JaggedShape, got .*DataBag'
    ):
      db.new_shaped(db)
    with self.assertRaisesRegex(
        TypeError,
        'expecting shape to be a JaggedShape, got JaggedArrayShape',
    ):
      # Using JaggedArrayShape, instead of JaggedDenseArrayShape
      shape = arolla_jagged_shape.JaggedArrayShape.from_edges(
          arolla.types.ArrayEdge.from_sizes(arolla.array([3]))
      )
      db.new_shaped(shape)

  def test_new_shaped_str_as_schema_arg(self):
    shape = jagged_shape.create_shape([3])
    db = bag()
    x = db.new_shaped(shape, schema='name', a=42)
    expected_schema = db.named_schema('name')
    testing.assert_equal(x.get_schema(), expected_schema)
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_new_shaped_str_slice_as_schema_arg(self):
    shape = jagged_shape.create_shape([3])
    db = bag()
    x = db.new_shaped(shape, schema=ds('name'), a=42)
    expected_schema = db.named_schema('name')
    testing.assert_equal(x.get_schema(), expected_schema)
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_new_shaped_schema_arg_errors(self):
    shape = jagged_shape.create_shape([3])
    db = bag()
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: STRING"
    ):
      _ = db.new_shaped(shape, schema=ds(['name']), a=42)

  def test_new_like(self):
    db = bag()
    shape_and_mask_from = ds([[1, None, 1], [None, 2]])
    x = db.new_like(shape_and_mask_from)
    testing.assert_equal(
        kde.has._eval(x).with_bag(None),  # pylint: disable=protected-access
        ds([[arolla.unit(), None, arolla.unit()], [None, arolla.unit()]]),
    )
    self.assertIsInstance(x, data_slice.DataSlice)
    x.a = ds([[1, 2, 3], [4, 5]])
    testing.assert_equal(x.a, ds([[1, None, 3], [None, 5]]).with_bag(db))

    # Rank 0.
    shape_and_mask_from = ds(1)
    self.assertIsInstance(db.new_like(shape_and_mask_from), data_item.DataItem)
    shape_and_mask_from = ds(None)
    self.assertIsInstance(db.new_like(shape_and_mask_from), data_item.DataItem)

    with self.assertRaisesRegex(
        TypeError, r'expected mandatory \'shape_and_mask_from\' argument'
    ):
      db.new_like()
    with self.assertRaisesRegex(
        TypeError, 'expecting shape_and_mask_from to be a DataSlice, got int'
    ):
      db.new_like(4)

  def test_new_like_str_as_schema_arg(self):
    shape_and_mask_from = ds([[6, 7], [8]])
    db = bag()
    x = db.new_like(shape_and_mask_from, schema='name', a=42)
    expected_schema = db.named_schema('name')
    testing.assert_equal(x.get_schema(), expected_schema)
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_new_like_str_slice_as_schema_arg(self):
    shape_and_mask_from = ds([[6, 7], [8]])
    db = bag()
    x = db.new_like(shape_and_mask_from, schema=ds('name'), a=42)
    expected_schema = db.named_schema('name')
    testing.assert_equal(x.get_schema(), expected_schema)
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_new_like_schema_arg_errors(self):
    shape_and_mask_from = ds([[6, 7], [8]])
    db = bag()
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: STRING"
    ):
      _ = db.new_like(shape_and_mask_from, schema=ds(['name']), a=42)

  def test_obj_shaped(self):
    db = bag()
    shape = jagged_shape.create_shape([3])
    x = db.obj_shaped(shape)
    self.assertIsInstance(x, data_slice.DataSlice)
    # Allows assignment without touching schema first.
    x.a = ds([1, 2, 3])
    testing.assert_equal(x.a.get_schema(), schema_constants.INT32.with_bag(db))
    testing.assert_equal(x.a, ds([1, 2, 3]).with_bag(db))
    testing.assert_equal(
        x.get_attr('__schema__').a,
        ds([schema_constants.INT32] * 3).with_bag(db),
    )

    with self.assertRaisesRegex(
        TypeError, r'expected mandatory \'shape\' argument'
    ):
      db.obj_shaped()
    with self.assertRaisesRegex(
        TypeError, 'expecting shape to be a JaggedShape, got int'
    ):
      db.obj_shaped(1)

  def test_obj_like(self):
    db = bag()
    shape_and_mask_from = ds([[1, None, 1], [None, 2]])
    x = db.obj_like(shape_and_mask_from)
    testing.assert_equal(
        kde.has._eval(x).no_bag(),  # pylint: disable=protected-access
        ds([[arolla.unit(), None, arolla.unit()], [None, arolla.unit()]]),
    )
    x.a = ds([[1, 2, 3], [4, 5]])
    testing.assert_equal(x.a, ds([[1, None, 3], [None, 5]]).with_bag(db))

    # Rank 0.
    shape_and_mask_from = ds(1)
    self.assertIsInstance(db.obj_like(shape_and_mask_from), data_item.DataItem)
    shape_and_mask_from = ds(None)
    self.assertIsInstance(db.obj_like(shape_and_mask_from), data_item.DataItem)

    with self.assertRaisesRegex(
        TypeError, r'expected mandatory \'shape_and_mask_from\' argument'
    ):
      db.obj_like()
    with self.assertRaisesRegex(
        TypeError, 'expecting shape_and_mask_from to be a DataSlice, got int'
    ):
      db.obj_like(4)

  def test_obj_merging(self):
    db = bag()
    x = db.obj(a=bag().list([1, 2, 3]), b=bag().list([4, 5, 6]))
    testing.assert_equal(x.a[:], ds([1, 2, 3]).with_bag(db))
    testing.assert_equal(x.b[:], ds([4, 5, 6]).with_bag(db))

  def test_empty_shaped(self):
    shape = jagged_shape.create_shape([3])
    res = data_bag._empty_shaped(shape, schema_constants.MASK)
    testing.assert_equal(res, ds([None, None, None], schema_constants.MASK))

    res = data_bag._empty_shaped(shape, schema_constants.OBJECT, db=None)
    testing.assert_equal(res, ds([None, None, None], schema_constants.OBJECT))

    db = bag()
    res = data_bag._empty_shaped(shape, schema_constants.INT32, db=db)
    testing.assert_equal(
        res, ds([None, None, None], schema_constants.INT32).with_bag(db)
    )
    res = data_bag._empty_shaped(
        shape, schema_constants.INT32, db=data_bag.null_bag()
    )
    testing.assert_equal(
        res, ds([None, None, None], schema_constants.INT32).with_bag(None)
    )

    with self.assertRaisesRegex(
        ValueError, r'missing required argument to _empty_shaped: `shape`'
    ):
      _ = data_bag._empty_shaped()

    with self.assertRaisesRegex(
        ValueError, r'missing required argument to _empty_shaped: `schema`'
    ):
      _ = data_bag._empty_shaped(shape)

    with self.assertRaisesRegex(
        TypeError,
        'expecting shape to be a JaggedShape, got NoneType',
    ):
      data_bag._empty_shaped(None, schema_constants.INT32)

    with self.assertRaisesRegex(
        TypeError,
        'expecting schema to be a DataSlice, got NoneType',
    ):
      data_bag._empty_shaped(shape, schema=None)

  def test_dict(self):
    db = bag()

    # 0-arg
    x = db.dict()
    x['a'] = 1
    testing.assert_dicts_equal(
        x,
        db.dict(
            {'a': 1},
            key_schema=schema_constants.OBJECT,
            value_schema=schema_constants.OBJECT,
        ),
    )

    x = db.dict(
        key_schema=schema_constants.INT64, value_schema=schema_constants.STRING
    )
    self.assertEqual(
        x.get_schema().get_attr('__keys__'), schema_constants.INT64
    )
    self.assertEqual(
        x.get_schema().get_attr('__values__'), schema_constants.STRING
    )

    # 1-arg
    x = db.dict({'a': 42})
    testing.assert_equal(x['a'], ds(42).with_bag(db))
    x = db.dict({'a': {b'x': 42, b'y': 12}, 'b': {b'z': 15}})
    testing.assert_equal(
        x[['a', 'b']][[b'x', b'x'], [b'z']],
        ds([[42, 42], [15]]).with_bag(db),
    )

    # 2-arg
    x = db.dict(ds(['a', 'b']), 1)
    self.assertEqual(x.get_shape().rank(), 0)
    testing.assert_equal(x[['a', 'b']], ds([1, 1]).with_bag(db))

    x = db.dict(ds([['a', 'b'], ['c']]), 1)
    # NOTE: Dimension of dicts is reduced by 1.
    self.assertEqual(x.get_shape().rank(), 1)
    testing.assert_equal(
        x[['a', 'b'], ['d']],
        ds([[1, 1], [None]]).with_bag(db),
    )

  def test_dict_errors(self):
    db = bag()
    with self.assertRaisesRegex(
        TypeError,
        r'`items_or_keys` must be a DataSlice or DataItem \(or convertible '
        r'to DataItem\) if `values` is provided, but got dict',
    ):
      db.dict({'a': 42}, 12)
    with self.assertRaisesRegex(
        TypeError,
        '`items_or_keys` must be a Python dict if `values` is not provided,'
        ' but got str',
    ):
      db.dict('a')

  def test_dict_shaped(self):
    # NOTE: more tests for dict_shaped in
    # //py/koladata/functions/tests/dict_shaped_test.py

    db = bag()
    shape = jagged_shape.create_shape([3])
    x = db.dict_shaped(
        shape, ds('a'), ds([1, 2, 3]), value_schema=schema_constants.INT64
    )
    self.assertIsInstance(x, data_slice.DataSlice)
    testing.assert_dicts_keys_equal(x, ds([['a'], ['a'], ['a']]).with_bag(db))
    testing.assert_equal(
        x['a'], ds([1, 2, 3], schema_constants.INT64).with_bag(db)
    )

  def test_dict_like(self):
    # NOTE: more tests for dict_like in
    # //py/koladata/functions/tests/dict_like_test.py

    db = bag()
    x = db.dict_like(
        ds([None, 0]),
        ds([['a'], ['b', 'c']]),
        ds(42),
        value_schema=schema_constants.INT64,
    )
    testing.assert_dicts_keys_equal(x, ds([[], ['b', 'c']]))
    testing.assert_equal(
        x['a'], ds([None, None], schema_constants.INT64).with_bag(db)
    )
    testing.assert_equal(
        x['b'], ds([None, 42], schema_constants.INT64).with_bag(db)
    )

  def test_empty_list(self):
    db = bag()
    l = db.list()
    self.assertEqual(l.get_shape().rank(), 0)
    testing.assert_equal(l[:], ds([]).with_bag(db))
    testing.assert_equal(
        l.get_schema().get_attr('__items__'),
        schema_constants.OBJECT.with_bag(db),
    )

  def test_list_errors(self):
    db = bag()
    with self.assertRaisesRegex(
        ValueError,
        'creating a list from values requires at least one dimension',
    ):
      db.list(42)
    with self.assertRaisesRegex(
        ValueError,
        'creating a list from values requires at least one dimension',
    ):
      db.list(data_item.DataItem.from_vals('a'))
    with self.assertRaisesRegex(
        ValueError, 'DataBag._list accepts exactly 4 arguments, got 3'
    ):
      db._list(ds([]), ds([]), ds([]))

  @parameterized.parameters(
      ([], 1),
      ([1, 2, 3], 1),
      ([1, 2, None, 4], 1),
      ([[1, 2, 3], [4, 5]], 2),
      ([[[1, 2, 3]], [[4, 5]]], 3),
  )
  def test_list_from_python_list(self, values, depth):
    db = bag()
    l = db.list(values)
    self.assertEqual(l.get_shape().rank(), 0)

    item_schema = l.get_schema()
    for _ in range(depth):
      item_schema = item_schema.get_attr('__items__')
    testing.assert_equal(item_schema.get_bag(), db)
    testing.assert_equal(
        item_schema.no_bag(),
        schema_constants.INT32 if values else schema_constants.OBJECT,
    )

    exploded_ds = l
    for _ in range(depth):
      exploded_ds = exploded_ds[:]
    testing.assert_equal(exploded_ds, ds(values).with_bag(db))

  @parameterized.parameters(
      ([], []),
      ([1, 2, 3], []),
      ([[1, 2, 3], [4, 5]], [[2]]),
      ([[[1, 2, 3]], [[4, 5]]], [[2], [1, 1]]),
  )
  def test_list_from_slice(self, values, shape_sizes):
    db = bag()
    values_ds = ds(values)
    l = db.list(values_ds)
    testing.assert_equal(
        l.get_schema().get_attr('__items__'),
        values_ds.get_schema().with_bag(db),
    )
    exploded_ds = l[:]
    testing.assert_equal(exploded_ds, ds(values).with_bag(db))
    testing.assert_equal(l.get_shape(), jagged_shape.create_shape(*shape_sizes))

  def test_list_shaped(self):
    # NOTE: more tests for list_shaped in
    # //py/koladata/functions/tests/list_shaped_test.py

    db = bag()
    shape = jagged_shape.create_shape([3])
    l = db.list_shaped(shape, ds([[1, 2], [3], []]))
    self.assertIsInstance(l, data_slice.DataSlice)
    testing.assert_equal(l[:], ds([[1, 2], [3], []]).with_bag(db))

  def test_list_like(self):
    # NOTE: more tests for list_like in
    # //py/koladata/functions/tests/list_like_test.py

    db = bag()
    l = db.list_like(ds([[1, None], [1]]), ds([[[1, 2], [3]], [[4, 5]]]))
    testing.assert_equal(l[:], ds([[[1, 2], []], [[4, 5]]]).with_bag(db))

  def test_list_like_impl(self):
    db = bag()
    with self.assertRaisesRegex(
        ValueError, 'DataBag._list_like accepts exactly 5 arguments, got 3'
    ):
      db._list_like(ds([]), ds([]), ds([]))
    with self.assertRaisesRegex(
        TypeError, 'expecting shape_and_mask_from to be a DataSlice, got int'
    ):
      db._list_like(56, 57, 58, 59, 60)
    with self.assertRaisesRegex(
        TypeError, 'expecting shape_and_mask_from to be a DataSlice, got Int'
    ):
      db._list_like(arolla.int32(56), 57, 58, 59, 60)

  def test_implode_impl(self):
    # NOTE: more tests for implode in
    # //py/koladata/functions/tests/implode_test.py

    db = bag()
    with self.assertRaisesRegex(
        ValueError, 'DataBag._implode accepts exactly 2 arguments, got 3'):
      db._implode(ds([]), 1, 2)
    with self.assertRaisesRegex(
        TypeError, 'expecting x to be a DataSlice, got int'
    ):
      db._implode(1, 2)
    with self.assertRaisesRegex(TypeError, 'an integer is required'):
      db._implode(ds([]), ds([]))

  def test_concat_lists_impl(self):
    # NOTE: more tests for concat_lists in
    # //py/koladata/functions/tests/concat_lists_test.py

    db = bag()
    with self.assertRaisesRegex(
        TypeError, re.escape('expecting *lists to be a DataSlice, got NoneType')
    ):
      db.concat_lists(ds(0), None)

  def test_exactly_equal_impl_raises(self):
    with self.assertRaisesRegex(
        ValueError, 'DataBag._exactly_equal accepts exactly 1 argument, got 2'
    ):
      bag()._exactly_equal(42, 42)

    with self.assertRaisesRegex(
        ValueError, 'DataBag._exactly_equal accepts exactly 1 argument, got 0'
    ):
      bag()._exactly_equal()

    with self.assertRaisesRegex(TypeError, 'cannot compare DataBag with int'):
      bag()._exactly_equal(42)

  def test_get_fallbacks(self):
    db1 = bag()
    db2 = bag()
    o = db1.obj(a=1)
    res = o.with_bag(db2).enriched(db1)
    fallbacks = res.get_bag().get_fallbacks()
    self.assertLen(fallbacks, 2)
    testing.assert_equal(fallbacks[0], db2)
    testing.assert_equal(fallbacks[1], db1)

    with self.subTest('three-fallbacks'):
      db1 = bag()
      db2 = bag()
      db3 = bag()
      o = db1.obj(a=1)
      res1 = o.enriched(db2)
      db4 = res1.get_bag()
      res2 = res1.enriched(db3)
      fallbacks = res2.get_bag().get_fallbacks()
      self.assertLen(fallbacks, 2)
      testing.assert_equal(fallbacks[0], db4)
      testing.assert_equal(fallbacks[1], db3)
      self.assertLen(db4.get_fallbacks(), 2)
      testing.assert_equal(db4.get_fallbacks()[0], db1)
      testing.assert_equal(db4.get_fallbacks()[1], db2)

    with self.subTest('no-fallbacks'):
      db = bag()
      self.assertEmpty(db.get_fallbacks())

  def test_exactly_equal_impl(self):
    db1 = bag()
    db2 = bag()
    self.assertTrue(db1._exactly_equal(db2))
    _ = db1.obj(a=1)
    self.assertFalse(db1._exactly_equal(db2))
    _ = db2.obj(a=1)
    self.assertFalse(db1._exactly_equal(db2))

  def test_exactly_equal_impl_fallbacks(self):
    db1 = bag()
    db2 = bag()
    x = data_slice.DataSlice.from_vals([1, 2, 3])
    ds12 = x.with_bag(db1).enriched(db2)
    ds1 = x.with_bag(db1)
    self.assertFalse(ds12.get_bag()._exactly_equal(ds1.get_bag()))

    ds21 = x.with_bag(db2).enriched(db1)
    self.assertTrue(ds12.get_bag()._exactly_equal(ds21.get_bag()))

    _ = db1.obj(x=1)
    self.assertFalse(ds12.get_bag()._exactly_equal(ds21.get_bag()))

  def test_merge_inplace(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    db2 = bag()
    x2 = x1.with_bag(db2)
    x2.set_attr('a', 3)
    self.assertIs(db1.merge_inplace(db2), db1)
    self.assertEqual(x1.a, ds(3))
    self.assertEqual(x1.b, ds(2))

  def test_merge_inplace_no_overwrite(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    db2 = bag()
    x2 = x1.with_bag(db2)
    x2.set_attr('a', 3)
    db1.merge_inplace(db2, overwrite=False)
    self.assertEqual(x1.a, ds(1))
    self.assertEqual(x1.b, ds(2))

  def test_merge_inplace_conflict(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    db2 = bag()
    x2 = x1.with_bag(db2)
    x2.set_attr('a', 3)
    with self.assertRaisesRegex(ValueError, 'conflicting values'):
      db1.merge_inplace(db2, allow_data_conflicts=False)

  def test_merge_inplace_schema_conflict(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    db2 = bag()
    x2 = x1.with_bag(db2)
    x2.set_attr('a', 'foo')
    with self.assertRaisesRegex(ValueError, 'conflicting dict values'):
      db1.merge_inplace(db2)

  def test_merge_inplace_schema_overwrite(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    db2 = bag()
    x2 = x1.with_bag(db2)
    x2.set_attr('a', 'foo')
    db1.merge_inplace(db2, allow_schema_conflicts=True)
    self.assertEqual(x1.a, ds('foo'))
    self.assertEqual(x1.b, ds(2))

  def test_merge_inplace_zero_bags(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    db1.merge_inplace([])
    self.assertEqual(x1.a, ds(1))
    self.assertEqual(x1.b, ds(2))
    db1.merge_inplace([], overwrite=False)
    self.assertEqual(x1.a, ds(1))
    self.assertEqual(x1.b, ds(2))

  def test_merge_inplace_two_bags(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    db2 = bag()
    x2 = x1.with_bag(db2)
    x2.set_attr('a', 3)
    x2.set_attr('b', 5)
    db3 = bag()
    x3 = x1.with_bag(db3)
    x3.set_attr('a', 4)
    db1.merge_inplace([db2, db3])
    self.assertEqual(x1.a, ds(4))
    self.assertEqual(x1.b, ds(5))

  def test_merge_inplace_two_bags_no_overwrite(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    db2 = bag()
    x2 = x1.with_bag(db2)
    x2.set_attr('a', 3)
    x2.set_attr('c', 5)
    db3 = bag()
    x3 = x1.with_bag(db3)
    x3.set_attr('c', 4)
    db1.merge_inplace([db2, db3], overwrite=False)
    self.assertEqual(x1.a, ds(1))
    self.assertEqual(x1.b, ds(2))
    self.assertEqual(x1.c, ds(5))

  def test_merge_inplace_nonbools(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    db2 = bag()
    x2 = x1.with_bag(db2)
    x2.set_attr('a', 3)
    db1.merge_inplace(db2, overwrite=0)
    self.assertEqual(x1.a, ds(1))
    db1.merge_inplace(db2, overwrite=1)
    self.assertEqual(x1.a, ds(3))
    with self.assertRaisesRegex(TypeError, '__bool__ disabled'):
      db1.merge_inplace(db2, overwrite=arolla.L.x)

  def test_merge_inplace_not_databags(self):
    db1 = bag()
    x1 = db1.new(a=1, b=2)
    with self.assertRaisesRegex(TypeError, 'must be an iterable'):
      db1.merge_inplace(57)
    with self.assertRaisesRegex(
        TypeError,
        'expecting each DataBag to be merged to be a DataBag, got int',
    ):
      db1.merge_inplace([57])
    with self.assertRaisesRegex(
        TypeError,
        'expecting each DataBag to be merged to be a DataBag, got None',
    ):
      db1.merge_inplace([None])
    with self.assertRaisesRegex(
        TypeError,
        'expecting each DataBag to be merged to be a DataBag, got None',
    ):
      db1.merge_inplace([data_bag.null_bag()])
    with self.assertRaisesRegex(
        TypeError,
        'expecting each DataBag to be merged to be a DataBag, got'
        ' data_item.DataItem',
    ):
      db1.merge_inplace([x1])

  def test_adopt_args_errors(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'DataBag.adopt accepts exactly 1 argument, got 0'
    ):
      bag().adopt()
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'DataBag.adopt accepts exactly 1 argument, got 2'
    ):
      bag().adopt(ds(1), ds(2))

  def test_adopt_immutable(self):
    db1 = bag()
    db1 = db1.fork(mutable=False)
    x = bag().obj()
    with self.assertRaisesRegex(
        ValueError, 'DataBag is immutable, try DataSlice.fork_db()'
    ):
      db1.adopt(x)

  def test_adopt(self):
    db1 = bag()
    o1 = db1.uuobj(seed='1')
    o1.x = 1
    db2 = bag()
    db2.uuobj(seed='1').y = 2
    o3 = db2.adopt(o1)
    self.assertEqual(o3.db.fingerprint, db2.fingerprint)
    testing.assert_equivalent(o3.x.no_bag(), ds(1))
    testing.assert_equivalent(o3.y.no_bag(), ds(2))

  def test_lshift(self):
    db1 = bag()
    o1 = db1.uuobj(x=1)
    db2 = bag()
    o2 = db2.uuobj(x=1)
    o2.x = 2
    o2.y = 3
    db3 = bag()
    o3 = db3.uuobj(x=1)
    o3.x = 2
    o3.y = 4
    o4 = o1.with_bag(db1 << db2 << db3)
    self.assertEqual(o4.x.no_bag(), ds(2))
    self.assertEqual(o4.y.no_bag(), ds(4))

  def test_shift(self):
    db1 = bag()
    o1 = db1.uuobj(x=1)
    db2 = bag()
    o2 = db2.uuobj(x=1)
    o2.x = 2
    o2.y = 3
    db3 = bag()
    o3 = db3.uuobj(x=1)
    o3.x = 2
    o3.y = 4
    o3 = o1.with_bag(db1 >> db2 >> db3)
    testing.assert_equal(o3.x.no_bag(), ds(1))
    self.assertEqual(o3.y.no_bag(), ds(3))

  def test_ilshift(self):
    db1 = bag()
    o1 = db1.uuobj(x=1)
    db2 = bag()
    o2 = db2.uuobj(x=1)
    o2.x = 2
    o2.y = 3
    db1 <<= db2
    self.assertEqual(o1.x.no_bag(), ds(2))
    self.assertEqual(o1.y.no_bag(), ds(3))

  def test_irshift(self):
    db1 = bag()
    o1 = db1.uuobj(x=1)
    db2 = bag()
    o2 = db2.uuobj(x=1)
    o2.x = 2
    o2.y = 3
    db1 >>= db2
    self.assertEqual(o1.x.no_bag(), ds(1))
    self.assertEqual(o1.y.no_bag(), ds(3))

  def test_merge_fallbacks(self):
    db1 = bag()
    x1 = db1.new(a=1)
    x2 = x1.with_bag(bag()).enriched(db1)
    db2 = x2.get_bag()

    db3 = db2.merge_fallbacks()
    self.assertIsInstance(db3, data_bag.DataBag)
    x3 = x2.with_bag(db3)

    # Check that subsequent modifications of x1 and x3 are independent.
    x3.set_attr('a', 2)
    self.assertEqual(x2.a, ds(1))

    x1.set_attr('a', 3)
    self.assertEqual(x3.a, ds(2))

  def test_fork(self):
    db1 = bag()
    x1 = db1.new(a=1)

    db2 = db1.fork()
    self.assertIsInstance(db2, data_bag.DataBag)
    x2 = x1.with_bag(db2)

    x2.set_attr('a', 2)
    self.assertEqual(x1.a, ds(1))

    x1.set_attr('a', 3)
    self.assertEqual(x2.a, ds(2))

  def test_fork_immutable(self):
    db1 = bag()
    x1 = db1.new(a=1)

    db2 = db1.fork(mutable=False)
    self.assertIsInstance(db2, data_bag.DataBag)
    x2 = x1.with_bag(db2)

    with self.assertRaisesRegex(
        ValueError, re.escape('DataBag is immutable')):
      x2.set_attr('a', 2)

    with self.assertRaisesRegex(
        TypeError, re.escape("got an unexpected keyword 'foo'")):
      _ = db1.fork(foo=True)

  def test_freeze(self):
    db1 = bag().new(x=1).get_bag()
    self.assertTrue(db1.is_mutable())
    db2 = db1.freeze()
    self.assertFalse(db2.is_mutable())
    testing.assert_equivalent(db2, db1)
    db3 = db2.freeze()
    self.assertFalse(db3.is_mutable())
    testing.assert_equivalent(db3, db1)

  # TODO: Re-think forking in the context of DataBag with mutable
  # fallbacks.
  def test_freeze_with_fallbacks(self):
    db = bag().new().enriched(bag())
    with self.assertRaisesRegex(
        ValueError, 'freezing with fallbacks is not supported'
    ):
      db.freeze()

  def test_with_name(self):
    x = bag()
    y = x.with_name('foo')
    self.assertIs(y, x)

  def test_from_proto_minimal(self):
    # NOTE: more tests for from_proto in
    # //py/koladata/functions/tests/from_proto_test.py

    db = bag()
    x = db._from_proto([], [], None, None)
    self.assertEqual(x.get_bag().fingerprint, db.fingerprint)
    testing.assert_equal(x.no_bag(), ds([]))

  def test_from_proto_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'DataBag._from_proto accepts exactly 4 arguments, got 3',
    ):
      db = bag()
      _ = db._from_proto((), [], None)

    with self.assertRaisesRegex(
        ValueError,
        'DataBag._from_proto expects messages to be a list, got tuple',
    ):
      db = bag()
      _ = db._from_proto((), [], None, None)

    with self.assertRaisesRegex(
        ValueError,
        re.escape('message cast from python to C++ failed, got type NoneType'),
    ):
      db = data_bag.DataBag.empty()
      _ = db._from_proto([None], [], None, None)

    with self.assertRaisesRegex(
        ValueError,
        re.escape('message cast from python to C++ failed, got type tuple'),
    ):
      db = data_bag.DataBag.empty()
      _ = db._from_proto([()], [], None, None)

    with self.assertRaisesRegex(
        ValueError,
        'DataBag._from_proto expects extensions to be a list, got tuple',
    ):
      db = bag()
      _ = db._from_proto([], (), None, None)

    with self.assertRaisesRegex(
        ValueError,
        'expected extension to be bytes, got str',
    ):
      db = bag()
      _ = db._from_proto([], ['x.y.z'], None, None)

    with self.assertRaisesRegex(
        TypeError,
        'expecting itemid to be a DataSlice, got str',
    ):
      db = bag()
      _ = db._from_proto([], [], 'foo', None)

    with self.assertRaisesRegex(
        TypeError,
        'expecting schema to be a DataSlice, got str',
    ):
      db = bag()
      _ = db._from_proto([], [], None, 'foo')

  def test_signatures(self):
    # Tests that all methods have an inspectable signature. This is not added
    # automatically for methods defined in CPython and requires the docstring
    # to follow a specific format.
    for fn_name in dir(data_bag.DataBag):
      if fn_name.startswith('_'):
        continue
      fn = getattr(data_bag.DataBag, fn_name)
      if callable(fn):
        _ = inspect.signature(fn)  # Shouldn't raise.


class NullDataBagTest(absltest.TestCase):

  def test_qvalue(self):
    self.assertIsInstance(data_bag.null_bag(), arolla.QValue)

  def test_with_name(self):
    x = data_bag.null_bag()
    y = x.with_name('foo')
    self.assertIs(y, x)

  def test_fingerprint(self):
    self.assertEqual(
        data_bag.null_bag().fingerprint, data_bag.null_bag().fingerprint
    )
    self.assertNotEqual(data_bag.null_bag().fingerprint, bag().fingerprint)

  def test_with_bag(self):
    x = ds([1, 2, 3]).with_bag(data_bag.null_bag())
    self.assertIsNone(x.get_bag())

  def test_repr(self):
    self.assertEqual(repr(data_bag.null_bag()), 'DataBag(null)')

  def test_str(self):
    self.assertEqual(str(data_bag.null_bag()), 'DataBag(null)')


if __name__ == '__main__':
  absltest.main()
