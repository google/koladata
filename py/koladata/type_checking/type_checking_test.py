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

import contextlib
from absl.testing import absltest
from arolla import arolla
from koladata import kd
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.type_checking import type_checking
from koladata.types import data_slice


ds = data_slice.DataSlice.from_vals
kdf = kd.functor


@contextlib.contextmanager
def override_operator(operator_name: str, new_operator: arolla.abc.Operator):
  old_operator = arolla.abc.decay_registered_operator(operator_name)
  arolla.abc.unsafe_override_registered_operator(operator_name, new_operator)

  try:
    yield
  finally:
    arolla.abc.unsafe_override_registered_operator(operator_name, old_operator)


class TypeCheckingTest(absltest.TestCase):

  def test_primitive_input_type(self):
    @type_checking.check_inputs(x=kd.INT32)
    def f(x):
      return x

    testing.assert_equal(f(ds([1, 2, 3])), ds([1, 2, 3]))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter `x`. Expected type INT32,'
        ' got FLOAT32',
    ):
      _ = f(ds([1.0, 2, 3]))

  def test_primitive_output_type(self):
    @type_checking.check_output(kd.INT32)
    def f(x):
      return x

    testing.assert_equal(f(ds([1, 2, 3])), ds([1, 2, 3]))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_output: type mismatch for output. Expected type INT32, got'
        ' FLOAT32',
    ):
      _ = f(ds([1.0, 2, 3]))

  def test_check_inputs_data_item(self):
    @type_checking.check_inputs(x=kd.INT32)
    def f(x):
      return x

    testing.assert_equal(f(ds(1)), ds(1))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter `x`. Expected type INT32,'
        ' got FLOAT32',
    ):
      _ = f(ds(1.0))

  def test_check_output_data_item(self):
    @type_checking.check_output(kd.INT32)
    def f(x):
      return x

    testing.assert_equal(f(ds(1)), ds(1))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_output: type mismatch for output. Expected type INT32, got'
        ' FLOAT32',
    ):
      _ = f(ds(1.0))

  def test_check_inputs_entity_schema(self):
    person = kd.schema.new_schema(age=kd.INT32, name=kd.STRING)

    @type_checking.check_inputs(x=person)
    def f(x):
      return x

    # Assert does not raise.
    _ = f(kd.new(age=30, name='John', schema=person))

    with self.assertRaisesRegex(
        TypeError,
        r'kd.check_inputs: type mismatch for parameter `x`. Expected type'
        r' ENTITY\(age=INT32, name=STRING\) with id .*, got'
        r' ENTITY\(age=INT32, name=STRING\) with id .*',
    ):
      _ = f(kd.new(age=32, name='Alice'))

  def test_check_output_entity_schema(self):
    person = kd.schema.new_schema(age=kd.INT32, name=kd.STRING)

    @type_checking.check_output(person)
    def f(x):
      return x

    # Assert does not raise.
    _ = f(kd.new(age=30, name='John', schema=person))

    with self.assertRaisesRegex(
        TypeError,
        r'kd.check_output: type mismatch for output. Expected type'
        r' ENTITY\(age=INT32, name=STRING\) with id .*, got'
        r' ENTITY\(age=INT32, name=STRING\) with id .*',
    ):
      _ = f(kd.new(age=32, name='Alice'))

  def test_check_inputs_named_entity_schema(self):
    person = kd.schema.named_schema(
        'Person', age=kd.INT32, first_name=kd.STRING
    )

    @type_checking.check_inputs(x=person)
    def f(x):
      return x

    # Assert does not raise.
    _ = f(kd.new(age=30, first_name='John', schema=person))

    with self.assertRaisesRegex(
        TypeError,
        r'kd.check_inputs: type mismatch for parameter `x`. Expected type'
        r' Person, got ENTITY\(age=INT32, first_name=STRING\) with id .*',
    ):
      _ = f(kd.uu(age=32, first_name='Alice'))

  def test_check_output_named_entity_schema(self):
    person = kd.schema.named_schema(
        'Person', age=kd.INT32, first_name=kd.STRING
    )

    @type_checking.check_output(person)
    def f(x):
      return x

    # Assert does not raise.
    _ = f(kd.new(age=30, first_name='John', schema=person))

    with self.assertRaisesRegex(
        TypeError,
        r'kd.check_output: type mismatch for output. Expected type'
        r' Person, got ENTITY\(age=INT32, first_name=STRING\) with id .*',
    ):
      _ = f(kd.uu(age=32, first_name='Alice'))

  def test_check_inputs_uu_entity_schema(self):
    person = kd.schema.uu_schema(age=kd.INT32, name=kd.STRING)

    @type_checking.check_inputs(x=person)
    def f(x):
      return x

    # Assert does not raise.
    _ = f(kd.new(age=30, name='John', schema=person))
    _ = f(kd.uu(age=32, name='Alice'))

    with self.assertRaisesRegex(
        TypeError,
        r'kd.check_inputs: type mismatch for parameter `x`. Expected type'
        r' ENTITY\(age=INT32, name=STRING\) with id .*, got'
        r' ENTITY\(age=FLOAT32, name=STRING\) with id .*',
    ):
      _ = f(kd.uu(age=32.0, name='Alice'))

  def test_check_output_uu_entity_schema(self):
    person = kd.schema.uu_schema(age=kd.INT32, name=kd.STRING)

    @type_checking.check_output(person)
    def f(x):
      return x

    # Assert does not raise.
    _ = f(kd.new(age=30, name='John', schema=person))
    _ = f(kd.uu(age=32, name='Alice'))

    with self.assertRaisesRegex(
        TypeError,
        r'kd.check_output: type mismatch for output. Expected type'
        r' ENTITY\(age=INT32, name=STRING\) with id .*, got'
        r' ENTITY\(age=FLOAT32, name=STRING\) with id .*',
    ):
      _ = f(kd.uu(age=32.0, name='Alice'))

  def test_check_inputs_default_argument(self):
    @type_checking.check_inputs(x=kd.INT32)
    def f(x=ds([1.0, 2, 3])):
      return x

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter `x`. Expected type INT32,'
        ' got FLOAT32',
    ):
      _ = f()

  def test_using_both_shorthands_triggers_inputs_error_first(self):
    @type_checking.check_inputs(x=kd.INT32)
    @type_checking.check_output(kd.INT32)
    def f(x):
      return x

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter `x`. Expected type INT32,'
        ' got FLOAT32',
    ):
      _ = f(ds([1.0, 2, 3]))

  def test_list_schemas(self):
    doc = kd.schema.named_schema('Doc', doc_id=kd.INT64, score=kd.FLOAT32)

    query = kd.schema.named_schema('Query', docs=kd.list_schema(doc))

    @type_checking.check_inputs(query=query)
    @type_checking.check_output(doc)
    def get_docs(query):
      return query.docs[:]

    q = kd.new(
        docs=[
            kd.new(doc_id=1, score=0.5, schema=doc),
            kd.new(doc_id=2, score=0.7, schema=doc),
        ],
        schema=query,
    )
    # Assert does not raise.
    _ = get_docs(q)

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter `query`. Expected type'
        ' Query, got INT32',
    ):
      _ = get_docs(ds([1, 2, 3]))

  def test_timestamp_example(self):

    @type_checking.check_inputs(hours=kd.INT32, minutes=kd.INT32)
    @type_checking.check_output(kd.STRING)
    def timestamp(hours, minutes):
      return kd.str(hours) + ':' + kd.str(minutes)

    testing.assert_equal(
        timestamp(ds([10, 10, 10]), ds([15, 30, 45])),
        ds(['10:15', '10:30', '10:45']),
    )

  def test_invalid_input_constraint_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: invalid constraint: expected constraint for parameter'
        ' `x` to be a schema DataItem, got 0',
    ):

      @type_checking.check_inputs(x=kd.int32(0))
      def _(x):
        return x

  def test_invalid_output_constraint_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_output: invalid constraint: expected constraint for output to'
        ' be a schema DataItem, got 0',
    ):

      @type_checking.check_output(kd.int32(0))
      def _(x):
        return x

  def test_variadic_parameter_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs does not support variadic parameters in the decorated'
        ' function',
    ):

      @type_checking.check_inputs(x=kd.INT32)
      def _(x, *args):  # pylint: disable=unused-argument
        return x

  def test_invalid_keyword_constraint_error(self):

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: parameter name `y` does not match any parameter in'
        ' function signature',
    ):

      @type_checking.check_inputs(y=kd.INT32)
      def _(x):
        return x

  def test_non_data_slice_input_with_autoboxing(self):
    @type_checking.check_inputs(x=kd.INT32)
    def f(x):
      return x

    # Assert does not raise.
    _ = f(1)

  def test_non_data_slice_input_with_wrong_autoboxing(self):
    @type_checking.check_inputs(x=kd.INT32)
    def f(x):
      return x

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter `x`. Expected type INT32,'
        ' got STRING',
    ):
      _ = f('hello')

  def test_non_data_slice_output_error(self):
    @type_checking.check_inputs(x=kd.INT32)
    @type_checking.check_output(kd.INT32)
    def f(x):  # pylint: disable=unused-argument
      return 1

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_output: expected DataItem/DataSlice output, got <class'
        " 'int'>",
    ):
      _ = f(kd.int32(1))

  def test_check_inputs_preserves_docstring(self):
    def f(x):
      """Some docstring."""
      return x

    decorated_f = type_checking.check_inputs(x=kd.INT32)(f)

    self.assertEqual(f.__doc__, decorated_f.__doc__)

  def test_check_output_preserves_docstring(self):
    def f(x):
      """Some docstring."""
      return x

    decorated_f = type_checking.check_output(kd.INT32)(f)

    self.assertEqual(f.__doc__, decorated_f.__doc__)

  def test_check_inputs_traced(self):
    @type_checking.check_inputs(x=kd.INT32)
    def f(x):
      return x

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter `x`. Expected type INT32,'
        ' got FLOAT32',
    ):
      _ = f(ds([1.0, 2, 3]))

    fn = kdf.fn(f)

    with self.assertRaisesRegex(
        ValueError,
        # TODO: Could the .*assertion.* prefix be removed?
        'kd.assertion.with_assertion: kd.check_inputs: type mismatch'
        ' for parameter `x`. Expected type INT32, got FLOAT32',
    ):
      _ = fn(ds([1.0, 2, 3]))

  def test_check_inputs_traced_does_not_reserve_keywords(self):
    @type_checking.check_inputs(traced=kd.INT32)
    def f(traced):
      return traced

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter `traced`. Expected type'
        ' INT32, got FLOAT32',
    ):
      _ = f(ds([1.0, 2, 3]))

    fn = kdf.fn(f)

    with self.assertRaisesRegex(
        ValueError,
        # TODO: Could the .*assertion.* prefix be removed?
        'kd.assertion.with_assertion: kd.check_inputs: type mismatch'
        ' for parameter `traced`. Expected type INT32, got FLOAT32',
    ):
      _ = fn(ds([1.0, 2, 3]))

  def test_check_output_traced(self):
    @type_checking.check_output(kd.INT32)
    def f(x):
      return x

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_output: type mismatch for output. Expected type INT32,'
        ' got FLOAT32',
    ):
      _ = f(ds([1.0, 2, 3]))

    fn = kdf.fn(f)

    with self.assertRaisesRegex(
        ValueError,
        # TODO: Could the .*assertion.* prefix be removed?
        'kd.assertion.with_assertion: kd.check_output: type mismatch'
        ' for output. Expected type INT32, got FLOAT32',
    ):
      _ = fn(ds([1.0, 2, 3]))

  def test_check_inputs_traced_named_schema(self):
    doc = kd.schema.named_schema('Doc', doc_id=kd.INT64, score=kd.FLOAT32)
    query = kd.schema.named_schema('Query', docs=kd.list_schema(doc))

    docs = ds([
        kd.new(doc_id=1, score=0.5, schema=doc),
        kd.new(doc_id=2, score=0.7, schema=doc),
    ])
    q = kd.new(
        docs=kd.implode(docs),
        schema=query,
    )

    @type_checking.check_inputs(q=query)
    def get_docs(q):
      return q.docs[:]

    get_docs_fn = kdf.fn(get_docs)
    # Assert does not raise.
    _ = get_docs_fn(q)

    with self.assertRaisesRegex(
        ValueError,
        'kd.assertion.with_assertion: kd.check_inputs: type mismatch'
        ' for parameter `q`. Expected type Query, got Doc',
    ):
      _ = get_docs_fn(docs)

  def test_check_inputs_traced_static_input(self):
    @type_checking.check_inputs(pick_a=kd.BOOLEAN)
    def choose(pick_a=True):
      if pick_a:
        return ds('a')
      else:
        return ds('b')

    def f():
      return choose(pick_a=True)

    # Assert does not raise.
    _ = kdf.fn(f)

  def test_check_output_traced_named_schema(self):
    doc = kd.schema.named_schema('Doc', doc_id=kd.INT64, score=kd.FLOAT32)
    query = kd.schema.named_schema('Query', docs=kd.list_schema(doc))

    docs = ds([
        kd.new(doc_id=1, score=0.5, schema=doc),
        kd.new(doc_id=2, score=0.7, schema=doc),
    ])
    q = kd.new(
        docs=kd.implode(docs),
        schema=query,
    )

    @type_checking.check_output(doc)
    def get_docs(q):
      return q

    get_docs_fn = kdf.fn(get_docs)

    with self.assertRaisesRegex(
        ValueError,
        'kd.assertion.with_assertion: kd.check_output: type mismatch'
        ' for output. Expected type Doc, got Query',
    ):
      _ = get_docs_fn(q)

  def test_check_inputs_traced_two_inputs(self):
    @type_checking.check_inputs(x=kd.INT32, y=kd.INT32)
    def f(x, y):
      return (x, y)

    fn = kdf.fn(f)

    with self.assertRaisesRegex(
        ValueError,
        'kd.assertion.with_assertion: kd.check_inputs: type mismatch'
        ' for parameter `x`. Expected type INT32, got FLOAT32',
    ):
      _ = fn(ds([1.0]), ds([1]))

  def test_traced_error_messages_not_constructed_on_success(self):
    @type_checking.check_inputs(x=kd.INT32)
    @type_checking.check_output(kd.INT32)
    def f(x):
      return x

    fn = kdf.fn(f)

    @optools.as_lambda_operator('kd.schema.get_repr')
    def get_repr(x):  # pylint: disable=unused-argument
      return kde_operators.kde.assertion.with_assertion(
          'fake repr',
          kd.missing,
          '`kd.schema.get_repr` should not have been called',
      )

    with override_operator('kd.schema.get_repr', get_repr):
      _ = fn(ds([1]))  # Does not raise.


if __name__ == '__main__':
  absltest.main()
