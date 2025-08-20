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

import contextlib
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.type_checking import type_checking
from koladata.types import data_slice


ds = data_slice.DataSlice.from_vals
kdf = kd.functor
Person = kd.schema.new_schema(age=kd.INT32, name=kd.STRING)
Person_named = kd.schema.named_schema(
    'Person', age=kd.INT32, first_name=kd.STRING
)
UuPerson = kd.schema.uu_schema(age=kd.INT32, name=kd.STRING)


@contextlib.contextmanager
def override_operator(operator_name: str, new_operator: arolla.abc.Operator):
  old_operator = arolla.abc.decay_registered_operator(operator_name)
  arolla.abc.unsafe_override_registered_operator(operator_name, new_operator)

  try:
    yield
  finally:
    arolla.abc.unsafe_override_registered_operator(operator_name, old_operator)


ERROR_CASES = (
    (
        '_primitive',
        kd.INT32,
        ds([1.0, 2, 3]),
        (
            r'{decorator}: type mismatch for {parameter}; expected type'
            r' INT32, got FLOAT32'
        ),
    ),
    (
        '_data_item',
        kd.INT32,
        ds(1.0),
        (
            '{decorator}: type mismatch for {parameter}; expected'
            ' type INT32, got FLOAT32'
        ),
    ),
    (
        '_entity_schema',
        Person,
        kd.new(age=30, name='Alice'),
        (
            '{decorator}: type mismatch for {parameter}; expected'
            r' type ENTITY\(age=INT32, name=STRING\) with id .*, got'
            r' ENTITY\(age=INT32, name=STRING\) with id .*'
        ),
    ),
    (
        '_named_schema',
        Person_named,
        kd.new(age=30, name='Alice'),
        (
            '{decorator}: type mismatch for {parameter}; expected'
            r' type Person, got ENTITY\(age=INT32, name=STRING\) with id .*'
        ),
    ),
    (
        '_uu_entity_schema',
        UuPerson,
        kd.uu(age=30.0, name='Alice'),
        (
            '{decorator}: type mismatch for {parameter}; expected'
            r' type ENTITY\(age=INT32, name=STRING\) with id .*, got'
            r' ENTITY\(age=FLOAT32, name=STRING\) with id .*'
        ),
    ),
    (
        '_list_schema',
        kd.list_schema(kd.INT32),
        kd.list([1.0, 2, 3]),
        (
            '{decorator}: type mismatch for {parameter}; expected'
            r' type LIST\[INT32\] with id .*,'
            r' got LIST\[FLOAT32\] with id .*'
        ),
    ),
    (
        '_duck_type_and_primitive',
        type_checking.duck_type(
            a=type_checking.duck_type(),
        ),
        ds(1),
        (
            '{decorator}: expected {parameter} to have attribute `a`;'
            ' no attribute `a` on {parameter}=INT32'
        ),
    ),
    (
        '_duck_type_and_non_scalar',
        type_checking.duck_type(
            a=type_checking.duck_type(),
        ),
        kd.new(b=ds([1, 2, 3])),
        (
            '{decorator}: expected {parameter} to have attribute `a`;'
            r' no attribute `a` on {parameter}=ENTITY\(b=INT32\)'
        ),
    ),
    (
        '_duck_type_and_entity_schema',
        type_checking.duck_type(
            age=kd.INT32,
            name=kd.STRING,
            address=kd.STRING,
        ),
        kd.new(age=30, name='John', schema=Person),
        (
            '{decorator}: expected {parameter} to have attribute `address`;'
            r' no attribute `address` on {parameter}=ENTITY\(age=INT32,'
            r' name=STRING\)'
        ),
    ),
    (
        '_duck_type_and_named_schema',
        type_checking.duck_type(
            age=kd.INT32,
            name=kd.STRING,
            address=kd.STRING,
        ),
        kd.new(age=30, name='John', schema=Person_named),
        (
            '{decorator}: expected {parameter} to have attribute `address`;'
            r' no attribute `address` on {parameter}=Person$'
        ),
    ),
    (
        '_wrong_schema_nested_in_duck_type',
        type_checking.duck_type(
            a=kd.INT32,
        ),
        kd.new(a='hello'),
        (
            r'{decorator}: type mismatch for {parameter}\.a; expected type'
            ' INT32, got STRING'
        ),
    ),
    (
        '_duck_type_and_entity_with_wrong_attr',
        type_checking.duck_type(
            a=kd.INT32,
        ),
        kd.new(b=1),
        (
            '{decorator}: expected {parameter} to have attribute `a`;'
            r' no attribute `a` on {parameter}=ENTITY\(b=INT32\)'
        ),
    ),
    (
        '_duck_type_and_nested_entity_with_wrong_attr',
        type_checking.duck_type(
            a=type_checking.duck_type(aa=kd.INT32),
        ),
        kd.new(a=kd.new(b=1)),
        (
            r'{decorator}: expected {parameter}\.a to have attribute `aa`;'
            r' no attribute `aa` on {parameter}\.a=ENTITY\(b=INT32\)'
        ),
    ),
    (
        '_duck_type_and_nested_entity_with_wrong_schema',
        type_checking.duck_type(
            a=type_checking.duck_type(aa=kd.INT32),
        ),
        kd.new(a=kd.new(aa='hello')),
        (
            r'{decorator}: type mismatch for {parameter}\.a\.aa; expected type'
            ' INT32, got STRING'
        ),
    ),
    (
        '_duck_type_and_object',
        type_checking.duck_type(
            a=kd.INT32,
        ),
        kd.obj(a=1),
        (
            '{decorator}: expected {parameter} to have attribute `a`;'
            ' no attribute `a` on {parameter}=OBJECT'
        ),
    ),
    (
        '_duck_list',
        type_checking.duck_list(type_checking.duck_type(a=kd.INT32)),
        kd.list([kd.uu(b=1), kd.uu(b=1)]),
        (
            r'{decorator}: expected {parameter}\.get_items\(\) to have'
            r' attribute `a`;'
            r' no attribute `a` on {parameter}\.get_items\(\)=ENTITY\(b=INT32\)'
        ),
    ),
    (
        '_duck_dict_key_error',
        type_checking.duck_dict(
            key_constraint=type_checking.duck_type(a=kd.INT32),
            value_constraint=type_checking.duck_type(a=kd.INT32),
        ),
        kd.dict({kd.uu(b=1): kd.uu(a=1)}),
        (
            r'{decorator}: expected {parameter}\.get_keys\(\) to have'
            r' attribute `a`; no attribute `a` on'
            r' {parameter}\.get_keys\(\)=ENTITY\(b=INT32\)'
        ),
    ),
    (
        '_duck_dict_value_error',
        type_checking.duck_dict(
            key_constraint=type_checking.duck_type(a=kd.INT32),
            value_constraint=type_checking.duck_type(a=kd.INT32),
        ),
        kd.dict({kd.uu(a=1): kd.uu(b=1)}),
        (
            r'{decorator}: expected {parameter}\.get_values\(\) to have'
            r' attribute `a`; no attribute `a` on'
            r' {parameter}\.get_values\(\)=ENTITY\(b=INT32\)'
        ),
    ),
)

EAGER_ERROR_CASES = (
    (
        '_object_with_tip',
        Person,
        kd.obj(age=30.0, name='Alice'),
        (
            '{decorator}: type mismatch for {parameter}; expected'
            r' type ENTITY\(age=INT32, name=STRING\) with id .*, got'
            ' OBJECT'
            '\n\nIt seems you ran a type checking decorator on a Koda Object.'
            ' Each object stores its own schema, so, in a slice of objects,'
            ' each object may have a different type. If the types of the'
            ' objects in your slice are the same, you should be using entities'
            ' instead of'
            r' objects\( go/koda-common-pitfalls#non-objects-vs-objects \).'
            ' You can create entities by using kd.new/kd.uu instead of'
            ' kd.obj/kd.uuobj.'
        ),
    ),
    (
        '_duck_type_and_object_with_tip',
        type_checking.duck_type(
            a=kd.INT32,
        ),
        kd.obj(a=1),
        (
            '{decorator}: expected {parameter} to have attribute `a`; no'
            ' attribute `a` on {parameter}=OBJECT\n\nIt seems you ran a type'
            ' checking decorator on a Koda Object. Each object stores its own'
            ' schema, so, in a slice of objects, each object may have a'
            ' different type. If the types of the objects in your slice are the'
            ' same, you should be using entities instead of'
            r' objects\( go/koda-common-pitfalls#non-objects-vs-objects \).'
            r' You can'
            ' create entities by using kd.new/kd.uu instead of'
            ' kd.obj/kd.uuobj.'
        ),
    ),
)

OK_CASES = (
    (
        '_primitive',
        kd.INT32,
        ds(1),
    ),
    (
        '_entity_schema',
        Person,
        kd.new(age=30, name='John', schema=Person),
    ),
    (
        '_named_entity_schema',
        Person_named,
        kd.new(age=30, name='John', schema=Person_named),
    ),
    (
        '_uu_entity_schema',
        UuPerson,
        kd.uu(age=30, name='John'),
    ),
    (
        '_list_schema',
        kd.list_schema(kd.INT32),
        kd.list([1, 2, 3]),
    ),
    (
        '_empty_duck_type_matches_string',
        type_checking.duck_type(),
        ds('hello'),
    ),
    (
        '_empty_duck_type_matches_int',
        type_checking.duck_type(),
        ds(0),
    ),
    (
        '_empty_duck_type_matches_entity',
        type_checking.duck_type(),
        kd.new(),
    ),
    (
        '_nested_duck_type',
        type_checking.duck_type(
            a=type_checking.duck_type(),
        ),
        kd.new(a=1),
    ),
    (
        '_schema_nested_in_duck_type',
        type_checking.duck_type(
            a=kd.INT32,
        ),
        kd.new(a=1),
    ),
    (
        '_deeply_nested_duck_type',
        type_checking.duck_type(
            a=type_checking.duck_type(
                aa=kd.INT32,
            ),
        ),
        kd.new(a=kd.new(aa=1)),
    ),
    (
        '_duck_list',
        type_checking.duck_list(type_checking.duck_type(a=kd.INT32)),
        kd.list([kd.uu(a=1), kd.uu(a=1)]),
    ),
    (
        '_duck_dict',
        type_checking.duck_dict(
            key_constraint=type_checking.duck_type(a=kd.INT32),
            value_constraint=type_checking.duck_type(a=kd.INT32),
        ),
        kd.dict({kd.uu(a=1): kd.uu(a=1)}),
    ),
    (
        '_partial_match',
        type_checking.duck_type(
            a=kd.INT32,
            b=kd.INT32,
        ),
        kd.new(a=1, b=2, c=3),
    ),
)


class TypeCheckingTest(parameterized.TestCase):

  @parameterized.named_parameters(*(ERROR_CASES + EAGER_ERROR_CASES))
  def test_check_inputs_error(self, constraint, value, error_message):
    @type_checking.check_inputs(x=constraint)
    def f(x):
      return x

    with self.assertRaisesRegex(
        TypeError,
        error_message.format(
            decorator='kd.check_inputs', parameter=r'(parameter\s)?x'
        ),
    ):
      _ = f(value)

  @parameterized.named_parameters(*ERROR_CASES)
  def test_check_inputs_traced_error(self, constraint, value, error_message):
    @type_checking.check_inputs(x=constraint)
    def f(x):
      return x

    fn = kdf.fn(f)
    with self.assertRaisesRegex(
        ValueError,
        # TODO: Could the .*assertion.* prefix be removed?
        'kd.assertion.with_assertion: '
        + error_message.format(
            decorator='kd.check_inputs', parameter=r'(parameter\s)?x'
        ),
    ):
      _ = fn(value)

  @parameterized.named_parameters(*(ERROR_CASES + EAGER_ERROR_CASES))
  def test_check_output_error(self, constraint, value, error_message):
    @type_checking.check_output(constraint)
    def f(x):
      return x

    with self.assertRaisesRegex(
        TypeError,
        error_message.format(decorator='kd.check_output', parameter='output'),
    ):
      _ = f(value)

  @parameterized.named_parameters(*ERROR_CASES)
  def test_check_output_traced_error(self, constraint, value, error_message):
    @type_checking.check_output(constraint)
    def f(x):
      return x

    fn = kdf.fn(f)
    with self.assertRaisesRegex(
        ValueError,
        # TODO: Could the .*assertion.* prefix be removed?
        'kd.assertion.with_assertion: '
        + error_message.format(decorator='kd.check_output', parameter='output'),
    ):
      _ = fn(value)

  @parameterized.named_parameters(*OK_CASES)
  def test_check_inputs_ok(self, constraint, value):
    @type_checking.check_inputs(x=constraint)
    def f(x):
      return x

    testing.assert_equal(value, f(value))  # Assert does not raise.

  @parameterized.named_parameters(
      ('base_type', kd.BOOLEAN),
      ('no_base_type', None),
  )
  def test_check_inputs_eager_constant_when_tracing(self, base_type):
    @type_checking.check_inputs(x=kd.static_when_tracing(base_type))
    def f(x):
      return x

    testing.assert_equal(ds(True), f(ds(True)))  # Assert does not raise.

  def test_check_inputs_eager_constant_when_tracing_type_error(self):
    @type_checking.check_inputs(x=kd.static_when_tracing(kd.INT32))
    def f(x):
      return x

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter x; expected type INT32,'
        ' got BOOLEAN',
    ):
      _ = f(ds(True))

  @parameterized.named_parameters(*OK_CASES)
  def test_check_inputs_traced_ok(self, constraint, value):
    @type_checking.check_inputs(x=constraint)
    def f(x):
      return x

    fn = kdf.fn(f)
    testing.assert_equal(value, fn(value))  # Assert does not raise.

  @parameterized.named_parameters(*OK_CASES)
  def test_check_output_ok(self, constraint, value):
    @type_checking.check_output(constraint)
    def f(x):
      return x

    testing.assert_equal(value, f(value))  # Assert does not raise.

  @parameterized.named_parameters(*OK_CASES)
  def test_check_output_traced_ok(self, constraint, value):
    @type_checking.check_output(constraint)
    def f(x):
      return x

    fn = kdf.fn(f)
    testing.assert_equal(value, fn(value))  # Assert does not raise.

  def test_check_inputs_default_argument(self):
    @type_checking.check_inputs(x=kd.INT32)
    def f(x=ds([1.0, 2, 3])):
      return x

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter x; expected type INT32,'
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
        'kd.check_inputs: type mismatch for parameter x; expected type INT32,'
        ' got FLOAT32',
    ):
      _ = f(ds([1.0, 2, 3]))

  def test_query_doc_example(self):
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
        'kd.check_inputs: type mismatch for parameter query; expected type'
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
        ' x to be a schema DataItem, a DuckType or ConstantWhenTraced,'
        ' got 0',
    ):

      @type_checking.check_inputs(x=kd.int32(0))
      def _(x):
        return x

  def test_invalid_output_constraint_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_output: invalid constraint: expected constraint for output to'
        ' be a schema DataItem or a DuckType, got 0',
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
        'kd.check_inputs: type mismatch for parameter x; expected type INT32,'
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

  def test_check_inputs_traced_does_not_reserve_keywords(self):
    @type_checking.check_inputs(traced=kd.INT32)
    def f(traced):
      return traced

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.check_inputs: type mismatch for parameter traced; expected type'
        ' INT32, got FLOAT32',
    ):
      _ = f(ds([1.0, 2, 3]))

    fn = kdf.fn(f)

    with self.assertRaisesRegex(
        ValueError,
        # TODO: Could the .*assertion.* prefix be removed?
        'kd.assertion.with_assertion: kd.check_inputs: type mismatch'
        ' for parameter traced; expected type INT32, got FLOAT32',
    ):
      _ = fn(ds([1.0, 2, 3]))

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

  def test_check_inputs_traced_two_inputs(self):
    @type_checking.check_inputs(x=kd.INT32, y=kd.INT32)
    def f(x, y):
      return (x, y)

    fn = kdf.fn(f)

    with self.assertRaisesRegex(
        ValueError,
        'kd.assertion.with_assertion: kd.check_inputs: type mismatch'
        ' for parameter x; expected type INT32, got FLOAT32',
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

  @parameterized.named_parameters(('boolean', kd.BOOLEAN), ('none', None))
  def test_check_inputs_traced_boolean_constant_when_tracing(self, base_type):
    @type_checking.check_inputs(pick_a=kd.static_when_tracing(base_type))
    def choose(pick_a=True):
      if pick_a:
        return ds('a')
      else:
        return ds('b')

    def f():
      return choose(pick_a=True)

    # Assert does not raise.
    _ = kdf.fn(f)

  def test_check_inputs_traced_int_constant_when_tracing(self):
    @type_checking.check_inputs(value=kd.static_when_tracing(kd.INT32))
    def choose(value):
      if value > 0:
        return ds('a')
      else:
        return ds('b')

    def f():
      return choose(value=3)

    # Assert does not raise.
    _ = kdf.fn(f)

  @parameterized.named_parameters(
      ('base_type', kd.BOOLEAN),
      ('no_base_type', None),
  )
  def test_check_inputs_traced_constant_when_tracing_raises(self, base_type):
    @type_checking.check_inputs(pick_a=kd.static_when_tracing(base_type))
    def choose(pick_a=True):
      if pick_a:
        return ds('a')
      else:
        return ds('b')

    def f():
      return choose(kd.bool(True) == kd.bool(True))

    with self.assertRaisesRegex(
        TypeError,
        'argument "pick_a" must be resolved statically during tracing and not'
        ' depend on the inputs',
    ):
      _ = kdf.fn(f)

  def test_check_inputs_traced_int_constant_when_tracing_raises(self):
    @type_checking.check_inputs(value=kd.static_when_tracing(kd.INT32))
    def choose(value):
      if value > 0:
        return ds('a')
      else:
        return ds('b')

    def f():
      return choose(kd.int32(3) + kd.int32(3))

    with self.assertRaisesRegex(
        TypeError,
        'argument "value" must be resolved statically during tracing and not'
        ' depend on the inputs',
    ):
      _ = kdf.fn(f)

  def test_disable_traced_type_checking_manager(self):
    @type_checking.check_inputs(x=kd.INT32)
    @type_checking.check_output(kd.INT32)
    def f(x):
      return x

    with type_checking.disable_traced_type_checking():
      # Check that a nested context manager exiting does not reset the flag.
      with type_checking.disable_traced_type_checking():
        pass
      fn = kd.fn(f)

    _ = fn(ds([1.0]))  # Does not raise. Type checking was disabled.

    fn = kd.fn(f)
    with self.assertRaises(ValueError):
      _ = fn(ds([1.0]))  # Raises again. Type checking was re-enabled.


if __name__ == '__main__':
  absltest.main()
