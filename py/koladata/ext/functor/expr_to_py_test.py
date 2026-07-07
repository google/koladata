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

import ast
import inspect
import math
import textwrap

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd

from koladata.ext.functor import expr_to_py


class QValueToPyTest(parameterized.TestCase):

  @parameterized.parameters(
      # None
      (kd.item(None), 'None'),  # pyrefly: ignore[missing-attribute]
      # Ints
      (kd.item(42), '42'),  # pyrefly: ignore[missing-attribute]
      (kd.int32(None), 'kd.int32(None)'),  # pyrefly: ignore[missing-attribute]
      (kd.int64(42), 'kd.int64(42)'),  # pyrefly: ignore[missing-attribute]
      # Floats
      (kd.item(1.0), '1.0'),  # pyrefly: ignore[missing-attribute]
      (kd.float32(3.14), '3.14'),  # pyrefly: ignore[missing-attribute]
      (kd.float32(-0.0), '-0.0'),  # pyrefly: ignore[missing-attribute]
      (kd.float32(3.14), '3.14'),  # pyrefly: ignore[missing-attribute]
      (kd.float32(math.inf), "float('inf')"),  # pyrefly: ignore[missing-attribute]
      (kd.float32(-math.inf), "float('-inf')"),  # pyrefly: ignore[missing-attribute]
      (kd.float32(math.nan), "float('nan')"),  # pyrefly: ignore[missing-attribute]
      (kd.float64(3.14), 'kd.float64(3.14)'),  # pyrefly: ignore[missing-attribute]
      (kd.float64(-0.0), 'kd.float64(-0.0)'),  # pyrefly: ignore[missing-attribute]
      (kd.float64(math.inf), "kd.float64(float('inf'))"),  # pyrefly: ignore[missing-attribute]
      (kd.float64(-math.inf), "kd.float64(float('-inf'))"),  # pyrefly: ignore[missing-attribute]
      (kd.float64(math.nan), "kd.float64(float('nan'))"),  # pyrefly: ignore[missing-attribute]
      # Strings
      (kd.item('hello'), "'hello'"),  # pyrefly: ignore[missing-attribute]
      (kd.str('hello'), "'hello'"),  # pyrefly: ignore[missing-attribute]
      (kd.str(None), 'kd.str(None)'),  # pyrefly: ignore[missing-attribute]
      # Bytes
      (kd.item(b'hello'), "b'hello'"),  # pyrefly: ignore[missing-attribute]
      # Booleans
      (kd.item(True), 'True'),  # pyrefly: ignore[missing-attribute]
      (kd.bool(False), 'False'),  # pyrefly: ignore[missing-attribute]
      (kd.bool(None), 'kd.bool(None)'),  # pyrefly: ignore[missing-attribute]
      # Masks
      (kd.present, 'kd.present'),
      (kd.missing, 'kd.missing'),
      # ExprQuote
      (kd.expr_quote(None), 'kd.expr_quote(None)'),  # pyrefly: ignore[missing-attribute]
      # Multi-dimensional slices
      (kd.slice([1, 2, 3]), 'kd.slice([1, 2, 3])'),  # pyrefly: ignore[missing-attribute]
      (kd.slice([[None, None], [None]]), 'kd.slice([[None, None], [None]])'),  # pyrefly: ignore[missing-attribute]
      (kd.bytes([[None, None], [None]]), 'kd.bytes([[None, None], [None]])'),  # pyrefly: ignore[missing-attribute]
      (
          kd.float64([[1.0, 2.71], [3.14, None]]),  # pyrefly: ignore[missing-attribute]
          'kd.float64([[1.0, 2.71], [3.14, None]])',
      ),
      (
          kd.float64([[1.0, math.nan], [3.14, None, -math.inf]]),  # pyrefly: ignore[missing-attribute]
          "kd.float64([[1.0, float('nan')], [3.14, None, float('-inf')]])",
      ),
      (
          kd.slice([[1.0, math.nan], [2.0, None, -math.inf]]),  # pyrefly: ignore[missing-attribute]
          "kd.slice([[1.0, float('nan')], [2.0, None, float('-inf')]])",
      ),
      (
          kd.slice([[kd.present, kd.missing], [kd.missing, kd.present]]),  # pyrefly: ignore[missing-attribute]
          'kd.slice([[kd.present, kd.missing], [kd.missing, kd.present]])',
      ),
      # Schema constants
      (kd.INT32, 'kd.INT32'),
      (kd.NONE, 'kd.NONE'),
      (kd.OBJECT, 'kd.OBJECT'),
      (kd.slice([kd.INT32, kd.FLOAT64]), 'kd.slice([kd.INT32, kd.FLOAT64])'),  # pyrefly: ignore[missing-attribute]
      (
          kd.slice([[kd.INT32, kd.FLOAT64], [kd.STRING]]),  # pyrefly: ignore[missing-attribute]
          'kd.slice([[kd.INT32, kd.FLOAT64], [kd.STRING]])',
      ),
      (
          kd.slice([  # pyrefly: ignore[missing-attribute]
              kd.INT64,
              kd.FLOAT32,
              kd.BOOLEAN,
              kd.BYTES,
              kd.EXPR,
              kd.MASK,
              kd.SCHEMA,
              kd.ITEMID,
          ]),
          (
              'kd.slice([kd.INT64, kd.FLOAT32, kd.BOOLEAN, kd.BYTES, kd.EXPR,'
              ' kd.MASK, kd.SCHEMA, kd.ITEMID])'
          ),
      ),
      # kd.obj on primitives
      (kd.obj(42), 'kd.obj(42)'),  # pyrefly: ignore[missing-attribute]
      (kd.obj(1.5), 'kd.obj(1.5)'),  # pyrefly: ignore[missing-attribute]
      (kd.obj(float('inf')), "kd.obj(float('inf'))"),  # pyrefly: ignore[missing-attribute]
      (kd.obj(kd.float64(float('-inf'))), "kd.obj(kd.float64(float('-inf')))"),  # pyrefly: ignore[missing-attribute]
      (kd.obj(float('nan')), "kd.obj(float('nan'))"),  # pyrefly: ignore[missing-attribute]
      (kd.obj('hello'), "kd.obj('hello')"),  # pyrefly: ignore[missing-attribute]
      (kd.obj(b'hello'), "kd.obj(b'hello')"),  # pyrefly: ignore[missing-attribute]
      (kd.obj(True), 'kd.obj(True)'),  # pyrefly: ignore[missing-attribute]
      (kd.obj(kd.present), 'kd.obj(kd.present)'),  # pyrefly: ignore[missing-attribute]
      (kd.obj(None), 'kd.obj(None)'),  # pyrefly: ignore[missing-attribute]
      (
          kd.slice([1, 2, 3], schema=kd.OBJECT),  # pyrefly: ignore[missing-attribute]
          'kd.slice([1, 2, 3], schema=kd.OBJECT)',
      ),
      (
          kd.slice([1.0, float('nan')], schema=kd.OBJECT),  # pyrefly: ignore[missing-attribute]
          "kd.slice([1.0, float('nan')], schema=kd.OBJECT)",
      ),
      (
          kd.slice([kd.present, kd.missing], schema=kd.OBJECT),  # pyrefly: ignore[missing-attribute]
          'kd.slice([kd.present, None], schema=kd.OBJECT)',
      ),
      (
          kd.slice([[1, 'str'], [1.5]]),  # pyrefly: ignore[missing-attribute]
          'kd.slice([[1, \'str\'], [1.5]], schema=kd.OBJECT)',
      ),
      (
          kd.slice([[1, 2], [1.5]], schema=kd.OBJECT),  # pyrefly: ignore[missing-attribute]
          'kd.slice([[1, 2], [1.5]], schema=kd.OBJECT)',
      ),
      (
          kd.slice([kd.present, kd.INT32], schema=kd.OBJECT),  # pyrefly: ignore[missing-attribute]
          'kd.slice([kd.present, kd.INT32], schema=kd.OBJECT)',
      ),
      (
          kd.slice([[None, kd.present], [float('inf')]]),  # pyrefly: ignore[missing-attribute]
          "kd.slice([[None, kd.present], [float('inf')]], "
          + 'schema=kd.OBJECT)',
      ),
      (
          kd.slice([[None, kd.present], [kd.float64(3.14)]], schema=kd.OBJECT),  # pyrefly: ignore[missing-attribute]
          'kd.slice([[None, kd.present], [kd.float64(3.14)]], '
          + 'schema=kd.OBJECT)',
      ),
      # Arolla unspecified
      (arolla.unspecified(), 'arolla.unspecified()'),
  )
  def test_qvalue_to_py(self, qvalue, expected_code):
    ast_node = expr_to_py.qvalue_to_py(qvalue)
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_non_primitive_data_slice_error(self):
    with self.assertRaisesRegex(
        ValueError, 'non-primitive DataSlice literals are not yet supported'
    ):
      expr_to_py.qvalue_to_py(kd.obj())  # pyrefly: ignore[missing-attribute]

  def test_unsupported_qvalue_type_error(self):
    with self.assertRaisesRegex(
        ValueError, 'DataBag cannot be converted to a Python AST expression'
    ):
      expr_to_py.qvalue_to_py(kd.bag())  # pyrefly: ignore[missing-attribute]


class ExprToPyTest(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(
          testcase_name='add',
          expr=kd.I.x + kd.I.y,
          expected_return='return x + y',
      ),
      dict(
          testcase_name='subtract',
          expr=kd.I.x - kd.I.y,
          expected_return='return x - y',
      ),
      dict(
          testcase_name='multiply',
          expr=kd.I.x * kd.I.y,
          expected_return='return x * y',
      ),
      dict(
          testcase_name='divide',
          expr=kd.I.x / kd.I.y,
          expected_return='return x / y',
      ),
      dict(
          testcase_name='floordiv',
          expr=kd.I.x // kd.I.y,
          expected_return='return x // y',
      ),
      dict(
          testcase_name='mod',
          expr=kd.I.x % kd.I.y,
          expected_return='return x % y',
      ),
      dict(
          testcase_name='pow',
          expr=kd.I.x ** kd.I.y,
          expected_return='return x ** y',
      ),
      dict(
          testcase_name='apply_mask',
          expr=kd.I.x & kd.I.y,  # kd.masking.apply_mask
          expected_return='return x & y',
      ),
      dict(
          testcase_name='coalesce',
          # Test decay_registered_operator.
          expr=kd.lazy.coalesce(kd.I.x, kd.I.y),
          expected_return='return x | y',
      ),
      dict(
          testcase_name='xor',
          expr=kd.I.x ^ kd.I.y,  # kd.masking.xor
          expected_return='return x ^ y',
      ),
      dict(
          testcase_name='equal',
          expr=kd.I.x == kd.I.y,  # kd.comparison.equal
          expected_return='return x == y',
      ),
      dict(
          testcase_name='not_equal',
          expr=kd.I.x != kd.I.y,  # kd.comparison.not_equal
          expected_return='return x != y',
      ),
      dict(
          testcase_name='greater',
          expr=kd.I.x > kd.I.y,
          expected_return='return x > y',
      ),
      dict(
          testcase_name='greater_equal',
          expr=kd.I.x >= kd.I.y,
          expected_return='return x >= y',
      ),
      dict(
          testcase_name='less',
          expr=kd.I.x < kd.I.y,
          expected_return='return x < y',
      ),
      dict(
          testcase_name='less_equal',
          expr=kd.I.x <= kd.I.y,
          expected_return='return x <= y',
      ),
  )
  def test_python_binary_operators(self, expr, expected_return):
    sig = inspect.signature(lambda x, y: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent(f"""\
      def my_func(x, y):
          {expected_return}
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  @parameterized.named_parameters(
      dict(
          testcase_name='has_not',
          expr=~kd.I.x,  # kd.masking.has_not
          expected_return='return ~x',
      ),
      dict(
          testcase_name='neg',
          expr=-kd.I.x,  # kd.math.neg
          expected_return='return -x',
      ),
      dict(
          testcase_name='pos',
          expr=+kd.I.x,  # kd.math.pos
          expected_return='return +x',
      ),
  )
  def test_python_unary_operators(self, expr, expected_return):
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent(f"""\
      def my_func(x):
          {expected_return}
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  @parameterized.named_parameters(
      dict(
          testcase_name='slice_empty',
          expr=kd.I.x[:],
          expected_return='return x[:]',
      ),
      dict(
          testcase_name='slice_bounds',
          expr=kd.I.x[1:2],
          expected_return='return x[1:2]',
      ),
      dict(
          testcase_name='slice_bounds_step',
          expr=kd.I.x[1:2:3],
          expected_return='return x[1:2:3]',
      ),
      dict(
          testcase_name='slice_bounds_no_step',
          expr=kd.I.x[1:2:],
          expected_return='return x[1:2]',
      ),
      dict(
          testcase_name='slice_no_bounds_step',
          expr=kd.I.x[:2:3],
          expected_return='return x[:2:3]',
      ),
      dict(
          testcase_name='slice_no_bounds_no_step',
          expr=kd.I.x[:2:],
          expected_return='return x[:2]',
      ),
      dict(
          testcase_name='slice_no_bounds_no_stop_step',
          expr=kd.I.x[::2],
          expected_return='return x[::2]',
      ),
      dict(
          testcase_name='expr_subscript',
          expr=kd.I.x[kd.I.x.index + 1],
          expected_return=(
              "return x[kd.get_attr(x, 'index', arolla.unspecified()) + 1]"
          ),
      ),
      dict(
          testcase_name='expr_slice_subscript',
          expr=kd.I.x[kd.lazy.index(kd.I.x) : kd.lazy.index(kd.I.x) + 1 : None],
          # TODO: Support kd.tuples.slice to be rendered as
          # ast.Slice.
          expected_return=(
              '_1 = kd.index(x, -1)\n'
              'return x[kd.tuples.slice(_1, _1 + 1, arolla.unspecified())]'
          ),
      ),
  )
  def test_get_item_operator(self, expr, expected_return):
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = 'def my_func(x):\n' + textwrap.indent(
        expected_return, '    '
    )
    self.assertEqual(ast.unparse(ast_node), expected_code)

  @parameterized.named_parameters(
      dict(
          testcase_name='single_positional',
          fn=lambda x: None,
          expected_code='def f(x):\n    return x',
      ),
      dict(
          testcase_name='multiple_positional',
          fn=lambda x, y, z: None,
          expected_code='def f(x, y, z):\n    return x',
      ),
      dict(
          testcase_name='default_value',
          fn=lambda x, y=2: None,
          expected_code='def f(x, y=2):\n    return x',
      ),
      dict(
          testcase_name='multiple_defaults',
          fn=lambda x, y=2, z=False, w=b'x', v='x': None,
          expected_code=(
              'def f(x, y=2, z=False, w=b\'x\', v=\'x\'):\n    return x'
          ),
      ),
      dict(
          testcase_name='keyword_only',
          fn=lambda x, *, y: None,
          expected_code='def f(x, *, y):\n    return x',
      ),
      dict(
          testcase_name='keyword_only_with_default',
          fn=lambda x, *, y=3: None,
          expected_code='def f(x, *, y=3):\n    return x',
      ),
      dict(
          testcase_name='pos_kw_with_default_keyword_only',
          fn=lambda x, y=3, *, z: None,
          expected_code='def f(x, y=3, *, z):\n    return x',
      ),
      dict(
          testcase_name='mixed',
          fn=lambda x, b=2, *, c=None: None,
          expected_code='def f(x, b=2, *, c=None):\n    return x',
      ),
      dict(
          testcase_name='positional_only',
          fn=lambda x, y, /: None,
          expected_code='def f(x, y, /):\n    return x',
      ),
      dict(
          testcase_name='mixed_positional',
          fn=lambda x, /, y: None,
          expected_code='def f(x, /, y):\n    return x',
      ),
      dict(
          testcase_name='data_item_default',
          fn=lambda x, y=kd.item(42): None,
          expected_code='def f(x, y=42):\n    return x',
      ),
      dict(
          testcase_name='data_item_float64_default',
          fn=lambda x, y=kd.float64(42.1): None,
          expected_code='def f(x, y=kd.float64(42.1)):\n    return x',
      ),
      dict(
          testcase_name='slice_and_unspecified_default',
          fn=lambda x=arolla.unspecified(), y=kd.slice([1, 2, 3]): None,
          expected_code=(
              'def f(x=arolla.unspecified(), y=kd.slice([1, 2, 3])):\n'
              + '    return x'
          ),
      ),
      dict(
          testcase_name='positional_only_with_default',
          fn=lambda x=kd.float64(1.0), /, *, y=2, z=kd.slice([1, 2, 3]): None,
          expected_code=(
              'def f(x=kd.float64(1.0), /, *, y=2, z=kd.slice([1, 2, 3])):\n'
              + '    return x'
          ),
      ),
  )
  def test_signature(self, fn, expected_code):
    expr = kd.I.x
    sig = inspect.signature(fn)
    ast_node = expr_to_py.expr_to_py(expr, 'f', sig, set())
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_signature_with_unsupported_default(self):
    sig = inspect.signature(lambda x=kd.new(): None)
    expr = kd.I.x
    with self.assertRaises(ValueError) as e:
      _ = expr_to_py.expr_to_py(expr, 'f', sig, set())
    self.assertIn('unsupported default value for x', e.exception.__notes__)
    self.assertIn('[f] unsupported signature', e.exception.__notes__)

  def test_local_variables(self):
    x = kd.I.x + kd.I.y
    expr = x * x
    sig = inspect.signature(lambda x, y: None)
    expected_code = textwrap.dedent("""\
      def my_func(x, y):
          _1 = x + y
          return _1 * _1
    """).strip()
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_local_variables_chained(self):
    a = kd.I.x + kd.I.x
    for _ in range(6):
      a = a + a
    expr = a
    sig = inspect.signature(lambda x: None)
    expected_code = textwrap.dedent("""\
      def my_func(x):
          _1 = x + x
          _2 = _1 + _1
          _3 = _2 + _2
          _4 = _3 + _3
          _5 = _4 + _4
          _6 = _5 + _5
          return _6 + _6
    """).strip()
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_literal_in_expr(self):
    item = kd.int64(2)  # pyrefly: ignore[missing-attribute]
    expr = kd.lazy.math.add(kd.I.x, kd.lazy.math.multiply(kd.I.y, item))
    sig = inspect.signature(lambda x, y: None)
    expected_code = textwrap.dedent("""\
      def my_func(x, y):
          return x + y * kd.int64(2)
    """).strip()
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_list_explode_literal(self):
    def f(x):
      return x + kd.slice([[1, 2], [3]])  # pyrefly: ignore[missing-attribute]

    fn = kd.fn(f)
    assert fn.has_attr('_aux_0')
    expr = kd.expr.sub(
        # Stripping the annotation.source_location, which is done anyway
        # "upstream".
        kd.expr.unpack_expr(fn.returns).node_deps[0],
        (kd.V._aux_0, fn.get_attr('_aux_0')),
    )
    sig = inspect.signature(lambda x: None)

    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent("""\
      def my_func(x):
          return x + kd.slice([[1, 2], [3]])
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_list_explode_non_multidim_literal(self):
    sig = inspect.signature(lambda x: None)
    with self.assertRaisesRegex(
        ValueError,
        'non-primitive DataSlice literals are not yet supported',
    ) as e:
      _ = expr_to_py.expr_to_py(
          kd.lazy.explode(kd.list([[1, 2], [3]]), 1), 'my_func', sig, {'sub_fn'}  # pyrefly: ignore[missing-attribute]
      )
    self.assertIn('[my_func] unsupported literal', e.exception.__notes__)

  def test_list_explode_non_list_literal(self):
    expr = kd.lazy.lists.explode(kd.I.x, 1)
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent("""\
      def my_func(x):
          return kd.lists.explode(x, 1)
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_arolla_unspecified_literal_rendered(self):
    expr = kd.lazy.tuples.slice(kd.I.t, arolla.unspecified(), kd.I.stop)
    sig = inspect.signature(lambda t, stop: None)

    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent("""\
      def my_func(t, stop):
          return kd.tuples.slice(t, arolla.unspecified(), stop)
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_unsupported_literal_error(self):
    expr = kd.expr.literal(kd.bag())  # pyrefly: ignore[missing-attribute]
    sig = inspect.signature(lambda x: None)

    with self.assertRaisesRegex(
        ValueError,
        'value of type DataBag cannot be converted to a Python AST expression',
    ) as e:
      _ = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    self.assertIn('[my_func] unsupported literal', e.exception.__notes__)

  def test_arolla_leaf_error(self):
    expr = arolla.L.x
    sig = inspect.signature(lambda: None)
    with self.assertRaisesRegex(
        ValueError, r'\[my_func\] Arolla leaves are not supported'
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  def test_arolla_literal_error(self):
    expr = arolla.literal(arolla.int32(1))
    sig = inspect.signature(lambda: None)
    with self.assertRaisesRegex(
        ValueError, r'\[my_func\] Arolla literals are not supported'
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  def test_invalid_input_name_error(self):
    expr = kd.I['!'] + kd.I.y
    sig = inspect.signature(lambda x, y: None)
    with self.assertRaisesRegex(ValueError, r'\[my_func\] invalid input name'):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  def test_input_not_in_signature_error(self):
    expr = kd.I.x + kd.I.y
    sig = inspect.signature(lambda x: None)
    with self.assertRaisesRegex(
        ValueError, r'\[my_func\] input y not in signature parameters'
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  @parameterized.named_parameters(
      dict(
          testcase_name='slice_2d',
          expr=kd.lazy.slice([[kd.I.x, kd.I.x], [kd.I.y]]),
          sig=inspect.signature(lambda x, y: None),
          expected_code=textwrap.dedent("""\
            def my_func(x, y):
                return kd.slice([[x, x], [y]])
          """).strip(),
      ),
      dict(
          testcase_name='slice_1d',
          expr=kd.lazy.slice([kd.I.x, kd.I.y, kd.I.z]),
          sig=inspect.signature(lambda x, y, z: None),
          expected_code=textwrap.dedent("""\
            def my_func(x, y, z):
                return kd.slice([x, y, z])
          """).strip(),
      ),
      dict(
          testcase_name='slice_scalar',
          expr=kd.lazy.slice(kd.I.x),
          sig=inspect.signature(lambda x: None),
          expected_code=textwrap.dedent("""\
            def my_func(x):
                return kd.slice(x)
          """).strip(),
      ),
      dict(
          testcase_name='slice_mixed',
          expr=kd.lazy.slice([[1, kd.I.x], [kd.I.y]]),
          sig=inspect.signature(lambda x, y: None),
          expected_code=textwrap.dedent("""\
            def my_func(x, y):
                return kd.slice([[1, x], [y]])
          """).strip(),
      ),
      dict(
          testcase_name='item',
          expr=kd.lazy.item(kd.I.x),
          sig=inspect.signature(lambda x: None),
          expected_code=textwrap.dedent("""\
            def my_func(x):
                return kd.item(x)
          """).strip(),
      ),
      dict(
          testcase_name='float32_scalar',
          expr=kd.lazy.float32(kd.I.x),
          sig=inspect.signature(lambda x: None),
          expected_code=textwrap.dedent("""\
            def my_func(x):
                return kd.float32(x)
          """).strip(),
      ),
      dict(
          testcase_name='float32_multidim',
          expr=kd.lazy.float32([[kd.I.x, kd.I.y], [kd.I.z]]),
          sig=inspect.signature(lambda x, y, z: None),
          expected_code=textwrap.dedent("""\
            def my_func(x, y, z):
                return kd.float32([[x, y], [z]])
          """).strip(),
      ),
      dict(
          testcase_name='int64_multidim',
          expr=kd.lazy.int64([[kd.I.x, kd.I.y], [kd.I.z]]),
          sig=inspect.signature(lambda x, y, z: None),
          expected_code=textwrap.dedent("""\
            def my_func(x, y, z):
                return kd.int64([[x, y], [z]])
          """).strip(),
      ),
      dict(
          testcase_name='slice_with_explicit_schema_scalar',
          expr=kd.lazy.slice(kd.I.x, kd.INT32),
          sig=inspect.signature(lambda x: None),
          expected_code=textwrap.dedent("""\
            def my_func(x):
                return kd.slice(x, schema=kd.INT32)
          """).strip(),
      ),
      dict(
          testcase_name='slice_with_explicit_schema_multidim',
          expr=kd.lazy.slice([[kd.I.x, kd.I.y], [kd.I.z]], kd.FLOAT64),
          sig=inspect.signature(lambda x, y, z: None),
          expected_code=textwrap.dedent("""\
            def my_func(x, y, z):
                return kd.slice([[x, y], [z]], schema=kd.FLOAT64)
          """).strip(),
      ),
      dict(
          testcase_name='slice_with_items_with_explicit_schema',
          expr=kd.lazy.slice(
              [kd.lazy.slices.item(kd.I.x, kd.FLOAT64), kd.I.y], kd.INT32
          ),
          sig=inspect.signature(lambda x, y: None),
          # NOTE: _1 is generated because in the original expression this item
          # is used both in with_assertion and the assert expression itself.
          expected_code=textwrap.dedent("""\
            def my_func(x, y):
                _1 = kd.item(x, schema=kd.FLOAT64)
                return kd.slice([_1, y], schema=kd.INT32)
          """).strip(),
      ),
      dict(
          # NOTE: `x` is first downcasted and then upcasted with intentional
          # loss of precision.
          testcase_name='float64_with_items_with_explicit_schema',
          expr=kd.lazy.float64([kd.lazy.slices.item(kd.I.x, kd.INT32), kd.I.y]),
          sig=inspect.signature(lambda x, y: None),
          # NOTE: _1 is generated because in the original expression this item
          # is used both in with_assertion and the assert expression itself.
          expected_code=textwrap.dedent("""\
            def my_func(x, y):
                _1 = kd.item(x, schema=kd.INT32)
                return kd.float64([_1, y])
          """).strip(),
      ),
      dict(
          testcase_name='slice_3d',
          expr=kd.lazy.slice([[[kd.I.x, kd.I.y]], [[kd.I.z]]]),
          sig=inspect.signature(lambda x, y, z: None),
          expected_code=textwrap.dedent("""\
            def my_func(x, y, z):
                return kd.slice([[[x, y]], [[z]]])
          """).strip(),
      ),
  )
  def test_traced_slices_and_items(self, expr, sig, expected_code):
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_var_args_error(self):
    expr = kd.I.x
    sig = inspect.signature(lambda x, *args: None)
    with self.assertRaisesRegex(
        ValueError, r'\[my_func\] \*args is not supported'
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  def test_var_kwargs_error(self):
    expr = kd.I.x
    sig = inspect.signature(lambda x, **kwargs: None)
    with self.assertRaisesRegex(
        ValueError, r'\[my_func\] \*\*kwargs is not supported'
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  def test_non_deterministic_ops_not_supported(self):
    expr = kd.lazy.list()
    sig = inspect.signature(lambda x: None)
    with self.assertRaisesRegex(
        ValueError,
        r'\[my_func\] non-deterministic operators are not supported',
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  # TODO: Add support for stripping trailing args (where the
  # argument is equal to the default value).
  def test_arolla_unspecified_trailing_args_not_stripped(self):
    expr = kd.lazy.tuples.slice(kd.I.t, kd.I.start)
    sig = inspect.signature(lambda t, start: None)

    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent("""\
      def my_func(t, start):
          return kd.tuples.slice(t, start, arolla.unspecified())
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_keyword_only_args(self):
    expr = kd.lazy.ids.deep_uuid(kd.I.x, seed='my_seed')
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent("""\
      def my_func(x):
          return kd.ids.deep_uuid(x, arolla.unspecified(), seed='my_seed')
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_varargs_operator_unified_aux_policy(self):
    expr = kd.I.x.reshape(kd.lazy.shapes.new(2, 3, 1))
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent("""\
      def my_func(x):
          return kd.reshape(x, kd.shapes.new(2, 3, 1))
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_varargs_operator_classic_aux_policy(self):
    expr = kd.lazy.strings.printf('hello %s', kd.I.x)
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent("""\
      def my_func(x):
          return kd.strings.printf('hello %s', x)
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_kwargs_operator(self):
    expr = kd.lazy.uu(x=kd.I.x, a=2, b=3)
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent("""\
      def my_func(x):
          return kd.uu('', schema=arolla.unspecified(), overwrite_schema=False, x=x, a=2, b=3)
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_kwargs_operator_with_positional_and_keyword_args(self):
    expr = kd.lazy.with_attrs(kd.I.x, a=kd.I.y, b=42)
    sig = inspect.signature(lambda x, y: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, set())
    expected_code = textwrap.dedent("""\
      def my_func(x, y):
          return kd.with_attrs(x, overwrite_schema=False, a=y, b=42)
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_unsupported_aux_policy_error(self):
    expr = kd.lazy.lists.uu(kd.I.x)
    sig = inspect.signature(lambda x: None)
    with self.assertRaisesRegex(
        ValueError,
        r'\[my_func\] operator with unsupported aux policy',
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  def test_non_registered_operator_error(self):
    lambda_op = arolla.LambdaOperator('x, y', arolla.P.x + arolla.P.y)
    expr = lambda_op(kd.I.x, kd.I.y)
    sig = inspect.signature(lambda x, y: None)
    with self.assertRaisesRegex(
        ValueError,
        r'\[my_func\] uses non-registered operator',
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  def test_nested_function_call_with_literal_args(self):
    expr = kd.expr.unpack_expr(
        kd.fn(kd.V.sub_fn(42, kd.float64(3.14)), sub_fn=None).returns  # pyrefly: ignore[missing-attribute]
    )
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, {'sub_fn'})

    expected_code = textwrap.dedent("""\
      def my_func(x):
          return sub_fn(42, kd.float64(3.14))
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_nested_function_call_with_complex_args(self):
    expr = kd.expr.unpack_expr(
        kd.fn(kd.V.sub_fn(kd.I.x, kd.lazy.math.log(kd.I.y), 42), sub_fn=None)
        .returns
    )
    sig = inspect.signature(lambda x, y: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, {'sub_fn'})

    expected_code = textwrap.dedent("""\
      def my_func(x, y):
          return sub_fn(x, kd.math.log(y), 42)
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_nested_function_call_with_literal_kwargs(self):
    expr = kd.expr.unpack_expr(
        kd.fn(kd.V.sub_fn(kd.I.x, y=42), sub_fn=None).returns
    )
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, {'sub_fn'})

    expected_code = textwrap.dedent("""\
      def my_func(x):
          return sub_fn(x, y=42)
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_nested_function_call_with_complex_kwargs(self):
    expr = kd.expr.unpack_expr(
        kd.fn(kd.V.sub_fn(x=kd.lazy.math.log(kd.I.x), y=42), sub_fn=None)
        .returns
    )
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, {'sub_fn'})

    expected_code = textwrap.dedent("""\
      def my_func(x):
          return sub_fn(x=kd.math.log(x), y=42)
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_multiple_nested_function_calls(self):
    sub_expr = kd.V.sub_fn(kd.I.x, y=42)
    expr = kd.expr.unpack_expr(kd.fn(sub_expr + sub_expr, sub_fn=None).returns)
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, {'sub_fn'})

    expected_code = textwrap.dedent("""\
      def my_func(x):
          _1 = sub_fn(x, y=42)
          return _1 + _1
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_nested_function_call_via_kd_functor_call(self):
    expr = kd.expr.unpack_expr(
        kd.fn(kd.lazy.functor.call(kd.V.sub_fn, kd.I.x, y=42), sub_fn=None)
        .returns
    )
    sig = inspect.signature(lambda x: None)
    ast_node = expr_to_py.expr_to_py(expr, 'my_func', sig, {'sub_fn'})

    expected_code = textwrap.dedent("""\
      def my_func(x):
          return sub_fn(x, y=42)
    """).strip()
    self.assertEqual(ast.unparse(ast_node), expected_code)

  def test_calls_non_variable_error(self):
    literal_fn = kd.fn(lambda x: x)
    expr = kd.lazy.functor.call(kd.expr.literal(literal_fn), kd.I.x)
    sig = inspect.signature(lambda x: None)

    with self.assertRaisesRegex(
        ValueError, r'\[my_func\] only functor variables can be called'
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, set())

  def test_calls_undefined_nested_function_error(self):
    expr = kd.lazy.call(kd.V.sub_fn, kd.I.x)
    sig = inspect.signature(lambda x: None)

    with self.assertRaisesRegex(
        ValueError, r'\[my_func\] sub_fn is not defined'
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, {'x'})

  def test_invalid_variable_name_error(self):
    expr = kd.V['!']
    sig = inspect.signature(lambda: None)
    with self.assertRaisesRegex(
        ValueError, r'\[my_func\] expected a valid identifier, got'
    ):
      expr_to_py.expr_to_py(expr, 'my_func', sig, {'!'})


if __name__ == '__main__':
  absltest.main()
