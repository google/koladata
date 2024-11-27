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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.operators import kde_operators
from koladata.operators import op_repr
from koladata.types import data_slice
from koladata.types import py_boxing

M = arolla.M
L = arolla.L
I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


def register(op, repr_fn):
  arolla.abc.register_op_repr_fn_by_registration_name(op.display_name, repr_fn)


def clear(op):
  # Falls back to the default repr fn.
  arolla.abc.register_op_repr_fn_by_registration_name(
      op.display_name, lambda *args: None
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('test.unary_op')
def unary_op(x):
  return x


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('test.binary_op')
def binary_op(x, y):
  del y
  return x


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'test.full_signature_op',
    experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
)
def full_signature_op(
    a=py_boxing.positional_only(),
    x=py_boxing.positional_or_keyword(),
    args=py_boxing.var_positional(),
    y=py_boxing.keyword_only(),
    z=py_boxing.keyword_only(),
    kwargs=py_boxing.var_keyword(),
):
  return a, x, args, y, z, kwargs


# NOTE: These tests depend on the existing behavior of corresponding arolla
# operators in order to not have to test the relationship between operators.
class OpReprTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    clear(unary_op)
    clear(binary_op)
    clear(full_signature_op)

  def test_default_op_repr(self):
    self.assertEqual(repr(unary_op(1)), 'M.test.unary_op(1)')
    register(unary_op, op_repr.default_op_repr)
    self.assertEqual(repr(unary_op(1)), 'test.unary_op(1)')

  @parameterized.parameters(
      (unary_op(L.x), '+L.x'),
      (M.math.pos(unary_op(L.x)), '+(+L.x)'),
      (unary_op(M.math.pos(L.x)), '+(+L.x)'),
  )
  def test_pos_repr(self, expr, expected_repr):
    register(unary_op, op_repr.pos_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (unary_op(L.x), '-L.x'),
      (M.math.neg(unary_op(L.x)), '-(-L.x)'),
      (unary_op(M.math.neg(L.x)), '-(-L.x)'),
  )
  def test_neg_repr(self, expr, expected_repr):
    register(unary_op, op_repr.neg_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (unary_op(L.x), '~L.x'),
      (M.core.presence_not(unary_op(L.x)), '~(~L.x)'),
      (unary_op(M.core.presence_not(L.x)), '~(~L.x)'),
  )
  def test_not_repr(self, expr, expected_repr):
    register(unary_op, op_repr.not_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x ** L.y'),
      (M.math.pow(binary_op(L.x, L.y), L.z), '(L.x ** L.y) ** L.z'),
      (M.math.pow(L.x, binary_op(L.y, L.z)), 'L.x ** L.y ** L.z'),
      (binary_op(M.math.pow(L.x, L.y), L.z), '(L.x ** L.y) ** L.z'),
      (binary_op(L.x, M.math.pow(L.y, L.z)), 'L.x ** L.y ** L.z'),
  )
  def test_pow_repr(self, expr, expected_repr):
    register(binary_op, op_repr.pow_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x * L.y'),
      (M.math.multiply(binary_op(L.x, L.y), L.z), 'L.x * L.y * L.z'),
      (M.math.multiply(L.x, binary_op(L.y, L.z)), 'L.x * (L.y * L.z)'),
      (binary_op(M.math.multiply(L.x, L.y), L.z), 'L.x * L.y * L.z'),
      (binary_op(L.x, M.math.multiply(L.y, L.z)), 'L.x * (L.y * L.z)'),
  )
  def test_multiply_repr(self, expr, expected_repr):
    register(binary_op, op_repr.multiply_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x / L.y'),
      (M.math.divide(binary_op(L.x, L.y), L.z), 'L.x / L.y / L.z'),
      (M.math.divide(L.x, binary_op(L.y, L.z)), 'L.x / (L.y / L.z)'),
      (binary_op(M.math.divide(L.x, L.y), L.z), 'L.x / L.y / L.z'),
      (binary_op(L.x, M.math.divide(L.y, L.z)), 'L.x / (L.y / L.z)'),
  )
  def test_divide_repr(self, expr, expected_repr):
    register(binary_op, op_repr.divide_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x // L.y'),
      (M.math.floordiv(binary_op(L.x, L.y), L.z), 'L.x // L.y // L.z'),
      (M.math.floordiv(L.x, binary_op(L.y, L.z)), 'L.x // (L.y // L.z)'),
      (binary_op(M.math.floordiv(L.x, L.y), L.z), 'L.x // L.y // L.z'),
      (binary_op(L.x, M.math.floordiv(L.y, L.z)), 'L.x // (L.y // L.z)'),
  )
  def test_floordiv_repr(self, expr, expected_repr):
    register(binary_op, op_repr.floordiv_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x % L.y'),
      (M.math.mod(binary_op(L.x, L.y), L.z), 'L.x % L.y % L.z'),
      (M.math.mod(L.x, binary_op(L.y, L.z)), 'L.x % (L.y % L.z)'),
      (binary_op(M.math.mod(L.x, L.y), L.z), 'L.x % L.y % L.z'),
      (binary_op(L.x, M.math.mod(L.y, L.z)), 'L.x % (L.y % L.z)'),
  )
  def test_mod_repr(self, expr, expected_repr):
    register(binary_op, op_repr.mod_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x + L.y'),
      (M.math.add(binary_op(L.x, L.y), L.z), 'L.x + L.y + L.z'),
      (M.math.add(L.x, binary_op(L.y, L.z)), 'L.x + (L.y + L.z)'),
      (binary_op(M.math.add(L.x, L.y), L.z), 'L.x + L.y + L.z'),
      (binary_op(L.x, M.math.add(L.y, L.z)), 'L.x + (L.y + L.z)'),
  )
  def test_add_repr(self, expr, expected_repr):
    register(binary_op, op_repr.add_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x - L.y'),
      (M.math.subtract(binary_op(L.x, L.y), L.z), 'L.x - L.y - L.z'),
      (M.math.subtract(L.x, binary_op(L.y, L.z)), 'L.x - (L.y - L.z)'),
      (binary_op(M.math.subtract(L.x, L.y), L.z), 'L.x - L.y - L.z'),
      (binary_op(L.x, M.math.subtract(L.y, L.z)), 'L.x - (L.y - L.z)'),
  )
  def test_subtract_repr(self, expr, expected_repr):
    register(binary_op, op_repr.subtract_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x & L.y'),
      (M.core.presence_and(binary_op(L.x, L.y), L.z), 'L.x & L.y & L.z'),
      (M.core.presence_and(L.x, binary_op(L.y, L.z)), 'L.x & (L.y & L.z)'),
      (binary_op(M.core.presence_and(L.x, L.y), L.z), 'L.x & L.y & L.z'),
      (binary_op(L.x, M.core.presence_and(L.y, L.z)), 'L.x & (L.y & L.z)'),
  )
  def test_apply_mask_repr(self, expr, expected_repr):
    register(binary_op, op_repr.apply_mask_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x | L.y'),
      (M.core.presence_or(binary_op(L.x, L.y), L.z), 'L.x | L.y | L.z'),
      (M.core.presence_or(L.x, binary_op(L.y, L.z)), 'L.x | (L.y | L.z)'),
      (binary_op(M.core.presence_or(L.x, L.y), L.z), 'L.x | L.y | L.z'),
      (binary_op(L.x, M.core.presence_or(L.y, L.z)), 'L.x | (L.y | L.z)'),
  )
  def test_coalesce_repr(self, expr, expected_repr):
    register(binary_op, op_repr.coalesce_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x < L.y'),
      (M.core.less(binary_op(L.x, L.y), L.z), '(L.x < L.y) < L.z'),
      (M.core.less(L.x, binary_op(L.y, L.z)), 'L.x < (L.y < L.z)'),
      (binary_op(M.core.less(L.x, L.y), L.z), '(L.x < L.y) < L.z'),
      (binary_op(L.x, M.core.less(L.y, L.z)), 'L.x < (L.y < L.z)'),
  )
  def test_less_repr(self, expr, expected_repr):
    register(binary_op, op_repr.less_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x <= L.y'),
      (M.core.less_equal(binary_op(L.x, L.y), L.z), '(L.x <= L.y) <= L.z'),
      (M.core.less_equal(L.x, binary_op(L.y, L.z)), 'L.x <= (L.y <= L.z)'),
      (binary_op(M.core.less_equal(L.x, L.y), L.z), '(L.x <= L.y) <= L.z'),
      (binary_op(L.x, M.core.less_equal(L.y, L.z)), 'L.x <= (L.y <= L.z)'),
  )
  def test_less_equal_repr(self, expr, expected_repr):
    register(binary_op, op_repr.less_equal_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x > L.y'),
      (M.core.greater(binary_op(L.x, L.y), L.z), '(L.x > L.y) > L.z'),
      (M.core.greater(L.x, binary_op(L.y, L.z)), 'L.x > (L.y > L.z)'),
      (binary_op(M.core.greater(L.x, L.y), L.z), '(L.x > L.y) > L.z'),
      (binary_op(L.x, M.core.greater(L.y, L.z)), 'L.x > (L.y > L.z)'),
  )
  def test_greater_repr(self, expr, expected_repr):
    register(binary_op, op_repr.greater_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x >= L.y'),
      (M.core.greater_equal(binary_op(L.x, L.y), L.z), '(L.x >= L.y) >= L.z'),
      (M.core.greater_equal(L.x, binary_op(L.y, L.z)), 'L.x >= (L.y >= L.z)'),
      (binary_op(M.core.greater_equal(L.x, L.y), L.z), '(L.x >= L.y) >= L.z'),
      (binary_op(L.x, M.core.greater_equal(L.y, L.z)), 'L.x >= (L.y >= L.z)'),
  )
  def test_greater_equal_repr(self, expr, expected_repr):
    register(binary_op, op_repr.greater_equal_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x == L.y'),
      (M.core.equal(binary_op(L.x, L.y), L.z), '(L.x == L.y) == L.z'),
      (M.core.equal(L.x, binary_op(L.y, L.z)), 'L.x == (L.y == L.z)'),
      (binary_op(M.core.equal(L.x, L.y), L.z), '(L.x == L.y) == L.z'),
      (binary_op(L.x, M.core.equal(L.y, L.z)), 'L.x == (L.y == L.z)'),
  )
  def test_equal_repr(self, expr, expected_repr):
    register(binary_op, op_repr.equal_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (binary_op(L.x, L.y), 'L.x != L.y'),
      (M.core.not_equal(binary_op(L.x, L.y), L.z), '(L.x != L.y) != L.z'),
      (M.core.not_equal(L.x, binary_op(L.y, L.z)), 'L.x != (L.y != L.z)'),
      (binary_op(M.core.not_equal(L.x, L.y), L.z), '(L.x != L.y) != L.z'),
      (binary_op(L.x, M.core.not_equal(L.y, L.z)), 'L.x != (L.y != L.z)'),
  )
  def test_not_equal_repr(self, expr, expected_repr):
    register(binary_op, op_repr.not_equal_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (kde.subslice(I.x, I.y, I.z), 'kde.subslice(I.x, I.y, I.z)'),
      (
          kde.subslice(I.x, slice(1, None)),
          'kde.subslice(I.x, slice(1, None))',
      ),
      (
          kde.subslice(I.x, slice(1)),
          'kde.subslice(I.x, slice(None, 1))',
      ),
      (
          kde.subslice(I.x, slice(None)),
          'kde.subslice(I.x, slice(None, None))',
      ),
      (kde.subslice(I.x, ...), 'kde.subslice(I.x, ...)'),
      (
          kde.subslice(I.x, ds(1), ..., slice(1)),
          'kde.subslice(I.x, DataItem(1, schema: INT32), ..., slice(None, 1))',
      ),
  )
  def test_subslice_repr(self, expr, expected_repr):
    register(kde.subslice, op_repr.subslice_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (kde.core._subslice_for_slicing_helper(I.x, I.y), 'I.x.S[I.y]'),
      (kde.core._subslice_for_slicing_helper(I.x, I.y, I.z), 'I.x.S[I.y, I.z]'),
      (
          kde.core._subslice_for_slicing_helper(I.x, I.y, slice(1, 2)),
          'I.x.S[I.y, 1:2]',
      ),
      (
          kde.core._subslice_for_slicing_helper(I.x, I.y, slice(2)),
          'I.x.S[I.y, :2]',
      ),
      (
          kde.core._subslice_for_slicing_helper(I.x, I.y, slice(1, None)),
          'I.x.S[I.y, 1:]',
      ),
      (
          kde.core._subslice_for_slicing_helper(I.x, I.y, slice(None)),
          'I.x.S[I.y, :]',
      ),
      (
          kde.core._subslice_for_slicing_helper(I.x, I.y, ...),
          'I.x.S[I.y, ...]',
      ),
      (
          kde.core._subslice_for_slicing_helper(
              I.x,
              I.y,
              ...,
              kde.core._subslice_for_slicing_helper(I.z, I.w),
          ),
          'I.x.S[I.y, ..., I.z.S[I.w]]',
      ),
  )
  def test_subslicehelper_repr(self, expr, expected_repr):
    register(kde.core._subslice_for_slicing_helper, op_repr.subslicehelper_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (kde.get_item(I.x, slice(1)), 'I.x[:1]'),
      (kde.get_item(I.x, slice(1, None)), 'I.x[1:]'),
      (kde.get_item(I.x, slice(1, -1)), 'I.x[1:-1]'),
      (
          kde.get_item(I.x, arolla.M.core.make_slice(I.start, I.end)),
          'I.x[M.core.make_slice(I.start, I.end, unspecified)]',
      ),
      (
          kde.get_item(I.x, ds(1).no_bag()),
          'I.x[DataItem(1, schema: INT32)]',
      ),
  )
  def test_get_item_repr(self, expr, expected_repr):
    register(kde.core.get_item, op_repr.subslicehelper_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      (kde.get_attr(I.x, ds('a')), 'I.x.a'),
      (kde.get_attr(I.x, ds('_a_b_123')), 'I.x._a_b_123'),
      (kde.get_attr(M.core.getattr(I.x, 'a'), 'b'), 'I.x.a.b'),
      (M.core.getattr(kde.get_attr(I.x, 'a'), 'b'), 'I.x.a.b'),
      (kde.get_attr(I.x < I.y, 'a'), '(I.x < I.y).a'),
      # Fallbacks due to failures.
      #
      # With default.
      (
          kde.get_attr(I.x, ds('a'), ds(1)),
          (
              "kde.get_attr(I.x, DataItem('a', schema: STRING), DataItem(1,"
              ' schema: INT32))'
          ),
      ),
      # Not text.
      (
          kde.get_attr(I.x, ds(1)),
          'kde.get_attr(I.x, DataItem(1, schema: INT32), unspecified)',
      ),
      # Not a literal.
      (
          kde.get_attr(I.x, I.a),
          'kde.get_attr(I.x, I.a, unspecified)',
      ),
      # Not an identifier.
      (
          kde.get_attr(I.x, ds('')),
          "kde.get_attr(I.x, DataItem('', schema: STRING), unspecified)",
      ),
      (
          kde.get_attr(I.x, ds('')),
          "kde.get_attr(I.x, DataItem('', schema: STRING), unspecified)",
      ),
      (
          kde.get_attr(I.x, ds('123')),
          "kde.get_attr(I.x, DataItem('123', schema: STRING), unspecified)",
      ),
      (
          kde.get_attr(I.x, ds('a%')),
          "kde.get_attr(I.x, DataItem('a%', schema: STRING), unspecified)",
      ),
  )
  def test_getattr_repr(self, expr, expected_repr):
    register(kde.get_attr, op_repr.getattr_repr)
    self.assertEqual(repr(expr), expected_repr)

  @parameterized.parameters(
      # Simple.
      (
          full_signature_op(I.a, x=I.x, y=I.y, z=I.z),
          'test.full_signature_op(I.a, I.x, y=I.y, z=I.z)',
      ),
      # Varargs.
      (
          full_signature_op(I.a, I.x, I.b, y=I.y, z=I.z),
          'test.full_signature_op(I.a, I.x, I.b, y=I.y, z=I.z)',
      ),
      (
          full_signature_op(I.a, I.x, I.b, I.b, y=I.y, z=I.z),
          'test.full_signature_op(I.a, I.x, I.b, I.b, y=I.y, z=I.z)',
      ),
      (
          full_signature_op(I.a, I.x, 1, 2, y=I.y, z=I.z),
          (
              'test.full_signature_op(I.a, I.x, DataItem(1, schema: INT32),'
              ' DataItem(2, schema: INT32), y=I.y, z=I.z)'
          ),
      ),
      # Varkwargs.
      (
          full_signature_op(I.a, I.x, y=I.y, z=I.z, w=I.w),
          'test.full_signature_op(I.a, I.x, y=I.y, z=I.z, w=I.w)',
      ),
      (
          full_signature_op(I.a, I.x, y=I.y, z=I.z, w=I.w, v=I.v),
          'test.full_signature_op(I.a, I.x, y=I.y, z=I.z, w=I.w, v=I.v)',
      ),
      (
          full_signature_op(I.a, I.x, y=I.y, z=I.z, w=1, v=2),
          (
              'test.full_signature_op(I.a, I.x, y=I.y, z=I.z, w=DataItem(1,'
              ' schema: INT32), v=DataItem(2, schema: INT32))'
          ),
      ),
      # Both.
      (
          full_signature_op(I.a, I.x, I.b, I.c, y=I.y, z=I.z, w=I.w, v=I.v),
          (
              'test.full_signature_op(I.a, I.x, I.b, I.c, y=I.y, z=I.z, w=I.w,'
              ' v=I.v)'
          ),
      ),
      # Non-namedtuple varargs and varkwargs.
      (
          arolla.abc.bind_op(
              full_signature_op,
              I.a,
              I.b,
              M.core.concat_tuples(
                  M.core.make_tuple(I.c), M.core.make_tuple(I.d)
              ),
              I.y,
              I.z,
              arolla.M.namedtuple.union(I.nt1, I.nt2),
          ),
          (
              'test.full_signature_op(I.a, I.b,'
              ' M.core.concat_tuples(M.core.make_tuple(I.c),'
              ' M.core.make_tuple(I.d)), y=I.y, z=I.z,'
              ' M.namedtuple.union(I.nt1, I.nt2))'
          ),
      ),
      (
          arolla.abc.bind_op(
              full_signature_op,
              I.a,
              I.b,
              arolla.namedtuple(nt=1),  # wrong, but we don't care here.
              I.y,
              I.z,
              arolla.tuple(1, 2),  # wrong, but we don't care here.
          ),
          (
              'test.full_signature_op(I.a, I.b, namedtuple<nt=INT32>{(1)},'
              ' y=I.y, z=I.z, (1, 2))'
          ),
      ),
  )
  def test_full_signature_repr(self, expr, expected_repr):
    register(full_signature_op, op_repr.default_op_repr)
    self.assertEqual(repr(expr), expected_repr)

  def test_full_signature_repr_hidden_seed(self):
    @arolla.optools.add_to_registry()
    @arolla.optools.as_lambda_operator(
        'test.full_signature_with_hidden_seed_op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def full_signature_with_hidden_seed_op(
        x,
        args=py_boxing.var_positional(),
        y=py_boxing.keyword_only(),
        kwargs=py_boxing.var_keyword(),
        hidden_seed=py_boxing.hidden_seed(),
    ):
      del hidden_seed
      return x, args, y, kwargs

    register(full_signature_with_hidden_seed_op, op_repr.default_op_repr)
    self.assertEqual(
        repr(full_signature_with_hidden_seed_op(I.x, I.a, y=I.y, z=I.z)),
        'test.full_signature_with_hidden_seed_op(I.x, I.a, y=I.y, z=I.z)',
    )


if __name__ == '__main__':
  absltest.main()
