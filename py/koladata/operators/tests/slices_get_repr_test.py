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
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants


eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
ENTITY_SCHEMA = data_bag.DataBag.empty_mutable().new().get_schema()
bag = data_bag.DataBag.empty_mutable
db = bag()


class SlicesGetReprTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('int', ds(1), ds('1')),
      ('int_slice', ds([1, 2, 3]), ds('[1, 2, 3]')),
      ('float', ds(1.0), ds('1.0')),
      ('float_schema', schema_constants.FLOAT32, ds('FLOAT32')),
      ('string', ds('foo'), ds("'foo'")),
      ('string_schema', schema_constants.STRING, ds('STRING')),
      ('mask', ds(mask_constants.present), ds('present')),
      ('mask_schema', schema_constants.MASK, ds('MASK')),
      ('object', db.obj(a=1, b='foo'), ds("Obj(a=1, b='foo')")),
      ('object_schema', schema_constants.OBJECT, ds('OBJECT')),
      ('entity', db.new(a=1, b='foo'), ds("Entity(a=1, b='foo')")),
      (
          'entity_schema',
          db.new_schema(a=schema_constants.INT32, b=schema_constants.STRING),
          ds('ENTITY(a=INT32, b=STRING)'),
      ),
      ('list', db.list([1, 2, 3]), ds('List[1, 2, 3]')),
      (
          'list_schema',
          db.list_schema(schema_constants.INT32),
          ds('LIST[INT32]'),
      ),
      (
          'dict_schema',
          db.dict_schema(schema_constants.STRING, schema_constants.INT32),
          ds('DICT{STRING, INT32}'),
      ),
      (
          'multi_dim_entity',
          db.new(a=ds([1, 2])),
          ds('[Entity(a=1), Entity(a=2)]'),
      ),
      ('multi_dim_object', db.obj(a=ds([1, 2])), ds('[Obj(a=1), Obj(a=2)]')),
  )
  def test_eval(self, x, expected):
    res = eval_op('kd.slices.get_repr', x)
    testing.assert_equal(res, expected)

  def test_depth(self):
    res = eval_op('kd.slices.get_repr', db.obj(a=db.obj(a=1), b='foo'), depth=1)
    self.assertRegex(res.to_py(), r"^Obj\(a=\$.{22}, b='foo'\)$")

  def test_item_limit(self):
    res = eval_op('kd.slices.get_repr', ds([1, 2]), item_limit=1)
    testing.assert_equal(res, ds('[1, ...]'))

  def test_item_limit_per_dimension(self):
    res = eval_op(
        'kd.slices.get_repr',
        ds([[1, 2, 3], [4, 5, 6]]),
        item_limit=3,
        item_limit_per_dimension=2,
    )
    testing.assert_equal(res, ds('[[1, 2, ...], [4, ...]]'))

  def test_format_html(self):
    res = eval_op('kd.slices.get_repr', ds(['foo', 'bar']), format_html=True)
    testing.assert_equal(
        res,
        ds(
            '[<span slice-index="0">\'foo\'</span>, <span'
            ' slice-index="1">\'bar\'</span>]'
        ),
    )

  def test_max_str_len(self):
    res = eval_op(
        'kd.slices.get_repr',
        ds(['foo', 'bar']),
        max_str_len=1,
    )
    testing.assert_equal(res, ds("['f'...'o', 'b'...'r']"))

  def test_show_attributes(self):
    res = eval_op(
        'kd.slices.get_repr',
        db.obj(a=ds([1, 2])),
        show_attributes=False,
    )
    self.assertRegex(res.to_py(), r'^\[Obj:\$.{22}, Obj:\$.{22}\]$')

  def test_show_databag_id(self):
    res = eval_op(
        'kd.slices.get_repr',
        db.obj(a=ds([1, 2])),
        show_databag_id=True,
    )
    self.assertRegex(
        res.to_py(),
        r'^DataSlice\(\[Obj\(a=1\), Obj\(a=2\)\], bag_id: \$.{4}\)$',
    )

  def test_show_shape(self):
    res = eval_op(
        'kd.slices.get_repr',
        ds([[1, 2], [3, 4]]),
        show_shape=True,
    )
    testing.assert_equal(
        res, ds('DataSlice([[1, 2], [3, 4]], shape: JaggedShape(2, 2))')
    )

  def test_show_schema(self):
    res = eval_op(
        'kd.slices.get_repr',
        ds([1, 2]),
        show_schema=True,
    )
    testing.assert_equal(res, ds('DataSlice([1, 2], schema: INT32)'))

  def test_show_itemid(self):
    res = eval_op(
        'kd.slices.get_repr',
        db.new(a=1),
        show_schema=True,
        show_item_id=True,
    )
    self.assertRegex(
        res.internal_as_py(),
        r'DataItem\(Entity\(a=1\), schema: ENTITY\(a=INT32\), item_id:'
        r' Entity:\$.{22}\)',
    )

  def test_show_itemid_schema(self):
    res = eval_op(
        'kd.slices.get_repr',
        db.new_schema(a=schema_constants.INT32),
        show_schema=True,
        show_item_id=True,
    )
    self.assertRegex(
        res.internal_as_py(),
        r'DataItem\(ENTITY\(a=INT32\), schema: SCHEMA, item_id:'
        r' Schema:\$.{22}\)',
    )

  def test_functor_itemid(self):
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y))).eval()
    res = eval_op(
        'kd.slices.get_repr',
        fn,
        show_schema=True,
        show_item_id=True,
    )
    self.assertRegex(
        res.internal_as_py(),
        r'DataItem\(Functor\[self=Entity\(self_not_specified=present\), .*\]'
        r'\(returns=I.x \+ I.y\), schema: OBJECT, item_id: Entity:\$.{22}\)',
    )

  def test_print_only_attr_names(self):
    res = eval_op(
        'kd.slices.get_repr',
        db.new(a=ds([1, 2])),
        item_limit=1,
        show_attributes=True,
    )
    testing.assert_equal(res, ds('DataSlice(attrs: [a])'))

  @parameterized.named_parameters(
      # Dict order is not deterministic.
      (
          'dict',
          db.dict({'a': 1, 'b': 2}),
          r'^Dict{\'a\'=1, \'b\'=2}|Dict{\'b\'=2, \'a\'=1}$',
      ),
      ('empy_object', db.obj(), r'^Obj\(\):\$.{22}$'),
  )
  def test_eval_like(self, x, expected_regex):
    res = eval_op('kd.slices.get_repr', x).internal_as_py()
    self.assertRegex(res, expected_regex)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.get_repr(I.x)))


if __name__ == '__main__':
  absltest.main()
