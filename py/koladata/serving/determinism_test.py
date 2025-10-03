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
from koladata import kd
from koladata.operators import koda_internal_parallel
from koladata.serving import determinism


def _extract_non_deterministic_tokens(
    expr: arolla.Expr,
) -> tuple[arolla.Expr, list[arolla.QValue]]:
  """Extracts all the non-deterministic tokens from the Expr and replaces them with a counter."""
  tokens = []

  def extract_tokens(node: arolla.Expr):
    if (
        node.op == arolla.abc.lookup_operator('koda_internal.non_deterministic')
        and node.node_deps[1].is_literal
    ):
      tokens.append(node.node_deps[1].qvalue)
      return node.op(node.node_deps[0], arolla.int64(len(tokens)))
    return node

  stripped_expr = arolla.abc.transform(expr, extract_tokens)
  return stripped_expr, tokens


class DeterminizerTest(parameterized.TestCase):

  def test_primitive(self):
    determinizer = determinism.Determinizer(seed='test')
    ds = kd.slice(57)
    kd.testing.assert_equal(determinizer.make_deterministic(ds), ds)
    ds = kd.slice('a')
    kd.testing.assert_equal(determinizer.make_deterministic(ds), ds)
    ds = kd.slice(True)
    kd.testing.assert_equal(determinizer.make_deterministic(ds), ds)

  def test_list(self):
    determinizer = determinism.Determinizer(seed='test')
    l1 = kd.list([1, 2, 3])
    det_l1 = determinizer.make_deterministic(l1)
    self.assertTrue(kd.ids.is_uuid(det_l1.get_itemid()))
    self.assertNotEqual(l1.get_itemid().no_bag(), det_l1.get_itemid().no_bag())
    kd.testing.assert_equal(
        determinizer.make_deterministic(l1).get_itemid().no_bag(),
        det_l1.get_itemid().no_bag(),
    )
    kd.testing.assert_deep_equivalent(det_l1, l1, schemas_equality=True)

    l2 = kd.list([1, 2, 3])
    det_l2 = determinizer.make_deterministic(l2)
    self.assertNotEqual(
        det_l1.get_itemid().no_bag(), det_l2.get_itemid().no_bag()
    )

  def test_uulist(self):
    determinizer = determinism.Determinizer(seed='test')
    l = kd.list([1, 2, 3], itemid=kd.uuid_for_list(seed='test1'))
    det_l = determinizer.make_deterministic(l)
    self.assertTrue(kd.ids.is_uuid(det_l.get_itemid()))
    kd.testing.assert_equal(
        det_l.get_itemid().no_bag(), l.get_itemid().no_bag()
    )
    kd.testing.assert_deep_equivalent(det_l, l, schemas_equality=True)

  def test_list_with_entity(self):
    determinizer = determinism.Determinizer(seed='test')
    uu_schema = kd.schema.uu_schema(seed='test', a=kd.INT32)
    l = kd.list([kd.new(a=1, schema=uu_schema), kd.new(a=2, schema=uu_schema)])
    det_l = determinizer.make_deterministic(l)
    self.assertTrue(kd.ids.is_uuid(det_l.get_itemid()))
    self.assertNotEqual(l.get_itemid().no_bag(), det_l.get_itemid().no_bag())
    self.assertNotEqual(
        det_l[0].get_itemid().no_bag(), det_l[1].get_itemid().no_bag()
    )
    kd.testing.assert_deep_equivalent(det_l, l, schemas_equality=True)

  def test_dict(self):
    determinizer = determinism.Determinizer(seed='test')
    d1 = kd.dict({1: 2, 3: 4})
    det_d1 = determinizer.make_deterministic(d1)
    self.assertTrue(kd.ids.is_uuid(det_d1.get_itemid()))
    self.assertNotEqual(det_d1.get_itemid().no_bag(), d1.get_itemid().no_bag())
    kd.testing.assert_equal(
        determinizer.make_deterministic(d1).get_itemid().no_bag(),
        det_d1.get_itemid().no_bag(),
    )
    kd.testing.assert_deep_equivalent(det_d1, d1, schemas_equality=True)

    d2 = kd.dict({1: 2, 3: 4})
    det_d2 = determinizer.make_deterministic(d2)
    self.assertNotEqual(
        det_d1.get_itemid().no_bag(), det_d2.get_itemid().no_bag()
    )

  def test_uu_dict(self):
    determinizer = determinism.Determinizer(seed='test')
    d = kd.dict({1: 2, 3: 4}, itemid=kd.uuid_for_dict(seed='test1'))
    det_d = determinizer.make_deterministic(d)
    self.assertTrue(kd.ids.is_uuid(det_d.get_itemid()))
    kd.testing.assert_equal(
        det_d.get_itemid().no_bag(), d.get_itemid().no_bag()
    )
    kd.testing.assert_deep_equivalent(det_d, d, schemas_equality=True)

  def test_dict_with_entity(self):
    determinizer = determinism.Determinizer(seed='test')
    uu_schema = kd.schema.uu_schema(seed='test', a=kd.INT32)
    e1 = kd.new(a=1, schema=uu_schema)
    e2 = kd.new(a=1, schema=uu_schema)
    d = kd.dict({'e1': e1, 'e2': e2})
    det_d = determinizer.make_deterministic(d)
    self.assertTrue(kd.ids.is_uuid(det_d.get_itemid()))
    self.assertNotEqual(d.get_itemid().no_bag(), det_d.get_itemid().no_bag())
    kd.testing.assert_deep_equivalent(det_d, d, schemas_equality=True)

    det_e1 = det_d['e1']
    self.assertTrue(kd.ids.is_uuid(det_e1.get_itemid()))
    self.assertNotEqual(det_e1.get_itemid().no_bag(), e1.get_itemid().no_bag())

  def test_uu_entity(self):
    determinizer = determinism.Determinizer(seed='test')
    uu_schema = kd.schema.uu_schema(seed='test', a=kd.INT32)
    e = kd.uu(a=1, schema=uu_schema)
    det_e = determinizer.make_deterministic(e)
    self.assertTrue(kd.ids.is_uuid(det_e.get_itemid()))
    kd.testing.assert_equal(
        det_e.get_itemid().no_bag(), e.get_itemid().no_bag()
    )
    kd.testing.assert_deep_equivalent(det_e, e)

  def test_entity(self):
    determinizer = determinism.Determinizer(seed='test')
    uu_schema = kd.schema.uu_schema(seed='test', a=kd.INT32)
    e = kd.new(a=1, schema=uu_schema)
    det_e = determinizer.make_deterministic(e)
    self.assertTrue(kd.ids.is_uuid(det_e.get_itemid()))
    self.assertNotEqual(det_e.get_itemid().no_bag(), e.get_itemid().no_bag())
    kd.testing.assert_deep_equivalent(det_e, e)

    e2 = kd.new(a=1, schema=uu_schema)
    det_e2 = determinizer.make_deterministic(e2)
    self.assertNotEqual(
        det_e.get_itemid().no_bag(), det_e2.get_itemid().no_bag()
    )

  def test_uu_object(self):
    determinizer = determinism.Determinizer(seed='test')
    o = kd.uuobj(a=1)
    det_o = determinizer.make_deterministic(o)
    self.assertTrue(kd.ids.is_uuid(det_o.get_itemid()))
    kd.testing.assert_equal(
        det_o.get_itemid().no_bag(), o.get_itemid().no_bag()
    )
    kd.testing.assert_deep_equivalent(det_o, o)

  def test_object(self):
    determinizer = determinism.Determinizer(seed='test')
    o = kd.obj(a=1)
    det_o = determinizer.make_deterministic(o)
    self.assertTrue(kd.ids.is_uuid(det_o.get_itemid()))
    self.assertNotEqual(det_o.get_itemid().no_bag(), o.get_itemid().no_bag())
    kd.testing.assert_deep_equivalent(det_o, o)

    o2 = kd.obj(a=1)
    det_o2 = determinizer.make_deterministic(o2)
    self.assertNotEqual(
        det_o.get_itemid().no_bag(), det_o2.get_itemid().no_bag()
    )

  def test_packed_expr(self):
    determinizer = determinism.Determinizer(seed='test')

    @kd.optools.as_lambda_operator('test_op')
    def op(x):
      return x

    expr = kd.expr.pack_expr(op(arolla.L.x))
    det_expr = determinizer.make_deterministic(expr)
    kd.testing.assert_equal(expr, det_expr)

    @kd.optools.as_lambda_operator('test_non_det_op', deterministic=False)
    def non_det_op(x):
      return x

    expr = kd.expr.pack_expr(non_det_op(arolla.L.x))
    det_expr = determinizer.make_deterministic(expr)
    self.assertNotEqual(expr, det_expr)
    kd.testing.assert_equal(det_expr, determinizer.make_deterministic(expr))

  def test_nested(self):
    determinizer = determinism.Determinizer(seed='test')
    schema = kd.schema.uu_schema(
        seed='test', a=kd.INT32, l=kd.list_schema(kd.list_schema(kd.OBJECT))
    )
    o = kd.uuobj(b=7)
    e = kd.uu(a=1, l=kd.list([kd.list([o, o]), kd.list([o, o])]), schema=schema)
    det_e = determinizer.make_deterministic(e)
    kd.testing.assert_equal(det_e.no_bag(), e.no_bag())
    self.assertNotEqual(
        e.l.get_itemid().no_bag(), det_e.l.get_itemid().no_bag()
    )
    self.assertNotEqual(
        e.l[0].get_itemid().no_bag(), det_e.l[0].get_itemid().no_bag()
    )
    kd.testing.assert_deep_equivalent(det_e, e, schemas_equality=True)

  def test_functor(self):
    determinizer = determinism.Determinizer(seed='test')

    schema = kd.schema.uu_schema(x=kd.INT32, y=kd.INT32)

    @kd.trace_as_fn()
    @kd.check_inputs(arg=schema)
    def test_nested_functor(arg):
      literal = kd.eager.new(x=1, y=2, schema=schema)
      # A non-deterministic operator.
      args_obj = kd.obj(x=arg.x, y=arg.y, literal=literal)
      # And one more non-deterministic operator.
      return schema.new(
          x=args_obj.x // args_obj.y,
          y=args_obj.x % args_obj.y,
          literal=args_obj.literal,
      )

    def test_functor(x, y):
      return test_nested_functor(schema.new(x=x, y=y))

    fn = kd.fn(test_functor)

    det_fn = determinizer.make_deterministic(fn)

    # No non-deterministic tokens in expr — no change.
    kd.testing.assert_equal(
        kd.expr.unpack_expr(det_fn.returns), kd.expr.unpack_expr(fn.returns)
    )

    fn_stripped_expr, fn_tokens = _extract_non_deterministic_tokens(
        kd.expr.unpack_expr(fn.test_nested_functor.returns)
    )
    det_fn_stripped_expr, det_fn_tokens = _extract_non_deterministic_tokens(
        kd.expr.unpack_expr(det_fn.test_nested_functor.returns)
    )
    kd.testing.assert_equal(det_fn_stripped_expr, fn_stripped_expr)

    self.assertLen(fn_tokens, 2)
    self.assertLen(det_fn_tokens, 2)
    self.assertNotEqual(set(det_fn_tokens), set(fn_tokens))

    kd.testing.assert_deep_equivalent(
        det_fn(57, 38),
        schema.new(x=1, y=19, literal=schema.new(x=1, y=2)),
        schemas_equality=True,
    )
    kd.testing.assert_deep_equivalent(
        det_fn(57, 38), fn(57, 38), schemas_equality=True
    )

  def test_functor_parallel_transform(self):
    determinizer = determinism.Determinizer(seed='test')

    schema = kd.schema.uu_schema(x=kd.INT32, y=kd.INT32)

    @kd.parallel.transform
    @kd.check_inputs(arg=schema)
    def test_parallel_functor(arg):
      literal = kd.eager.new(x=1, y=2, schema=schema)
      # A non-deterministic operator.
      args_obj = kd.obj(x=arg.x, y=arg.y, literal=literal)
      # And one more non-deterministic operator.
      return schema.new(
          x=args_obj.x // args_obj.y,
          y=args_obj.x % args_obj.y,
          literal=args_obj.literal,
      )

    def test_functor(x, y):
      # TODO: b/442760508 - Stop using koda_internal_parallel once possible.
      return kd.streams.sync_wait(
          koda_internal_parallel.stream_from_future(
              test_parallel_functor(
                  kd.streams.current_executor(),
                  koda_internal_parallel.as_future(schema.new(x=x, y=y)),
                  return_type_as=koda_internal_parallel.as_future(
                      kd.types.DataSlice
                  ),
              )
          )
      )

    fn = kd.fn(test_functor)

    det_fn = determinizer.make_deterministic(fn)

    for attr in kd.dir(fn):
      if not kd.expr.is_packed_expr(kd.get_attr(fn, attr)):
        continue
      with self.subTest(attr=attr):
        fn_stripped_expr, fn_tokens = _extract_non_deterministic_tokens(
            kd.expr.unpack_expr(kd.get_attr(fn, attr))
        )
        det_fn_stripped_expr, det_fn_tokens = _extract_non_deterministic_tokens(
            kd.expr.unpack_expr(kd.get_attr(det_fn, attr))
        )
        kd.testing.assert_equal(det_fn_stripped_expr, fn_stripped_expr)
        self.assertEqual(len(det_fn_tokens), len(fn_tokens))
        self.assertNotEqual(set(det_fn_tokens), set(fn_tokens))

    kd.testing.assert_deep_equivalent(
        det_fn(57, 38),
        schema.new(x=1, y=19, literal=schema.new(x=1, y=2)),
        schemas_equality=True,
    )
    kd.testing.assert_deep_equivalent(
        det_fn(57, 38), fn(57, 38), schemas_equality=True
    )

  def test_strip_source_locations(self):
    determinizer = determinism.Determinizer(
        seed='test', strip_source_locations=True
    )
    fn = kd.fn(lambda x: x + 1)

    kd.testing.assert_equal(
        kd.expr.unpack_expr(fn.returns).op,
        arolla.abc.lookup_operator('kd.annotation.source_location'),
    )

    det_fn = determinizer.make_deterministic(fn)

    kd.testing.assert_equal(
        kd.expr.unpack_expr(det_fn.returns).op,
        arolla.abc.lookup_operator('kd.math.add'),
    )

  def test_non_scalar_slice(self):
    determinizer = determinism.Determinizer(seed='test')
    ds = kd.slice([1, 2, 3])
    with self.assertRaisesRegex(
        NotImplementedError, 'Non-scalar DataSlices are not supported'
    ):
      determinizer.make_deterministic(ds)

  def test_dict_with_non_primitive_keys(self):
    determinizer = determinism.Determinizer(seed='test')
    d = kd.dict({kd.obj(a=1): 2})
    with self.assertRaisesRegex(
        ValueError, 'must be a slice of orderable values'
    ):
      determinizer.make_deterministic(d)

  def test_schema_without_uuid(self):
    determinizer = determinism.Determinizer(seed='test')
    schema = kd.schema.new_schema(a=kd.INT32)
    e = kd.new(a=1, schema=schema)
    with self.assertRaisesRegex(
        ValueError, 'All schemas used it the functors for serving must have'
    ):
      determinizer.make_deterministic(e)


if __name__ == '__main__':
  absltest.main()
