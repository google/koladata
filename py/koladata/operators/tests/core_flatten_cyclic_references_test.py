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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals


class CoreFlattenCyclicReferencesTest(parameterized.TestCase):

  def test_entity(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = expr_eval.eval(
        kde.core.flatten_cyclic_references(o, max_recursion_depth=1)
    )

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=db.new(
            self=ds([None, None, None]),
            b=db.new(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        ),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_entity_depth_3(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = expr_eval.eval(
        kde.core.flatten_cyclic_references(o, max_recursion_depth=3)
    )

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=db.new(
            self=db.new(
                self=db.new(
                    self=ds([None, None, None]),
                    b=db.new(a=ds([1, None, 2])),
                    c=ds(['foo', 'bar', 'baz']),
                ),
                b=db.new(a=ds([1, None, 2])),
                c=ds(['foo', 'bar', 'baz']),
            ),
            b=db.new(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        ),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_entity_depth_5(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = expr_eval.eval(
        kde.core.flatten_cyclic_references(o, max_recursion_depth=5)
    )

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=db.new(
            self=db.new(
                self=db.new(
                    self=db.new(
                        self=db.new(
                            self=ds([None, None, None]),
                            b=db.new(a=ds([1, None, 2])),
                            c=ds(['foo', 'bar', 'baz']),
                        ),
                        b=db.new(a=ds([1, None, 2])),
                        c=ds(['foo', 'bar', 'baz']),
                    ),
                    b=db.new(a=ds([1, None, 2])),
                    c=ds(['foo', 'bar', 'baz']),
                ),
                b=db.new(a=ds([1, None, 2])),
                c=ds(['foo', 'bar', 'baz']),
            ),
            b=db.new(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        ),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_entity_unbalanced(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', ds([o.S[0], o.S[2], o.S[1]]))
    o.S[1].set_attr('self', o.S[2])
    result = expr_eval.eval(
        kde.core.flatten_cyclic_references(o, max_recursion_depth=1)
    )

    self.assertFalse(result.get_bag().is_mutable())

    expected_next = db.new(
        self=db.new(
            self=ds([None, None]),
            b=db.new(a=ds([2, None])),
            c=ds(['baz', 'bar']),
        ),
        b=db.new(a=ds([None, 2])),
        c=ds(['bar', 'baz']),
    )
    expected_ds = db.new(
        self=db.new(
            self=ds([
                None,
                expected_next.S[0],
                expected_next.S[1],
            ]),
            b=db.new(a=ds([1, 2, None])),
            c=ds(['foo', 'baz', 'bar']),
        ),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_zero_depth(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = expr_eval.eval(
        kde.core.flatten_cyclic_references(o, max_recursion_depth=0)
    )

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=ds([None, None, None]),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_objec(self):
    db = bag()
    b_slice = db.obj(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = expr_eval.eval(
        kde.core.flatten_cyclic_references(o, max_recursion_depth=1)
    )

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.obj(
        self=db.obj(
            self=ds([None, None, None]),
            b=db.obj(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        ),
        b=db.obj(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_list(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.implode(db.new(b=b_slice, c=ds(['foo', 'bar', 'baz'])))
    o[:].set_attr('self', o[:])
    result = expr_eval.eval(
        kde.core.flatten_cyclic_references(o, max_recursion_depth=1)
    )

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.implode(
        db.new(
            self=db.new(
                self=ds([None, None, None]),
                b=db.new(a=ds([1, None, 2])),
                c=ds(['foo', 'bar', 'baz']),
            ),
            b=db.new(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        )
    )
    testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_dict(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(db.dict_like(b_slice))
    o['b'] = b_slice
    o['c'] = ds(['foo', 'bar', 'baz'])
    o['self'] = o
    result = expr_eval.eval(
        kde.core.flatten_cyclic_references(o, max_recursion_depth=1)
    )

    self.assertFalse(result.get_bag().is_mutable())

    expected_self = db.dict_like(b_slice)
    expected_self['self'] = ds([None, None, None])
    expected_self['b'] = b_slice
    expected_self['c'] = ds(['foo', 'bar', 'baz'])
    expected_ds = db.dict_like(b_slice)
    expected_ds['self'] = expected_self
    expected_ds['b'] = b_slice
    expected_ds['c'] = ds(['foo', 'bar', 'baz'])
    testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.core.flatten_cyclic_references(
                I.x, max_recursion_depth=I.max_recursion_depth
            )
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            kde.core.flatten_cyclic_references(
                I.x, max_recursion_depth=I.max_recursion_depth
            )
        ),
        'kd.core.flatten_cyclic_references(I.x,'
        ' max_recursion_depth=I.max_recursion_depth)',
    )


if __name__ == '__main__':
  absltest.main()
