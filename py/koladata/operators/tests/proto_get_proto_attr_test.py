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
from koladata.functions import functions as fns
from koladata.functions.tests import test_cc_proto_py_ext as _
from koladata.functions.tests import test_pb2
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class ProtoGetProtoAttrTest(parameterized.TestCase):

  def test_from_proto_missing_uses_default(self):
    m = test_pb2.MessageA()
    kd_m = fns.from_proto(m, schema=fns.schema_from_proto(type(m)))

    result = expr_eval.eval(kde.proto.get_proto_attr(kd_m, 'some_text'))
    testing.assert_equal(result.no_bag(), ds('aaa'))

  def test_from_proto_present_uses_value(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto(m, schema=fns.schema_from_proto(type(m)))

    result = expr_eval.eval(kde.proto.get_proto_attr(kd_m, 'some_text'))
    testing.assert_equal(result.no_bag(), ds('hello'))

  def test_from_proto_slice(self):
    kd_m = fns.from_proto(
        [
            test_pb2.MessageA(),
            test_pb2.MessageA(some_text='hello'),
        ],
        schema=fns.schema_from_proto(test_pb2.MessageA),
    )

    result = expr_eval.eval(kde.proto.get_proto_attr(kd_m, 'some_text'))
    testing.assert_equal(result.no_bag(), ds(['aaa', 'hello']))

  def test_from_proto_object_slice(self):
    kd_m = fns.obj(
        fns.from_proto(
            [
                test_pb2.MessageA(),
                test_pb2.MessageA(some_text='hello'),
            ],
            schema=fns.schema_from_proto(test_pb2.MessageA),
        )
    )

    result = expr_eval.eval(kde.proto.get_proto_attr(kd_m, 'some_text'))
    testing.assert_equal(result.no_bag(), ds(['aaa', 'hello']))

  def test_from_proto_non_scalar_field(self):
    kd_m = fns.from_proto(
        [
            test_pb2.MessageA(),
            test_pb2.MessageA(message_b_list=[test_pb2.MessageB(text='x')]),
        ],
        schema=fns.schema_from_proto(test_pb2.MessageA),
    )

    result = expr_eval.eval(
        kde.proto.get_proto_attr(kd_m, 'message_b_list')[:].text
    )
    testing.assert_equal(result.no_bag(), ds([[], ['x']]))

  def test_from_proto_bool_to_mask(self):
    schema = fns.schema_from_proto(test_pb2.MessageC)
    m_unset = test_pb2.MessageC()
    m_false = test_pb2.MessageC(bool_field=False)
    m_true = test_pb2.MessageC(bool_field=True)

    kd_m_unset = fns.from_proto(m_unset, schema=schema)
    kd_m_false = fns.from_proto(m_false, schema=schema)
    kd_m_true = fns.from_proto(m_true, schema=schema)

    testing.assert_equal(
        expr_eval.eval(kde.proto.get_proto_attr(kd_m_unset, 'bool_field')),
        mask_constants.missing,
    )
    testing.assert_equal(
        expr_eval.eval(kde.proto.get_proto_attr(kd_m_false, 'bool_field')),
        mask_constants.missing,
    )
    testing.assert_equal(
        expr_eval.eval(kde.proto.get_proto_attr(kd_m_true, 'bool_field')),
        mask_constants.present,
    )

  def test_non_proto_entity_bool_to_mask(self):
    schema = fns.new(bool_field=False).get_schema()
    m_unset = test_pb2.MessageC()
    m_false = test_pb2.MessageC(bool_field=False)
    m_true = test_pb2.MessageC(bool_field=True)

    kd_m_unset = fns.from_proto(m_unset, schema=schema)
    kd_m_false = fns.from_proto(m_false, schema=schema)
    kd_m_true = fns.from_proto(m_true, schema=schema)

    testing.assert_equal(
        expr_eval.eval(kde.proto.get_proto_attr(kd_m_unset, 'bool_field')),
        mask_constants.missing,
    )
    testing.assert_equal(
        expr_eval.eval(kde.proto.get_proto_attr(kd_m_false, 'bool_field')),
        mask_constants.missing,
    )
    testing.assert_equal(
        expr_eval.eval(kde.proto.get_proto_attr(kd_m_true, 'bool_field')),
        mask_constants.present,
    )

  def test_primitive_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.proto.get_proto_attr: failed to get attribute'
            " '__schema_metadata__': primitives do not have attributes, got"
            ' SCHEMA DataItem with primitive BOOLEAN'
        ),
    ):
      expr_eval.eval(kde.proto.get_proto_attr(False, 'field'))

  def test_nonexistent_field(self):
    m = test_pb2.MessageA()
    kd_m = fns.from_proto(m, schema=fns.schema_from_proto(type(m)))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.proto.get_proto_attr: failed to get attribute 'foo': the"
            " attribute 'foo' is missing on the schema."
        ),
    ):
      expr_eval.eval(kde.proto.get_proto_attr(kd_m, 'foo'))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.proto.get_proto_attr,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.proto.get_proto_attr(I.x, I.y)))

  def test_repr(self):
    self.assertEqual(
        repr(kde.proto.get_proto_attr(I.x, I.y)),
        'kd.proto.get_proto_attr(I.x, I.y)',
    )


if __name__ == '__main__':
  absltest.main()
