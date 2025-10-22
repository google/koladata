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
from koladata.types import qtypes


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class ProtoGetProtoFieldCustomDefaultTest(parameterized.TestCase):

  def test_from_proto(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto(m)

    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'some_text')
    )
    testing.assert_equal(result.no_bag(), ds('aaa'))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'some_float')
    )
    testing.assert_allclose(result.no_bag(), ds(123.4))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'message_b_list')
    )
    testing.assert_equivalent(result.no_bag(), ds(None))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'nonexistent_field')
    )
    testing.assert_equivalent(result.no_bag(), ds(None))

  def test_from_proto_slice(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto([m, m])

    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'some_text')
    )
    testing.assert_equal(result.no_bag(), ds(['aaa', 'aaa']))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'some_float')
    )
    testing.assert_allclose(result.no_bag(), ds([123.4, 123.4]))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'message_b_list')
    )
    testing.assert_equivalent(result.no_bag(), ds([None, None]))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'nonexistent_field')
    )
    testing.assert_equivalent(result.no_bag(), ds([None, None]))

  def test_from_proto_object_slice(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.obj(fns.from_proto([m, m]))

    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'some_text')
    )
    testing.assert_equal(result.no_bag(), ds(['aaa', 'aaa']))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'some_float')
    )
    testing.assert_allclose(result.no_bag(), ds([123.4, 123.4]))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'message_b_list')
    )
    testing.assert_equivalent(result.no_bag(), ds([None, None]))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m, 'nonexistent_field')
    )
    testing.assert_equivalent(result.no_bag(), ds([None, None]))

  def test_from_proto_schema(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto(m)

    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m.get_schema(), 'some_text')
    )
    testing.assert_equal(result.no_bag(), ds('aaa'))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(
            kd_m.get_schema(), 'some_float'
        )
    )
    testing.assert_allclose(result.no_bag(), ds(123.4))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(
            kd_m.get_schema(), 'message_b_list'
        )
    )
    testing.assert_equal(result.no_bag(), ds(None))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(
            kd_m.get_schema(), 'nonexistent_field'
        )
    )
    testing.assert_equivalent(result.no_bag(), ds(None))

  def test_from_proto_schema_slice(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto(m)

    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(
            ds([kd_m.get_schema(), kd_m.get_schema()]), 'some_text'
        )
    )
    testing.assert_equal(result.no_bag(), ds(['aaa', 'aaa']))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(
            ds([kd_m.get_schema(), kd_m.get_schema()]), 'some_float'
        )
    )
    testing.assert_allclose(result.no_bag(), ds([123.4, 123.4]))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(
            ds([kd_m.get_schema(), kd_m.get_schema()]), 'message_b_list'
        )
    )
    testing.assert_equal(result.no_bag(), ds([None, None]))
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(
            ds([kd_m.get_schema(), kd_m.get_schema()]), 'nonexistent_field'
        )
    )
    testing.assert_equal(result.no_bag(), ds([None, None]))

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
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto(m)
    result = expr_eval.eval(
        kde.proto.get_proto_field_custom_default(kd_m.get_schema(), 'foo')
    )
    testing.assert_equal(result.no_bag(), ds(None))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.proto.get_proto_field_custom_default,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.proto.get_proto_field_custom_default(I.x, I.y))
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.proto.get_proto_field_custom_default(I.x, I.y)),
        'kd.proto.get_proto_field_custom_default(I.x, I.y)',
    )


if __name__ == '__main__':
  absltest.main()
