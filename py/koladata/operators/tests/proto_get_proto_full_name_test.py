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
    (DATA_SLICE, DATA_SLICE),
])


class ProtoGetProtoFullNameTest(parameterized.TestCase):

  def test_from_proto(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto(m)
    result = expr_eval.eval(kde.proto.get_proto_full_name(kd_m))
    testing.assert_equal(
        result.no_bag(), ds('koladata.functions.testing.MessageA')
    )

  def test_from_proto_slice(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto([m, m])
    result = expr_eval.eval(kde.proto.get_proto_full_name(kd_m))
    testing.assert_equal(
        result.no_bag(),
        ds([
            'koladata.functions.testing.MessageA',
            'koladata.functions.testing.MessageA',
        ]),
    )

  def test_from_proto_object_slice(self):
    m1 = test_pb2.MessageA(some_text='hello')
    m2 = test_pb2.MessageB(text='bar')
    kd_m = ds([
        fns.obj(fns.from_proto(m1)),
        fns.obj(fns.from_proto(m2)),
    ])
    result = expr_eval.eval(kde.proto.get_proto_full_name(kd_m))
    testing.assert_equal(
        result.no_bag(),
        ds([
            'koladata.functions.testing.MessageA',
            'koladata.functions.testing.MessageB',
        ]),
    )

  def test_from_proto_schema(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto(m)
    result = expr_eval.eval(kde.proto.get_proto_full_name(kd_m.get_schema()))
    testing.assert_equal(
        result.no_bag(), ds('koladata.functions.testing.MessageA')
    )

  def test_from_proto_schema_slice(self):
    m = test_pb2.MessageA(some_text='hello')
    kd_m = fns.from_proto(m)
    result = expr_eval.eval(
        kde.proto.get_proto_full_name(
            ds([kd_m.get_schema(), kd_m.get_schema()])
        )
    )
    testing.assert_equal(
        result.no_bag(),
        ds([
            'koladata.functions.testing.MessageA',
            'koladata.functions.testing.MessageA',
        ]),
    )

  def test_entity_not_from_proto(self):
    # Regular entity DataSlice
    result = expr_eval.eval(kde.proto.get_proto_full_name(fns.new()))
    testing.assert_equal(result.no_bag(), ds(None))

  def test_none_not_from_proto(self):
    result = expr_eval.eval(kde.proto.get_proto_full_name(None))
    testing.assert_equal(result.no_bag(), ds(None))

  def test_non_entity(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('primitives do not have attributes')
    ):
      expr_eval.eval(kde.proto.get_proto_full_name(123))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.proto.get_proto_full_name,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.proto.get_proto_full_name(I.x)))

  def test_repr(self):
    self.assertEqual(
        repr(kde.proto.get_proto_full_name(I.x)),
        'kd.proto.get_proto_full_name(I.x)',
    )


if __name__ == '__main__':
  absltest.main()
