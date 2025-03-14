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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functions.tests import test_cc_proto_py_ext as _
from koladata.functions.tests import test_pb2
from koladata.operators import kde_operators
from koladata.operators import optools
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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class ProtoSchemaFromProtoPathTest(parameterized.TestCase):

  def test_simple(self):
    result = expr_eval.eval(
        kde.proto.schema_from_proto_path('koladata.functions.testing.MessageA')
    )
    testing.assert_equal(
        result.no_bag(), fns.schema_from_proto(test_pb2.MessageA).no_bag()
    )
    self.assertFalse(result.get_bag().is_mutable())

  def test_extensions(self):
    result = expr_eval.eval(
        kde.proto.schema_from_proto_path(
            'koladata.functions.testing.MessageA',
            extensions=ds([
                '(koladata.functions.testing.MessageAExtension.message_a_extension)'
            ]),
        )
    )
    testing.assert_equal(
        result.no_bag(), fns.schema_from_proto(test_pb2.MessageA).no_bag()
    )
    testing.assert_equal(
        getattr(
            result,
            '(koladata.functions.testing.MessageAExtension.message_a_extension)',
        ).no_bag(),
        fns.schema_from_proto(test_pb2.MessageAExtension).no_bag(),
    )

  def test_proto_path_not_found(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.proto.schema_from_proto_path: proto message `not.a.Message` not'
            ' found in C++ generated descriptor pool'
        ),
    ):
      expr_eval.eval(kde.proto.schema_from_proto_path('not.a.Message'))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.proto.schema_from_proto_path,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.proto.schema_from_proto_path(I.proto_path))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.proto.schema_from_proto_path, kde.schema_from_proto_path
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            kde.proto.schema_from_proto_path(
                I.proto_path,
                extensions=I.extensions,
            )
        ),
        'kd.proto.schema_from_proto_path(I.proto_path,'
        ' extensions=I.extensions)',
    )


if __name__ == '__main__':
  absltest.main()
