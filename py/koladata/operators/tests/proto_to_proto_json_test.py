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
from koladata.types import data_slice
from koladata.types import qtypes


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class ProtoToProtoJsonTest(parameterized.TestCase):

  def test_none(self):
    result = expr_eval.eval(
        kde.proto.to_proto_json(None, 'koladata.functions.testing.MessageA')
    )
    self.assertIsNone(result.to_py(), None)

  def test_empty(self):
    result = expr_eval.eval(
        kde.proto.to_proto_json(
            fns.new(), 'koladata.functions.testing.MessageA'
        )
    )
    self.assertEqual(result.to_py(), '{}')

  def test_nonempty(self):
    result = expr_eval.eval(
        kde.proto.to_proto_json(
            fns.new(some_text='xyz'),
            'koladata.functions.testing.MessageA',
        )
    )
    self.assertEqual(result.to_py(), '{"someText":"xyz"}')

  def test_nonempty_extension(self):
    m = test_pb2.MessageA()
    m.Extensions[test_pb2.MessageAExtension.message_a_extension].extra = 1
    result = expr_eval.eval(
        kde.proto.to_proto_json(
            fns.from_proto(
                m,
                extensions=[
                    '(koladata.functions.testing.MessageAExtension.message_a_extension)'
                ],
            ),
            'koladata.functions.testing.MessageA',
        )
    )
    self.assertEqual(
        result.to_py(),
        '{"[koladata.functions.testing.MessageAExtension.message_a_extension]":{"extra":1}}',
    )

  def test_sparse_jagged_input(self):
    x = fns.new()
    result = expr_eval.eval(
        kde.proto.to_proto_json(
            ds([[x, x], [x, None, x]]),
            'koladata.functions.testing.MessageA',
        )
    )
    self.assertEqual(result.to_py(), [['{}', '{}'], ['{}', None, '{}']])

  def test_proto_path_not_found(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.proto.to_proto_json: proto message `not.a.Message` not'
            ' found in C++ generated descriptor pool'
        ),
    ):
      expr_eval.eval(kde.proto.to_proto_json(fns.new(), 'not.a.Message'))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.proto.to_proto_json,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.proto.to_proto_json(I.x, I.proto_path))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.proto.to_proto_json, kde.to_proto_json
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.proto.to_proto_json(I.x, I.proto_path)),
        'kd.proto.to_proto_json(I.x, I.proto_path)',
    )


if __name__ == '__main__':
  absltest.main()
