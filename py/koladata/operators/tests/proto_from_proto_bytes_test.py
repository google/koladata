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

import itertools
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions.tests import test_cc_proto_py_ext as _
from koladata.functions.tests import test_pb2
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


QTYPES = frozenset(
    (
        qtypes.DATA_SLICE,
        qtypes.DATA_SLICE,
        *args,
        qtypes.NON_DETERMINISTIC_TOKEN,
        qtypes.DATA_SLICE,
    )
    for args in itertools.product(
        [qtypes.DATA_SLICE, arolla.UNSPECIFIED], repeat=4
    )
)


class FromProtoBytesTest(parameterized.TestCase):

  def test_none_input(self):
    result = expr_eval.eval(
        kde.proto.from_proto_bytes(None, 'koladata.functions.testing.MessageA')
    )
    self.assertIsNone(result.to_pytree())
    self.assertFalse(result.get_bag().is_mutable())

  def test_empty_input(self):
    result = expr_eval.eval(
        kde.proto.from_proto_bytes(b'', 'koladata.functions.testing.MessageA')
    )
    self.assertEqual(result.to_pytree(), {})

  def test_nonempty_input(self):
    result = expr_eval.eval(
        kde.proto.from_proto_bytes(
            test_pb2.MessageA(some_text='xyz').SerializeToString(),
            'koladata.functions.testing.MessageA',
        )
    )
    self.assertEqual(result.to_pytree(), {'some_text': 'xyz'})

  def test_sparse_jagged_input(self):
    result = expr_eval.eval(
        kde.proto.from_proto_bytes(
            ds([[b'', b''], [b'', None, b'']]),
            'koladata.functions.testing.MessageA',
        )
    )
    self.assertEqual(result.to_pytree(), [[{}, {}], [{}, None, {}]])

  def test_schema_itemid(self):
    itemid = kde.to_itemid(kde.new()).eval()
    result = expr_eval.eval(
        kde.proto.from_proto_bytes(
            b'',
            'koladata.functions.testing.MessageA',
            schema=schema_constants.OBJECT,
            itemids=itemid,
        )
    )
    self.assertEqual(result.to_pytree(), {})
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    self.assertEqual(kde.to_itemid(result).eval(), itemid)

  def test_extensions(self):
    m = test_pb2.MessageA()
    m.Extensions[test_pb2.MessageAExtension.message_a_extension].extra = 2

    result = expr_eval.eval(
        kde.proto.from_proto_bytes(
            m.SerializeToString(),
            'koladata.functions.testing.MessageA',
            extensions=ds([
                '(koladata.functions.testing.MessageAExtension.message_a_extension)'
            ]),
        )
    )
    self.assertEqual(
        result.to_pytree(),
        {
            '(koladata.functions.testing.MessageAExtension.message_a_extension)': {
                'extra': 2
            }
        },
    )

  def test_proto_path_not_found(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.proto.from_proto_bytes: kd.proto._from_proto_bytes: proto'
            ' message `not.a.Message` not found in C++ generated descriptor'
            ' pool'
        ),
    ):
      expr_eval.eval(kde.proto.from_proto_bytes(b'', 'not.a.Message'))

  def test_parse_failure(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.proto.from_proto_bytes: kd.proto._from_proto_bytes: failed to'
            ' parse input as a binary proto of type'
            ' `koladata.functions.testing.MessageA`'
        ),
    ):
      expr_eval.eval(
          kde.proto.from_proto_bytes(
              b'asdf', 'koladata.functions.testing.MessageA'
          )
      )

  def test_parse_failure_on_invalid(self):
    result = expr_eval.eval(
        kde.proto.from_proto_bytes(
            ds([b'', b'asdf']),
            'koladata.functions.testing.MessageA',
            on_invalid=None,
        )
    )
    self.assertEqual(result.to_pytree(), [{}, None])

  def test_qtype_signatures(self):
    self.maxDiff = None
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.proto.from_proto_bytes,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.proto.from_proto_bytes(I.x, I.proto_path))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.proto.from_proto_bytes, kde.from_proto_bytes)
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            kde.proto.from_proto_bytes(
                I.x,
                I.proto_path,
                extensions=I.extensions,
                itemids=I.itemids,
                schema=I.schema,
                on_invalid=I.on_invalid,
            )
        ),
        (
            'kd.proto.from_proto_bytes(I.x, I.proto_path,'
            ' extensions=I.extensions, itemids=I.itemids, schema=I.schema,'
            ' on_invalid=I.on_invalid)'
        ),
    )


if __name__ == '__main__':
  absltest.main()
