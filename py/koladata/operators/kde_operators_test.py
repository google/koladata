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

"""Tests for kde_operators."""

import collections

from absl.testing import absltest
from arolla import arolla
from koladata.operators import kde_operators

kde = kde_operators.kde


class KdeOperatorsTest(absltest.TestCase):

  def test_ops_in_qualified_namespace(self):
    # Tests that all `kde` operators are present in a subnamespace of
    # `kde` as well.
    kde_top_ops = set()
    op_from_basic_name = collections.defaultdict(set)
    for op_name in arolla.abc.list_registered_operators():
      op = arolla.abc.lookup_operator(op_name)
      split_name = op_name.split('.')
      if op_name.startswith('kde.') and len(split_name) == 2:
        kde_top_ops.add(op)
      op_from_basic_name[split_name[-1]].add(op)

    has_corresponding_op = set()
    for op in kde_top_ops:
      decayed_op = arolla.abc.decay_registered_operator(op)
      for corresponding_op in op_from_basic_name[
          op.display_name.split('.')[-1]
      ]:
        if (
            corresponding_op != op
            and arolla.abc.decay_registered_operator(corresponding_op)
            == decayed_op
        ):
          has_corresponding_op.add(op)
          break
    self.assertEmpty(
        kde_top_ops - has_corresponding_op,
        msg=(
            'operators directly in `kde` namespace without a corresponding'
            ' operator in a subnamespace'
        ),
    )


if __name__ == '__main__':
  absltest.main()
