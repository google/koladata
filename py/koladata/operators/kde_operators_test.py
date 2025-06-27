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

import types

from absl.testing import absltest
from arolla import arolla
from koladata.functions import functions
from koladata.operators import kde_operators

kde = kde_operators.kde

FUNCTIONS_NOT_PRESENT_IN_KDE_OPERATORS = frozenset([
    # Mutating functions.
    'del_attr',
    'set_attr',
    'set_attrs',
    'set_schema',
    'embed_schema',
    'update_schema',
    # Serialization and conversion.
    'dumps',
    'loads',
    'from_proto',
    'from_py',
    'from_pytree',
    'py_reference',
    'to_proto',
    'to_py',
    'to_pylist',
    'to_pytree',
    'schema_from_proto',
    'schema_from_py',
    'schema.schema_from_py',
    # Runtime type checking.
    'is_expr',
    'is_item',
    'is_slice',
    # Waits on the parallel execution result in Python.
    'parallel.call_multithreaded',
    'parallel.yield_multithreaded',
    # Misc.
    'container',
    'core.container',
    'dir',
    'list',  # TODO: Remove this once `list` is added to `kde`.
])


class KdeOperatorsTest(absltest.TestCase):

  def test_ops_in_qualified_namespace(self):
    # Tests that all `kde` operators are present in a subnamespace of
    # `kde` as well.
    kde_top_ops = set()
    subnamespace_op_fingerprints = set()
    for op_name in arolla.abc.list_registered_operators():
      op = arolla.abc.lookup_operator(op_name)
      split_name = op_name.split('.')
      if op_name.startswith('kde.') and len(split_name) == 2:
        kde_top_ops.add(op)
      else:
        fp = arolla.abc.decay_registered_operator(op).fingerprint
        subnamespace_op_fingerprints.add(fp)

    has_corresponding_op = set()
    for op in kde_top_ops:
      fp = arolla.abc.decay_registered_operator(op).fingerprint
      if fp in subnamespace_op_fingerprints:
        has_corresponding_op.add(op)
    self.assertEmpty(
        kde_top_ops - has_corresponding_op,
        msg=(
            'operators directly in `kde` namespace without a corresponding'
            ' operator in a subnamespace'
        ),
    )

  def test_all_functions_in_kde_operators(self):
    sub_namespace_functions = set()
    for ns_name in dir(functions):
      ns = getattr(functions, ns_name)
      if isinstance(ns, types.SimpleNamespace):
        sub_namespace_functions.update(((ns_name, f) for f in dir(ns)))

    missing_list = []
    for f in dir(functions):
      if (
          isinstance(getattr(functions, f), types.SimpleNamespace)
          or f.startswith('_')
          or f in FUNCTIONS_NOT_PRESENT_IN_KDE_OPERATORS
      ):
        continue
      if not hasattr(kde, f):
        missing_list.append(f)

    for ns, f in sub_namespace_functions:
      full_name = '%s.%s' % (ns, f)
      if (
          f.startswith('_')
          or full_name in FUNCTIONS_NOT_PRESENT_IN_KDE_OPERATORS
      ):
        continue
      if isinstance(getattr(getattr(functions, ns), f), types.SimpleNamespace):
        self.fail("This test doesn't expect nested namespaces.")

      if not hasattr(getattr(kde, ns), f):
        missing_list.append(full_name)

    self.assertEmpty(
        missing_list,
        msg=(
            'operators in functions namespace but not in kde_operators:'
            f' {missing_list}'
        ),
    )


if __name__ == '__main__':
  absltest.main()
