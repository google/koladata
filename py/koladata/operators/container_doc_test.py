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

"""Tests for formatting help representation of Koda operator containers."""

import textwrap
import types

from absl.testing import absltest
from arolla import arolla
from koladata.operators import container_doc
from koladata.operators import eager_op_utils
from koladata.operators import optools


class ContainerDocTest(absltest.TestCase):

  def test_empty_container(self):
    obj = types.SimpleNamespace()
    self.assertEqual(container_doc.format_container_doc("empty", obj), "empty")

  def test_custom_doc(self):
    obj = types.SimpleNamespace()
    self.assertEqual(
        container_doc.format_container_doc(
            "custom", obj, doc="Custom docstring."
        ),
        "Custom docstring.",
    )

  def test_nested_namespaces_and_operators(self):
    ns_prefix = "container_doc_test_ns"
    optools.set_namespace_docstring(ns_prefix, "Overview doc.")
    optools.set_namespace_docstring(f"{ns_prefix}.sub_ns", "Sub namespace doc.")
    optools.set_namespace_docstring(
        f"{ns_prefix}.sub_ns_2", "Sub namespace 2 doc."
    )

    @arolla.optools.add_to_registry(if_present="unsafe_override")
    @arolla.optools.as_lambda_operator(f"{ns_prefix}.op_1")
    def op_1(x):
      """Doc 1."""
      return x

    @arolla.optools.add_to_registry(if_present="unsafe_override")
    @arolla.optools.as_lambda_operator(f"{ns_prefix}.op_2")
    def op_2(x):
      return x

    @arolla.optools.add_to_registry(if_present="unsafe_override")
    @arolla.optools.as_lambda_operator(f"{ns_prefix}.sub_ns.sub_op")
    def sub_op(x):
      return x

    @arolla.optools.add_to_registry(if_present="unsafe_override")
    @arolla.optools.as_lambda_operator(f"{ns_prefix}.sub_ns_2.sub_op")
    def sub_op_2(x):
      return x

    arolla_container = arolla.OperatorsContainer(
        unsafe_extra_namespaces=[
            ns_prefix,
            f"{ns_prefix}.sub_ns",
            f"{ns_prefix}.sub_ns_2",
        ]
    )
    kd_container = getattr(
        eager_op_utils.operators_container(
            top_level_arolla_container=arolla_container
        ),
        ns_prefix,
    )

    expected = textwrap.dedent("""\
        Overview doc.

        Nested namespaces:
         - sub_ns: Sub namespace doc.
         - sub_ns_2: Sub namespace 2 doc.

        Operators:
         - op_1(x): Doc 1.
         - op_2(x)""")
    self.assertEqual(
        container_doc.format_container_doc(ns_prefix, kd_container), expected
    )

  def test_other_entities(self):
    class FunctorFactory:
      """Factory class."""

      def __init__(self, fn):
        self.fn = fn

    def helper_fn(a, b=1):
      """Helper function."""
      return a + b

    class Const:
      pass

    class MockInputContainer:
      """Input container doc."""

      def __getattr__(self, key):
        return arolla.M.math.add

    obj = types.SimpleNamespace(
        my_fn=helper_fn,
        MyClass=FunctorFactory,
        MY_CONST=Const(),
        const_qval=arolla.int32(1),
        I=MockInputContainer(),
    )
    expected = textwrap.dedent("""\
        my_ns

        Other:
         - I: Input container doc.
         - MY_CONST
         - MyClass: Factory class.
         - const_qval: QValue specialization for integral qtypes.
         - my_fn(a, b=1): Helper function.""")
    self.assertEqual(container_doc.format_container_doc("my_ns", obj), expected)

  def test_mixed_container_with_overrides(self):
    ns_prefix = "container_doc_test_mixed"
    optools.set_namespace_docstring(ns_prefix, "Overview doc.")
    optools.set_namespace_docstring(f"{ns_prefix}.sub_ns", "Sub namespace doc.")

    @arolla.optools.add_to_registry(if_present="unsafe_override")
    @arolla.optools.as_lambda_operator(f"{ns_prefix}.op_1")
    def op_1(x):
      """Doc 1."""
      return x

    arolla_container = arolla.OperatorsContainer(
        unsafe_extra_namespaces=[ns_prefix, f"{ns_prefix}.sub_ns"]
    )
    kd_container = getattr(
        eager_op_utils.operators_container(
            top_level_arolla_container=arolla_container
        ),
        ns_prefix,
    )

    def my_decorator(fn):
      """A decorator override."""
      return fn

    kd_with_overrides = eager_op_utils.add_overrides(
        kd_container,
        types.SimpleNamespace(
            decorator=my_decorator,
        ),
    )

    expected = textwrap.dedent("""\
        Overview doc.

        Nested namespaces:
         - sub_ns: Sub namespace doc.

        Operators:
         - op_1(x): Doc 1.

        Other:
         - decorator(fn): A decorator override.""")
    self.assertEqual(
        container_doc.format_container_doc(ns_prefix, kd_with_overrides),
        expected,
    )

  def test_operator_lookup_in_registry(self):
    ns_prefix = "container_doc_test_registry"

    @arolla.optools.add_to_registry(if_present="unsafe_override")
    @arolla.optools.as_lambda_operator(f"{ns_prefix}.reg_op")
    def reg_op(x, y):
      """Registered operator docstring."""
      return x + y

    def local_callable(x, y):
      del x, y

    obj = types.SimpleNamespace(
        reg_op=local_callable,
    )

    expected = textwrap.dedent(f"""\
        {ns_prefix}

        Operators:
         - reg_op(x, y): Registered operator docstring.""")
    self.assertEqual(
        container_doc.format_container_doc(ns_prefix, obj),
        expected,
    )

  def test_unregistered_namespace_doc(self):
    ns_prefix = "container_doc_test_unregistered"

    @arolla.optools.add_to_registry(if_present="unsafe_override")
    @arolla.optools.as_lambda_operator(f"{ns_prefix}.op_1")
    def op_1(a, b):
      return a + b

    arolla_container = arolla.OperatorsContainer(
        unsafe_extra_namespaces=[ns_prefix]
    )
    kd_container = getattr(
        eager_op_utils.operators_container(
            top_level_arolla_container=arolla_container
        ),
        ns_prefix,
    )

    expected = textwrap.dedent(f"""\
        {ns_prefix}

        Operators:
         - op_1(a, b)""")
    self.assertEqual(
        container_doc.format_container_doc(ns_prefix, kd_container), expected
    )

  def test_overrides_doc_not_used(self):
    ns_prefix = "container_doc_test_overrides_doc_ignored"
    optools.set_namespace_docstring(ns_prefix, "Registered docstring.")
    obj = types.SimpleNamespace(__doc__="Overrides docstring.")
    self.assertEqual(
        container_doc.format_container_doc(ns_prefix, obj),
        "Registered docstring.",
    )

  def test_show_operators_false(self):
    ns_prefix = "container_doc_test_show_ops_false"
    optools.set_namespace_docstring(ns_prefix, "Overview doc.")
    optools.set_namespace_docstring(f"{ns_prefix}.sub_ns", "Sub namespace doc.")

    @arolla.optools.add_to_registry(if_present="unsafe_override")
    @arolla.optools.as_lambda_operator(f"{ns_prefix}.op_1")
    def op_1(x):
      return x

    def helper():
      pass

    arolla_container = arolla.OperatorsContainer(
        unsafe_extra_namespaces=[ns_prefix, f"{ns_prefix}.sub_ns"]
    )
    kd_container = getattr(
        eager_op_utils.operators_container(
            top_level_arolla_container=arolla_container
        ),
        ns_prefix,
    )
    kd_with_overrides = eager_op_utils.add_overrides(
        kd_container,
        types.SimpleNamespace(helper=helper),
    )

    expected = textwrap.dedent("""\
        Overview doc.

        Nested namespaces:
         - sub_ns: Sub namespace doc.

        Other:
         - helper()""")
    self.assertEqual(
        container_doc.format_container_doc(
            ns_prefix, kd_with_overrides, show_operators=False
        ),
        expected,
    )


if __name__ == "__main__":
  absltest.main()
