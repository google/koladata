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

"""Tools to test method signature compatibility between related APIs."""

import inspect
from typing import Any, Collection, Iterator

from arolla import arolla
from koladata.testing import testing


def generate_method_function_signature_compatibility_cases(
    obj_with_methods: Any,
    obj_with_functions: Any,
    skip_methods: Collection[str] = tuple(),
    skip_params: Collection[tuple[str, int]] = tuple(),
) -> Iterator[dict[str, Any]]:
  """Generates test cases for checking compatibility of methods and functions.

  It is expected to be passed to @parameterized.named_parameters, and the
  test body is expected to call check_method_function_signature_compatibility.

  Only methods that are not private and that are present in both classes are
  considered. We skip comparing the first argument since it is expected to be
  different between method and function, it is called 'self' in the method but
  not in the function.

  Args:
    obj_with_methods: An object instance with methods.
    obj_with_functions: An object instance or module with functions.
    skip_methods: Names of methods to skip.
    skip_params: Pairs of (method_name, param_index) to skip. Param index refers
      to the 0-based index of the parameter in the method signature, which is 1
      less than the index in the function signature (because the method already
      has 'self' bound).

  Yields:
    Dicts with 'method_name' and 'param_index' keys.
  """
  # We use object.__dir__ here since dir() on DataSlice currently does something
  # different.
  # TODO: switch to dir() once DataSlice is fixed.
  for name in object.__dir__(obj_with_methods):
    if (
        name.startswith('_')
        or not hasattr(obj_with_functions, name)
        or name in skip_methods
    ):
      continue
    method = getattr(obj_with_methods, name)
    function = getattr(obj_with_functions, name)
    # We skip arg 0 in function since it is expected to be different between
    # method and function, it is called 'self' in the method and is already
    # bound. We iterate up to max of numbers of parameters, since we do not want
    # to raise here to avoid exceptions during test class definition.
    # Instead, check_method_function_signature_compatibility will raise
    # when it sees a param index that is out of bounds.
    for i in range(
        max(
            len(inspect.signature(method).parameters),
            len(inspect.signature(function).parameters) - 1,
        ),
    ):
      if (name, i) not in skip_params:
        yield {
            'testcase_name': f'{name}_arg_{i}',
            'method_name': name,
            'param_index': i,
            'obj_with_methods': obj_with_methods,
            'obj_with_functions': obj_with_functions,
        }


def check_method_function_signature_compatibility(
    testcase: Any,
    *,
    obj_with_methods: Any,
    obj_with_functions: Any,
    method_name: str,
    param_index: int,
):
  """Checks a single parameter compatibility between a method and a function.

  This function is expected to be called from a test body that is decorated with
  @parameterized.named_parameters with the output of
  generate_method_function_signature_compatibility_cases.

  Args:
    testcase: The test case to use for assertions.
    obj_with_methods: Class or module with methods (first argument is usually
      'self').
    obj_with_functions: Class or module with functions (first argument can be
      named arbitrarily).
    method_name: Name of the method to compare.
    param_index: Index of the parameter to compare.
  """
  method = getattr(obj_with_methods, method_name)
  function = getattr(obj_with_functions, method_name)
  method_signature = inspect.signature(method)
  function_signature = inspect.signature(function)
  if param_index >= len(method_signature.parameters) or param_index + 1 >= len(
      function_signature.parameters
  ):
    testcase.fail(
        'Different number of parameters.\n'
        f'Signature in method: {method_signature}\n'
        f'Signature in function: {function_signature}'
    )
  method_arg = list(method_signature.parameters.values())[param_index]
  function_arg = list(function_signature.parameters.values())[param_index + 1]

  testcase.assertEqual(method_arg.name, function_arg.name)
  testcase.assertEqual(method_arg.kind, function_arg.kind)
  if isinstance(function_arg.default, arolla.QValue) or isinstance(
      method_arg.default, arolla.QValue
  ):
    if not isinstance(function_arg.default, arolla.QValue) or not isinstance(
        method_arg.default, arolla.QValue
    ):
      # assert_equal would also fail but we want a nicer error message.
      testcase.fail(
          f'Either both or none of the defaults for arg [{method_arg.name}]'
          f' must be QValues.\nMethod has: {method_arg.default}\nFunction'
          f' has: {function_arg.default}'
      )
    testing.assert_equal(method_arg.default, function_arg.default)
  else:
    testcase.assertEqual(method_arg.default, function_arg.default)
