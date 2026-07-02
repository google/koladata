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

"""Execution correctness test for fn_to_py generated code."""

import glob
import importlib
import os

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd

_TEST_CASES_DIR = os.path.join(os.path.dirname(__file__), 'test_cases')
TEST_CASES = sorted([
    os.path.basename(f)[:-3]
    for f in glob.glob(os.path.join(_TEST_CASES_DIR, '*.py'))
    if not f.endswith('__init__.py')
])


class FnToPyExecutionTest(parameterized.TestCase):

  @parameterized.parameters(TEST_CASES)
  def test_execution(self, name):
    # 1. Import the test case module to get original MODEL and INPUTS
    module_name = f'koladata.ext.functor.testing.test_cases.{name}'
    try:
      test_case_module = importlib.import_module(module_name)
    except ImportError as e:
      self.fail(f'Could not import test case module {module_name}: {e}')

    original_functor = test_case_module.MODEL
    inputs_list = test_case_module.INPUTS

    # 2. Read the generated Python code
    output_path = os.path.join(
        os.path.dirname(__file__), 'reference', f'{name}.py.output'
    )
    with open(output_path, 'r') as f:
      generated_code = f.read()

    # 3. Execute the generated code
    exec_namespace = {}
    try:
      exec(generated_code, exec_namespace)  # pylint: disable=exec-used
    except Exception as e:  # pylint: disable=broad-except
      self.fail(f'Failed to execute generated code for {name}: {e}')

    self.assertIn(
        'top',
        exec_namespace,
        f"Generated code for {name} does not define 'top' function.",
    )
    py_top_fn = exec_namespace['top']

    # 4. Verify semantic equivalence
    for inputs in inputs_list:
      expected_output = original_functor(**inputs)
      actual_output = py_top_fn(**inputs)

      kd.testing.assert_equal(
          actual_output,
          expected_output,
          msg=f'Execution output mismatch for case {name} with inputs {inputs}',
      )


if __name__ == '__main__':
  absltest.main()
