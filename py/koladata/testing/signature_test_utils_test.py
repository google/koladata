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
from koladata.testing import signature_test_utils
from koladata.types import data_slice


ds = data_slice.DataSlice.from_vals


class SignatureTestUtilsTest(absltest.TestCase):

  def test_generate_method_function_signature_compatibility_cases(self):

    class Foo:

      def bar(self, x, y):
        pass

      def baz(self, x, y):
        pass

      def qux(self):
        pass

      def _internal(self, x):
        pass

    obj_with_methods = Foo()
    obj_with_functions = types.SimpleNamespace(
        bar=lambda a, x: None,
        qux=lambda a, x: None,
        _internal=lambda a, x: None,
    )

    self.assertCountEqual(
        signature_test_utils.generate_method_function_signature_compatibility_cases(
            obj_with_methods, obj_with_functions
        ),
        [
            dict(
                testcase_name='bar_arg_0',
                method_name='bar',
                param_index=0,
                obj_with_methods=obj_with_methods,
                obj_with_functions=obj_with_functions,
            ),
            dict(
                testcase_name='bar_arg_1',
                method_name='bar',
                param_index=1,
                obj_with_methods=obj_with_methods,
                obj_with_functions=obj_with_functions,
            ),
            dict(
                testcase_name='qux_arg_0',
                method_name='qux',
                param_index=0,
                obj_with_methods=obj_with_methods,
                obj_with_functions=obj_with_functions,
            ),
        ],
    )

    self.assertCountEqual(
        signature_test_utils.generate_method_function_signature_compatibility_cases(
            obj_with_methods, obj_with_functions, skip_methods={'bar'}
        ),
        [
            dict(
                testcase_name='qux_arg_0',
                method_name='qux',
                param_index=0,
                obj_with_methods=obj_with_methods,
                obj_with_functions=obj_with_functions,
            ),
        ],
    )

    self.assertCountEqual(
        signature_test_utils.generate_method_function_signature_compatibility_cases(
            obj_with_methods, obj_with_functions, skip_params={('bar', 0)}
        ),
        [
            dict(
                testcase_name='bar_arg_1',
                method_name='bar',
                param_index=1,
                obj_with_methods=obj_with_methods,
                obj_with_functions=obj_with_functions,
            ),
            dict(
                testcase_name='qux_arg_0',
                method_name='qux',
                param_index=0,
                obj_with_methods=obj_with_methods,
                obj_with_functions=obj_with_functions,
            ),
        ],
    )

  def test_check_method_function_signature_compatibility(self):

    class Foo:

      def bar(self, x, name1, *, y=1, z=3, t=ds([1, 2, 3]), u=5, v=6):
        pass

      def baz(self):
        pass

    obj_with_methods = Foo()
    obj_with_functions = types.SimpleNamespace(
        bar=lambda a, x, name2, y=1, *, z=2, t=ds([1, 2, 3]), u=ds(5): None,
        baz=lambda a, x: None,
    )

    signature_test_utils.check_method_function_signature_compatibility(
        self,
        obj_with_methods=obj_with_methods,
        obj_with_functions=obj_with_functions,
        method_name='bar',
        param_index=0,
    )
    with self.assertRaisesRegex(
        AssertionError,
        'name1',
    ):
      signature_test_utils.check_method_function_signature_compatibility(
          self,
          obj_with_methods=obj_with_methods,
          obj_with_functions=obj_with_functions,
          method_name='bar',
          param_index=1,
      )
    with self.assertRaisesRegex(
        AssertionError,
        'KEYWORD_ONLY.*POSITIONAL_OR_KEYWORD',
    ):
      signature_test_utils.check_method_function_signature_compatibility(
          self,
          obj_with_methods=obj_with_methods,
          obj_with_functions=obj_with_functions,
          method_name='bar',
          param_index=2,
      )
    with self.assertRaisesRegex(
        AssertionError,
        '3 != 2',
    ):
      signature_test_utils.check_method_function_signature_compatibility(
          self,
          obj_with_methods=obj_with_methods,
          obj_with_functions=obj_with_functions,
          method_name='bar',
          param_index=3,
      )
    signature_test_utils.check_method_function_signature_compatibility(
        self,
        obj_with_methods=obj_with_methods,
        obj_with_functions=obj_with_functions,
        method_name='bar',
        param_index=4,
    )
    with self.assertRaisesRegex(
        AssertionError,
        'Either both or none.*must be QValues',
    ):
      signature_test_utils.check_method_function_signature_compatibility(
          self,
          obj_with_methods=obj_with_methods,
          obj_with_functions=obj_with_functions,
          method_name='bar',
          param_index=5,
      )
    with self.assertRaisesRegex(
        AssertionError,
        'Different number of parameters',
    ):
      signature_test_utils.check_method_function_signature_compatibility(
          self,
          obj_with_methods=obj_with_methods,
          obj_with_functions=obj_with_functions,
          method_name='bar',
          param_index=6,
      )
    with self.assertRaisesRegex(
        AssertionError,
        'Different number of parameters',
    ):
      signature_test_utils.check_method_function_signature_compatibility(
          self,
          obj_with_methods=obj_with_methods,
          obj_with_functions=obj_with_functions,
          method_name='baz',
          param_index=0,
      )


if __name__ == '__main__':
  absltest.main()
