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

import functools
import inspect

from absl.testing import absltest
from absl.testing import parameterized
from koladata.functions import functions as fns
from koladata.functor import stack_trace
from koladata.functor import testing_clib
from koladata.testing import testing as kd_testing
from koladata.util import kd_functools


def simple_function(x):
  return x


@functools.lru_cache()
# Some extra comment to ignore.


# Empty lines above and another decorator below.
@functools.lru_cache()
def decorated_function(x):
  return x


class SomeClass:
  def simple_method(self, x):
    return x

  @classmethod
  def class_method(cls, x):
    return x

  @staticmethod
  def static_method(x):
    return x

  def __call__(self, x):
    return x


class StackTraceTest(parameterized.TestCase):

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    with open(__file__, 'r') as f:
      cls._current_file_lines = f.readlines()

  def test_create_stack_trace_frame(self):
    frame = stack_trace.create_stack_trace_frame(
        function_name='foo',
        file_name='bar.py',
        line_number=57,
        line_text='x = y + z',
    )
    kd_testing.assert_equal(frame.function_name.no_bag(), fns.str('foo'))
    kd_testing.assert_equal(frame.file_name.no_bag(), fns.str('bar.py'))
    kd_testing.assert_equal(frame.line_number.no_bag(), fns.int32(57))
    kd_testing.assert_equal(frame.line_text.no_bag(), fns.str('x = y + z'))
    another_frame = stack_trace.create_stack_trace_frame(
        function_name='bar', file_name='baz.py', line_number=57, line_text=''
    )
    kd_testing.assert_equivalent(
        frame.get_schema().extract(), another_frame.get_schema().extract()
    )

  @parameterized.parameters(
      dict(
          fn=simple_function,
          name_regex='simple_function',
          file=__file__,
          line='def simple_function(x):',
      ),
      dict(
          fn=decorated_function,
          name_regex='decorated_function',
          file=__file__,
          line='def decorated_function(x):',
      ),
      dict(
          fn=SomeClass.simple_method,
          name_regex='simple_method',
          file=__file__,
          line='def simple_method(self, x):',
      ),
      dict(
          fn=SomeClass().simple_method,
          name_regex='simple_method',
          file=__file__,
          line='def simple_method(self, x):',
      ),
      dict(
          fn=SomeClass.class_method,
          name_regex='class_method',
          file=__file__,
          line='def class_method(cls, x):',
      ),
      dict(
          fn=SomeClass.static_method,
          name_regex='static_method',
          file=__file__,
          line='def static_method(x):',
      ),
      dict(
          fn=SomeClass(),
          name_regex='<__main__.SomeClass object at 0x.*>',
          file=__file__,
          line='def __call__(self, x):',
      ),
      dict(
          fn=lambda x: x,
          name_regex='<lambda>',
          file=__file__,
          line='fn=lambda x: x,',
      ),
      dict(
          fn=str,
          name_regex='str',
          file=None,
          line=None,
      ),
      dict(
          fn=[].index,
          name_regex='index',
          file=None,
          line=None,
      ),
      dict(
          fn=testing_clib.some_pybind_function,
          name_regex='some_pybind_function',
          file=None,
          line=None,
      ),
  )
  def test_function_frame(self, fn, name_regex, file, line):
    frame = stack_trace.function_frame(fn)
    self.assertRegex(frame.function_name.to_py(), name_regex)
    kd_testing.assert_equal(
        frame.file_name.no_bag(), fns.str(file)
    )
    if line is not None:
      detected_line = self._current_file_lines[
          frame.line_number.to_py() - 1
      ].strip()
      self.assertEqual(detected_line, line)
    else:
      kd_testing.assert_equal(frame.line_number.no_bag(), fns.int32(None))

  def test_function_frame_not_a_function(self):
    frame = stack_trace.function_frame(57)  # pytype: disable=wrong-arg-types
    self.assertIsNone(frame)

  def test_current_frame(self):
    @kd_functools.skip_from_functor_stack_trace
    def foo():
      return stack_trace.current_frame()

    class Foo:
      @staticmethod
      @kd_functools.skip_from_functor_stack_trace
      def static_method():
        return foo()

      @kd_functools.skip_from_functor_stack_trace
      def method(self):
        return self.static_method()

    def bar():
      return Foo().method()

    def baz():
      return bar()

    frame = baz()
    self.assertEqual(frame.function_name.to_py(), 'bar')
    self.assertIn('stack_trace_test.py', frame.file_name.to_py())
    self.assertEqual(
        frame.line_number.to_py(),
        inspect.getsourcelines(bar)[1] + 1  # first line of the function.
    )
    self.assertEqual(frame.line_text.to_py(), '      return Foo().method()')

if __name__ == '__main__':
  absltest.main()
