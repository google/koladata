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
import traceback

from absl.testing import absltest
from arolla import arolla as _  # trigger InitArolla()
from koladata.functor import stack_trace
from koladata.functor import testing_clib


class ErrorConvertersTest(absltest.TestCase):

  def test_error_stack_trace_frame_conversion(self):
    def raise_error():
      raise ValueError('error message')

    try:
      testing_clib.reraise_error_with_stack_trace_frame(
          raise_error,
          stack_trace.create_stack_trace_frame(
              function_name='foo',
              file_name='bar.py',
              line_number=57,
              line_text='x = y // 0',
          ),
      )
    except ValueError as e:
      ex = e

    self.assertIn('error message', str(ex))
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))
    self.assertRegex(
        tb,
        'py/koladata/functor/error_converters_test.py", line .*, in'
        ' raise_error',
    )
    self.assertRegex(tb, 'File "bar.py", line 57, in foo')

  def test_error_with_incorrect_stack_trace_frame(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "invalid StackTraceFrame(status.code=9, status.message='error"
            " message', function_name='foo', file_name='bar.py',"
            " line_number=57, line_text='x = y // 0')"
        ),
    ):
      testing_clib.raise_error_with_incorrect_stack_trace_frame(
          'error message',
          stack_trace.create_stack_trace_frame(
              function_name='foo',
              file_name='bar.py',
              line_number=57,
              line_text='x = y // 0',
          ),
      )


if __name__ == '__main__':
  absltest.main()
