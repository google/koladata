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

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd  # pylint: disable=unused-import
from koladata.ext.view import kv
from koladata.operators.tests.testdata import lists_implode_testdata


def _nested_list_to_tuple(x, depth):
  if depth == 0:
    return x
  return tuple(_nested_list_to_tuple(y, depth - 1) for y in x)


class ImplodeTest(parameterized.TestCase):

  @parameterized.parameters(*lists_implode_testdata.TEST_CASES)
  def test_call(self, x, ndim, expected):
    x = kv.view(x.to_py(max_depth=-1)).explode(x.get_ndim())
    expected = kv.view(expected.to_py(max_depth=-1)).explode(
        expected.get_ndim()
    )
    # kv.implode creates tuples, while expected.to_py() creates lists. We want
    # to avoid comparing implode output with implode output, so we convert lists
    # to tuples manually.
    expected = expected.map(
        lambda y: _nested_list_to_tuple(y, ndim if ndim >= 0 else x.get_depth())
    )

    res = kv.implode(x, ndim=ndim)
    self.assertEqual(res.get(), expected.get())
    self.assertEqual(res.get_depth(), expected.get_depth())

  def test_error(self):
    x = [[[1, 2], [3]], [[4, None]]]
    view_3d = kv.view(x)[:][:][:]
    with self.assertRaisesRegex(
        ValueError,
        'Cannot implode by 4 dimensions, the shape has only 3 dimensions.',
    ):
      _ = kv.implode(view_3d, ndim=4)

  def test_auto_boxing(self):
    with self.assertRaisesRegex(ValueError, 'has only 0 dimensions'):
      _ = kv.implode(None)
    with self.assertRaisesRegex(ValueError, 'Cannot automatically box'):
      _ = kv.implode([1, 2])  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
