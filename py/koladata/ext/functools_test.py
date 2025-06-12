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
from koladata import kd
from koladata.ext import functools

MaybeEval = functools.MaybeEval


S = kd.S
kdf = kd.functor


class FunctoolsTest(absltest.TestCase):

  def test_maybe_eval(self):

    with self.subTest('simple'):
      self.assertIsNone(MaybeEval(None))
      kd.testing.assert_equal(
          MaybeEval(kd.slice([1, 2, 3])), kd.slice([1, 2, 3])
      )
      kd.testing.assert_equal(
          MaybeEval(S.x + S.y, root=kd.obj(x=1, y=2)).no_bag(), kd.item(3)
      )
      kd.testing.assert_equal(
          MaybeEval(kd.fn(S.x + S.y), kd.obj(x=1, y=2)).no_bag(), kd.item(3)
      )
      kd.testing.assert_equal(
          MaybeEval(lambda s: s.x + s.y, kd.obj(x=1, y=2)).no_bag(), kd.item(3)
      )

    with self.subTest('list'):
      self.assertEqual(MaybeEval([1, 2, 3]), [1, 2, 3])
      self.assertEqual(
          MaybeEval([S.x + S.y, 2, [S.x]], root=kd.obj(x=1, y=2)),
          [kd.item(3), 2, [kd.item(1)]],
      )

    with self.subTest('tuple'):
      self.assertEqual(
          MaybeEval((S.x + S.y, 2, S.x), root=kd.obj(x=1, y=2)),
          (kd.item(3), 2, kd.item(1)),
      )

    with self.subTest('dict'):
      d = MaybeEval(
          {'a': S.x, 'b': kd.slice([3, 2, 1])},
          root=kd.obj(x=kd.slice([1, 2, 3])),
      )
      kd.testing.assert_equal(d['a'].no_bag(), kd.slice([1, 2, 3]))
      kd.testing.assert_equal(d['b'].no_bag(), kd.slice([3, 2, 1]))


if __name__ == '__main__':
  absltest.main()
