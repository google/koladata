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

"""Interface to the koladata internal pseudo-random functions."""

import os
from koladata.ext.pseudo_random import clib

next_uint64 = clib.next_uint64
next_fingerprint = clib.next_fingerprint
epoch_id = clib.epoch_id


def reseed(entropy: int | bytes | None = None) -> None:
  """Reseeds the pseudo-random sequence.

  A successful call affects all subsequent calls to `next_uint64()`,
  `next_fingerprint()`, and `epoch_id()`. During the execution of this function,
  concurrent calls to these functions may return inconsistent or non-random
  values.

  The reseeding mechanism is primarily intended for cases where a process is
  snapshotted and respawned multiple times. In such scenarios, multiple
  instances sharing the same origin will also share the same pseudo-random
  sequence, which can lead to collisions.

  NOTE: This function does not guarantee that the resulting sequence will be
  reproducible, even if the same `entropy` is used. It only guarantees that
  distinct `entropy` values will produce distinct sequences.

  Args:
    entropy: Entropy to use for reseeding; either an integer or a byte string.
      If not provided, uses `os.urandom(16)`.
  """
  if isinstance(entropy, int):
    entropy = entropy.to_bytes(
        1 + entropy.bit_length() // 8, byteorder='little'
    )
  elif entropy is None:
    entropy = os.urandom(16)
  clib.reseed(entropy)
