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

"""Initializes Koda operators."""

from arolla import arolla
from koladata.operators import cc_operators_py_clib as _
from koladata.operators import py as _

# The Arolla operators are coming from :cc_operators package, importing this
# module in order to register Koda-specific parts like view and repr.
from koladata.operators import operators_in_cc_operator_package as _  # pylint: disable=g-bad-import-order


kde = arolla.OperatorsContainer(
    unsafe_extra_namespaces=[
        # go/keep-sorted start
        'kd.allocation',
        'kd.annotation',
        'kd.assertion',
        'kd.bags',
        'kd.bitwise',
        'kd.comparison',
        'kd.core',
        'kd.curves',
        'kd.dicts',
        'kd.entities',
        'kd.extension_types',
        'kd.functor',
        'kd.ids',
        'kd.iterables',
        'kd.json',
        'kd.lists',
        'kd.masking',
        'kd.math',
        'kd.objs',
        'kd.proto',
        'kd.py',
        'kd.random',
        'kd.schema',
        'kd.shapes',
        'kd.slices',
        'kd.streams',
        'kd.strings',
        'kd.tuples',
        # go/keep-sorted end
    ]
).kd

internal = arolla.OperatorsContainer(
    unsafe_extra_namespaces=[
        # go/keep-sorted start
        'koda_internal',
        'koda_internal.functor',
        'koda_internal.iterables',
        'koda_internal.parallel',
        'koda_internal.view',
        # go/keep-sorted end
    ]
).koda_internal
