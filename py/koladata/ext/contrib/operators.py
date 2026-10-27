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

"""Contrib operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata import kd

M = arolla.M | jagged_shape.M

P = arolla.P
to_arolla_int64 = arolla.abc.lookup_operator('koda_internal.to_arolla_int64')
future_iterable_from_stream = arolla.abc.lookup_operator(
    'koda_internal.parallel.future_iterable_from_stream'
)


kd.optools.set_namespace_docstring('kd_ext.contrib', __doc__)


def _create_batched_cascade_step_fn_adapter():
  """Returns a `step_fn` adapter for the `batched_cascade` operator.

  The adapter is a functor with signature:
      _koda_internal_batch,     # a closed stream with the batch of items
      /,
      *,
      _koda_internal_step_fn,   # the original `step_fn`
      _koda_internal_defaults,  # namedtuple with empty iterables 'yields' and
                                # 'cascades'
      **_koda_internal_state    # state variables
  """
  # NOTE: Using `unsafe_blocking_wait` is safe since the batch is closed.
  batch = kd.lazy.streams.unsafe_blocking_wait(
      future_iterable_from_stream(kd.I['_koda_internal_batch'])
  )
  update = arolla.abc.bind_op(
      kd.lazy.functor.call_and_update_namedtuple,
      kd.I['_koda_internal_step_fn'],
      args=M.core.make_tuple(batch),
      namedtuple_to_update=M.namedtuple.union(
          kd.I['_koda_internal_state'], kd.I['_koda_internal_defaults']
      ),
      kwargs=kd.I['_koda_internal_state'],
      **kd.optools.unified_non_deterministic_kwarg(),
  )
  parameter = kd.functor.signature_utils.parameter
  kind = kd.functor.signature_utils.ParameterKind
  return kd.functor.expr_fn(
      M.namedtuple.union(
          update,
          M.namedtuple.make(
              yields=kd.lazy.streams.from_iterable(
                  M.namedtuple.get_field(update, 'yields')
              ),
              cascades=kd.lazy.streams.from_iterable(
                  M.namedtuple.get_field(update, 'cascades')
              ),
          ),
      ),
      signature=kd.functor.signature_utils.signature([
          parameter('_koda_internal_batch', kind.POSITIONAL_ONLY),
          parameter('_koda_internal_step_fn', kind.KEYWORD_ONLY),
          parameter('_koda_internal_defaults', kind.KEYWORD_ONLY),
          parameter('_koda_internal_state', kind.VAR_KEYWORD),
      ]),
  )


def _create_parallel_batched_cascade_step_fn_adapter():
  """Returns a `step_fn` adapter for the parallel version of `batched_cascade`.

  The adapter is a functor with signature:
      _koda_internal_batch,     # a closed stream with the batch of items
      /,
      *,
      _koda_internal_transformed_step_fn,  # the parallel-transformed `step_fn`
      _koda_internal_executor,  # the executor to use for the computations
      _koda_internal_defaults,  # namedtuple with empty streams 'yields' and
                                # 'cascades'
      **_koda_internal_transformed_state    # the transformed state variables
  """
  parameter = kd.functor.signature_utils.parameter
  kind = kd.functor.signature_utils.ParameterKind
  return kd.functor.expr_fn(
      arolla.abc.bind_op(
          kd.lazy.functor.call_and_update_namedtuple,
          kd.I['_koda_internal_transformed_step_fn'],
          # NOTE: A stream is already the parallel form of an iterable.
          args=M.core.make_tuple(
              kd.I['_koda_internal_executor'], kd.I['_koda_internal_batch']
          ),
          namedtuple_to_update=M.namedtuple.union(
              kd.I['_koda_internal_transformed_state'],
              kd.I['_koda_internal_defaults'],
          ),
          kwargs=kd.I['_koda_internal_transformed_state'],
          **kd.optools.unified_non_deterministic_kwarg(),
      ),
      signature=kd.functor.signature_utils.signature([
          parameter('_koda_internal_batch', kind.POSITIONAL_ONLY),
          parameter('_koda_internal_transformed_step_fn', kind.KEYWORD_ONLY),
          parameter('_koda_internal_executor', kind.KEYWORD_ONLY),
          parameter('_koda_internal_defaults', kind.KEYWORD_ONLY),
          parameter('_koda_internal_transformed_state', kind.VAR_KEYWORD),
      ]),
  )


_BATCHED_CASCADE_STEP_FN_ADAPTER = _create_batched_cascade_step_fn_adapter()
_PARALLEL_BATCHED_CASCADE_STEP_FN_ADAPTER = (
    _create_parallel_batched_cascade_step_fn_adapter()
)


@kd.optools.as_backend_operator(
    'kd_ext.contrib._stream_batched_cascade',
    qtype_inference_expr=kd.lazy.streams.get_stream_qtype(P.initial_yields),
    deterministic=False,
)
def _stream_batched_cascade(
    executor, stream, step_fn, yields_param_name, initial_yields, initial_state
):
  """(internal) Writes the initial yields and the yields of all steps.

  The backend for kd_ext.contrib.batched_cascade; see StreamBatchedCascade() in
  //koladata/contrib/cascade.h.
  """
  del executor, stream, step_fn  # Unused.
  del yields_param_name, initial_yields, initial_state  # Unused.
  raise NotImplementedError('implemented in the backend')


@kd.optools.add_to_registry(via_cc_operator_package=True)
@kd.optools.as_lambda_operator(
    'kd_ext.contrib.batched_cascade',
    qtype_constraints=[
        kd.optools.constraints.expect_iterable(P.cascade),
        kd.optools.constraints.expect_data_slice(P.step_fn),
        kd.optools.constraints.expect_iterable(P.yields),
    ],
)
def batched_cascade(cascade, step_fn, /, *, yields, **initial_state):
  """Processes batches of items with `step_fn`, which can feed back new items.

  `step_fn(batch, **state)` is called sequentially on batches of items,
  threading `state` across calls. Each call must return a namedtuple
  (see `kd.namedtuple`) with updated `state` values. The namedtuple doesn't
  have to contain all `state` variables, only those that change. Additionally,
  it may contain the following fields:

    * `yields`: an iterable of items for the result; it must have the same
      type as the `yields` argument.
    * `cascades`: an iterable of new items (of the same type as the `cascade`
      argument), which are processed by `step_fn` as well.

  How items are grouped into batches is unspecified; only the relative order
  of items within the initial `cascade` and each returned `cascades` is
  preserved. Items from different cascades can be interleaved.

  The result chains the initial `yields` and the subsequent `yields` returned
  by `step_fn`.

  Example:
    kd_ext.contrib.batched_cascade(
        kd.iterables.make(2, 1),
        lambda batch: kd.namedtuple(
            yields=batch,
            cascades=kd.functor.flat_map_chain(
                batch,
                lambda n: kd.iterables.from_1d_slice(
                    kd.select(kd.stack(n - 1), n > 0)
                ),
            ),
        ),
        yields=kd.iterables.make(),
    )
    # -> An iterable with items 2, 1, 1, 0, 0 (batches [2, 1], [1, 0], [0]).

  Args:
    cascade: The input iterable.
    step_fn: A functor `step_fn(batch, **state)` that returns updates for the
      state and optional `yields` and `cascades`.
    yields: The initial value of `yields`; usually an empty iterable, e.g.
      `kd.iterables.make()`.
    **initial_state: The initial values of the state variables.

  Returns:
    An iterable that chains the items of all `yields`.
  """
  initial_state = arolla.optools.fix_trace_kwargs(initial_state)
  hidden_state = M.namedtuple.make(
      _koda_internal_step_fn=step_fn,
      _koda_internal_defaults=M.namedtuple.make(
          yields=kd.iterables.empty_as(yields),
          cascades=kd.iterables.empty_as(cascade),
      ),
  )
  # NOTE: Using `unsafe_blocking_wait` is safe since we use the eager executor,
  # which guarantees that the resulting stream is fully computed before the
  # operator returns.
  return kd.streams.unsafe_blocking_wait(
      future_iterable_from_stream(
          kd.streams.chain_from_stream(
              arolla.abc.bind_op(
                  _stream_batched_cascade,
                  kd.streams.get_eager_executor(),
                  kd.streams.from_iterable(cascade),
                  _BATCHED_CASCADE_STEP_FN_ADAPTER,
                  arolla.text('yields'),
                  kd.streams.from_iterable(yields),
                  M.namedtuple.union(initial_state, hidden_state),
                  kd.eager.optools.unified_non_deterministic_arg(),
              )
          )
      )
  )


# NOTE: This operator is the replacement for kd_ext.contrib.batched_cascade in
# the parallel transform, which is registered in
# //koladata/contrib/operators.cc.
@kd.optools.add_to_registry(via_cc_operator_package=True)
@kd.optools.as_lambda_operator(
    'kd_ext.contrib._parallel_batched_cascade',
    qtype_constraints=[
        kd.optools.constraints.expect_executor(P.executor),
        kd.optools.constraints.expect_data_slice(P.transformed_step_fn),
    ],
)
def _parallel_batched_cascade(
    executor, stream, transformed_step_fn, yields, initial_state
):
  """(internal) The parallel version of kd_ext.contrib.batched_cascade."""
  hidden_state = M.namedtuple.make(
      _koda_internal_transformed_step_fn=transformed_step_fn,
      _koda_internal_executor=executor,
      _koda_internal_defaults=M.namedtuple.make(
          yields=kd.streams.empty_as(yields),
          cascades=kd.streams.empty_as(stream),
      ),
  )
  return kd.streams.chain_from_stream(
      arolla.abc.bind_op(
          _stream_batched_cascade,
          executor,
          stream,
          _PARALLEL_BATCHED_CASCADE_STEP_FN_ADAPTER,
          arolla.text('yields'),
          yields,
          M.namedtuple.union(initial_state, hidden_state),
          kd.eager.optools.unified_non_deterministic_arg(),
      )
  )


@kd.optools.as_backend_operator('kd_ext.contrib._flatten_cyclic_references')
def _flatten_cyclic_references(x, max_recursion_depth, non_deterministic):  # pylint: disable=unused-argument
  """Creates a DataSlice with tree-like copy of the input DataSlice."""
  raise NotImplementedError('implemented in the backend')


@kd.optools.add_to_registry(via_cc_operator_package=True)
@kd.optools.as_lambda_operator(
    'kd_ext.contrib.flatten_cyclic_references',
    qtype_constraints=[
        kd.optools.constraints.expect_data_slice(P.x),
        kd.optools.constraints.expect_data_slice(P.max_recursion_depth),
    ],
)
def flatten_cyclic_references(x, *, max_recursion_depth):  # pylint: disable=unused-argument
  """Creates a DataSlice with tree-like copy of the input DataSlice.

  The entities themselves and all their attributes including both top-level and
  non-top-level attributes are cloned (with new ItemIds) while creating the
  tree-like copy. The max_recursion_depth argument controls the maximum number
  of times the same entity can occur on the path from the root to a leaf.
  Note: resulting DataBag might have an exponential size, compared to the input
  DataBag.
  Note: a slice of schemas is not supported as input.

  Args:
    x: DataSlice to flatten.
    max_recursion_depth: Maximum recursion depth.

  Returns:
    A DataSlice with tree-like attributes structure.
  """
  max_recursion_depth = to_arolla_int64(max_recursion_depth)
  return _flatten_cyclic_references(
      P.x, max_recursion_depth, kd.eager.optools.unified_non_deterministic_arg()
  )


@kd.optools.add_to_registry(via_cc_operator_package=True)
@kd.optools.as_lambda_operator('kd_ext.contrib.value_counts')
def value_counts(x):
  """Returns Dicts mapping entries in `x` to their count over the last dim.

  Similar to Pandas' `value_counts`.

  The output is a `x.get_ndim() - 1`-dimensional DataSlice containing one
  Dict per aggregated row in `x`. Each Dict maps the values to the number of
  occurrences (as an INT64) in the final dimension.

  Example:
    x = kd.slice([[4, 3, 4], [None, 2], [2, 1, 4, 1], [None]])
    kd_ext.contrib.value_counts(x)
      # -> [Dict{4: 2, 3: 1}, Dict{2: 1}, Dict{2: 1, 1: 2, 4: 1}, Dict{}]

  Args:
    x: the non-scalar DataSlice to compute occurrences for.
  """
  grouped = kd.group_by(x)
  return kd.dict(kd.collapse(grouped), kd.agg_count(grouped))


@kd.optools.add_to_registry(via_cc_operator_package=True)
@kd.optools.as_backend_operator('kd_ext.contrib.sanitize_names')
def sanitize_names(x):
  """Returns a DataSlice which recursivelly sanitizes all attribute names.

  Invalid attribute names (those that are not valid Python identifiers) are
  replaced with valid ones. Invalid characters are replaced with '_', and
  modified names are all prefixed with "san_". Collisions are resolved by
  appending suffixes like '_0', '_1', etc.

  The sanitization is done per-schema: attributes on different schemas are
  sanitized independently without interfering with each other.

  The returned DataSlice preserves the original Object IDs; only attribute
  names are changed.

  Example:
    o = kd.obj(**{'#': 1, '?': 2})
    result = kd_ext.contrib.sanitize_names(o)
    # result has attributes 'san__' and 'san___0' (or similar)

  Args:
    x: DataSlice to sanitize.

  Returns:
    A new DataSlice with sanitized attribute names in a new immutable DataBag.
  """
  raise NotImplementedError('implemented in the backend')


@kd.optools.add_to_registry(via_cc_operator_package=True)
@kd.optools.as_backend_operator(
    'kd_ext.contrib.get_attr_or_item',
    qtype_constraints=[
        kd.optools.constraints.expect_data_slice(P.ds),
        kd.optools.constraints.expect_data_slice(P.key),
    ],
)
def get_attr_or_item(ds, key):  # pylint: disable=unused-argument
  """Looks up a scalar key in a DataSlice of entities, dicts, lists, or OBJECTs.

  - If `ds` has an explicit ENTITY, DICT, or LIST schema, performs standard
    attribute, dict-key, or list-index lookup (failing on schema violations).
  - If `ds` has NONE schema, returns it as is.
  - If `ds` has OBJECT schema, looks up `key` in each item where applicable:
    string keys in entities whose schema has such an attribute, integer keys in
    lists, and keys castable to the key schema in dicts. Items where `key` is
    not applicable result in missing values. `ds` must not contain primitives.
    The result schema is:
      - NONE if all values are missing,
      - the attribute / list item / dict value schema, if it is the same for
        all present values,
      - OBJECT otherwise (without implicit casting of primitives). In this
        case, schemas of the returned non-primitive values are embedded as
        `__schema__` into a new immutable DataBag, failing if some object gets
        conflicting schemas.

  Args:
    ds: DataSlice of entities, dicts, lists, or OBJECTs.
    key: Scalar DataItem key or index to look up.

  Returns:
    DataSlice of looked-up values with the same shape as `ds`.
  """
  raise NotImplementedError('implemented in the backend')
