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

"""py.* operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants

P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


def _expect_py_callable(param):
  """Returns a constraint that the argument is a python callable."""
  return (
      (param == arolla.abc.PY_OBJECT),
      (
          'expected a python callable, got'
          f' {arolla.optools.constraints.name_type_msg(param)}'
      ),
  )


def _expect_optional_py_callable(param):
  """Returns a constraint that the argument is a python callable or None.

  Important: The constraint `_expect_optional_py_callable(P.fn)` should be
  complemented with runtime checks:

    isinstance(fn, arolla.abc.PyObject)  # `fn` is a PY_OBJECT, presumably
                                         # holding a Python callable.

    (isinstance(fn, data_item.DataItem) and
     fn.get_schema() == schema_constants.NONE)  # `fn` is None.

  Args:
    param: A placeholder expr-node with the parameter name.

  Returns;
    A qtype constraint.
  """

  return (
      (param == arolla.abc.PY_OBJECT) | (param == qtypes.DATA_SLICE),
      (
          'expected a python callable, got'
          f' {arolla.optools.constraints.name_type_msg(param)}'
      ),
  )


@optools.add_to_registry(aliases=['kde.apply_py'])
@optools.as_lambda_operator(
    'kde.py.apply_py',
    qtype_constraints=[_expect_py_callable(P.fn)],
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
)
def apply_py(
    fn,
    args=py_boxing.var_positional(),
    return_type_as=py_boxing.keyword_only(data_slice.DataSlice),
    kwargs=py_boxing.var_keyword(),
):
  # pylint: disable=g-doc-args  # *args, **kwargs
  """Applies Python function `fn` on args.

  It is equivalent to fn(*args, **kwargs).

  Args:
    fn: function to apply to `*args` and `**kwargs`. It is required that this
      function returns a DataSlice/DataItem or a primitive that will be
      automatically wrapped into a DataItem.
    *args: positional arguments to pass to `fn`.
    return_type_as: The return type of the function is expected to be the same
      as the return type of this expression. In most cases, this will be a
      literal of the corresponding type. This needs to be specified if the
      function does not return a DataSlice/DataItem or a primitive that would be
      auto-boxed into a DataItem. kd.types.DataSlice and kd.types.DataBag can
      also be passed here.
    **kwargs: keyword arguments to pass to `fn`.

  Returns:
    Result of fn applied on the arguments.
  """

  @arolla.optools.as_py_function_operator(
      'kde.py.apply_py._impl',
      qtype_inference_expr=P.return_type_as,
  )
  def impl(fn, args, return_type_as, kwargs):
    del return_type_as  # Only used for type inference.
    fn = fn.py_value()
    return py_boxing.as_qvalue(fn(*args, **kwargs.as_dict()))

  return impl(fn, args, return_type_as, kwargs)


@optools.add_to_registry(aliases=['kde.apply_py_on_cond'])
@optools.as_lambda_operator(
    'kde.py.apply_py_on_cond',
    qtype_constraints=[
        _expect_py_callable(P.yes_fn),
        _expect_optional_py_callable(P.no_fn),
        qtype_utils.expect_data_slice(P.cond),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
)
def apply_py_on_cond(
    yes_fn,
    no_fn,
    cond,
    args=py_boxing.var_positional(),
    kwargs=py_boxing.var_keyword(),
):
  # pylint: disable=g-doc-args  # *args, **kwargs
  """Applies Python functions on args filtered with `cond` and `~cond`.

  It is equivalent to

    yes_fn(
        *( x & cond for x in args ),
        **{ k: (v & cond) for k, v in kwargs.items() },
    ) | no_fn(
        *( x & ~cond for x in args ),
        **{ k: (v & ~cond) for k, v in kwargs.items() },
    )

  Args:
    yes_fn: function to apply on filtered args.
    no_fn: function to apply on inverse filtered args (this parameter can be
      None).
    cond: filter dataslice.
    *args: arguments to filter and then pass to yes_fn and no_fn.
    **kwargs: keyword arguments to filter and then pass to yes_fn and no_fn.

  Returns:
    The union of results of yes_fn and no_fn applied on filtered args.
  """

  @arolla.optools.as_py_function_operator(
      'kde.py.apply_py_on_cond._impl', qtype_inference_expr=qtypes.DATA_SLICE
  )
  def impl(yes_fn, no_fn, cond, args, kwargs):
    yes_fn = yes_fn.py_value()

    if isinstance(no_fn, arolla.abc.PyObject):
      no_fn = no_fn.py_value()
    elif (
        isinstance(no_fn, data_item.DataItem)
        and no_fn.get_schema() == schema_constants.NONE
    ):
      no_fn = None
    else:
      raise TypeError(f'expected a python callable, got no_fn: {no_fn.qtype}')

    args = tuple(args)
    kwargs = kwargs.as_dict()

    result = yes_fn(
        *(x & cond for x in args), **{k: v & cond for k, v in kwargs.items()}
    )
    if no_fn is not None:
      inv_cond = ~cond
      result = result | no_fn(
          *(x & inv_cond for x in args),
          **{k: v & inv_cond for k, v in kwargs.items()},
      )
    return result

  return impl(yes_fn, no_fn, cond, args, kwargs)


@optools.add_to_registry(aliases=['kde.apply_py_on_selected'])
@optools.as_lambda_operator(
    'kde.py.apply_py_on_selected',
    qtype_constraints=[
        _expect_py_callable(P.fn),
        qtype_utils.expect_data_slice(P.cond),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
)
def apply_py_on_selected(
    fn,
    cond,
    args=py_boxing.var_positional(),
    kwargs=py_boxing.var_keyword(),
):
  # pylint: disable=g-doc-args  # *args, **kwargs
  """Applies Python function `fn` on args filtered with cond.

  It is equivalent to

    fn(
        *( x & cond for x in args ),
        **{ k: (v & cond) for k, v in kwargs.items() },
    )

  Args:
    fn: function to apply on filtered args.
    cond: filter dataslice.
    *args: arguments to filter and then pass to fn.
    **kwargs: keyword arguments to filter and then pass to fn.

  Returns:
    Result of fn applied on filtered args.
  """

  @arolla.optools.as_py_function_operator(
      'kde.py.apply_py_on_selected._impl',
      qtype_inference_expr=qtypes.DATA_SLICE,
  )
  def impl(fn, cond, args, kwargs):
    fn = fn.py_value()
    return fn(
        *(x & cond for x in args),
        **{k: v & cond for k, v in kwargs.as_dict().items()},
    )

  return impl(fn, cond, args, kwargs)
