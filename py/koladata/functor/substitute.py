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

"""Allows replacing variables in a functor subtree by name."""

from typing import Any

from koladata.functions import attrs
from koladata.functor import functor_factories
from koladata.functor.visitor import visitor
from koladata.operators import eager_op_utils
from koladata.types import data_item
from koladata.types import schema_constants
from koladata.types import signature_utils

_kd = eager_op_utils.operators_container('kd')
# We use this temporary schema to be able to check if default values
# exist when their schemas might be incompatible, and therefore just
# ".default_value" cannot be called on a slice of parameters.
_DEFAULT_VALUE_AS_OBJECT_SCHEMA = _kd.schema.new_schema(
    default_value=schema_constants.OBJECT
)


def assert_signatures_compatible(old_fn, new_fn):
  """Validates that new_fn is compatible with old_fn's signature.

  The signatures need not be equal, but compatible. New functor is allowed to be
  less restrictive than the old functor. This means that, if there is one way of
  invoking old_fn that would crash when calling new_fn, then the signatures are
  not compatible, and this function will raise an exception.

  This also means that the function is not commutative.
  assert_signatures_compatible(f1, f2) may be valid, while
  assert_signatures_compatible(f2, f1) may not be.

  Here are some rules:

  1) Moving a POSITIONAL_OR_KEYWORD argument to POSITIONAL_ONLY or KEYWORD_ONLY
  is not allowed, since it makes the signature more restrictive. For the same
  reason, every argument that can be passed as keyword in old_fn must be allowed
  to be passed as keyword in new_fn.

  Examples:
  - old_fn(x) -> new_fn(y)  # Not ok, new_fn(x=1) would crash.
  - old_fn(x) -> new_fn(x, /)  # Not ok, new_fn(x=1) would crash.
  - old_fn(x) -> new_fn(*, x)  # Not ok, new_fn(1) would crash.
  - old_fn(x, /) -> new_fn(y, /)  # OK, x is positional only, new_fn(1) works.
  - old_fn(*, x) -> new_fn(*, y)  # Not ok, new_fn(x=1) would crash.

  2) Adding an argument with a default value is allowed. Also, adding a default
  value to an existing argument is allowed, since it makes the signature less
  restrictive. Adding an argument without a default is not allowed, since it
  would make the signature more restrictive.

  Examples:
  - old_fn(x) -> new_fn(x, y)  # Not ok, new_fn(x) would crash.
  - old_fn(x) -> new_fn(x, y=100)  # OK.
  - old_fn(x, y) -> new_fn(x, y=100)  # OK.

  3) Removing an argument is never allowed, neither is removing a default value
  from an existing argument. Renaming an argument that is POSITIONAL_ONLY is
  fine, however.

  Examples:
  - old_fn(x, y) -> new_fn(x)  # Not ok, new_fn(1, 2) would crash.
  - old_fn(x, y=100) -> new_fn(x, y)  # Not ok, new_fn(1) would crash.
  - old_fn(x, y=100) -> new_fn(x)  # Not ok, new_fn(1, 2) would crash.
  - old_fn(x, /) -> new_fn(y, /)  # OK, x is positional only, new_fn(1) works.

  Args:
    old_fn: The original functor.
    new_fn: The new functor.

  Raises:
    ValueError: If the signatures are incompatible.
  """
  if not functor_factories.is_fn(new_fn):
    raise ValueError('replacement for subfunctor must be a functor')

  old_sig = functor_factories.get_signature(old_fn)
  new_sig = functor_factories.get_signature(new_fn)
  new_py_sig = signature_utils.to_inspect_signature(new_sig)

  kind = old_sig.parameters[:].kind
  positional_only = kind == signature_utils.ParameterKind.POSITIONAL_ONLY
  positional_or_kw = kind == signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
  var_positional = kind == signature_utils.ParameterKind.VAR_POSITIONAL
  kw_only = kind == signature_utils.ParameterKind.KEYWORD_ONLY
  var_keyword = kind == signature_utils.ParameterKind.VAR_KEYWORD
  has_default = ~(
      old_sig.parameters[:]
      .with_schema(_DEFAULT_VALUE_AS_OBJECT_SCHEMA)
      .default_value
      == signature_utils.NO_DEFAULT_VALUE.with_schema(schema_constants.OBJECT)
  )

  # The way we try to assert that signatures are compatible is by building two
  # extreme invocations:
  # 1) Pass positional-only arguments without default by position, and other
  #    arguments without default by name. "min" invocation.
  # 2) Pass all positional-only, positional-or-keyword and var-positional
  #    arguments by position, and all keyword-only and var-keyword arguments by
  #    name. "max" invocation.
  # If both invocations succeed, we consider the signatures to be compatible.
  # If there are extra or missing arguments in either call, this will raise an
  # exception, which we wrap in a ValueError and re-raise.

  def verify(num_positional_args, kw_arg_names):
    pos_args = [None] * num_positional_args
    kw_args = {p: None for p in kw_arg_names}
    try:
      new_py_sig.bind(*pos_args, **kw_args)
    except TypeError as e:
      raise ValueError(
          'Incompatible functor signatures when invoking with '
          f'{num_positional_args} positional args and {kw_arg_names} keyword '
          'args'
      ) from e

  min_positional_args = _kd.agg_count(positional_only & ~has_default).to_py()
  min_kw_arg_names = (
      old_sig.parameters[:]
      .select((positional_or_kw | kw_only) & ~has_default)
      .name.to_py()
  )
  verify(min_positional_args, min_kw_arg_names)

  max_positional_args = (
      _kd.agg_count(positional_only | positional_or_kw)
      + 10 * _kd.agg_count(var_positional)
  ).to_py()
  max_kw_arg_names = (old_sig.parameters[:].select(kw_only).name.to_py()) + [
      '__argument_that_does_not_exist__'
  ] * _kd.agg_count(var_keyword).to_py()
  verify(max_positional_args, max_kw_arg_names)


def sub(
    functor: Any,
    replacements: dict[Any, Any],
    *,
    ignore_signature_checks: bool = False,
) -> Any:
  """Replaces variables on the functor graph by their value or itemids.

  This function traverses the entire tree of functor and its subfunctors and
  replaces every occurrence of the keys in `replacements` with the corresponding
  value, returning a new, modified functor. All appearances of a given value as
  a variable in all sub-functors will be replaced.

  Example 1:
  f = kd.fn(lambda: kd.with_name(1, 'x') + kd.with_name(2, 'y'))
  f1 = kd.functor.sub(f, {1: 100})
  f()      # Returns 3
  f1()     # Returns 102

  Example 2:
  @kd.trace_as_fn()
  def mean(values):
    return kd.math.agg_mean(values)

  @kd.trace_as_fn()
  def harmonic_mean(values):
    return kd.math.agg_mean(values)

  @kd.fn
  def my_program():
    ...

  # Replaces all uses of arithmetic mean with harmonic mean in my_program and
  # its subfunctors.
  new_program = kd.functor.sub(my_program, {mean: harmonic_mean})

  If the value being replaced is not sufficiently unique (e.g. the number 1 in
  example 1) this may end up doing unintended replacements that are hard to
  debug. Consider this other example:

  @kd.trace_as_fn()
  def g(x):
    return x + kd.with_name(1, 'p')  # p has value 1

  @kd.fn
  def f(x):
    return g(x) * kd.with_name(1, 'q')  # q also has value 1

  f(1)  # returns (1 + 1) * 1 = 2
  f = substitute.sub(f, {f.g.p: 10})
  f(1)  # returns (1 + 10) * 10 = 110, not 11.

  Both f.q and f.g.p are named variables and have the same value, so both are
  replaced. Therefore, for safety, if you know exactly the variable you want to
  replace, it is safer to use with_attrs on the functor that needs the change.

  For example:
  f.with_attrs(foo=bar)  # simple and preferred: updates the outermost functor.
  f.updated(kd.attrs(f.child, qux=baz))  # updates a child functor, also good.

  However, if the change you need is in a deeply nested functor, or it is a
  variable that shows up in many places, this gets harder to write correctly. In
  this case, kd.functor.sub can help, provided that the original value is
  sufficiently unique.

  Beware when replacing DataSlices. Functors are represented as scalar objects,
  so all slices are imploded into lists during tracing, and exploded when read.
  Therefore, we disallow replacement values that are not scalar DataItems. If
  you need to replace a named DataSlice, pass a list as a replacement.

  Args:
    functor: the functor to be traversed.
    replacements: a mapping from original values to their replacements.
    ignore_signature_checks: If True, bypasses signature validation for
      subfunctor replacements.

  Returns:
    A copy of the input functor with variables replaced.

  Raises:
    ValueError if a functor is being replaced with a functor that doesn't have a
    compatible signature and ignore_signature_checks is True (the default). See
    `kd.functor.assert_signatures_compatible` for details.
  """
  def get_key(attr_val):
    return data_item.DataItem.from_vals(attr_val).with_schema(
        schema_constants.OBJECT
    )

  keys = {}
  vals = []
  for i, (k, replacement) in enumerate(replacements.items()):
    try:
      val = data_item.DataItem.from_vals(replacement)
    except ValueError as e:
      raise ValueError(
          f'replacement values must be scalars, got: {replacement}'
      ) from e
    keys[get_key(k)] = i
    vals.append(val)
  keys = _kd.dict(keys)

  def transform_callback(node, subvars):
    subvars = dict(**subvars)
    if not functor_factories.is_fn(node):
      return
    for attr_name in attrs.dir(node):
      attr_val = node.get_attr(attr_name)
      attr_key = get_key(attr_val)
      if attr_key not in keys:
        continue
      replacement = vals[keys[attr_key]]
      if functor_factories.is_fn(attr_val) and not ignore_signature_checks:
        assert_signatures_compatible(attr_val, replacement)
      subvars[attr_name] = replacement

    if subvars:
      node = node.with_attrs(**subvars)
    return node

  return visitor.visit_variables(functor, transform_callback)


def sub_by_name(
    functor: Any,
    replacements: dict[str, Any] | list[tuple[str, Any]],
    *,
    ignore_signature_checks: bool = False,
) -> Any:
  """Replaces variables on the functor graph, matching by their names.

  Traverses the entire graph of a non-recursive functor, including subfunctors,
  replacing variables on each functor with the provided replacements. Matching
  is done by name, and can match both variables like kd.V.x and subfunctors.

  Only variables are replaced. No inputs or arbitrary operations within the
  functor are performed. All values passed as replacement must be eager values.

  When replacing functors, we require that both the original and the replacement
  have a compatible signature, so that the program structure remains valid.

  Also, there must not be two different variables with the same name in the
  functor subtree. In this case, we raise a ValueError, because both would be
  pointing to the same new value, likely causing hard to debug issues.

  Examples:

  1. Replacing vars.
  f = kd.fn(lambda y: kd.with_name(1, 'x') + y)
  f_new = kd.functor.sub_by_name(f, {'x': 5})
  f_new(1)  # Returns 6

  2. Replacing subfunctors.
  @kd.trace_as_fn
  def double(x):
    return x * 2

  @kd.trace_as_fn()
  def halve(x):
    return x / 2

  f = kd.fn(lambda x: double(x) + 1)
  f_new = kd.functor.sub_by_name(f, {'double': kd.fn(halve)})
  f(10)      # Returns 21
  f_new(10)  # Returns 6.0 (with schema kd.FLOAT32)

  3. Variable name collision
  g = kd.fn(lambda y: kd.with_name(1, 'x') + y)
  h = kd.fn(lambda y: kd.with_name(1000, 'x') * y)
  f = kd.fn(lambda y: g(y) + h(y))
  kd.functor.sub_by_name(f, {'x': 5})  # ValueError: two different `x` found.

  Args:
    functor: The root functor to modify.
    replacements: A dictionary or list of pairs mapping names to their new
      values/functors.
    ignore_signature_checks: If True, bypasses signature validation for
      subfunctor replacements.

  Returns:
    The new functor with recursively updated dependencies.
  """
  subs = dict(replacements) if replacements else {}

  # Pass 1: Traversal & Validation
  # If there are two different variables with the same name in the tree, and
  # that name is being replaced, we raise a ValueError because both variables
  # would be pointing to the same replacement, which is probably not the
  # intention.
  names_to_fns = {}

  def validation_callback(node):
    for attr_name in attrs.dir(node):
      if attr_name not in subs:
        continue
      val = node.get_attr(attr_name)
      if attr_name in names_to_fns:
        prev = names_to_fns[attr_name]
        is_different_schema = val.get_schema() != prev.get_schema()
        if is_different_schema or not _kd.full_equal(val, prev):
          raise ValueError(
              f'different variables share the same name: {attr_name}: '
              f'{prev} vs {val}'
          )
      else:
        names_to_fns[attr_name] = val
    return None

  visitor.visit_subfunctors(functor, validation_callback)

  # Pass 2: Substitution
  def transform_callback(node, subvars):
    subvars = dict(**subvars)
    if not functor_factories.is_fn(node):
      return
    for attr_name in attrs.dir(node):
      if attr_name not in subs:
        continue
      replacement = subs[attr_name]
      attr_val = node.get_attr(attr_name)
      if functor_factories.is_fn(attr_val) and not ignore_signature_checks:
        assert_signatures_compatible(attr_val, replacement)
      subvars[attr_name] = replacement

    if subvars:
      node = node.with_attrs(**subvars)
    return node

  return visitor.visit_variables(functor, transform_callback)
