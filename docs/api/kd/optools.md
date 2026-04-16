<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.optools API

Operator definition and registration tooling.


Subcategory | Description
----------- | ------------
[constraints](optools/constraints.md) | Operator argument type constraints.
[eager](optools/eager.md) | Eager operator utilities.




### `kd.optools.add_alias(name: str, alias: str, unsafe_override: bool = False, via_cc_operator_package: bool = False)` {#kd.optools.add_alias}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Adds an alias for an operator.</code></pre>

### `kd.optools.add_to_registry(name: str | None = None, *, aliases: Collection[str] = (), unsafe_override: bool = False, repr_fn: Callable[[Expr, NodeTokenView], ReprToken] = <default_op_repr>, via_cc_operator_package: bool = False)` {#kd.optools.add_to_registry}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Wrapper around Arolla&#39;s add_to_registry with Koda functionality.

Args:
  name: Optional name of the operator. Otherwise, inferred from the op.
  aliases: Optional aliases for the operator.
  unsafe_override: Whether to override an existing operator.
  repr_fn: Repr function for the operator and its aliases.
  via_cc_operator_package: If True, the operator will be only registered
    during koladata_cc_operator_package construction, and just looked up in
    the global registry during normal execution. Note that this flag does not
    set up any C++ operator package, this has to be done separately via
    building_cc_operator_package context manager and
    koladata_cc_operator_package BUILD rule.

Returns:
  Registered operator.</code></pre>

### `kd.optools.add_to_registry_as_overload(name: str | None = None, *, overload_condition_expr: Any, unsafe_override: bool = False, via_cc_operator_package: bool = False)` {#kd.optools.add_to_registry_as_overload}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda wrapper around Arolla&#39;s add_to_registry_as_overload.

Note that for e.g. `name = &#34;foo.bar.baz&#34;`, the wrapped operator will
be registered as an overload `&#34;baz&#34;` of the overloadable operator `&#34;foo.bar&#34;`.

Performs no additional Koda-specific registration.

Args:
  name: Optional name of the operator. Otherwise, inferred from the op.
  overload_condition_expr: Condition for the overload.
  unsafe_override: Whether to override an existing operator.
  via_cc_operator_package: If True, the operator will be only registered
    during koladata_cc_operator_package construction, and just looked up in
    the global registry during normal execution. Note that this flag does not
    set up any C++ operator package, this has to be done separately via
    building_cc_operator_package context manager and
    koladata_cc_operator_package BUILD rule.

Returns:
  A decorator that registers an overload for the operator with the
  corresponding name.</code></pre>

### `kd.optools.add_to_registry_as_overloadable(name: str, *, unsafe_override: bool = False, repr_fn: Callable[[Expr, NodeTokenView], ReprToken] = <default_op_repr>, aux_policy: str = 'koladata_classic_aux_policy', via_cc_operator_package: bool = False)` {#kd.optools.add_to_registry_as_overloadable}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda wrapper around Arolla&#39;s add_to_registry_as_overloadable.

Performs additional Koda-specific registration, such as setting the view and
repr function.

Args:
  name: Name of the operator.
  unsafe_override: Whether to override an existing operator.
  repr_fn: Repr function for the operator and its aliases.
  aux_policy: Aux policy for the operator.
  via_cc_operator_package: If True, the operator will be only registered
    during koladata_cc_operator_package construction, and just looked up in
    the global registry during normal execution. Note that this flag does not
    set up any C++ operator package, this has to be done separately via
    building_cc_operator_package context manager and
    koladata_cc_operator_package BUILD rule.

Returns:
  An overloadable registered operator.</code></pre>

### `kd.optools.as_backend_operator(name: str, *, qtype_inference_expr: Expr | QType = DATA_SLICE, qtype_constraints: Iterable[tuple[Expr, str]] = (), deterministic: bool = True, custom_boxing_fn_name_per_parameter: dict[str, str] | None = None, view: str | type[ExprView] = '') -> Callable[[function], BackendOperator]` {#kd.optools.as_backend_operator}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decorator for Koladata backend operators with a unified binding policy.

Args:
  name: The name of the operator.
  qtype_inference_expr: Expression that computes operator&#39;s output type.
    Argument types can be referenced using `arolla.P.arg_name`.
  qtype_constraints: List of `(predicate_expr, error_message)` pairs.
    `predicate_expr` may refer to the argument QType using
    `arolla.P.arg_name`. If a type constraint is not fulfilled, the
    corresponding `error_message` is used. Placeholders, like `{arg_name}`,
    get replaced with the actual type names during the error message
    formatting.
  deterministic: If set to False, a hidden parameter (with the name
    `optools.UNIFIED_NON_DETERMINISTIC_PARAM_NAME`) is added to the end of the
    signature. This parameter receives special handling by the binding policy
    implementation.
  custom_boxing_fn_name_per_parameter: A dictionary specifying a custom boxing
    function per parameter (constants with the boxing functions look like:
    `koladata.types.py_boxing.WITH_*`, e.g. `WITH_PY_FUNCTION_TO_PY_OBJECT`).
  view: The view for the for the operator, with the default being KodaView
    (supported values: &#39;&#39;|KodaView, &#39;base&#39;|BaseKodaView, &#39;arolla&#39;|ArollaView).

Returns:
  A decorator that constructs a backend operator based on the provided Python
  function signature.</code></pre>

### `kd.optools.as_lambda_operator(name: str, *, qtype_constraints: Iterable[tuple[Expr, str]] = (), deterministic: bool | None = None, custom_boxing_fn_name_per_parameter: dict[str, str] | None = None, suppress_unused_parameter_warning: bool = False, view: str | type[ExprView] = '') -> Callable[[function], LambdaOperator | RestrictedLambdaOperator]` {#kd.optools.as_lambda_operator}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decorator for Koladata lambda operators with a unified binding policy.

Args:
  name: The name of the operator.
  qtype_constraints: List of `(predicate_expr, error_message)` pairs.
    `predicate_expr` may refer to the argument QType using
    `arolla.P.arg_name`. If a type constraint is not fulfilled, the
    corresponding `error_message` is used. Placeholders, like `{arg_name}`,
    get replaced with the actual type names during the error message
    formatting.
  deterministic: If True, the resulting operator will be deterministic and may
    only use deterministic operators. If False, the operator will be declared
    non-deterministic. By default, the decorator attempts to detect the
    operator&#39;s determinism.
  custom_boxing_fn_name_per_parameter: A dictionary specifying a custom boxing
    function per parameter (constants with the boxing functions look like:
    `koladata.types.py_boxing.WITH_*`, e.g. `WITH_PY_FUNCTION_TO_PY_OBJECT`).
  suppress_unused_parameter_warning: If True, unused parameters will not cause
    a warning.
  view: The view for the for the operator, with the default being KodaView
    (supported values: &#39;&#39;|KodaView, &#39;base&#39;|BaseKodaView, &#39;arolla&#39;|ArollaView).

Returns:
  A decorator that constructs a lambda operator by tracing a Python function.</code></pre>

### `kd.optools.as_py_function_operator(name: str, *, qtype_inference_expr: Expr | QType = DATA_SLICE, qtype_constraints: Iterable[tuple[Expr, str]] = (), codec: bytes | None = None, deterministic: bool = True, custom_boxing_fn_name_per_parameter: dict[str, str] | None = None, view: str | type[ExprView] = '') -> Callable[[function], Operator]` {#kd.optools.as_py_function_operator}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a decorator for defining Koladata-specific py-function operators.

The decorated function should accept QValues as input and returns a single
QValue. Variadic positional and keyword arguments are passed as tuples and
dictionaries of QValues, respectively.

Importantly, it is recommended that the function on which the operator is
based be pure -- that is, deterministic and without side effects.
If the function is not pure, please specify deterministic=False.

Args:
  name: The name of the operator.
  qtype_inference_expr: expression that computes operator&#39;s output qtype; an
    argument qtype can be referenced as P.arg_name.
  qtype_constraints: QType constraints for the operator.
  codec: A PyObject serialization codec for the wrapped function, compatible
    with `arolla.types.encode_py_object`. The resulting operator is
    serializable only if the codec is specified.
  deterministic: Set this to `False` if the wrapped function is not pure
    (i.e., non-deterministic or has side effects).
  custom_boxing_fn_name_per_parameter: A dictionary specifying a custom boxing
    function per parameter (constants with the boxing functions look like:
    `koladata.types.py_boxing.WITH_*`, e.g. `WITH_PY_FUNCTION_TO_PY_OBJECT`).
  view: The view for the for the operator, with the default being KodaView
    (supported values: &#39;&#39;|KodaView, &#39;base&#39;|BaseKodaView, &#39;arolla&#39;|ArollaView).</code></pre>

### `kd.optools.as_qvalue(arg: Any) -> QValue` {#kd.optools.as_qvalue}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts Python values into QValues.</code></pre>

### `kd.optools.as_qvalue_or_expr(arg: Any) -> Expr | QValue` {#kd.optools.as_qvalue_or_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts Python values into QValues or Exprs.</code></pre>

### `kd.optools.equiv_to_op(this_op: Operator | str, that_op: Operator | str) -> bool` {#kd.optools.equiv_to_op}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true iff the impl of `this_op` equals the impl of `that_op`.</code></pre>

### `kd.optools.fix_non_deterministic_tokens(expr: Expr, *, param: Expr = L._koladata_non_deterministic_token_leaf) -> Expr` {#kd.optools.fix_non_deterministic_tokens}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an expression with derandomized non-deterministic tokens.

Its primary application is derandomizing expressions within operator
declarations, making operator definitions more reproducible.

This function only targets non-deterministic tokens; the rest of the
expression is left unchanged. Therefore, if other parts contain embedded
&#34;random&#34; values, the overall expression will remain &#34;random&#34;.

Importantly, derandomized expressions must not be directly combined with
each other, as this can easily cause seed collisions.

Args:
  expr: The expression to fix.
  param: The parameter to replace NON_DETERMINISTIC_TOKEN_LEAF with.</code></pre>

### `kd.optools.make_operators_container(*namespaces: str) -> OperatorsContainer` {#kd.optools.make_operators_container}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an OperatorsContainer for the given namespaces.

Note that the returned container accesses the global namespace. A common
pattern is therefore:
  foo = make_operators_container(&#39;foo&#39;, &#39;foo.bar&#39;, &#39;foo.baz&#39;).foo

Args:
  *namespaces: Namespaces to make available in the returned container.</code></pre>

### `kd.optools.unified_non_deterministic_arg() -> Expr` {#kd.optools.unified_non_deterministic_arg}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a non-deterministic token for use with `bind_op(..., arg)`.</code></pre>

### `kd.optools.unified_non_deterministic_kwarg() -> dict[str, Expr]` {#kd.optools.unified_non_deterministic_kwarg}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a non-deterministic token for use with `bind_op(..., **kwarg)`.</code></pre>

