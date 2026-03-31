<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.extension_types API

Extension type functionality.





### `kd.extension_types.NullableMixin()` {#kd.extension_types.NullableMixin}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Mixin class that adds nullability methods to the extension type.

Adds two methods:
  * `get_null` - Class method that returns a null instance of the extension
    type. Wrapper around `kd.extension_types.make_null`.
  * `is_null` - Returns present iff the object is null. Wrapper around
    `kd.extension_types.is_null`.

A null instance of an extension type has no attributes and calling `getattr`
or `with_attrs` on it will raise an error.

Example:
  @kd.extension_type()
  class A(kd.extension_types.NullableMixin):
    x: kd.INT32

  # Normal usage.
  a = A(1)
  a.x  # -&gt; 1.
  a.is_null()  # kd.missing

  # Null usage.
  a_null = A.get_null()
  a_null.x  # ERROR
  a_null.is_null()  # kd.present</code></pre>

### `kd.extension_types.dynamic_cast(value: Any, qtype: QType) -> Any` {#kd.extension_types.dynamic_cast}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Up-, down-, and side-casts `value` to `qtype`.</code></pre>

### `kd.extension_types.extension_type(unsafe_override=False) -> Callable[[type[Any]], type[Any]]` {#kd.extension_types.extension_type}
Aliases:

- [kd.extension_type](../kd.md#kd.extension_type)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a Koda extension type from the given original class.

This function is intended to be used as a class decorator. The decorated class
serves as a schema for the new extension type.

Internally, this function creates the following:
-  A new `QType` for the extension type, which is a labeled `QType` on top of
  an arolla::Object.
- A `QValue` class for representing evaluated instances of the extension type.
- An `ExprView` class for representing expressions that will evaluate to the
  extension type.

It replaces the decorated class with a new class that acts as a factory. This
factory&#39;s `__new__` method dispatches to either create an `Expr` or a `QValue`
instance, depending on the types of the arguments provided.

The fields of the dataclass are exposed as properties on both the `QValue` and
`ExprView` classes. Any methods defined on the dataclass are also carried
over.

Note:
- The decorated class must not have its own `__new__` method - it will be
  ignored.
- The decorated class must not have its own `__init__` method - it will be
  ignored.
- The type annotations on the fields of the dataclass are used to determine
  the schema of the underlying `DataSlice` (if relevant).
- All fields must have type annotations.
- Supported annotations include `SchemaItem`, `DataSlice`, `DataBag`,
  `JaggedShape`, and other extension types. Additionally, any QType can be
  used as an annotation.
- The `with_attrs` method is automatically added if not already present,
  allowing for attributes to be dynamically updated.
- If the class implements the `_extension_arg_boxing(self, value, annotation)`
  classmethod, it will be called on all input arguments (including defaults)
  passed to `MyExtension(...)`. The classmethod should return an arolla QValue
  or an Expression. If the class does not implement such a method, a default
  method will be used.
- If the class implements the `_extension_post_init(self)` method, it will be
  called as the final step of instantiating the extension through
  `MyExtension(...)`. The method should take `self`, do the necessary post
  processing, and then return the (potentially modified) `self`. As with other
  methods, it&#39;s required to be traceable in order to function in a tracing
  context.

Example:
  @extension_type()
  class MyPoint:
    x: kd.FLOAT32
    y: kd.FLOAT32

    def norm(self):
      return (self.x**2 + self.y**2)**0.5

  # Creates a QValue instance of MyPoint.
  p1 = MyPoint(x=1.0, y=2.0)

Extension type inheritance is supported through Python inheritance. Passing an
extension type argument to a functor will automatically upcast / downcast the
argument to the correct extension type based on the argument annotation. To
support calling a child class&#39;s methods after upcasting, the parent method
must be annotated with @kd.extension_types.virtual() and the child method
must be annotated with @kd.extension_types.override(). Internally, this traces
the methods into Functors. Virtual methods _require_ proper return
annotations (and if relevant, input argument annotations).

Example:
  @kd.extension_type(unsafe_override=True)
  class A:
    x: kd.INT32

    def fn(self, y):  # Normal method.
      return self.x + y

    @kd.extension_types.virtual()
    def virt_fn(self, y):  # Virtual method.
      return self.x * y

  @kd.extension_type(unsafe_override=True)
  class B(A):  # Inherits from A.
    y: kd.FLOAT32

    def fn(self, y):
      return self.x + self.y + y

    @kd.extension_types.override()
    def virt_fn(self, y):
      return self.x * self.y * y

  @kd.fn
  def call_a_fn(a: A):  # Automatically casts to A.
    return a.fn(4)      # Calls non-virtual method.

  @kd.fn
  def call_a_virt_fn(a: A):  # Automatically casts to A.
    return a.virt_fn(4)      # Calls virtual method.

  b = B(2, 3)
  # -&gt; 6. `fn` is _not_ marked as virtual, so the parent method is invoked.
  call_a_fn(b)
  # -&gt; 24.0. `virt_fn` is marked as virtual, so the child method is invoked.
  call_a_virt_fn(b)

Args:
  unsafe_override: Overrides existing registered extension types.

Returns:
  A new class that serves as a factory for the extension type.</code></pre>

### `kd.extension_types.get_annotations(cls: type[Any]) -> dict[str, Any]` {#kd.extension_types.get_annotations}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the annotations for the provided extension type class.</code></pre>

### `kd.extension_types.get_attr(ext: Any, attr: str | QValue, qtype: QType) -> Any` {#kd.extension_types.get_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the attribute of `ext` with name `attr` and type `qtype`.</code></pre>

### `kd.extension_types.get_attr_qtype(ext, attr)` {#kd.extension_types.get_attr_qtype}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the qtype of the `attr`, or NOTHING if the `attr` is missing.</code></pre>

### `kd.extension_types.get_extension_cls(qtype: QType) -> type[Any]` {#kd.extension_types.get_extension_cls}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the extension type class for the given QType.</code></pre>

### `kd.extension_types.get_extension_qtype(cls: type[Any]) -> QType` {#kd.extension_types.get_extension_qtype}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the QType for the given extension type class.</code></pre>

### `kd.extension_types.has_attr(ext, attr)` {#kd.extension_types.has_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `attr` is an attribute of `ext`.</code></pre>

### `kd.extension_types.is_koda_extension(x: Any) -> bool` {#kd.extension_types.is_koda_extension}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True iff the given object is an instance of a Koda extension type.</code></pre>

### `kd.extension_types.is_koda_extension_type(cls: type[Any]) -> bool` {#kd.extension_types.is_koda_extension_type}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True iff the given type is a registered Koda extension type.</code></pre>

### `kd.extension_types.is_null(ext)` {#kd.extension_types.is_null}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `ext` is null.</code></pre>

### `kd.extension_types.make(qtype: QType, prototype: Object | None = None, /, **attrs: Any)` {#kd.extension_types.make}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an extension type of the given `qtype` with the given `attrs`.

Args:
  qtype: the output qtype of the extension type.
  prototype: parent object (arolla.Object).
  **attrs: attributes of the extension type.</code></pre>

### `kd.extension_types.make_null(qtype: QType) -> Any` {#kd.extension_types.make_null}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a null instance of an extension type.</code></pre>

### `kd.extension_types.override()` {#kd.extension_types.override}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Marks the method as overriding a virtual method.</code></pre>

### `kd.extension_types.unwrap(ext)` {#kd.extension_types.unwrap}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Unwraps the extension type `ext` into an arolla::Object.</code></pre>

### `kd.extension_types.virtual()` {#kd.extension_types.virtual}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Marks the method as virtual, allowing it to be overridden.</code></pre>

### `kd.extension_types.with_attrs(ext, /, **attrs)` {#kd.extension_types.with_attrs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `ext` containing the given `attrs`.</code></pre>

### `kd.extension_types.wrap(x: Any, qtype: QType) -> Any` {#kd.extension_types.wrap}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Wraps `x` into an instance of the given extension type.</code></pre>

