<!-- go/g3mark-in-g3doc -->

# Extension Types

Koda Extension Types are custom, user-defined data structures that allow you to
extend Koda with new, modular types directly from Python. They are designed to
feel like native Python classes while integrating deeply with Koda's features,
such as tracing and operator overloading.

* TOC
{:toc}

## Background

Historically, Koda's native types (like `INT32`) have been tightly integrated
with its core C++ libraries. This integration meant that creating new,
full-featured types was complex and required modifying the core Koda library.

The primary goal of extension types is to provide a simple Python API that
empowers users to define their own custom types without needing to touch Koda's
core code. The result can be used from C++ as well. This enables the creation of
reusable and encapsulated components for various needs.

Internally, Koda extension types are built on top of the `arolla::Object` class.

## Core Features

Extension types combine the intuitive interface of a Python class with the
powerful, integrated features of native Koda types.

  * **Eager and Lazy Execution**: They work seamlessly in both interactive
    (eager) and traced/compiled (lazy) workflows.
  * **Custom Methods**: You can define your own methods that can access and
    operate on the attributes defined in the extension type and work in both
    eager and lazy execution modes.
  * **Integration with Koda Structures**: Extension types are supported in
    functors, can store `DataSlice` and `DataBag` instances as attributes, and
    automatically use implicit casting on `DataSlice` attributes.
  * **Inheritance and Polymorphism**: You can build class hierarchies. A
    function with inputs traced as a parent class can be evaluated with a child
    class input, with custom child class behavior implemented through virtual
    method overloading.
  * **Python-First API**: Extension types can be defined in Python through a
    simple decorator that handles the underlying complexity. The resulting types
    are fully usable from both Python and C++.
  * **Lower-Level Building Blocks**: [`kd.extension_types` operator
    module](/koladata/g3doc/api_reference.md#kd.extension_types)
    provides operators that act as lower-level building blocks.

## Defining and Using an Extension Type

Defining an extension type is designed to be as simple as creating a Python
dataclass. You use a decorator on a class and define its fields using Python
type annotations.

**Example: A Simple Point**

```python

from koladata import kd

@kd.extension_type()
class Point:
  # Define attrubutes with supported type annotations.
  x: kd.FLOAT32
  y: kd.FLOAT32

  # You can also add methods.
  def norm(self):
    return (self.x**2 + self.y**2)**0.5
```

Some things to note:

  * The decorated class must not have its own `__new__` or `__init__` methods.
  * The type annotations on the attributes of the dataclass are used to
    determine the schema of the underlying `DataSlice` (if relevant). For schema
    annotations, we support automatic implicit casting.
  * All attributes must have type annotations.
  * Supported annotations include `SchemaItem`, `DataSlice`, `DataBag`,
    `JaggedShape`, and other extension types. Additionally, any QType can be
    used as an annotation.
  * Attributes within an extension type are fully independent. We do not perform
    any `DataBag` merging or broadcasting between different attributes.
  * The body of any methods defined within the extension type should be
    tracing-compatible.

Once defined, your new type acts as a factory for creating instances. In eager
mode, an eager extension type instance (i.e. an Arolla QValue) is created. In
lazy mode, a symbolic extension type instance (i.e. a Koda Expr) is created.

The `with_attrs` method is automatically added to an extension types, allowing
for attributes to be dynamically updated.

```python
# Eager instantiation
p_value = Point(x=3.0, y=4.0)
print(p_value.norm())
# Output: 5.0

# Dynamically update the attributes.
updated_p_value = p_value.with_attrs(x=6.0)
print(updated_p_value.x) # Output: 6.0
```

The `_extension_arg_boxing` classmethod and `_extension_post_init` method can be
used to implement custom initialization behavior in case the automatic implicit
casting based on annotations is not enough.

If the `_extension_arg_boxing` classmethod is defined, it will be called for
each input argument during initialization:

```python
@kd.extension_type()
class PositivePoint:
  x: kd.FLOAT32
  y: kd.FLOAT32

  @classmethod
  def _extension_arg_boxing(self, value, annotation):
    # `annotation` will be `kd.FLOAT32` for both fields.
    return kd.cast_to(value + 1, annotation)

point = PositivePoint(1.0, 2.0)
point.x  # DataItem(2.0)
point.y  # DataItem(3.0)
```

The purpose of the `_extension_arg_boxing` is to implement custom boxing
behavior. As with other methods, it's required to be traceable in order to
function in a tracing context.

If the `_extension_post_init` method is defined, it will be called as the final
step upon initialization:

```python
@kd.extension_type()
class PositivePoint:
  x: kd.FLOAT32
  y: kd.FLOAT32

  def _extension_post_init(self):
    return kd.assertion.with_assertion(
        self, (self.x > 0) & (self.y > 0), 'x and y must be positive'
    )

PositivePoint(1.0, 2.0)  # Works.
PositivePoint(-1.0, 2.0)  # Raises with message 'x and y must be positive'.
```

The `_extension_post_init` method should take no arguments except for `self` and
should return (the potentially modified) `self`. The purpose of this method is
to implement verifications or modifications on the resulting extension type
instance. As with other methods, it's required to be traceable in order to
function in a tracing context.

## Tracing

Extension types are fully traceable, which allows them to be used directly as
top\-level inputs and outputs for functions compiled into Functors (e.g., with
`@kd.fn` or `@kd.trace_as_fn`).

To make this work, you must use a Python type hint to annotate the function
parameter with your extension type.

### **Example: Extension Types and Functors**

```python
# 1. Create a Point instance inside a traced function.
@kd.fn
def calculate_norm(x: kd.FLOAT32, y: kd.FLOAT32) -> kd.FLOAT32:
  # Point is instantiated with symbolic inputs x and y.
  point = Point(x=x, y=y)
  return point.norm()

print(calculate_norm(3.0, 4.0))
# Output: 5.0

# 2. Pass a Point instance as an input to a traced function.
@kd.fn
def calculate_norm(point: Point) -> kd.FLOAT32:
  return point.norm()

p = Point(x=3.0, y=4.0)
print(calculate_norm(p))
# Output: 5.0

# 3. Call a functor returning an extension type.
@kd.fn
def make_point(x: kd.FLOAT32, y: kd.FLOAT32) -> Point:
  return Point(x=x, y=y)

# You must specify `return_type_as`.
p = make_point(3.0, 4.0, return_type_as=Point)
print(p.norm())
# Output: 5.0
```

## Inheritance and Polymorphism

Extension types support method inheritance. For polymorphic behavior, you must
use the special decorators:

  * `@kd.extension_types.virtual()`: Marks a method in a parent class as
    overridable.
  * `@kd.extension_types.override()`: Marks a method in a child class that is
    overriding a virtual method.

With those decorators used, a functor traced on a `ParentClass` can accept a
`ChildClass` input and correctly invoke the child's overridden methods.

Normal methods are evaluated as-is (i.e. they stay python methods) while virtual
methods are traced as functors. Because of this, it's important that methods are
annotated correctly with argument and return types if they take or return
non-DataSlices (e.g. other extension types)

### **Example: Polymorphism and Functors**

Imagine a base `Shape` class and a `Circle` child class. We want `area()` to
behave polymorphically.

```python
from koladata import kd

@kd.extension_type()
class Shape:
  color: kd.STRING

  @kd.extension_types.virtual()
  def area(self):
    return 0.0

  def describe(self):
    return f'A shape'

@kd.extension_type()
class Circle(Shape):
  radius: kd.FLOAT32

  @kd.extension_types.override()
  def area(self):
    return 3.14159 * self.radius**2

  def describe(self):
    return f'A shape of area {self.area}'

# Define a method that accepts the base Shape type.
@kd.fn
def calculate_area(s: Shape):
  # Because area() is virtual, this will call the correct implementation.
  return s.area()

c = Circle(color='red', radius=10.0)
print(calculate_area)(c)
# Output: 314.159

# Non-virtual methods, in contrast, do not exhibit polymorphic behavior.
kd.fn
def describe_shape(s: Shape):
  # Because describe() is not virtual, parent method is called.
  return s.describe()

print(describe(c))
# Output: A shape

```
