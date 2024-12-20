<!--* css: "//koladata/g3doc/cheatsheet.css" *-->
<!-- For the best formatting, please limit the line_max_char to 50. -->

# Koda Cheatsheet

## Koda Setup

<section>

### Import

```py
from koladata import kd

# Useful aliases for Expr
kde = kd.kde
I = kd.I
V = kd.V
S = kd.S
```

</section>

## Basic APIs

<section>

### Primitives and DataItem

```py
# Primitive dtypes
kd.INT32
kd.INT64
kd.FLOAT32
kd.FLOAT64
kd.STRING
kd.BYTES
kd.BOOLEAN
kd.MASK

# MASK type values
kd.present
kd.missing

# DataItem creation
i = kd.item(1)

kd.is_item(i)
kd.is_primitive(i)
kd.get_dtype(i) # kd.INT32

# DataItem creation with explicit dtypes
kd.int32(1)
kd.int64(2)
kd.float32(1.1)
kd.float64(2.2)
kd.str('a')
kd.bytes(b'a')
kd.bool(True)
kd.mask(None)

# Or use kd.item with explicit dtype
kd.item(1, kd.INT32)
kd.item(2, kd.INT64)

# kd.from_py is a universal converter
# Same as kd.item
kd.from_py(1)
```

</section>

<section>

### DataSlice

```py
ds = kd.slice([[1, 2], [3, None, 5]])
kd.is_slice(ds)

ds.get_size()
ds.get_ndim()
ds.get_shape()

ds.get_dtype() # kd.INT32

# DataSlice creation with explicit dtypes
kd.int32([[1, 2], [3, None, 5]])
kd.int64([[1, 2], [3, None, 5]])
kd.float32([[1., 2.],[3., None, 5.]])
kd.float64([[1., 2.],[3., None, 5.]])
kd.str(['a', None, 'b'])
kd.bytes([b'a', None, b'b'])
kd.bool([True, None, False])
kd.mask([kd.present, kd.missing])

# Or use kd.slice with explicit dtype
kd.slice([[1, 2], [3, None, 5]], kd.INT32)
kd.slice([[1, 2], [3, None, 5]], kd.INT64)

# kd.from_py is a universal converter
# Same as kd.slice
kd.from_py([[1, 2], [3, None, 5]])

ds = kd.slice([[1, 2], [3, None, 5]])
# Navigate DataSlice as "nested" lists
ds.L[1].L[2] # 5
# Items at idx in the first dims
ds.L[1] # [3, None, 5]

# Use in Python for-loop
for i in ds.L:
  for j in i.L:
    print(j)

# Subslice
ds.S[1, 2] # 5
# Take the third items in the last dimension
ds.S[2] # [None, 5]
ds.S[1:, :2] # [[3, None]]

# Get items at idx
ds.take(2) # [None, 5]
ds.take(kd.slice([1, 2])) # [2, 5]

# Expand the outermost dimension into a list
kd.to_pylist(ds)
# Convert to a nested Python list
ds.to_py()

# Reverse the order of items in the last dimension
kd.reverse(ds) # [[2, 1], [5, None, 3]]

# DataItem is 0-dim DataSlice
i = kd.slice(1) # same as kd.item(1)
i.get_ndim() # 0

# Visualize a DataSlice
ds.display()
```

</section>

<section>

### Entities

```py
# Entity creation with named schema
e = kd.new(x=1, y=2, schema='Point')
es = kd.new(x=kd.slice([1, 2, None]),
            y=kd.slice([4, None, 6]),
            schema='Point')

e.get_schema()
assert e.get_schema() == es.get_schema()

# Use an existing schema
s = kd.named_schema('Point', x=kd.INT32, y=kd.INT32)
e = kd.new(x=1, y=2, schema=s)

# When `schema=` is not provided, a new
# schema is created for each invocation
e1 = kd.new(x=1, y=2)
e2 = kd.new(x=1, y=2)
assert e1.get_schema() != e2.get_schema()

# Use provided itemids
itemid = kd.new_itemid()
e3 = kd.new(x=1, y=2, itemid=itemid)
e4 = kd.new(x=1, y=2, itemid=itemid)
assert e3.get_itemid() == e4.get_itemid()

# Get available attributes
# As all entities share the same schema,
# the intersection of attribute names is the same
e.get_attr_names(intersection=True) # ['x', 'y']
e.get_attr_names(intersection=False) # ['x', 'y']

# Access attribute
e.x # 1
e.get_attr('y') # 2
e.maybe('z') # None
e.get_attr('z', default=0) # 0
es.get_attr('x', default=0) # [1, 2, 0]

# Entities are immutable by default, modification is done
# by creating a new entity with updated attributes

# Update attributes
# Update a single attribute
e1 = e.with_attr('x', 3)
e1 = e.with_attr('z', 4)
# Also override schema
e1 = e.with_attr('y', 'a', update_schema=True)

# Update multiple attributes
e2 = e.with_attrs(z=4, x=3)
# Also override schema for 'y'
e2 = e.with_attrs(z=4, y='a', update_schema=True)

# Create an update and apply it separately
upd = kd.attrs(e, z=4, y=10)
e3 = e.updated(upd)

# Allows mixing multiple updates
e4 = e.updated(kd.attrs(e, z=4), kd.attrs(e, y=10))
```

</section>

<section>

### Objects

```py
o = kd.obj(x=1, y=2)
os = kd.obj(x=kd.slice([1, 2, None]),
            y=kd.slice([4, None, 6]))

os = kd.slice([kd.obj(x=1),
               kd.obj(y=2.0),
               kd.obj(x=1.0, y='a')])

os.get_schema() # kd.OBJECT
os.get_obj_schema()
# [IMPLICIT_SCHEMA(x=INT32),
#  IMPLICIT_SCHEMA(y=FLOAT32),
#  IMPLICIT_SCHEMA(x=INT32, y=STRING)]

# Use provided itemids
itemid = kd.new_itemid()
o1 = kd.obj(x=1, y=2, itemid=itemid)
o2 = kd.obj(x=1, y=2, itemid=itemid)
assert o1.get_itemid() == o2.get_itemid()

# Get available attributes
os1 = kd.slice([kd.obj(x=1), kd.obj(x=1.0, y='a')])
os1.get_attr_names(intersection=True) # ['x']
os1.get_attr_names(intersection=False) # ['x', 'y']

# Access attribute
o.x # 1
o.get_attr('y') # 2
o.maybe('z') # None
o.get_attr('z', default=0) # 0
os.get_attr('x', default=0) # [1, 0, 'a']

# Objects are immutable by default, modification is done
# by creating a new object with updated attributes

# Update a single attribute
o1 = o.with_attr('x', 3)
o1 = o.with_attr('z', 4)
# Also override schema
# no update_schema=True is needed
o1 = o.with_attr('y', 'a')

# Update multiple attributes
o2 = o.with_attrs(z=4, x=3)
# Also override schema for 'y'
o2 = o.with_attrs(z=4, y='a')

# Create an update and apply it separately
upd = kd.attrs(o, z=4, y=10)
o3 = o.updated(upd)

# Allows mixing multiple updates
o4 = o.updated(kd.attrs(o, z=4), kd.attrs(o, y=10))
```

</section>

<section>

### Lists

```py
# Create a list from a Python list
l1 = kd.list([1, 2, 3])
l2 = kd.list([[1, 2], [3], [4, 5]])

# Create multiple lists by imploding
# the last dimension of a DataSlice
l3 = kd.list(kd.slice([[1, 2], [3], [4, 5]]))

# l2 and l3 are different
kd.is_item(l2)
l2.get_size() # 1
l3.get_size() # 3

kd.is_list(l1)
kd.list_size(l1) # 3

# Use provided itemids
itemid = kd.new_listid()
l4 = kd.list([1, 2, 3], itemid=itemid)
l5 = kd.list([4, 5, 6], itemid=itemid)
assert l4.get_itemid() == l5.get_itemid()

# Python-like list operations
l1[0] # 1
kd.get_item(l1, 0) # 1
l1[1:] # [2, 3]

# Slice by a DataSlice
l1[kd.slice([0, 2])] # [1, 3]
l1[kd.slice([[2, 1], [0, None]])] # [[3, 2], [1, None]]

# Explode a list
l1[:] # [1, 2, 3]
kd.explode(l1) # [1, 2, 3]

kd.explode(l2, ndim=2)
# Explode all lists repeatedly
kd.explode(l2, ndim=-1)

# Filter out items
l6 = kd.list([1, 2, None, 4])
l6.select_items(lambda x: x >= 2) # [2, 4]

# Returns a DataSlice of lists concatenated from
# the list items of arguments.
kd.concat_lists(kd.list([1, 2]), kd.list([3, 4]))
```

</section>

<section>

### Dicts

```py
# Create a dict from a Python dict
d1 = kd.dict({'a': 1, 'b': 2})
d1 = kd.dict(kd.slice(['a', 'b'],
             kd.slice([1, 2]))) # Same as above

# Create multiple dicts
d2 = kd.dict(kd.slice([['a', 'b'], ['c']],
             kd.slice([[1, 2], [3]])))

kd.is_dict(d1)
d1.dict_size() # 2

# Use provided itemids
itemid = kd.new_dictid()
d3 = kd.dict({'a': 1, 'b': 2}, itemid=itemid)
d4 = kd.dict({'c': 3, 'd': 4}, itemid=itemid)
assert d3.get_itemid() == d4.get_itemid()

d1.get_keys() # ['a', 'b']
d1.get_values() # [1, 2]
d1['a']
d1.get_item('a') # Same as above

# Filter out keys/values
d1.select_keys(lambda k: k != 'b') # ['a']
d1.select_values(lambda v: v > 1) # [2]

# Dicts are immutable by default, modification is done
# by creating a new dict with updated key/values

# Update a key/value
d4 = d1.with_dict_update('c', 5)
# Update multiple key/values
another_dict = kd.dict({'a': 3, 'c': 5})
d5 = d1.with_dict_update(another_dict)
# Same as above
d5 = d1.with_dict_update(kd.slice(['a', 'c']),
                         kd.slice([3, 5]))

# Create an update and apply it separately
upd = d1.dict_update(another_dict)
d6 = d1.updated(upd)

# Allows mixing multiple updates
d7 = d1.updated(d1.dict_update('c', 5),
                d1.dict_update(another_dict))
```

</section>

<section>

### Subslicing DataSlices

Subslicing is an operation of getting part of the items in a DataSlice.
Sub-slicing slices **all** dimensions of a DataSlice or the last dimension when
only one dimension is specified. The API is called "subslice" because it slices
a DataSlice to create a sub-DataSlice.

See [kd.subslice](koda_v1_api_reference.md#kd.slices.subslice) APIs for more details.

```py
ds = kd.slice([[1, 2, 3], [4, 5]])

# Slice by indices
kd.subslice(ds, 1, 1)
kd.subslice(ds, 1, -1)

# Slice by range
kd.subslice(ds, 0, slice(1))
kd.subslice(ds, 0, slice(2, 4))

# 'S[]' as Syntactic sugar
ds.S[1, 1]
ds.S[0, :2]

# Multi-dimension
ds = kd.slice([[[1, 2, 3], [4, 5, 6]],
            [[7, 8, 9], [10, 11, 12]]])

ds.S[1] # [[2, 5], [8, 11]]
ds.S[1:, 0, :2] # [[7, 8]]
ds.S[0, :2] # [[1, 2], [7, 8]]
ds.S[kd.slice([[0, 1], [1]]), 1] # [[2, 5], [11]]

# Advanced slicing: slice by DataSlice
ds = kd.slice([[1, 2, 3], [4, 5]])
ds.S[kd.slice([0, 0]), 1:] # [[[2, 3], [2, 3]]]
ds.S[kd.slice([[0, 2], [0, 1]]),
        kd.slice([[0, 0], [1, 0]])]
# -> [[1, None], [2, 4]]

# Ellipse
ds = kd.slice([[[1, 2, 3], [4, 5]],
                [[6, 7]], [[8], [9, 10]]])
ds.S[1:, ...] # [[[6, 7]], [[8], [9, 10]]]
ds.S[1:, ..., :1] # [[[6]], [[8], [9]]]
ds.S[1:, 0, ..., :1] # [[6], [8]]
```

</section>

<section>

### Python-like Slicing

While subslicing provides an efficient way to slice all dimensions, it is
sometimes convenient to slice dimensions one by one similar to how to iterate
through a nested Python list. DataSlice provides a way to slice the **first**
dimension using `.L` notation. *L* stands for *Python list*.

```py
ds = kd.slice([[1, 2, 3], [4, 5]])

# Note that 'l' is a one-dim DataSlice
# and 'i' is a DataItem
for l in ds.L:
  print(type(l))
  for i in l.L:
    print(type(i))
    print(i)

# A Python list of one-dim DataSlice
ds.L[:]
# [DataSlice([1, 2, 3], ...),
#  DataSlice([4, 5], ...)]

ds.L[1] # DataSlice([4, 5], ...)
```

</section>

<section>

### Editable Containers

```py
x = kd.container()
x.a = 1
x.d = kd.container()
x.d.e = 4
x.d.f = kd.list([1, 2, 3])
```

</section>

<section>

### DataSlice Shape (a.k.a. Partition Tree)

```py
ds = kd.slice([[[1, 2], [3]], [[4, 5]]])
shape = ds.get_shape()
# -> JaggedShape(2, [2, 1], [2, 1, 2])

kd.shapes.ndim(shape) # 3
kd.shapes.size(shape) # 5

# Get the shape with N-1 dimensions
shape[:-1] # JaggedShape(1, 2, [2, 1])
# Get the shape with first dimension
shape[:2] # JaggedShape(1, 2)

# Create a new shape directly
shape1 = kd.shapes.create(2, [2, 1], [2, 1, 2])
assert shape1 == shape
```

</section>

<section>

### Changing shape of a DataSlice

```py
ds = kd.slice([[[1, 2], [3]], [[4, 5]]])
# Flatten all dimensions
ds.flatten() # [1, 2, 3, 4, 5]
# Flatten dimensions from the second to the end
ds.flatten(2)  # [[1, 2, 3], [4, 5]]
# Flatten dimensions from the second to the end
ds.flatten(-2) # [[1, 2, 3], [4, 5]]
# Flatten dimensions from the first to the third
ds.flatten(0, 2) # [[1, 2], [3], [4, 5]]
# When the start and end dimensions are the same,
# insert a new dimension
ds.flatten(2, 2) # [[[[1, 2]], [[3]]], [[[4, 5]]]]

# Reshape to the shape of another DataSlice
ds1 = kd.slice([1, 2, 3, 4, 5])
ds.reshape_as(ds1)
ds.reshape(ds1.get_shape())

ds2 = kd.slice([1, None, 3])
# Repeats values
ds2.repeat(3) # [[1, 1], [None, None], [3, 3]]
# Repeats present values
ds2.repeat_present(2) # [[1, 1], [], [3, 3]]
```

</section>

<section>

### Broadcasting and Aligning

```py
# Expands x based on the shape of target
kd.slice([100, 200]).expand_to(
    kd.slice([[1,2],[3,4,5]]))
# Implodes last ndim dimensions into lists,
# expands the slice of lists, explodes the result
kd.slice([100, 200]).expand_to(
    kd.slice([[1,2],[3,4,5]]), ndim=1)

kd.is_expandable_to(x, target, ndim)
# Whether any of args is expandable to the other
kd.is_shape_compatible(x, y)

# Expands DataSlices to the same common shape
kd.align(kd.slice([[1, 2, 3], [4, 5]]),
         kd.slice('a'), kd.slice([1, 2]))
```

</section>

<section>

### ItemIds and UUIDs

```py
# A new ItemId is allocated when a new
# object/entity/list/dict/schema is created
o1 = kd.obj(x=1, y=2)
o2 = kd.obj(x=1, y=2)
assert o1.get_itemid() != o2.get_itemid()

# Get ItemId from object/entity/list/dict
itemid = o1.get_itemid()

# ItemId is a 128 integer
# Print out the ItemId in the base-62 format
str(itemid) # With Entity:$ prefix
str(kd.list().get_itemid()) # With List:$ prefix
str(kd.dict().get_itemid()) # With Dict:$ prefix
str(kd.schema.new_schema().get_itemid())
# With Schema:$ prefix

# Encode to/from base-62 number (as string).
str_id = kd.encode_itemid(itemid)
assert kd.decode_itemid(str_id) == itemid

# int64 hash values
kd.hash_itemid(itemid)

# ItemIds can be allocated directly
kd.new_itemid()
kd.new_listid()
kd.new_dictid()

# UUIDs are unique determined

# A new UUID is allocated when a new uu
# object/entity/schema is created
o3 = kd.uuobj(x=1, y=2)
o4 = kd.uuobj(x=1, y=2)
assert o3.get_itemid() == o4.get_itemid()

kd.uu(x=1, y=2) # UU Entity
kd.uuobj(x=1, y=2) # UU Object
kd.uu_schema(x=kd.INT32, y=kd.INT64) # UU Schema

# UUID has a # prefix compared to $ for ItemId
str(o3.get_itemid()) # With Entity:# prefix

# Compute UUID from attr values
i1 = kd.uuid(x=1, y=2)
i2 = kd.uuid(x=1, y=2)
assert i1 == i2

# Traverse attrs instead of using their UUIDs
kd.deep_uuid(obj)

# Compute UUIDs by aggregating items
# over the last dimension
kd.agg_uuid(kd.slice([[1, 2, 3], [4, 5, 6]]))
```

</section>

## DataSlice Operators

Koda provides a comprehensive list of operators under `kd` module. Most
pointwise operatorƒ automatically broadcast all input DataSlices to the same
shape.

<section>

### Item Creation Operators

`kd.***_like(x)` creates a DataSlice of *** with the **same shape and sparsity**
as DataSlice `x`.

`kd.***_shaped_as(x)` creates a DataSlice of *** with the **same shape** as
DataSlice `x`.

`kd.***_shaped(shape)` creates a DataSlice of *** with the **shape** `shape`.

```py
x = kd.slice([[1, None], [3, 4]])
s = x.get_shape()

kd.val_like(x, 1) # [[1, None], [1, 1]]
kd.val_shaped_as(x, 1) # [[1, 1], [1, 1]]
kd.val_shaped(s, 1) # [[1, 1], [1, 1]]

# Note there is kd.present_like
kd.present_shaped_as(x)
kd.present_shaped(s)

# Note there is kd.empty_like
kd.empty_shaped_as(x)
kd.empty_shaped(s)

# Empty Object creation
# Note obj_xxx does not take schema= argument
kd.obj_like(x)
kd.obj_shaped_as(x)
kd.obj_shaped(s)

# Non-empty Object creation
kd.obj_like(x, **attr_dss)
kd.obj_shaped_as(x, **attr_dss)
kd.obj_shaped(s, **attr_dss)

# With provided itemids
kd.obj_like(x, itemid=itemid)
kd.obj_shaped_as(x, itemid=itemid)
kd.obj_shaped(s, itemid=itemid)

# Empty Entity creation
kd.new_like(x)
kd.new_shaped_as(x)
kd.new_shaped(s)

# Non-empty Entity creation
kd.new_like(x, **attr_dss)
kd.new_shaped_as(x, **attr_dss)
kd.new_shaped(s, **attr_dss)

# With provided itemids
kd.new_like(x, itemid=itemid)
kd.new_shaped_as(x, itemid=itemid)
kd.new_shaped(s, itemid=itemid)

schema = kd.new_schema(...)
kd.new_like(x, schema=schema)

# Empty List creation
kd.list_like(x)
kd.list_shaped_as(x)
kd.list_shaped(s)

# Non-empty List creation
kd.list_like(x, list_item_ds)
kd.list_shaped_as(x, list_item_ds)
kd.list_shaped(s, list_item_ds)

# With provided itemids
kd.list_like(x, itemid=itemid)
kd.list_shaped_as(x, itemid=itemid)
kd.list_shaped(s, itemid=itemid)

schema = kd.list_schema(...)
kd.list_like(x, schema=schema)

# Empty Dict creation
kd.dict_like(x)
kd.dict_shaped_as(x)
kd.dict_shaped(s)

# Non-empty Dict creation
kd.dict_like(x, key_ds, value_ds)
kd.dict_shaped_as(x, key_ds, value_ds)
kd.dict_shaped(s, key_ds, value_ds)

# With provided itemids
kd.dict_like(x, itemid=itemid)
kd.dict_shaped_as(x, itemid=itemid)
kd.dict_shaped(s, itemid=itemid)

schema = kd.dict_schema(...)
kd.dict_like(x, schema=schema)
```

</section>

<section>

### Comparison Operators

```py
a = kd.slice([1, 2, 3])
b = kd.slice([3, 2, 1])

# Use infix operators
a > b
a <= b
a == b
a != b

# Use kd operators
kd.greater(a, b)
kd.less_equal(a, b)
kd.equal(a, b)
kd.not_equal(a, b)
```

</section>

<section>

### Presence Checking Operators

```py
a = kd.slice([1, None, 3])
kd.has(a)
kd.has_not(a)
```

</section>

<section>

### Masking and Coalesce Operators

```py
# Masking
a = kd.slice([1, 2, 3])
a & (a > 1)  # Use infix operator
kd.apply_mask(a, a > 1)  # Use kd operators

b = kd.slice([1, None, 3])
a | b  # Use infix operator
kd.coalesce(a, b)  # Use kd operators

c = kd.slice(['1', None, '3'])
c | a
```

</section>

<section>

### Conditional Selection Operators

```py
a = kd.slice([1, 2, 3])
kd.select(a, a > 1)
a.select(a > 1)
kd.select(a, lambda x: x > 1) # same as above

# Put items in present positions in filter.
kd.inverse_select(
    kd.slice([1, 2]),
    kd.slice([kd.missing, kd.present, kd.present])
)

kd.remove(a, a > 1)
a.remove(a > 1)
kd.remove(a, lambda x: x > 1) # same as above

kd.select_present(kd.slice([1, None, 3]))

b = kd.slice([3, 2, 1])
c = kd.slice([-1, 0, 1])
kd.cond(c > 1, a, b)
```

</section>

<section>

### Range Operator

```py
kd.range(1, 5) # [1, 2, 3, 4]
kd.range(1, kd.slice([3, 5]))
# -> [[1, 2], [1, 2, 3, 4]]
kd.range(kd.slice([0, 3]), kd.slice([3, 5]))
# -> [[0, 1, 2], [3, 4]]
```

</section>

<section>

### Math Operators

```py
x + y  # Short for kd.math.add(x, y)
x - y  # Short for kd.math.subtract(x, y)
x * y  # Short for kd.math.multiple(x, y)
x / y  # Short for kd.math..divide(x, y)
x % y  # Short for kd.math.mod(x, y)
x ** y  # Short for kd.math.pow(x, y)
x // y  # Short for kd.math.floor_divide(x, y)

kd.math.log(x)
kd.math.round(x)
kd.math.float(x)
kd.math.ceil(x)
kd.math.pos(x)
kd.math.abs(x)
kd.math.neg(x)
kd.math.maximum(x, y)
kd.math.mimimum(x, y)

kd.agg_min(x)
kd.agg_max(x)
kd.agg_sum(x)
kd.min(x)
kd.max(x)
kd.sum(x)

kd.math.cum_min(x)
kd.math.cum_max(x)
kd.math.cum_sum(x)
kd.math.agg_mean(x)
kd.math.agg_median(x)
kd.math.agg_std(x)
kd.math.agg_var(x)
kd.math.cdf(x)
kd.math.agg_inverse_cdf(x)
```

</section>

<section>

### String Operators

```py
kd.strings.length(x)
kd.strings.upper(x)
kd.strings.lower(x)
kd.strings.split(x, sep)
kd.strings.contains()
kd.strings.contains_regex(x, regex)
kd.strings.find(x, str)
kd.strings.replace(x, old, new)
kd.strings.substr(x, start, end)
kd.strings.count(x, sub)
kd.strings.agg_join(x, sep)
kd.strings.join(*args)

kd.strings.encode('abc') # b'abc'
kd.strings.decode(b'abc') # 'abc'

# Encode any bytes into base64 format string
# Useful for putting bytes into JSON
kd.strings.encode_base64(b'abc') # 'YWJj'
kd.strings.decode_base64('YWJj') # b'abc'
```

</section>

<section>

### String Format Operators

```py
kd.strings.printf('Hello %s', 'World') # 'Hello World'

kd.strings.printf('Hello %s',
  kd.slice(['World', 'Universe']))
# -> ['Hello World', 'Hello Universe']

o = kd.obj(greeting='Hello', name='World')
kd.strings.printf('%s %s', o.greeting, o.name) # 'Hello World'

kd.strings.printf('%s %d', "Hello", 1) # 'Hello 1'

# Python like f-string Format API
# Note that ":s" after the DataSlice var name
name = kd.str('World')
kd.strings.fstr(f'Hello {name:s}') # 'Hello World'

name = kd.slice(['World', 'Universe'])
kd.strings.fstr(f'Hello {name:s}')
# -> ['Hello World', 'Hello Universe']

o=kd.obj(greeting='Hello', name='World')
kd.strings.fstr(f'{o.greeting:s} {o.name:s}') # 'Hello World'

greeting, count = kd.str('Hello'), kd.int32(1)
kd.strings.fstr(f'{greeting:s} {count:s}') # 'Hello 1'

# Use format to specify kwargs
kd.strings.format('Hello {name}', name='World') # 'Hello World'

kd.strings.format('Hello {name}',
  name=kd.slice(['World', 'Universe']))
# -> ['Hello World', 'Hello Universe']

kd.strings.format('{greeting} {count}',
  greeting='Hello', count=1) # 'Hello 1'
```

</section>

<section>

### Aggregational Operators

```py
# collapse over the last dimension and
# returns the value if all item has the
# same value or None otherwise
x = kd.slice([[1], [], [2, None], [3, 4]])
kd.collapse(x) # [1, None, 2, None]

# 'count' returns counts of present items
kd.agg_count(x) # [1, 0, 1, 2]
# aggregates over the last 2 dimensions
kd.agg_count(x, ndim=2) # 4
# aggregates across all dimensions
kd.count(x) # the same as above
# aggregates over the last dimension and
# returns the accumulated count of present items
kd.cum_count(x) # [[1], [], [1, None], [1, 2]]
# aggregates over the last 2 dimensions
kd.cum_count(x, ndim=2) # [[1], [], [2, None], [3, 4]]

# 'index' returns indices starting from 0
# aggregates over the last dimension
kd.index(x) # [[[0, None, 2], [], [0]], [[], [0]]]
# indices at the second dimension
# note it is `dim` rather than `ndim` and
# the result is aligned with x
kd.index(x, dim=1) # [[[0, 0, 0], [], [2]], [[], [1]]]
# indices at the first dimension
kd.index(x, dim=0) # [[[0, 0, 0], [], [0]], [[], [1]]]

x = kd.slice([[kd.present, kd.present], [],
              [kd.present, kd.missing], [kd.missing]])

# 'all' returns present if all items are present
# aggregates over the last dimension
kd.agg_all(x) # [present, present, missing, missing]
# aggregates across all dimensions
kd.all(x) # missing

# 'any' returns present if any item is present
kd.agg_any(x) # [present, missing, present, missing]
# aggregates across all dimensions
kd.any(x) # present
```

</section>

<section>

### Type casting operators

```py
kd.to_int32(kd.slice([1., 2., 3.]))
kd.to_int64(kd.slice([1, 2, 3]))
kd.to_float32(kd.slice([1, 2, 3]))
kd.to_float64(kd.slice([1., 2., 3.]))
kd.to_bool(kd.slice([0, 1, 2]))
kd.to_str(kd.slice([1, 2, 3]))
kd.to_bytes(kd.slice([b'1', b'2', b'3']))

# Dispatches to the relevant kd.to_* operator
# or casts into specific schema.
kd.cast_to(kd.slice([1, 2, 3]), kd.INT64)
kd.cast_to(kd.new(x=1), kd.uu_schema(x=kd.INT32))
```

</section>

<section>

### Expand_to Operators

```py
a = kd.slice([[1, 2], [3]])
b = kd.slice([[[1, 2], [3]], [[4, 5]]])

kd.expand_to(a, b)
# returns [[[1, 1], [2]], [[3, 3]]]
# which is equivalent to
a.expand_to(b)

c = kd.slice([[1], [2, 3]])

kd.expand_to(a, c)
# raises an exception
# which is equivalent to
kd.expand_to(a, c, last_ndim_to_implode=0)

kd.expand_to(a, c, last_ndim_to_implode=1)
# returns [[[1, 2]], [[3], [3]]]

kd.expand_to(a, c, last_ndim_to_implode=2)
# returns [[[[1, 2], [3]]], [[[1, 2], [3]],
#          [[1, 2], [3]]]]

kd.expand_to(a, c, last_ndim_to_implode=3)
# raises an exception
```

</section>

<section>

### Explosion and Implosion Operators

```py
# kd.explode explodes the Lists ndim times
a = kd.list([[[1, 2], [3]], [[4], [5]]])

kd.explode(a, ndim=1) # [List:$64, List:$65]
a.explode(ndim=1) # the same as above

kd.explode(a, ndim=2)
# -> [[List:$68, List:$69], [List:$6A, List:$6B]]

kd.explode(a, ndim=3)
# ->[[[1, 2], [3]], [[4], [5]]]

# explode until the DataSlice does not contain
# Lists any more.
kd.explode(a, ndim=-1)
# ->[[[1, 2], [3]], [[4], [5]]]

# kd.implode implodes the DataSlice ndim times
a = kd.slice([[[1, 2], [3]], [[4], [5]]])

kd.implode(a, ndim=1)
# -> [List:$68, List:$69], [List:$6A, List:$6B]]

# which is equivalent to
a.implode(ndim=1)

kd.implode(a, ndim=2) # [List:$64, List:$65]

kd.implode(a, ndim=3) # List:$66

# implode until the result is a List DataItem
kd.implode(a, ndim=-1) # List:$66
```

</section>

<section>

### Joining DataSlice Operators

```py
# Stacks DataSlices and creates a new
# dimension at `ndim`
a = kd.slice([[1, None], [4]])
b = kd.slice([[7, 7], [7]])

kd.stack(a, b, ndim=0) # [[[1, 7], [None, 7]], [[4, 7]]]
kd.stack(a, b) # the same as above
kd.stack(a, b, ndim=1) # [[[1, None], [7, 7]], [[4], [7]]]
kd.stack(a, b, ndim=2) # [[[1, None], [4]], [[7, 7], [7]]]
# raise an exception
# kd.stack(a, b, ndim=4)

# Zip DataSlices over the last dimension
kd.zip(a, b) # [[[1, 7], [None, 7]], [[4, 7]]]
# Zip supports auto-alignment
kd.zip(a, 1) # [[[1, 1], [None, 1]], [[4, 1]]]

# Zip is equivalent to kd.stack(..., ndim=0)
# but supports auto-alignment
kd.stack(a, b) # [[[1, 7], [None, 7]], [[4, 7]]]
# raise an exception
# kd.stack(a, 1)

# Concatenates DataSlices on dimension `ndim`
a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
b = kd.slice([[[1], [2]], [[3], [4]]])

kd.concat(a, b, ndim=1)
# -> [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]
kd.concat(a, b) # the same as above
kd.concat(a, b, ndim=2)
# -> [[[1, 2], [3], [1], [2]], [[5], [7, 8], [3], [4]]]
kd.concat(a, b, ndim=3)
# -> [[[1, 2], [3]], [[5], [7, 8]], [[1], [2]], [[3], [4]]]
# raise an exception
# kd.concat(a, b, ndim=4)
```

</section>

<section>

### Group_by Operators

</section>

```py
cities = kd.obj(
  name=kd.slice(['sf', 'sj', 'la', 'nyc', 'albany']),
  population=kd.slice([100, 200, 300, 400, 500]),
  state=kd.slice(['ca', 'ca', 'ca', 'ny', 'ny'])
)

# Group cities by state name over the last dimension
# Note the result has one more dimension
cities_grouped = kd.group_by(cities, cities.state)

state_population = kd.agg_sum(cities_grouped.population)

# We can create the state lists
states = kd.obj(
  cities=kd.implode(cities_grouped),
  name=kd.collapse(cities_grouped.state),
  population=state_population
)

# Note that 'state' for 'la' is missing
cities = kd.obj(
  name=kd.slice(['sf', 'sj', 'la', 'nyc', 'albany']),
  population=kd.slice([100, 200, 300, 400, 500]),
  state=kd.slice(['ca', 'ca', None, 'ny', 'ny'])
)

cities_grouped = kd.group_by(cities, cities.state)

# Note that 'la' is missing
cities_grouped.name
# [['sf', 'sj'], ['nyc', 'albany']]

# Sort by both state name and population
# Note that we need to convert MASK to BOOLEAN
# if we want to use it as group_by value
cities_grouped = kd.group_by(cities, cities.state,
  kd.cond(cities.population > 100, True, False))

cities_grouped.name
# [['sf'], ['sj'], ['nyc', 'albany']]
```

<section>

### Link Operators

</section>

```py
# One-to-many mapping
docs = kd.obj(
  did=kd.slice([[1, 2], [1, 2, 3]]),
  score=kd.slice([[4, 5], [6, 7, 8]]),
)
queries = kd.obj(did=kd.slice(
  [kd.list([1, 3, 1]), kd.list([3, 1])]))
kd.translate(queries.did[:], docs.did, docs.score)
# [[4, None, 4], [8, 6]]

# Many-to-one mapping
docs = kd.obj(
    qid=kd.slice([[1, 2, 2], [2, 2, 3]]),
    score=kd.slice([[1, 2, 3], [4, 5, 6]]),
)
queries = kd.obj(
  qid=kd.slice([[1, 2], [3, 2]]))

new_docs = kd.translate_group(
  queries.qid, docs.qid, docs)

# Add docs list since the result of
# translate_group is aligned with queries
queries.docs = kd.implode(new_docs)

queries.docs[:].score
# [[[1], [2, 3]], [[6], [4, 5]]]

# Many-to-many mapping is not directly
# supported as operator but can be done
# as multiple steps. E.g.
docs = kd.obj(
    qid=kd.slice([1, 2, 2, 2, 2, 3]),
    score=kd.slice([1, 2, 3, 4, 5, 6]),
)

query_terms = kd.obj(
    qid=kd.slice([1, 1, 1, 2, 2]),
    term=kd.slice(['a', 'b', 'c', 'd', 'e']),
)

docs_grouped = kd.group_by(docs, docs.qid)
terms_grouped = kd.group_by(query_terms, docs.qid)
queries = kd.obj(
  docs=kd.implode(docs_grouped),
  terms=kd.implode(terms_grouped)
)
```

<section>

### Random Number and Sampling Operators

```py
# Generate random integers
kd.randint_like(x)
# random int between 0 and 10
kd.randint_like(x, 10)
# random int between -5 and 200
kd.randint_like(x, -5, 200)
# setting the seed is supported
kd.randint_like(x, 10, seed=123)

kd.randint_shaped_as(x, ...)

kd.randint_shaped(shape, ...)

ds = kd.slice([[1, 2, None, 4],
            [5, None, None, 8]])
key = kd.slice([['a', 'b', 'c', 'd'],
             ['a', 'b', 'c', 'd']])

# Sampling is performed on flatten 'ds'
# rather than on the last dimension.
# Sample each element with .5 probability.
kd.sample(ds, 0.5, 123)
# Use 'key' for stability
kd.sample(ds, 0.5, 123, key)

# Select 2 items from last dimension
kd.sample_n(ds, 2, 123)
# Select one item from the first and
# two items from the second
kd.sample_n(ds, kd.slice([1, 2], 123))
# Use 'key' for stability
kd.sample_n(ds, 2, 123, key)
```

</section>

<section>

### Sort and Rank Operators

```py
ds1 = kd.slice([[10, 5, 10, 5], [30, 10]])
ds2 = kd.slice([[1, 2, 3, 4], [2, 1]])

# Ranking returns ranked positions
# Rank over the last dimension
kd.ordinal_rank(ds1)
# Rank with tie breaker
kd.ordinal_rank(ds1, tie_breaker=ds2)
# Rank with descending order
kd.ordinal_rank(ds1, descending=True)

# Sorting returns sorted DataSlice
# Sort over the last dimension
kd.sort(ds1)
# Sort with descending order
kd.sort(ds1, descending=True)

# Find the n-th element in the last dimension
kd.sort(ds1).take(2)
kd.sort(ds1).take(kd.slice([0, 1]))
```

</section>

<section>

### map_py Operator

```py
ds = kd.slice([1, 2, 3])

# Pointwise
kd.map_py(lambda x: x + 1, ds,
          schema=kd.INT64)

# Aggregational
kd.map_py(lambda x: len(x), ds, ndim=1)

# Aggregaional but no dimension change
kd.map_py(lambda x: sorted(x), ds, ndim=1)[:]

# Expansion
kd.map_py(lambda x: [x] * 10, ds,
          schema=kd.list_schema(kd.INT64))[:]

# Pass Objects
a = kd.obj(x=kd.slice([[1, 2], [3, None, 5]]),
           y=kd.slice([[6, 7], [8, 9, None]]))
kd.map_py(lambda a: a.x + a.y, a,
          schema=kd.INT32)
kd.map_py(lambda a: [obj.x - obj.y for obj in a],
          a, ndim=1)[:]

# Pass Objects and primitives
b = kd.slice([[1, 2], [3, None, 5]])
kd.map_py(lambda a, b: a.x + a.y + b, a, b)

# Return Objects
f = lambda x: kd.obj(x=1) if x.y < 3 else kd.obj(y=1)
res = kd.map_py(f, kd.obj(y=kd.slice([[1, 2], [3]])))

# With max_thread to speed up the I/O-bound
# executions (e.g. reading from disk or RPCs)
kd.map_py(fn, ds, max_thread=20)

# Pass multiple arguments
f = lambda x, y, z: x + y + z
kd.map_py(f, 1, 2, 3) # positional
kd.map_py(f, z=3, y=2, x=1) # keyword
kd.map_py(f, 1, z=3, y=2) # mix of positional and keyword
```

</section>

<section>

### Conditional Apply Operator

```py
# Note that 'a' is passed as a DataSlice
def f1(a):
  return a + 1

a = kd.slice([1, 2, 3])

# Apply Python python
c = kd.apply_py_on_selected(f1, a > 1, a) # [None, 3, 4]

# Fill the missing items with values from 'a'
c | a # [1, 3, 4]

def f2(a, b):
  return a + b

def f3(a, b):
  return a - b

b = kd.slice([4, 5, 6])

kd.apply_py_on_cond(f2, f3, a > 1, a, b) # [-3, 7, 9]

# use kwargs
kd.apply_py_on_cond(f2, f3, a > 1, b=b, a=a)

# similarly map_py can be applied conditionally
kd.map_py_on_selected(lambda a: a + 1, a > 1, a,
                      schema=kd.INT32)

kd.map_py_on_cond(lambda a, b: a + b,
                  lambda a, b: a - b, a > 1, b=b, a=a,
                  schema=kd.INT32)
```

</section>

## Schema

Koda schema is used to describe the type of contents. There are five types of
schemas:

-   **Primitive schema (a.k.a. dtype)** (e.g. kd.INT32, kd.FLOAT64, kd.STRING)
-   **Entity schema** used to describe what attributes the Entity has and what
    types these attribute are. Note that List schema and Dict schema are special
    entity schemas
-   **OBJECT** used to indicate the schema is stored as data
-   **ITEMID** used to indicate the content should be interpreted as opaque
    ItemIds rather than the data those ItemIds refer to
-   **ANY** used to indicate unknown/mixed schema

Schema is used to specify the behaviors for:

-   Attribute lookup (including Dict lookup)
-   Clone and extract
-   Attribute setting (including List/Dict content modification)

Schemas can be attached to **DataSlices** or **individual items**. Attaching a
schema to a DataSlice means declaring that each item has the **same** schema.
Individual items can store their own schema in a special attribute `__schema__`
and such item is called Object. It allows storing Objects with **different**
schemas in one DataSlice. Primitives can be considered as OBJECT schemas as
dtypes are embedded in their values.

**DataSlice Schema** refers to schemas attached to DataSlices. **Embedded
Schema** refers schemas embedded inside Koda Objects as `__schema__` attribute
or dtypes for primitives. DataSlice Schema can be primitive schema, Entity
schema, OBJECT, ITEMID, or ANY. Embedded Schema cannot be ITEMID or ANY.

Entity schemas can be either **explicit** or **implicit**. Explicit schemas are
created using `kd.new_schema/list_schema/dict_schema()` while implicit schemas
are created as a by-product of `kd.obj()`.

<section>

### Schema Creation

`kd.named_schema/list_schema/dict_schema/uu_schema(...)` creates an explicit
schema.

```py
# Create a named schema
Point = kd.named_schema('Point', x=kd.INT32, y=kd.FLOAT64)

# Attribute 'end' can be anything
Line = kd.named_schema('Line', start=Point, end=kd.ANY)

# Get the attribute start's schema
Line.start

# List schema
ls1 = kd.list_schema(kd.INT64)

# List entity schema is 's1'
ls2 = kd.list_schema(Point)

# Get the List item schema
ls2.get_item_schema()

# Dict schema
# Dict value schema is kd.OBJECT. That is,
# schemas are stored in entity __schema__
# attribute
ds1 = kd.dict_schema(kd.STRING, kd.OBJECT)

# Dict value schema is 's2'
ds2 = kd.dict_schema(kd.ITEMID, Line)

# Get the Dict key/value schema
ds2.get_key_schema()
ds2.get_value_schema()

# UU schema
uus1 = kd.uu_schema(x=kd.INT32, y=kd.FLOAT64)

# UU schemas with the same contents are the same
uus2 = kd.uu_schema(x=kd.INT32, y=kd.FLOAT64)
assert uus1 == uus2

# In fact, named, list and dict schemas are also
# UU schemas
Point1 = kd.named_schema('Point', x=kd.INT32, y=kd.FLOAT64)
assert Point1.as_itemid() == Point.as_itemid()

## Create non-uu schema whose ItemId is allocated
s1 = kd.schema.new_schema(x=kd.INT32, y=kd.FLOAT64)
s2 = kd.schema.new_schema(x=kd.INT32, y=kd.FLOAT64)

assert s1.as_itemid() != s2.as_itemid()
```

</section>

<section>

### Item Creation Using Schemas

Schemas can be used to create entities by calling these schema DataItems
directly as factory methods. Or we can use `kd.new`. That is, `schema(...)` is
equivalent to `kd.new(..., schema=schema)`.

```py
Point = kd.named_schema('Point', x=kd.INT32, y=kd.FLOAT64)
Line = kd.named_schema('Line', start=Point, end=kd.ANY)

i1 = Point(x=1, y=2.3)
# which is equivalent to
i1 = kd.new(x=1, y=2.3, schema=Point)
i2 = Line(z=i1, w='4')

s3 = kd.list_schema(kd.INT64)
s4 = kd.list_schema(Point)

i3 = s3([1, 2, 3, 4])
i4 = s4([i1, i1])

s5 = kd.dict_schema(kd.TEXT, kd.OBJECT)
s6 = kd.dict_schema(kd.ITEMID, Line)

i5 = s5({'a': kd.obj()})
i6 = s6(kd.obj().as_itemid(), i2)
```

</section>

<section>

### Item Creation without Schema

When no schema is provided to `kd.new`, an schema is automatically derived based
on provided arguments or keyword arguments. When a string is provided as schema,
an uu schema is created based on the name.

```py
# kd.new() creates entities with derived schema
i1 = kd.new(x=1, y=2.0, z='3')

# The result DataItem has a auto-drived schema
assert i1.get_schema().x == kd.INT32
assert i1.get_schema().y == kd.FLOAT32
assert i1.get_schema().z == kd.TEXT

i2 = kd.new(x=1, y=2.0, schema='Point')
i3 = kd.new(x=2, y=3.0, schema='Point')
assert i2.get_schema() == i3.get_schema()
```

</section>

<section>

### Object Creation With Implicit Schemas

`kd.obj(...)` creates an object with an implicit schema.

```py
# An implicit schema is created and
# attached to the Koda object
o1 = kd.obj(x=1, y=2.0, z='3')

# The schema of the DataItem is kd.OBJECT
assert o1.get_schema() == kd.OBJECT

# The implicit schema is stored
# as __schema__ attribute and can be accessed
# using get_obj_schema()
o1.get_obj_schema()
# -> IMPLICIT_SCHEMA(x=INT32, y=FLOAT32, z=STRING)

# A new and different implicit schema is created
# every time kd.obj() is called
o2 = kd.obj(x=1, y=2.0, z='3')
assert o1.get_obj_schema() != o2.get_obj_schema()

# A new implicit schema is created for each
# newly created objects
o_ds = kd.obj(x=kd.slice([1, 2]))

# The schema of the DataSlice is kd.OBJECT
assert o_ds.get_schema() == kd.OBJECT

obj_ss = o_ds.get_obj_schema()
obj_ss.get_size()# 2
obj_ss.take(0)
# -> IMPLICIT_SCHEMA(x=INT32, y=FLOAT32, z=TEXT)
assert obj_ss.take(0) != obj_ss.take(1)
```

</section>

## Interoperability

<section>

### From_py

```py
# Primitives
kd.from_py(1)
kd.from_py(1, schema=kd.INT64)
kd.from_py(1.0)
kd.from_py(1.0, schema=kd.FLOAT64)
kd.from_py('a')
kd.from_py(b'a')
kd.from_py(True)
kd.from_py(None) # NONE schema rather than MASK

# Entity/Object
@dataclasses.dataclass
class PyObj:
  x: int
  y: float

py_obj = PyObj(x=1, y=2.0)

kd.from_py(py_obj) # Object
kd.from_py(py_obj, schema=kd.OBJECT) # Same as above
s1 = kd.named_schema('Point', x=kd.INT32, y=kd.FLOAT64)
kd.from_py(py_obj, schema=s1) # Entity

# dict_as_obj=True
py_dict = {'x': 1, 'y': 2.0}
kd.from_py(py_dict, dict_as_obj=True) # Object
kd.from_py(py_dict, schema=kd.OBJECT) # Same as above
kd.from_py(py_dict, dict_as_obj=True,
           schema=s) # Entity

# List
py_list = [[1, 2], [3], [4, 5]]
kd.from_py(py_list)
s2 = kd.list_schema(kd.list_schema(kd.INT64))
kd.from_py(py_list, schema=s2)

# Dict
py_dict = {'x': 1, 'y': 2.0}
kd.from_py(py_dict)
s3 = kd.dict_schema(kd.STRING, kd.FLOAT64)
kd.from_py(py_dict, schema=s3)

# Use provided itemids
id1 = kd.new_itemid()
kd.from_py(py_obj, itemid=id1)
id2 = kd.new_listid()
kd.from_py(py_list, itemid=id2)
id3 = kd.new_dictid()
kd.from_py(py_dict, itemid=id3)

# Use from_dim to treat the first X Phthon
# lists as DataSlice dimensions
py_list = [[1, 2], [3], [4, 5]]
kd.from_py(py_list, from_dim=2)

py_dicts = [{'x': 1, 'y': 2.0}, {'x': 3, 'y': 4.0}]
# Dicts as Objects
kd.from_py(py_dicts, from_dim=1)
# Dicts as Objects
kd.from_py(py_dicts, from_dim=1, schema=s3)
```

</section>

<section>

### To_py

```py
# Primitive DataItem
kd.item(1.0).to_py() # 1.0
kd.int64(1).to_py() # 1
# There is no MASK type in Python
kd.present.to_py() # kd.present
kd.missing.to_py() # None

# DataSlice of primitives
ds = kd.slice([[1, 2], [3], [4, 5]])
py_list = kd.to_py(ds)
py_list = ds.to_py() # same as above
assert py_list == [[1, 2], [3], [4, 5]]

# Entity DataItem
e = kd.new(x=1, y=2.0)
e.to_py() # Obj(x=1, y=2.0)
e.to_py(obj_as_dict=True) # {'x': 1, 'y': 2.0}
s = kd.named_schema('Point', x=kd.INT32, y=kd.FLOAT32)
e1 = kd.new(x=1, schema=s)
e1.to_py(obj_as_dict=True) # {'x': 1, 'y': None}
e1.to_py(obj_as_dict=True,
         include_missing_attrs=False) # {'x': 1}

# Object DataItem
o = kd.obj(x=1, y=2.0)
o.to_py() # Obj(x=1, y=2.0)
o.to_py(obj_as_dict=True) # {'x': 1, 'y': 2.0}

# List DataItem
l = kd.list([1, 2, 3])
l.to_py() # [1, 2, 3]

# Dict DataItem
d = kd.dict({'a': 1, 'b': 2})
py_dict = d.to_py() # {'a': 1, 'b': 2}

# Nested Entity/Object/List/Dict item
# Each item is counted as one depth
# When max_depth (default to 2) is reached,
# the item is kept as a DataItem
i = kd.obj(a=kd.obj(b=kd.obj(c=1),
                    d=kd.list([2, 3]),
                    e=kd.dict({'f': 4})))
i.to_py()
# Obj(a=Obj(b=DataItem(Obj(c=1)),
#           d=DataItem(List[2, 3]),
#           e=DataItem(Dict{'f'=4})))
i.to_py(max_depth=3)
# Obj(a=Obj(b=Obj(c=1), d=[2, 3], e={'f': 4}))
# Use -1 to convert everything to Python
i.to_py(max_depth=-1)
# Obj(a=Obj(b=Obj(c=1), d=[2, 3], e={'f': 4}))

# Primitive DataSlice
ds = kd.slice([[1, 2], [None, 3]])
ds.to_py() # [[1, 2], [None, 3]]

# Entity DataSlice
ds = kd.new(a=kd.slice([1, None, 3]),
            y=kd.slice([4, 5, None]))
res = ds.to_py()
# [Obj(x=1, y=4),
#  Obj(x=None, y=5),
#  Obj(x=3, y=None)]
# Note that each object has its own Python class
assert res[0].__class__ != res[1].__class__

# Object DataSlice
ds = kd.obj(a=kd.slice([1, None, 3]),
            y=kd.slice([4, 5, None]))
res = ds.to_py()
# [Obj(x=1, y=4),
#  Obj(x=None, y=5),
#  Obj(x=3, y=None)]

# List DataSlice
lists = kd.implode(kd.slice([[1, 2], [None, 3]]))
lists.to_py() # [[1, 2], [None, 3]]

# Dict DataSlice
dicts = kd.dict(kd.slice([['a', 'b'], ['c']]),
                kd.slice([[1, None], [3]]))
dicts.to_py() # [{'a': 1}, {'c': 3}]
```

</section>

<section>

### From/To Numpy Array

```py
from koladata.ext import npkd

npkd.to_array(ds)
npkd.from_array(np.array([1, 2, 0, 3]))
```

</section>

<section>

### From/To Panda DataFrame

```py
from koladata.ext import pdkd

pdkd.to_dataframe(kd.obj(x=ds1, y=ds2),
                  cols=['x', 'y'])
pdkd.from_dataframe(
    pd.DataFrame(dict(x=[1,2,3], y=[10,20,30])))
```

</section>

<section>

### From/To Proto

```py
kd.to_proto(obj, proto.db.MyMessage)
kd.from_proto(protos)
```

</section>

<section>

### Serialization

```py
# Serialize DataSlice into bytes
s = kd.dumps(ds)
ds = kd.loads(s)

# Serialize DataBag into bytes
s = kd.dumps(db)
db = kd.loads(s)
```

</section>

## Bags of Attributes

<section>

### DataBag

```py
# Empty bag creation
db1 = kd.bag()

# Bag created directly is mutable
assert db1.is_mutable()

# Get the bag from the DataSlice
obj = kd.uuobj(x=1, y=2)
db2 = obj.get_bag()

# Bag created from item creation APIs
# under kd is immutable
assert not db2.is_mutable()

# Attach a bag to the DataSlice
obj1 = obj.with_bag(db1)

# Remove the bag from the DataSlice
obj2 = obj1.no_bag()

# Approximate number of triples in the bag
db2.get_approx_size()

# Print quick stats about the bag
repr(db2)

# Print out data and schema triples
db2.contents_repr()
# Print out data triples only
db2.data_triples_repr()
# Print out schema triples only
db2.schema_triples_repr()

# Create a new mutable bag by forking the bag
db3 = db2.fork()
assert db3.is_mutable()

# Create a new immutable bag by freezing the bag
db4 = db3.freeze()
assert not db4.is_mutable()
```

</section>

<section>

### Merging DataBags

```py
# Merge two bags by creating a new bag
# with db1 and db2 as fallbacks
# db2 overrides db1 in terms of conflicts
db1 << db2
kd.updated_bag(db1, db2) # Same as above

# Can be chained together
db1 << db2 << db3
kd.updated_bag(db1, db2, db3) # Same as above

# Merge two bags by creating a new bag
# with db2 and db1 as fallbacks
# db1 overrides db2 in terms of conflicts
db1 >> db2
kd.enriched_bag(db1, db2) # Same as above

# Can be chained together
db1 >> db2 >> db3
kd.enriched_bag(db1, db2, db3) # Same as above

# Merge db2 into db1 in place
# db2 overrides db1 in terms of conflicts
# schema conflicts are not allowed
db1.merge_inplace(db2)
# Merge db2 and db3 into db1 in place
db1.merge_inplace([db2, db3])
# Do not overwrite
db1.merge_inplace(db2, overwrite=False)
# Allow schema conflicts
db1.merge_inplace(db2, allow_schema_conflicts=True)
# Disallow data conflicts
db1.merge_inplace(db2, allow_data_conflicts=False)

# Merge db and its fallbacks into a single bag in place
# It improves lookup performance (e.g. accessing attrs,
# list items) but can be expensive to merge all bags
db3 = db.merge_fallbacks()
db3 = kd.with_merged_bag(db) # Same as above
```

</section>

<section>

### Extract/Clone

```py
s1 = kd.uu_schema(x=kd.INT32, y=kd.INT32)
s2 = kd.uu_schema(z=s1, w=s1)

i1 = s2(z=s1(x=1, y=2), w=s1(x=3, y=4))

# extract creates a copy of i1 in a new
# bag with the same ItemIds
i2 = i1.extract()
assert i2.get_itemid() == i1.get_itemid()
assert i2.z.get_itemid() == i1.z.get_itemid()

# Extract a subset of attributes
s3 = kd.uu_schema(w=s1)
i3 = i1.extract(schema=s3)
i3 = i1.set_schema(s3).extract() # same as above

assert not i3.has_attr('z')

# clone creates a copy of i1 in with a new ItemId
# and keep same ItemIds for attributes
i4 = i1.clone()
assert i4.get_itemid() != i1.get_itemid()
assert i4.z.get_itemid() == i1.z.get_itemid()
i4.z.get_attr_names(intersection=True) # ['x', 'y']

# shallow_clone creates a copy of i1 in with a new
# ItemId and keep same ItemIds for top-level attributes
i5 = i1.shallow_clone()
assert i5.get_itemid() != i1.get_itemid()
assert i5.z.get_itemid() == i1.z.get_itemid()
i5.z.get_attr_names(intersection=True) # []

# deep_clone creates a copy of i1 and its attributes
# with new ItemIds
i6 = i1.deep_clone()
assert i6.get_itemid() != i1.get_itemid()
assert i6.z.get_itemid() != i1.z.get_itemid()
i6.z.get_attr_names(intersection=True) # ['x', 'y']
```

</section>

<section>

### Stub/Ref

A Entity/Object/Dict stub is an empty Entity/Object/Dict with necessary metadata
needed to describe its schema in a new bag. A List stub is a list with list item
stubs in a new bag. It can be used to add new data to the *same* item in the
newly created bag which can merged into the original data.

A Entity/Object/List/Dict ref is a reference to the same Entity/Object/List/Dict
without a bag attached.

```py
obj = kd.obj(x=1, y=kd.obj(z=1))

# Create a obj stub without attributes
obj2 = obj.stub()
assert obj2.get_itemid() == obj.get_itemid()
assert not obj2.get_attr_names(intersection=False) # []

# Create a list stub with list item stubs
l = kd.list([kd.obj(x=1), kd.obj(x=2)])
l2 = l.stub()
assert l2.get_itemid() == l.get_itemid()
# List items are stubbed as well
assert kd.agg_all(l2[:] == l[:])
assert not l2[:].get_attr_names(intersection=False) # []

# Create a dict stub without entries
d = kd.dict({'a': kd.obj(x=1), 'b': kd.obj(x=2)})
d2 = d.stub()
assert d2.get_itemid() == d.get_itemid()
assert assert kd.dict_size(d2) == 0

# Get a ref
obj_ref = obj.ref()
assert obj_ref.get_itemid() == obj.get_itemid()
assert obj_ref.get_bag() is None
```

</section>

## Tracing and Functors

<section>

```py
# Create functor.
kd.fn(lambda x: x + 1)
# Functor with no tracing.
kd.py_fn(lambda x: x + 1)

# Check if obj represents a functor.
kd.is_fn(obj)

# Avoid inlining Python function during tracing.
@kd.trace_as_fn()
```

</section>

## Advanced: Mutable Workflow

<section>

```py
# TODO: Add examples
```

</section>
