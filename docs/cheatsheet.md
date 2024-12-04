<!--* css: "//koladata/g3doc/cheatsheet.css" *-->
<!-- For the best formatting, please limit the line_max_char to 50. -->

# Koda Cheatsheet
## Basic API

<section>

### Primitives and DataItem

```py
# Primitive dtypes.
kd.INT32
kd.INT64
kd.FLOAT32
kd.FLOAT64
kd.STRING
kd.BYTES
kd.BOOLEAN
kd.MASK

# MASK type values.
kd.present
kd.missing

# DataItem.
i = kd.item(1)
kd.is_item(i)
kd.is_primitive(i)

# DataItem creation.
kd.int32(1)
kd.int64(2)
kd.float32(1.1)
kd.float64(2.2)
kd.str('a')
kd.bytes(b'a')
kd.bool(True)
kd.mask(None)

kd.from_py(1)
```

</section>

<section>

### DataSlice

```py
ds = kd.slice([[1,2],[3,4,5]])
kd.is_slice(ds)

ds.get_size()
ds.get_ndim()
ds.get_shape()

# Navigate DataSlice as "nested" lists.
ds.L[1].L[2]
# Python list of one-dim DataSlice.
ds.L[:]
# Items at idx in the first dims.
ds.L[idx]

# Subslice.
ds.S[1, 2]
ds.S[2]  # Take 2nd item in the last dimension.
ds.S[1:, :2]

# Expand the outermost dimension into a list.
ds.to_pylist()

ds.to_py()
ds.to_str()
```
</section>

<section>

### Entities

```py
e = kd.new(x=1, y=2, schema='Point')

# Update attributes (creates a new entity).
e.with_attrs(z=4, y=10)
# Allows mixing multiple updates.
e.updated(kd.attrs(e, z=4) | kd.attrs(e, y=10))
```

</section>

<section>

### Objects

```py
o = kd.obj(a=1, b=2)

o.get_attr('a', default=4)
o.has_attr('a')
# Return missing if not found.
o.maybe('a')
```

</section>

<section>

### Lists

```py
l = kd.list([1, 2, 3])
# Nested lists.
l = kd.list([[1, 2], [3], [4, 5]])

kd.is_list(l)
kd.list_size(l)

# Python-like list operations.
l[0]  # Same as kd.get_item(l, 0)
l[1:]

# Explode the list to a DataSlice.
l1[:]

# Returns a DataSlice of lists concatenated from
# the list items of arguments.
kd.concat_lists(kd.list([1, 2]), kd.list([3, 4]))

# Filter out missing items from a list DataSlice.
kd.list([1, 2, None, 4]).select_items(
    lambda x: x > 2)
```

</section>

<section>

### Dicts

```py
d = kd.dict({'a': 1, 'b': 2})

kd.is_dict(d)
d.dict_size()

d.get_keys()
d.get_values()
d['a']  # Same as d.get_item('a')

# Create a new dict, since dicts are immutable.
d = d.with_dict_update('c', 5)
# Same as dict_update.
d.updated(
  kd.dict_update(d, 'c', 30) |
  kd.dict_update(d, 'a', 10))

# Select dict keys by filtering out
# missing items in fltr.
d.select_keys(fltr)
# Select dict values by filtering out
# missing items in fltr.
d.select_values(fltr)
```

</section>

<section>

### Editable containers

```py
x = kd.container()
x.a = 1
x.d = kd.container()
x.d.e = 4
x.d.f = kd.list([1, 2, 3])
```

</section>

### Schemas

```py
kd.new_schema(x=kd.INT32)

kd.slice([1, 2, 3]).get_schema()
kd.slice([1, 2, 3]).get_dtype()

# Schema can be set and created explicitly.
kd.slice([None, None]).with_schema(kd.INT32)
kd.named_schema('Point', x=kd.INT32, y=kd.INT32)

# Universally-unique schema.
kd.uu_schema(x=kd.INT32)

# For lists.
ds.get_item_schema()
# For dicts.
ds.get_key_schema()
ds.get_value_schema()

# Returns DataSlice of schemas for
# Objects and primitives in argument.
kd.obj(x=1, y=2).get_obj_schema()
```

</section>

<section>

### Changing shape of a DataSlice

```py
# Flatten to one dimension.
ds.flatten()
# Remove the last dimension.
ds.flatten(-2)
# Remove the first dimension.
ds.flatten(0, 2)

# Reshape to the shape of another DataSlice.
ds.reshape_as(ds1)
ds.reshape(ds1.get_shape())

# Repeats values of ds.
ds.repeat(3)
# Repeats present values of ds.
ds.repeat_present(3)

# Stacks the DataSlices,
# creating a new dimension at index rank-ndim.
kd.stack(ds, ds+1)
kd.stack(ds, ds, ndim=1)
# Zip DataSlices over the last ndim dimension.
kd.zip(ds, ds*10)

# Concats the DataSlices on dimension rank-ndim.
kd.concat(ds1, ds2)
```

</section>

<section>

### Broadcasting and aligning

```py
# Expands x based on the shape of target.
kd.slice([100, 200]).expand_to(
    kd.slice([[1,2],[3,4,5]]))
# Implodes last ndim dimensions into lists,
# expands the slice of lists, explodes the result.
kd.slice([100, 200]).expand_to(
    kd.slice([[1,2],[3,4,5]]), ndim=1)

kd.is_expandable_to(x, target, ndim)
# Whether any of args is expandable to the other.
kd.is_shape_compatible(x, y)

# Expands DataSlices to the same common shape.
kd.align(kd.slice([[1, 2, 3], [4, 5]]),
         kd.slice('a'), kd.slice([1, 2]))
```

</section>

<section>

### Aggregational operators

```py
# Collapse over the last dimension and
# return the value if all items has the
# same value or None otherwise.
kd.collapse(kd.slice([[1],[2,2],[2,3,4]]))

 # Aggregates over the last dimension.
kd.agg_all(x)
kd.agg_any(x)
kd.agg_has(x)

# Counts both present and missing items.
kd.agg_size(x)
# Counts present items only.
kd.agg_count(x)
kd.agg_count(x, ndim=2)

# Aggregate across all dimensions.
kd.all(x)
kd.any(x)
kd.size(x)
kd.count(x)

# Adds dimension after grouping items.
kd.group_by(kd.slice([4,3,4,2,2,1,4,1,2]))

# Returns indices starting from 0.
# Aggregates over the last dimension.
kd.index(x)
# Aggregates over the last 3 dimensions
kd.index(x, dim=3)
# Create a new slice with items at indices.
x.take(indices)

# Returns the rank of x.
kd.get_ndim(x)
```

</section>

<section>

### Other DataSlice manipulations

```py
# Slice of INT64s with range [start, end).
kd.range(1, 5)
kd.range(0, kd.slice([3,2,1]))

# Unique values within each dimension.
kd.unique(ds, sort=False)

# Reverse on the last dimension.
kd.reverse(ds)

# Map keys to values using k=>v mapping.
kd.translate(keys_to, keys_from, values_from)
kd.translate_group(
  keys_to, keys_from, values_from)
```

</section>

<section>

### Explosion and implosion

```py
a = kd.list([[[1, 2], [3]], [[4], [5]]])
# Explodes the list ndim times.
kd.explode(a, ndim=2)
# Explode until there are no more lists.
kd.explode(a, ndim=-1)

a = kd.slice([[[1, 2], [3]], [[4], [5]]])
# Implodes the list ndim times.
kd.implode(a, ndim=2)
# Implode until the result is a List DataItem.
kd.implode(a, ndim=-1)
```

</section>

<section>

### Comparison operators
```py
# Returns masks.
kd.greater(ds1, ds2)
kd.less_equal(ds1, ds2)
kd.equal(ds1, ds2)

# Infix operators
ds1 > ds2
ds1 <= ds2
ds1 == ds2

# Compare with Python objects.
a > 1
a < [3, 2, 1]
```

</section>

<section>

### Sparsity and logical operators

```py
kd.has(a)
kd.agg_has(a)
kd.has_not(a)

kd.apply_mask(a, a > 1)
a & (a > 1)  # Same

~a  # Invert mask.

kd.coalesce(a, b)
a | b # Same

kd.cond(c > 1, a, b)

kd.logical.mask_and(a, b)
kd.logical.mask_or(a, b)
kd.logical.mask_equal(a, b)
kd.logical.mask_not_equal(a, b)
```

</section>

<section>

### Filtering operators

```py
# Filter out missing items in filter.
x.select(x >= 3)
# Remove missing items.
x.select_present()

# Put items in present positions in filter.
kd.inverse_select(
    kd.slice([1, 2]),
    kd.slice([kd.missing, kd.present, kd.present])
)
```

</section>

<section>

### Math operators

```py
x + y  # Short for kd.add(x, y)
x - y  # Short for kd.subtract(x, y)
x * y  # Short for kd.multiple(x, y)
x / y  # Short for kd.divide(x, y)
x % y  # Short for kd.mod(x, y)

kd.math.abs(x)
kd.math.pow(x, y)
kd.math.exp(x)
kd.math.log(x)
kd.math.log10(x)
kd.math.round(x)
kd.math.ceil(x)
kd.math.floor(x)
kd.math.floordiv(x, y)
kd.math.maximum(x, y)
kd.math.mimimum(x, y)

kd.math.agg_max(x)
kd.math.agg_min(x)
kd.math.agg_median(x)
kd.math.agg_mean(x)
kd.math.agg_sum(x)
kd.math.agg_count(x)
kd.math.agg_std(x)
kd.math.agg_var(x)
kd.math.min(x)
kd.math.max(x)
kd.math.sum(x)
kd.math.count(x)

kd.math.cum_min(x)
kd.math.cum_max(x)
kd.math.cum_sum(x)
kd.math.cum_count(x)

kd.math.cdf(x)
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
kd.to_text(kd.slice([1, 2, 3])
kd.to_bytes(kd.slice([b'1', b'2', b'3'])

# Dispatches to the relevant kd.to_* operator
# or casts into specific schema.
kd.cast_to(kd.slice([1, 2, 3]), kd.INT64)
kd.cast_to(kd.new(x=1), kd.new_schema(x=kd.INT32))
```

</section>

<section>

### Random number and sampling operators

```py
# Random integers.
kd.randint_like(x)
kd.randint_like(x, 10)
kd.randint_like(x, -5, 200)

kd.randint_shaped_as(ds)

# Sample from the last dimension.
kd.sample(ds, ratio=0.7, seed=42)
# Use key to make item selection robust.
kd.sample(x, ratio=0.7, seed=42, key=kd.index(x))
# Select n items from last dimension.
kd.sample_n(ds, n, seed)
```

</section>

<section>

### Sort and rank operators

```py
# Sort over the last dimension.
kd.sort(ds)

# Equal items get the same rank,
# no gap between rank numbers.
kd.dense_rank(ds)
# Equal items get distinct ranks based on
# tie breaker.
kd.ordinal_rank(ds), tie_breaker=-kd.index(x))
```

</section>

<section>

### String operators

```py
kd.strings.length(x)
kd.strings.fstr(f'${ds:s}')
kd.strings.printf("%d-%s", kd.index(ds), ds)
kd.strings.format("{index}-{val}",
                  index=kd.index(ds), val=ds)

kd.strings.split(kd.slice(["a,b", "c,d"]), ',')
kd.strings.agg_join(
    kd.slice([['a', 'b'], ['c', 'd']]), ',')

kd.strings.regex_match(ds, '^(.*):')
kd.strings.regex_extract(ds, '^a')

kd.strings.contains(ds, 'a')
kd.strings.count(ds, 'a')
kd.strings.find(ds, 'a')
kd.strings.rfind(ds, 'a')
kd.strings.join(ds, ds)
ks.strings.length(ds)
kd.strings.lower(ds)
kd.strings.upper(ds)
kd.strings.strip(ds, 'a')
kd.strings.lstrip(ds, 'a')
kd.strings.rstrip(ds, 'a')
kd.strings.replace(ds, 'a', 'A')
kd.strings.substr(ds, 1, 3)

kd.encode(ds)  # Encode as bytes.
kd.bytes.length(ds)

kd.strings.decode(x)  # Decode as text.
kd.strings.encode(x)  # Encode as bytes.
```

</section>

<section>

### Item creation operators

```py
# *_like operators follow sparsity.
# *_shaped_as follow just shape.

# Const value.
kd.val_like(x, 1)
kd.val_shaped_as(x, 1)
kd.val_shaped(s, 1)

# Mask.
kd.present_like(x)
kd.present_shaped_as(x)
kd.present_shaped(s)

kd.empty_shaped_as(x)
kd.empty_shaped(s)

# Entity.
kd.new_like(x)
kd.new_shaped_as(x)
kd.new_shaped(s)

# Object.
kd.obj_like(x)
kd.obj_shaped_as(x)
kd.obj_shaped(s)

kd.list_like(x)
kd.list_shaped_as(x)
kd.list_shaped(s)

kd.dict_like(x)
kd.dict_shaped_as(x)
kd.dict_shaped(s)
```

</section>

<section>

### IDs and UUIDs

```py
# Dtype for ItemId.
kd.ITEMID

# Get unique id.
kd.obj(x=1, y=2).get_itemid()

# Encode to/from base-62 number (as string).
kd.encode_itemid(kd.obj(x=1, y=2))
kd.decode_itemid(str_id)
# int64 hash values.
kd.hash_itemid(kd.uuid(a=1, b=2))

# Compute deterministic id from attr values.
kd.uuid(x=1, y=2)
# Traverse attrs instead of using their UUIDs.
kd.deep_uuid(obj)

# Compute uuid for multiple items.
kd.agg_uuid(kd.slice([[1,2,3], [4,5,6]]))

# Create entity with UUID.
kd.uu(x=1, y=2)
# Create object with UUID.
kd.uuobj(x=1, y=2)
```

</section>

<section>

### Serialization and protos

```py
# Serialize slices into bytes.
s = kd.dumps(ds)
ds = kd.loads(s)

# Serialize DataBag into bytes.
s = kd.dumps(db)
db = kd.loads(s)

# Serialize to proto.
kd.to_proto(obj, proto.db.MyMessage)
kd.from_proto(protos)
```

</section>

<section>

### Using Python functions

```py
# Apply a Python function to slices.
kd.apply_py(lambda x,y: x+y, a, b)
kd.apply_py_on_cond(lambda x,y: x+y, a > 2, a, b)
kd.apply_py_on_selected(lambda x,y: x+y,
                        a > 2, a, b)

# Apply to individual items.
kd.map_py(lambda x, y: x*y, ds, 2, max_threads=4)
# Can change number of dimensions.
kd.map_py(
    lambda x: print(x) if x else None, ds, ndim=2)
# Schema passed to handle empty inputs.
kd.map_py_on_present(
    lambda x: x.upper(), s, schema=kd.STRING)
```

</section>

<section>

### Interoperability

<!-- TODO: Update names in this section. -->

```py
# Pandas
from koladata.ext import pdkd
pdkd.to_dataframe(kd.obj(x=ds1, y=ds2),
                  cols=['x', 'y'])
pdkd.from_dataframe(
    pd.DataFrame(dict(x=[1,2,3], y=[10,20,30])))

# Numpy
from koladata.ext import npkd
npkd.to_array(ds)
npkd.from_array(np.array([1, 2, 0, 3]))
```

</section>

<section>

### Bags of attributes

```py
# DataBag creation.
db = kd.bag()  # Empty.
db = x.attrs(a=1, b=2)
# Get DataBag corresponding to entity or object.
db = x.get_bag()

# Explore DataBag contents.
db.get_size()
db.contents_repr()
db.data_triples_repr()
db.schema_triples_repr()

# Merge two bags.
kd.updated_bag(kd.attrs(x, a=1), kd.attrs(x, b=3))
kd.attrs(x, a=1) << kd.attrs(x, b=3)  # Same.
# Keeps attributes of the first bag.
kd.enriched_bag(
    kd.attrs(x, a=1), kd.attrs(x, a=2, b=3))
```

</section>

<section>

### Extract and clone

```py
# Copy with a bag containing
# only attrs accessible from x.
x.extract()
# Empty copy containing only schema.
x.stub()

# Non-recursive copy in a new bag.
x.clone()
# Recursive copy in a new bag.
x.deep_clone()
# Top-level attributes are copied by reference.
x.shallow_clone()
```

</section>

<section>

### Tracing and functors

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
