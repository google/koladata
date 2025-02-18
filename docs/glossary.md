# koda Glossaries

* TOC
{:toc}

## Basics

#### .

The `.` operator, short for `__getattr__` method, is used to access attributes'
values for [Entities](#entity) or [Objects](#object). For example, `obj.attr`.

#### [] {#bracket}

The `[]` operator, short for `__getitem__` method, is used to access items in
[Lists](#list) or key/value in [Dicts](#dict).

If a DataSlice contains Lists,

-   `list_ds[:]` (a.k.a. [list explosion](#explosion)) returns all items and
    adds an additional [dimension](#dimension)
-   `list_ds[idx]` returns items corresponding to the index `idx` and does not
    add new [dimension](#dimension)
-   `list_ds[n:m]` returns items corresponding to the indices from `n` to `m`
    and adds an additional [dimension](#dimension)
-   `list_ds[ds]` returns items corresponding to indices in `ds` which must have
    compatible shape with `list_ds`

If a DataSlice contains Dicts,

-   `dict_ds[:]` returns all dict values
-   `dict_ds[key]` returns values corresponding to the key
-   `dict_ds[key_ds]` returns values corresponding to the keys in `key_ds`

#### Aggregation {#aggregation}

**Aggregation** is a process of performing operation(s) (e.g. `max`, `mean`)
over a vector of items. If an operator performs aggregation, it is referred as
an **aggregational** operator. Most commonly, aggregation (`min`) reduces
dimension. However, it is not true for all aggregations. For example, some
aggregations (e.g. `sort`, `cum_sum`) do not reduce dimension. Aggregation means
items need to be processed **together** rather than [**pointwise**](#pointwise).

#### Align {#align}

**Aligning** is a process of broadcasting a DataSlice with lower dimension to
another DataSlice with higher dimension. Aligning can happen **automatically**
when shapes of DataSlices in an operation are **compatible**. Or it can be
explicitly performed with `kd.align(ds1, ds2, ...)`.

#### Allocated ItemIds {#allocated_itemid}

**Allocated ItemIds** are allocated when new
Lists/Dicts/Entities/Objects/Schemas are created or new data is added to a
DataBag. An ItemId is 128-bit integer. Many ItemIds can be allocated at the same
time and share the same [Allocation](#allocation), which allows **performance
optimizations**.

#### Allocation {#allocation}

**Allocation** is a special internal object used to hold a group of ItemIds
consecutively allocated together.

#### Broadcast {#broadcast}

**Broadcasting** is the process of repeating items in one DataSlice based on a
compatible shape of another DataSlice such that the resulting DataSlice has the
same shape. Two shapes are **compatible** if [dimensions](#dimension) of one
DataSlice equal or are prefix of dimensions of another DataSlice. That is, two
DataSlices have exactly the same first N dimensions, where N is the number of
dimensions in the smaller DataSlice.

#### Computation Graph {#computation_graph}

A **computation graph/logic** in Koda is represented by a [Functor](#functor).
It can be [traced](#tracing) from a Python function or created directly.

#### DataBag {#databag}

**DataBag** is a collection of **data** and **schema metadata** stored as
[**Triples**](#triple) to represent **structured** data. Conceptually, a DataBag
is similar to a **triple-based database** (a.k.a.
[**graph database**](https://en.wikipedia.org/wiki/Graph_database)) storing a
list of protos and structs.

#### DataItem {#dataitem}

A **DataItem** is a special [DataSlice](#dataslice) without any dimension. Its
size is always `1`. It can hold either a single Koda [Item](#item) or a missing
item.

#### DataSlice {#dataslice}

A **DataSlice** is a **multi-dimension** [**jagged array**](#jagged_array) of
Koda [Items](#item). Conceptually, a DataSlice can be thought as an array with a
partition tree. Internally, it is implemented as a
[**flattened array**](#flattened) and a list of [dimensions](#dimension). It
also contains an [ItemId](#itemid) representing the **schema** of its items. It
can either have a **DataBag** or not.

#### Dimension {#dimension}

**Dimensions** of a DataSlice represent the [shape](#shape) of a jagged array.
Dimensions can be used for [aggregation](#aggregation) or
[broadcasting](#broadcast).

#### Dict {#dict}

**Dict** is a special built-in type in Koda. Similar to Python dict, a Koda Dict
contains a set of key/value pairs and the cost of key lookup is O(1). `dict[:]`
returns all keys and `dict[key]` returns the value corresponding to the key.

#### DType {#dtype}

**DTypes** refer to schemas of Koda [primitives](#primitive). Supported DTypes
are `INT32`, `INT64`, `FLOAT32`, `FLOAT64`, `STRING`, `BYTES`, `BOOL`, and
`MASK`.

#### Eager {#eager}

**Eager** is a programming model where computation is performed immediately
instead of lazily. In Koda, eager computation is performed using `kd` operators.

#### Enrich {#enrich}

**Enriching** is a process of adding a DataBag as a fallback to another DataBag.
`db1 >> db2` means enriching `db1` with `db2` and values for the same attributes
in `db1` have higher priorities than `db2`.

#### Enriched Bag {#enriched_bag}

An **enriched bag** is an empty DataBag enriched up provided other DataBags.

Also see [DataBag](#databag).

#### Entity {#entity}

**Entity** is a built-in type in Koda. A Koda Entity consists of a set of
attribute-values. Entity attributes can be accessed using `entity.attr`.

#### Entity Schema {#entity_schema}

**Entity schemas** are used to describe what attributes the Entity has and what
types these attribute are. Note that List schema and Dict schema are special
Entity schemas as well.

#### Explicit Schema {#explicit_schema}

[Entity schemas](#entity_schema) can be either **explicit** or **implicit**.
When handling conflicts of schemas for entity attribute setting: explicit schema
does not allow an override of its attribute's schema unless it is updated
explicitly. Explicit schemas can be created either directly (i.e.
`kd.new/list/dict_schema(...)`) or as a by-product of [Entity](#entity) creation
(i.e. `kd.new/list/dict(...)`) when no schema is provided.

Also see [implicit schema](#implicit_schema).

#### Extract, Clone, Shallow Clone, Deep Clone {#extract}

Clone and extraction are processes of getting items of a DataSlice and their
attributes from a DataBag based on schema. Cloned/extracted [triples](#triple)
are stored in a new DataBag.

-   `kd.extract` keeps the same ItemIds for extracted items and attributes.
-   `kd.deep_clone` creates new ItemIds for cloned items and attributes.
-   `kd.clone` creates new ItemIds for cloned items while keeping the same
    ItemIds for attributes.
-   `kd.shallow_clone` creates new ItemIds for cloned items while only keeping
    the top-level attributes.

#### Flatten {#flatten}

**Flattening** is an operation of reducing dimensions of DataSlice without
changing items inside the DataSlice. For example, `[[1, 2], [3, 4]] -> [1, 2, 3,
4]`.

#### Forking {#forking}

**Forking** is an operation of creating a DataBag copy from one DataBag. The
forked DataBag share the same [Triples](#triple) as the original DataBag. But
modifications in one DataBag do not propagate to another DataBag. The cost of
forking a DataBag is **O(1)**.

#### Functor {#functor}

A Koda **Functor** is a special [DataItem](#dataitem) containing a single Object
item representing a **callable function**. It can be called/run using
`fn(**inputs)`. While it can be created directly, it is more commonly created as
part of [tracing]{#tracing}.

#### I.self or S {#self_input}

`I.self` or `S` is a special Expr [input node](#input_node). When evaluating an
Expr, one can pass a single positional argument which will be available as the
input `I.self` or `S`. That is, `I.self` or `S` refers to `arg` in
`expr.eval(arg, **kwargs)`.

#### Implicit Schema {#implicit_schema}

[Entity schemas](#entity_schema) can be either **explicit** or **implicit**.
When handling conflicts of schemas for entity attribute setting, implicit schema
allows override of attribute schema and updates it implicitly. Implicit schemas
are created as a by-product of [object](#object) creation (i.e. `kd.obj(...)`).

Also see [explicit schema](#explicit_schema).

#### Immutable Workflow {#immutable_workflow}

**Immutable workflow** is a programming model where DataSlices are immutable and
cannot be modified in place and modifications are done by creating new
DataSlices. Modifications are represented by DataBags which normally called
[**updates**](#updates).

#### Item {#item}

Koda **Item** refers to the union of Koda [primitives](#primitive) and
[ItemId](#itemid) representing [List](#list), [Dict](#dict), [Entity](#entity),
[Object](#object) and [Schema](#schema).

#### ItemId {#itemid}

**ItemId**s are 128-bit integers and used to represent non-primitive Items (i.e.
Lists, Dicts, Entities, Objects). There are two types of ItemIds:
[**allocated ItemIds**](#allocated_itemid) or deterministically generated
[**UUID**](#uuid) derived from data.

#### Lazy {#lazy}

**Lazy** is a programming model where computation is performed lazily when
needed. In Koda, lazy computation is performed when evaluating an [Expr](#expr)
or [Functor](#functor).

#### List {#list}

**List** is a special built-in type in Koda. Similar to Python list, a Koda List
contains a group of ordered items. `list[:]` returns all items, `list[slice]`
returns items corresponding to the `slice` object and `list[idx]` returns the
item corresponding to the index `idx`.

#### List Explosion {#explosion}

**List explosion** is a special operation referring to getting list items from
[Lists](#list) using `list[:]` syntax. It adds an additional
[dimension](#dimension) to the resulting DataSlice.

#### List Implosion {#implosion}

**List implosion** is a special operation referring to folding items of a
DataSlice to [Lists](#list) over the last dimension using `kd.implode(ds)`. It
collapses the last [dimension](#dimension) from the resulting DataSlice.

#### Mask {#mask}

**MASK** is a special primitive dtype representing **presence**. It can only
have two states: `present` and `missing`. Therefore, it is different from the
**BOOLEAN** dtype which can have three states: `True`, `False` and `None`. In
Koda, comparisons (e.g. `==`, `<`) between DataSlices return DataSlices of MASK
type.

#### Named Schema {#named_schema}

A named schema is an uu schema whose UUID is derived from the schema name. It
can be created using `kd.named_schema('name')` or as a by-product of
`kd.new(**attrs, schema='name')`.

#### Object {#object}

**Object** is a special Koda Entity which stores its own schema directly as the
`__schema__` attribute similar to a Python object store its class as the
`__class__` attribute.

#### Operator {#operator}

Operators are functions performing operations on the provided inputs (normally
DataSlices). Koda provides a comprehensive list of operators under `kd` module.
Two common types of operators are [pointwise operator](#pointwise) and
[aggregational operator](#aggregation).

#### Pointwise {#pointwise}

A operation (e.g. `add`, `pow`, `or`) is **pointwise** if it can be done for
each item in a list independently. If an operator performs a pointwise
operation, it is referred as a pointwise operator. Also see
[aggregation](#aggregation).

#### Primitives {#primitive}

Koda **Primitives** are [Items](#item) representing primitives. There are eight
[dtypes](#dtype) of primitives: STRING, BYTES, INT32, INT64, FLOAT32, FLOAT64,
BOOLEAN, and MASK.

#### Present Count {#present_count}

The **present count** of a DataSlice is the number of present items across all
dimensions in the DataSlice. That is, present count = size - Count(missing
items).

#### Python-like Slicing {#py_slicing}

While [subslicing](#subslice) provides an efficient way to slice all dimensions,
it is sometimes convenient to slice dimensions **one by one** similar to how to
iterate through a nested Python list. DataSlice provides a way to slice the
first dimension using `.L` notation (e.g. `ds.L`, `ds.L[idx]`,
`ds.L[start:end]`). `L` stands for Python list. Subslicing using `.S[]` is a
**vectorized** slicing operation while Python-like slicing using `.L[]` allows
users to write code using the traditional **iterative** pattern.

As it needs to convert Koda objects to Python objects (e.g. Python list,
iterator), it is **not** efficient. We recommend using it only for debugging or
quick prototyping when it is hard to write vectorized code working on
multi-dimension.

#### Pytree {#pytree}

The concept of **pytree** is borrowed from
[JAX](https://jax.readthedocs.io/en/latest/pytrees.html) and refers to a
tree-like structure built out of container-like Python objects.

When importing a pytree to Koda, we can use `kd.from_py` or `kd.from_pytree` and
they are exactly the same. A Python list is converted to a Koda List and a
Python dict is converted to a Koda Dict if `dict_as_obj=False` or a Koda Object
if `dict_as_obj=True`.

To convert a Koda DataSlice to Python objects, we can use `kd.to_py()` or
`kd.to_pytree()` which is equivalent to `kd.to_py(..., obj_as_dict=True)`. A
Koda List is converted to a Python list and a Koda Dict/Entity/Object is
converted to a Python dict.

#### Reshape {#reshape}

**Reshaping** is an operation of changing the [shape](#shape) of a DataSlice
without changing items inside the DataSlice. For example, `[[1, 2, 3], [4, 5]]
-> [[1, 2], [3], [4], [5]]`. [Flattening](#flatten) is one possible reshaping.

#### Schema {#schema}

**Schema** is a Koda [DataItem](#dataitem) representing a type of DataSlice
items. There are different types of schemas: primitive schema (e.g. INT32),
Entity schema, List schema, Dict schema, OBJECT and ITEMID schema.

#### Serialization {#serialization}

**Serialization** is a process of converting a DataSlice or DataBag to bytes.
Serialized bytes can be stored on disks or transmitted over RPCs. They can be
**deserialized** later to get the exactly the same DataSlice or DataBag.

#### Serving {#serving}

**Serving** is a process of running the computation logic (e.g. defined as Koda
Expr or Functor) in production. The computation logic is normally defined in an
offline Colab environment and deployed to the online serving environment so that
there is no need to keep two versions of the same logic. Koda ensures the
fidelity of the outputs between offline and online environments given the same
inputs.

#### Shape {#shape}

The **shape** of a DataSlice represents all [dimensions](#dimension) in the
DataSlice. Dimensions (with plural), shape or partition tree are used
interchangeably.

#### Size {#size}

The **size** of a DataSlice is the number of items including
[missing](#sparsity) ones across all dimensions in the DataSlice.

#### Sparsity {#sparsity}

DataSlice can be **sparse**, which means some items in the DataSlice are
**missing**. Sparsity is supported natively.

#### Stub {#stub}

A **Object/Dict/Entity/List Stub** is an **empty** Entity/Object/Dict/List with
necessary metadata needed to describe its schema. To obtain the Stub, we can use
`ds.stub()`.

#### Subslicing {#sublicing}

**Subslicing** is an operation of getting part of items in a DataSlice as a new
DataSlice. It can be done using `ds.subslice(*slices)` or `ds.S[*slices]`. The
API is called "subslice" because it slices a DataSlice to create a
sub-DataSlice. Different from Python list, a DataSlice has multiple dimensions.
Therefore, we need to specify how to select each dimension (e.g. `ds.subslice(1,
2:4, 3)`).

Also see [List explosion](#explosion) and [Python-like Slicing](#py_slicing).

#### Tracing {#tracing}

**Tracing** is a process of transforming computation logic represented by a
Python function to a computation graph represented by a Koda
[Functor](#functor). Tracing provides a smooth transition from eager workflow
useful for interactive iteration to lazy workflow which can have better
performance and path to serving.

#### Update {#update}

**Updating** is a process of adding a DataBag as a fallback to another DataBag.
`db1 << db2` means updating `db1` with `db2` and values for the same attributes
in `db2` have higher priorities than `db1`.

#### Updated Bag {#updated_bag}

An **updated bag** is an empty DataBag updated by provided other DataBags.

Also see [Enriched Bag](#enriched_bag).

#### UUID {#uuid}

**UUIDs** are derived from the underlying data and types and are guaranteed to
be universally unique for the same data and types. It can be used as reference
across different DataBags or environments (e.g. different
processes/machines/binaries).

#### UU Schema {#uu_schema}

**Uu schemas** are Koda schemas whose ItemIds are [UUID](#uuid). Uu schemas can
be created directly (i.e. `kd.uu_schema()`) or as a by-product of uu
Entity/Object creation (i.e. `kd.uu()` or `kd.uuobj()`).

#### Vectorization {#vectorization}

**Vectorization** is a process of transforming a scalar operation acting on
**individual** data items to an operation where a single instruction operates on
multiple data items **together**. koda library leverages vectorization to
achieve better performance. Operations including both
[aggregational](#aggregation) and [pointwise](#pointwise) on DataSlices are
performed on all ItemIds or primitives together rather than one-by-one using
loops.

## Advanced

#### Adopt {#adopt}

**Adopting** is a process of consuming [triples](#triple) represented by a
DataSlice into a DataBag. The result is a new DataSlice with the same shape and
items but with the adopting DataBag. It is done using `new_ds = db.adopt(ds)`.

It is performed in four steps:

1.  Check if triples need to be adopted. If the DataSlice does not have a
    DataBag attached or the attached DataBag is the same as the adopting
    DataBag, no triples are adopted by the DataBag.
2.  Get a DataBag containing only triples to be adopted. Such DataBag is
    obtained by [extracting](#extract) triples represented by the DataSlice.
3.  Merge the DataBag containing triples to be adopted into the adopting DataBag
    **inplace**.
4.  Obtain a new DataSlice by attaching the adopting DataBag to the original
    DataSlice.

#### Append {#append}

**Appending** is an operation of adding an item to a [list](#list).

#### Attribute {#attribute}

**Attributes**, part of an Object.Attribute=>Value [triple](#triple), encode
semantic relationship between entities in a DataBag. Conceptually, triples in a
DataBag form a directed graph and attributes are the directed edges between
nodes in the graph.

#### Binding {#binding}

**Binding** is a process of creating a new Koda [Functor](#functor) by binding
parameters of another Koda Functor using `kdf.bind(kdf_fn, **inputs)`. It is
similar to Python `functools.partial()`. The bound values are used as defaults
only, meaning that one can still override them by passing a new value
explicitly.

#### Expr {#expr}

A Koda **Expr** is a tree-like structure representing computation logic. More
precisely, it is a **DAG** (Directed Acyclic Graph) consisting of
[input nodes](#input_node), [literal nodes](#literal_node), and
[operator nodes](#operator_node). Expr allows to define a computation once and
then apply the same computation on many different inputs.

#### Fallback DataBag {#fallback_db}

A **fallback** DataBag is an additional DataBag added to a DataSlice with one
DataBag. Conceptually, it can be thought as follow: when looking for a Value for
an Entity-Attribute pair, try to find it in the main DataBag first then fallback
to the fallback DataBag if not found. It can be done as
`ds.with_fallback_db(fallback_db)`.

#### Input Node {#input_node}

**Input nodes** are terminal nodes of Koda Exprs and represent data (e.g.
DataSlices) to be provided at Expr **evaluation** time. In Koda, inputs are
denoted as `I.input_name`.

#### Literal Node {#literal_node}

**Literal nodes** are terminal nodes of Koda Exprs and represent data (e.g.
DataSlices, primitives) provided at Expr **creation** time.

#### Merge {#merge}

**Merging DataBags** is a process of merging underlying triples of provided
DataBags including resolving conflicts based on configurations.

#### Mutability {#mutability}

DataBags can be **mutable** or **immutable**. Specifically, [Triples](#triple)
inside **immutable** DataBag cannot be added, modified or deleted. DataBag's
mutability cannot be changed directly after the DataBag is created. However, it
is possible to create a new copy with different mutability using
`db.fork(mutable=)`.

A DataSlice with a mutable DataBag is considered **mutable**. Similar to
DataBag, DataSlice's mutability cannot be changed directly. However, it is
possible to create a new DataSlice copy with the forked DataBag using
`ds.fork_bag(mutable=)`.

#### Operator Node {#operator_node}

Operator nodes are internal nodes of Koda Exprs and represent operations to be
performed on the values of sub-Exprs. For example, `I.a + I.b` returns an Expr
representing a `+` operation, with two input nodes as its children. Koda Expr
provides a comprehensive list of operators for basic operations under `kde`
module. As these operators are evaluated lazily, they are referred to as **lazy
operators**.

#### Reference {#reference}

A Entity/Object/List/Dict **reference** is a reference to the same
Entity/Object/List/Dict without a bag attached. It is created using `ds.ref()`.
It can be used to refer to the same Entity/Object/List/Dict in a different
DataBag without copying underlying triples.

#### Triples {#triple}

A **triple** is a set of three entities: an **object**, an **attribute** and a
**value** (a.k.a. OAV). The triple encodes the semantic information that the
*attribute* of the *object* is the *value*. It is worth mentioning that the
triple format is also known as subject, predicate, and object, which is more
commonly used. However, we prefer object/attribute/value naming in koda because
it fits Python terminologies and DataSlice syntax `object.attribute->value`
better. Objects and attributes in triples must be [ItemIds](#ItemId) while
values can be either ItemIds or [primitives](#primitive) (e.g. INT32, STRING,
BOOL).
