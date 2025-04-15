<!-- go/markdown -->

# Koda Persistent Storage

This document discusses serialization guarantees and recommendations for
persistent storage of Koda types. Please see the
[Koda Fundamentals](fundamentals.md#serialization) guide for examples using
`kd.dumps`, which supports serialization of DataSlices and DataBags.

* TOC
{:toc}

## Data storage

### Guarantees

We aim to keep serialization of DataBags and DataSlices that do *not* contain
Functors or Expressions stable for at least *one year*. Changes to the core
library are normally handled through an internal versioning system which is
fully automated through `kd.dumps` and `kd.loads`.

However, in exceptional cases, there may be backwards-incompatible changes to
the core library that cannot be addressed through versioning.

### Recommendation

For persistent storage of Koda data (i.e. excluding Functors and Expressions),
we recommend using `kd.dumps`. This allows for the same code to be used for both
long-term and short-term cases without further thought, using `kd.loads` will
load the same format as was stored without further work, and changes to the core
library that requires changes to the serialization library will be automatically
handled for you.

We recommend periodically validating that the data can be loaded, e.g. through a
test or other automated tooling, to detect potential issues sooner rather than
later.

## Functor and Expr storage

### Guarantees

Functors and Expressions have *no* guarantee for persistent storage through
`kd.dumps`. The core library may be updated in a backwards-incompatible way
without warning.

### Recommendation

For persistent storage of Functors and Expressions, we recommend checking in
Python code together with a test. This will allow the issue to be detected and
fixed while permitting developers to change the core Koda library.

NOTE: For short term usages, please use `kd.dumps` and `kd.loads` for all types
of data, including Functors. For example, defining a flow in Colab and sending
it to external workers through `kd.dumps` is a valid and recommended usage.
