# Koda

[TOC]

## Koda

Koda is a **Python + C++ library** that provides a **data representation** and
**domain-specific language** (a.k.a DSL) for **orchestrating** data
transformations and computations on the transformed data, which involve
structural changes to the data. The data can be in the form of protos, tables,
interconnected objects, graphs, tensors and more. Structural changes include
constructing new objects, protos, tables, graphs, tensors as an output or during
intermediate steps. Such transformations and computations can be designed in a
REPL (e.g. Colab), as well compiled and served in C++.

In particular, we will develop a family of data structures (**DataBag**,
**DataSlice**) that represent and manipulate pieces of data, sets of objects,
their attributes and their relations, as well as a **DSL** to work with them.

Everything will be designed **ML-first**, meaning tight integration with modern
ML libraries and taking advantage of modern ML models and model architectures
(e.g. embeddings to encode / retrieve objects and relations; transformer-based
models to process data; impute values and relations; generate sequences and
objects and more).

### Why do we need such a library?

Most of the transformations and computations we target here are written in C++
now, both in data preparation and serving code. However, C++ is not a good
“glue” language, and is better suited for a rather static logic that does not
need to be experimented with or frequently iterated on.

When using C++ as a glue in the modeling space (e.g. scorers, parsers, planners
etc.), we could observe many hacks that would “patch” models and training data
preparation. Additionally, important logic is often hidden, making it harder to
inspect, debug, change, optimize and learn. Such projects also often end up
dealing with tech debt (hard to make improvements) and rigidity (inability to
dramatically change logic after committing to a certain design).
