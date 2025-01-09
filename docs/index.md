# Koda

[TOC]

## Koda

Koda is a **Python + C++ library** that provides a **data representation** and
**domain-specific language** (a.k.a DSL) for **vectorized** data transformations
and computations on the transformed data with **high performance**. The data can
be in the form of protos, tables, interconnected objects, graphs, tensors and
more.

Such transformations and computations can be designed in an **interactive** REPL
(e.g. Colab), as well compiled into **computation graphs** and **served** in
production (i.e. C++ environments).

### Koda Distinguishing Features

*   **Vectorization for complex data**: supports vectorized operations with C++
    grade performance not only for tables and arrays of primitives, but also for
    nested dicts, protos, structs, and graphs.
*   **Immutability**: enables modifications and keeping multiple, slightly
    varied versions of data without duplicating memory.
*   **Modular data**: data can be efficiently (usually for O(1)) split and
    joined, overlaid or enriched, and the data modules can be not just tables,
    columns and rows, but anything - from single attributes to proto updates to
    graph modifications.
*   **Computational graphs, lazy evaluation and serving support**: utilizes
    computational graphs that can be introspected and modified, and enable
    optimized data processing and serving.

### Why to Use Koda

*   **Interactivity in Colab**: transform interactively training data, design
    decision-making logic and evaluation flows (scoring, ranking, metrics etc.),
    work with models and more, where your data is tables, protos, structs,
    graphs etc.
*   **What-if experiments**: change input data, change evaluation
    logic, and get insights instantly.
*   **Zoom in on your data**: utilize **data views, updates, overlays,
    versions**, including when working with **large data sources**, and use only
    parts of the data needed at the moment.
*   **Performance**: computation is vectorized and performed in highly optimized
    C++.
*   **Evaluate in distributed environment or serve in production**: convert
    evaluation logic into **computational graphs**
    (ASTs) that can be introspected, optimized then evaluated in distributed
    environment or served in production.
