<!-- go/markdown-->

# Performance Debugging

This page provides tips for debugging Koda flows.

* TOC
{:toc}

## Untraced functionality

When having access to the source code in a Python environment, disabling tracing
is one of the simplest and most reliable way to allow flows to be debugged and
profiled. The standard tools available in Python can be used:

### `timeit`

`timeit` allows the CPU evaluation time to be measured. It automatically runs
the code several times to find the average, thus reducing variance. It is most
easily used from an interactive environment through:

```py
code_to_run()  # Warmup.
---
%%timeit

code_to_run()
```

A more rudimentary approach is to manually insert `time()` as such:

```py
t1 = time()
code_to_run()
t2 = time()
print(f'code_to_run took {t2 - t1} seconds to run')
```

### `cpu_profile`

`cpu_profile` can be used from an interactive environment to profile Python
code, and creates a graph of the call hierarchy and time spent in each part.
This tool does _not_ profile C++ code, and is therefore only suitable to get a
superficial picture of the Python code.

```py
code_to_run()  # Warmup.
---
%%cpu_profile

code_to_run()
```

### `pprof`

`pprof` can be used to produce a flame graph of both Python code and internal
C++ code. `pprof` works through sampling and accurately reflects the time spent
inside of each Python and C++ function. See the "pprof" section below for more
information.

## Traced functionality

*Before attempting to profile a traced Functor, consider disabling tracing and
working with the tools described above.*

For traced Functors, Python code is no longer evaluated which makes many of the
standard Python profiling tools unsuitable. Instead, tools such as `pprof` and
`xprof` should be used.

NOTE: It's crucial that the Functor or Expr that should be profiled is evaluated
once before the profiling begins to allow compilation the caches to be
populated.

### `pprof`

`pprof` can be used to profile Functor evaluation. `pprof` works through
sampling and accurately reflects the time spent inside of each C++ function.
Since tracing removes the original Python flow, some of the hierarchical
relationship between subfunctors is lost within the traceview.

From an interactive notebook environment, `pprof_profile` is most easily used
through:

```py
my_functor(x)  # Warmup.
---
%%pprof_profile --pyspy

# Outputs a link to a flamegraph.
my_functor(x)
```

The sampling rate of `pprof` is sometimes too low to properly capture an
accurate profile. A simple workaround is to repeatedly evaluate in a loop:

```py
my_functor(x)  # Warmup.
---
%%pprof_profile --pyspy

for _ in range(100):
  my_functor(x)
```
