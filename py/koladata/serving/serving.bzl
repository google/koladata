"""Build scripts for Koda serving."""

load(
    "@com_google_arolla//arolla/codegen:utils.bzl",
    "call_python_function",
    "render_jinja2_template",
)

def koladata_trace_py_fn(
        function,
        deps = []):
    """Constructs call_python_function spec for tracing a Python function as a Koda functor.

    Pass the result to koladata_serialized_functors or koladata_cc_embedded_slices BUILD rules.

    Args:
      function: fully qualified Python function name to trace.
      deps: Dependencies for the function, e.g. the python library defining it.
    """
    return call_python_function(
        "koladata.serving.serving_impl.trace_py_fn",
        args = [function],
        deps = deps + ["//py/koladata/serving:serving_impl"],
    )

def koladata_cc_embedded_slices(
        name,
        cc_function_name,
        slices,
        deps = [],
        tool_deps = [],
        testonly = False,
        **kwargs):
    """Generates a C++ library with embedded Koda slices (e.g. functors).

    Library contains function with name `{cc_function_name}` with two overloads:
      - without arguments: returns a map of named slices `std::string` -> `StatusOr<DataSlice>`.
      - with a string argument: returns `StatusOr<DataSlice>` for the slice with the given name.

    In addition there is a function `{cc_function_name}_{slice_name}` for each slice. It is
    equivalent to `{cc_function_name}("{slice_name}")`.

    Usage example:

      pytype_strict_library(
          name = "my_functors",
          srcs = ["my_functors.py"],
          deps = [...],
      )
      koladata_cc_embedded_slices(
          name = "cc_my_functors",
          testonly = True,
          cc_function_name = "my_namespace::MyFunctors",
          slices = {
              "my_functor": koladata_trace_py_fn("path.to.my_functors.my_functor"),
              "their_functor": koladata_trace_py_fn("path.to.my_functors.their_functor"),
          },
          tool_deps = [":my_functors"],
          deps = ["//py/koladata/serving:standard_deps"],
      )

    Args:
      name: Name of the rule, will be used for the generated C++ header (with *.h suffux).
      cc_function_name: Fully qualified name of the generated C++ getter function, for example
          "::my_namespace::GetMyFunctors".
      slices: A dict from the slice name to a call_python_function spec constructing it, see
          koladata_trace_py_fn.
      deps: Runtime dependencies needed to load the slices. For functors, consider using
          //py/koladata/serving:standard_deps.
      tool_deps: Build time dependencies, e.g. the python library defining the slices. Note that the
          additional dependencies are added based on the `deps` inside `slices` argument.
      testonly: Whether the build target is testonly.
      **kwargs: extra arguments passed directly to the final cc_library
    """
    tags = list(kwargs.pop("tags", []))

    namespaces = [x for x in cc_function_name.split("::")[:-1] if x]
    serialized_slices = call_python_function(
        "koladata.serving.serving_impl.serialize_slices",
        args = [slices],
        deps = tool_deps + ["//py/koladata/serving:serving_impl"],
    )

    context = dict(
        build_target = "//{}:{}".format(native.package_name(), name),
        namespaces = namespaces,
        function_name = cc_function_name.split("::")[-1],
        slice_names = slices.keys(),
        serialized_slices = serialized_slices,
    )
    render_jinja2_template(
        name = "_{}_genrule_cc".format(name),
        out = name + ".cc",
        template = "//py/koladata/serving:embedded_slices.cc.jinja2",
        context = context,
        testonly = testonly,
        tags = tags,
    )
    render_jinja2_template(
        name = "_{}_genrule_h".format(name),
        out = name + ".h",
        template = "//py/koladata/serving:embedded_slices.h.jinja2",
        context = context,
        testonly = testonly,
        tags = tags,
    )
    native.cc_library(
        name = name,
        testonly = testonly,
        srcs = [name + ".cc"],
        hdrs = [name + ".h"],
        deps = deps + [
            # TODO: b/409476740 - Should we depend on codecs here?
            "@com_google_absl//absl/base:no_destructor",
            "@com_google_absl//absl/container:flat_hash_map",
            "@com_google_absl//absl/status:statusor",
            "@com_google_absl//absl/strings",
            "//koladata/serving:slice_registry",
            "//py/koladata/serving:embedded_slices_internal",
            "@com_google_arolla//arolla/util",
            "//koladata:data_slice",
        ],
        tags = tags,
        alwayslink = 1,
        **kwargs
    )
