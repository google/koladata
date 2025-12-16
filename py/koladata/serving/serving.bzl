"""Build scripts for Koda serving."""

load(
    "@com_google_arolla//arolla/codegen:utils.bzl",
    "call_python_function",
    "python_function_call_genrule",
    "render_jinja2_template",
    "write_file_function",
)
load("@rules_cc//cc:cc_library.bzl", "cc_library")

# TODO: Make disabling type checking optional.
def koladata_trace_py_fn(
        function,
        deps = [],
        experimental_deterministic_mode = False):
    """Constructs call_python_function spec for tracing a Python function as a Koda functor.

    Pass the result to koladata_serialized_functors or koladata_cc_embedded_slices BUILD rules.
    Disables type checking decorators on the function.

    Args:
      function: fully qualified Python function name to trace.
      deps: Dependencies for the function, e.g. the python library defining it.
      experimental_deterministic_mode: (Best-effort) try to freeze the generated functor to make the
          build deterministic.
          The logic is experimental and currently does not support functors with non-scalar
          literals, non-uu schema literals, dict literals with non-primitive keys, etc.
    """
    return call_python_function(
        "koladata.serving.serving_impl.trace_py_fn",
        args = [function, experimental_deterministic_mode],
        deps = deps + ["//py/koladata/serving:serving_impl"],
    )

def koladata_parallel_transform(fn_spec):
    """Constructs call_python_function spec for applying kd.parallel.transform to the given functor.

    Args:
      fn_spec: call_python_function spec for the functor to transform (see koladata_trace_py_fn).
    """
    return call_python_function(
        "koladata.serving.serving_impl.parallel_transform",
        args = [fn_spec],
        deps = ["//py/koladata/serving:serving_impl"],
    )

def koladata_export_dataslice(
        qualname,
        deps = []):
    """Constructs call_python_function spec for exporting a Koda DataSlice.

    Pass the result to koladata_cc_embedded_slices BUILD rules.

    Args:
      qualname: fully qualified Python object name.
      deps: Dependencies for the Python object, e.g. the python library defining it.
  """
    return call_python_function(
        "koladata.serving.serving_impl._import_object",
        args = [qualname],
        deps = deps + ["//py/koladata/serving:serving_impl"],
    )

def koladata_serialized_slices(
        name,
        slices,
        tool_deps = [],
        testonly = False,
        **kwargs):
    """Generates serialized *.kd files with Koda slices (e.g. functors).

    Usage example:

      pytype_strict_library(
          name = "my_functors",
          srcs = ["my_functors.py"],
          deps = [...],
      )
      koladata_serialized_slices(
          name = "my_functors",
          slices = {
              "format_prompt": koladata_trace_py_fn("path.to.my_functors.format_prompt"),
              "call_model": koladata_trace_py_fn("path.to.my_functors.call_model"),
          },
          tool_deps = [":my_functors"],
      )

      The rule generates my_functors_format_prompt.kd and my_functors_call_model.kd files.

      In C++ code one can deserialize the slices as follows:
      ```
      #include "py/koladata/serving/serialized_slices.h"
      ...
      ASSIGN_OR_RETURN(koladata::DataSlice slice,
                       koladata::serving::ParseSerializedSlice(data));
      ```

    Args:
      name: Name of the rule, will be used for the generated C++ header (with *.h suffux).
      slices: A dict from the slice name to a call_python_function spec constructing it, see
          koladata_trace_py_fn.
      tool_deps: Build time dependencies, e.g. the python library defining the slices. Note that the
          additional dependencies are added based on the `deps` inside `slices` argument.
      testonly: Whether the build target is testonly.
      **kwargs: Extra arguments passed directly to the final genrule.
    """
    filename_to_slice = {"{}_{}.kd".format(name, k): s for k, s in slices.items()}

    python_function_call_genrule(
        name = name,
        function = call_python_function(
            "koladata.serving.serving_impl.serialize_slices_into",
            args = [{"$(execpath {})".format(f): s for f, s in filename_to_slice.items()}],
            deps = list(tool_deps) + [
                "//py/koladata/serving:serving_impl",
            ],
        ),
        outs = filename_to_slice.keys(),
        testonly = testonly,
        **kwargs
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
      **kwargs: Extra arguments passed directly to the final cc_library.
    """
    tags = list(kwargs.pop("tags", []))

    namespaces = [x for x in cc_function_name.split("::")[:-1] if x]
    serialized_slices = call_python_function(
        "koladata.serving.serving_impl.serialize_slices",
        args = [slices],
        deps = list(tool_deps) + [
            "//py/koladata/serving:serving_impl",
        ],
    )

    context = dict(
        build_target = "//{}:{}".format(native.package_name(), name),
        namespaces = namespaces,
        function_name = cc_function_name.split("::")[-1],
        slice_names = slices.keys(),
        serialized_slices = serialized_slices,
        dynamic_reload = False,
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
    cc_library(
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
            "//py/koladata/serving:serialized_slices",
            "@com_google_arolla//arolla/util",
            "//koladata:data_slice",
        ],
        tags = tags,
        alwayslink = 1,
        **kwargs
    )
