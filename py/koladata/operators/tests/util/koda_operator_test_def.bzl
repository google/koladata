"""Build macro for generator arolla operator tests with a Koda evaluation backend."""

load(
    "@com_google_arolla//py/arolla/operator_tests:arolla_operator_test_def.bzl",
    "arolla_operator_test_with_specified_eval_fn",
)

def koda_operator_test(
        name,
        test_libraries,
        kd_op_mapping,
        args_for_reshape,
        skip_test_patterns = None,
        **test_flags):
    r"""Creates a test for `test_libraries` using koda_test_eval.eager_eval as backend.

    This macro is a convenience macro for:

        arolla_operator_test_with_specified_eval_fn(
            name = name,
            test_libraries = test_libraries,
            eval_fn_deps = [
                "//py/koladata/operators/tests/util:koda_test_eval"
            ],
            eval_fn_path = "koladata.operators.tests.util.koda_test_eval.eager_eval",
            **test_flags,
        )

    Args:
        name: The target name.
        test_libraries: arolla operator test bases to be run.
        kd_op_mapping: List of `<arolla_op>:<kd_op>` mappings. During expr eval, each `arolla_op`
            will be evaluated using the corresponding `kd_op`. If an arolla_op is encountered that
            is not listed, an exception will be raised. Example: ["strings.join:add","math.add:add"]
            indicates that `M.strings.join` and `M.math.add` should be evaluated using `kd.math.add`.
        args_for_reshape: List of argument names in the Arolla operator to reshape based on the
            shape derived from `over` or `into` . If None, the first argument will be reshaped.
        skip_test_patterns: List of test name patterns to skip. The patterns are matched with the
            beginning of the test ID in the form `<test_class>.<test_method>`. E.g.:
                skip_test_patterns = ['.*my_test_method', r'^TestFoo\.test_bar$']
            will:
            1. Skip all test methods of all test classes where the method name contains the
                substring 'my_test_method'.
            2. Skip the specific test method 'TestFoo.test_bar'.
        **test_flags: Additional test flags.
    """

    shard_count = test_flags.pop("shard_count", 4)
    kd_op_mapping_args = ["--extra_flags=kd_op_mapping:" + m for m in kd_op_mapping]
    args_for_reshape_args = ["--extra_flags=args_for_reshape:" + a for a in args_for_reshape]
    args = test_flags.pop("args", []) + kd_op_mapping_args + args_for_reshape_args
    arolla_operator_test_with_specified_eval_fn(
        name = name,
        test_libraries = test_libraries,
        eval_fn_deps = [
            "//py/koladata/operators/tests/util:koda_test_eval",
        ],
        eval_fn_path = "koladata.operators.tests.util.koda_test_eval.eager_eval",
        shard_count = shard_count,
        skip_test_patterns = skip_test_patterns,
        args = args,
        **test_flags
    )

def koda_operator_coverage_test(
        name,
        arolla_op,
        koda_op,
        kd_op_mapping = None,
        args_for_reshape = [],
        main = None,
        test_libraries = None,
        skip_test_patterns = None,
        **test_flags):
    """Creates a test that checks that `koda_op` covers the provided `arolla_op`.

    Example:
        # Runs all `M.math.add` tests for `kd.math.add`.
        koda_operator_coverage_test(
            name = "kd_add_covers_math_add_test",
            arolla_op = "math.add",
            koda_op = "math.add",
        )

    Args:
        name: Test name.
        arolla_op: Fully qualified arolla operator. Ex: "math.add".
        koda_op: Kola operator to test. Ex: "math.add".
        kd_op_mapping: List of `<arolla_op>:<kd_op>` mappings. During expr eval, each `arolla_op` will
            be evaluated using the corresponding `kd_op`. If an arolla_op is encountered that is not
            listed, an exception will be raised. Example: ["strings.join:add", "math.add:add"]
            indicates that `M.strings.join` and `M.math.add` should be evaluated using `kd.math.add`.
            If None, [f"{arolla_op}:{koda_op_name}"] will be used instead (eventually).
        args_for_reshape: List of argument names in the Arolla operator to reshape based on the
            shape derived from `over` or `into` . If None, the first argument will be reshaped.
        main: Python test file to be tested. If None, "{arolla_op_snake}_test.py" will be used.
        test_libraries: Libraries to test. If None, ["@com_google_arolla//py/arolla/operator_tests:" +
            arolla_op_snake + "_test_base"] will be used.
        skip_test_patterns: List of test name patterns to skip. The patterns are matched with the
            beginning of the test ID in the form `<test_class>.<test_method>`. E.g.:
                skip_test_patterns = ['.*my_test_method', r'^TestFoo\\.test_bar$']
            will:
            1. Skip all test methods of all test classes where the method name contains the
                substring 'my_test_method'.
            2. Skip the specific test method 'TestFoo.test_bar'.
        **test_flags: Additional test flags.
    """
    arolla_op_snake = arolla_op.replace(".", "_")
    if not main:
        main = arolla_op_snake + "_test.py"
    if not test_libraries:
        test_libraries = ["@com_google_arolla//py/arolla/operator_tests:" + arolla_op_snake + "_test_base"]
    if not kd_op_mapping:
        kd_op_mapping = [arolla_op + ":" + koda_op]
    koda_operator_test(
        name,
        test_libraries = test_libraries,
        kd_op_mapping = kd_op_mapping,
        args_for_reshape = args_for_reshape,
        skip_test_patterns = skip_test_patterns,
        main = main,
        **test_flags
    )
