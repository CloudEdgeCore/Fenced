import io
import unittest

from fenced_runtime import compat


class AliasLegacyEnvTests(unittest.TestCase):
    def test_copies_legacy_variables_to_current_names(self):
        env = {"AGENTOS_TOKEN": "secret", "AGENTOS_MCP_URL": "http://mcp"}
        aliased = compat.alias_legacy_env(env)
        self.assertEqual(aliased, ["FENCED_MCP_URL", "FENCED_TOKEN"])
        self.assertEqual(env["FENCED_TOKEN"], "secret")
        self.assertEqual(env["FENCED_MCP_URL"], "http://mcp")

    def test_current_name_wins_over_legacy(self):
        env = {"AGENTOS_TOKEN": "old", "FENCED_TOKEN": "new"}
        aliased = compat.alias_legacy_env(env)
        self.assertEqual(aliased, [])
        self.assertEqual(env["FENCED_TOKEN"], "new")

    def test_explicit_empty_current_value_wins(self):
        env = {"AGENTOS_TOKEN": "old", "FENCED_TOKEN": ""}
        aliased = compat.alias_legacy_env(env)
        self.assertEqual(aliased, [])
        self.assertEqual(env["FENCED_TOKEN"], "")

    def test_ignores_unrelated_variables(self):
        env = {"PATH": "/usr/bin", "AGENTOSITY": "nope"}
        self.assertEqual(compat.alias_legacy_env(env), [])
        self.assertNotIn("FENCEDITY", env)

    def test_is_idempotent(self):
        env = {"AGENTOS_TOKEN": "secret"}
        self.assertEqual(compat.alias_legacy_env(env), ["FENCED_TOKEN"])
        self.assertEqual(compat.alias_legacy_env(env), [])
        self.assertEqual(env["FENCED_TOKEN"], "secret")

    def test_mutates_process_environment_by_default(self):
        import os

        os.environ["AGENTOS_COMPAT_PROBE"] = "probe"
        self.addCleanup(os.environ.pop, "AGENTOS_COMPAT_PROBE", None)
        self.addCleanup(os.environ.pop, "FENCED_COMPAT_PROBE", None)
        os.environ.pop("FENCED_COMPAT_PROBE", None)

        compat.alias_legacy_env()
        self.assertEqual(os.environ["FENCED_COMPAT_PROBE"], "probe")


class WarnLegacyEnvTests(unittest.TestCase):
    def test_writes_one_notice_when_aliased(self):
        stream = io.StringIO()
        compat.warn_legacy_env(stream, ["FENCED_TOKEN"])
        message = stream.getvalue()
        self.assertIn("AGENTOS_*", message)
        self.assertIn("FENCED_*", message)
        self.assertIn("FENCED_TOKEN", message)

    def test_is_silent_when_nothing_aliased(self):
        stream = io.StringIO()
        compat.warn_legacy_env(stream, [])
        self.assertEqual(stream.getvalue(), "")

    def test_is_nil_safe(self):
        compat.warn_legacy_env(None, ["FENCED_TOKEN"])


class ApplyLegacyCompatTests(unittest.TestCase):
    def test_aliases_and_warns(self):
        import os

        os.environ["AGENTOS_COMPAT_APPLY"] = "applied"
        self.addCleanup(os.environ.pop, "AGENTOS_COMPAT_APPLY", None)
        self.addCleanup(os.environ.pop, "FENCED_COMPAT_APPLY", None)
        os.environ.pop("FENCED_COMPAT_APPLY", None)

        stream = io.StringIO()
        aliased = compat.apply_legacy_compat(stream)
        self.assertEqual(aliased, ["FENCED_COMPAT_APPLY"])
        self.assertEqual(os.environ["FENCED_COMPAT_APPLY"], "applied")
        self.assertIn("FENCED_COMPAT_APPLY", stream.getvalue())


class NormalizeProtocolTests(unittest.TestCase):
    def test_maps_legacy_prefix(self):
        cases = {
            "agentos.runtime.interface/v1": "fenced.runtime.interface/v1",
            "agentos.dev/v1alpha1": "fenced.dev/v1alpha1",
            "agentos.oci/v1": "fenced.oci/v1",
        }
        for legacy, current in cases.items():
            with self.subTest(legacy=legacy):
                self.assertEqual(compat.normalize_protocol(legacy), current)

    def test_leaves_canonical_and_empty_values_unchanged(self):
        for value in ("fenced.dev/v1", "", "not-a-protocol", "x.agentosx/y"):
            with self.subTest(value=value):
                self.assertEqual(compat.normalize_protocol(value), value)


class NormalizeHeaderTests(unittest.TestCase):
    def test_maps_legacy_header_case_insensitively(self):
        self.assertEqual(
            compat.normalize_header("X-Agentos-Execution"),
            "X-Fenced-Execution",
        )
        self.assertEqual(
            compat.normalize_header("x-agentos-execution"),
            "X-Fenced-Execution",
        )

    def test_leaves_other_headers_unchanged(self):
        self.assertEqual(compat.normalize_header("X-Fenced-Execution"), "X-Fenced-Execution")
        self.assertEqual(compat.normalize_header("Content-Type"), "Content-Type")


if __name__ == "__main__":
    unittest.main()
