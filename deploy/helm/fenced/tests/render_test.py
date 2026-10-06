"""Render-only checks. Requires Helm and PyYAML; never contacts a cluster."""

import copy
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parents[1]
# These names identify hypothetical existing resources, not usable credentials.
PRODUCTION = {
    "database": {"existingSecret": "test-db"},
    "security": {
        "oidc": {"issuer": "https://identity.example.test", "clientID": "test-control"},
        "tls": {"existingSecret": "test-tls"},
        "audit": {"existingSecret": "test-audit"},
        "packageTrust": {"existingSecret": "test-trust"},
    },
    "embedding": {"endpoint": "https://embedding.example.test/embed"},
}


class RenderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.helm = shutil.which(os.environ.get("HELM", "helm"))
        if not cls.helm:
            raise RuntimeError("Helm is required; put it on PATH or set HELM to its path")

    def invoke(self, values=None, release="alpha", lint=False):
        with tempfile.TemporaryDirectory() as directory:
            command = [self.helm, "lint", str(CHART), "--strict"] if lint else [
                self.helm, "template", release, str(CHART), "--namespace", "test"
            ]
            if values is not None:
                values_path = Path(directory) / "values.yaml"
                values_path.write_text(yaml.safe_dump(values), encoding="utf-8")
                command += ["-f", str(values_path)]
            return subprocess.run(command, capture_output=True, text=True, check=False)

    def render(self, values=None, release="alpha"):
        result = self.invoke(PRODUCTION if values is None else values, release)
        self.assertEqual(result.returncode, 0, result.stderr)
        documents = list(yaml.safe_load_all(result.stdout))
        return {document["kind"]: document for document in documents if document}

    def test_unconfigured_and_incomplete_production_values_fail(self):
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("security.oidc.issuer is required", result.stderr)
        fields = [
            ("database", "existingSecret"),
            ("security", "oidc", "issuer"),
            ("security", "oidc", "clientID"),
            ("security", "tls", "existingSecret"),
            ("security", "audit", "existingSecret"),
            ("security", "packageTrust", "existingSecret"),
            ("embedding", "endpoint"),
        ]
        for field in fields:
            with self.subTest(field=field):
                values = copy.deepcopy(PRODUCTION)
                target = values
                for name in field[:-1]:
                    target = target[name]
                target[field[-1]] = "   "
                result = self.invoke(values)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(".".join(field) + " is required", result.stderr)

    def test_public_identity_and_embedding_require_https(self):
        for section in ("oidc", "embedding"):
            with self.subTest(section=section):
                values = copy.deepcopy(PRODUCTION)
                if section == "oidc":
                    values["security"]["oidc"]["issuer"] = "http://identity.example.test"
                else:
                    values["embedding"]["endpoint"] = "http://embedding.example.test"
                result = self.invoke(values)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must use HTTPS", result.stderr)

    def test_production_startup_and_https_probes(self):
        documents = self.render()
        pod = documents["Deployment"]["spec"]["template"]["spec"]
        container = pod["containers"][0]
        args = dict(argument[1:].split("=", 1) for argument in container["args"])
        self.assertNotIn("dev-tenant", args)
        self.assertEqual(args["oidc-issuer"], PRODUCTION["security"]["oidc"]["issuer"])
        self.assertEqual(args["oidc-client-id"], "test-control")
        self.assertEqual(args["oidc-tenant-claim"], "tenant")
        self.assertEqual(args["embedding-endpoint"], PRODUCTION["embedding"]["endpoint"])
        for probe in ("livenessProbe", "readinessProbe"):
            self.assertEqual(container[probe]["httpGet"]["scheme"], "HTTPS")
            self.assertEqual(container[probe]["httpGet"]["port"], "https")
        self.assertFalse(pod["automountServiceAccountToken"])
        self.assertEqual(documents["Service"]["spec"]["ports"][0]["targetPort"], "https")
        self.assertNotIn("Secret", documents)

    def test_external_secrets_and_tls_mounts_match_startup(self):
        documents = self.render()
        pod = documents["Deployment"]["spec"]["template"]["spec"]
        container = pod["containers"][0]
        env = {item["name"]: item["valueFrom"]["secretKeyRef"] for item in container["env"]}
        self.assertEqual(env["DATABASE_URL"], {"name": "test-db", "key": "database-url"})
        self.assertEqual(env["FENCED_AUDIT_SIGNING_KEY"], {"name": "test-audit", "key": "audit-signing-key"})
        self.assertEqual(env["FENCED_PACKAGE_TRUST_KEYS"], {"name": "test-trust", "key": "package-trust-keys"})
        secret = pod["volumes"][0]["secret"]
        self.assertEqual(secret["secretName"], "test-tls")
        self.assertEqual(secret["items"], [{"key": "tls.crt", "path": "tls.crt"}, {"key": "tls.key", "path": "tls.key"}])
        mount = container["volumeMounts"][0]
        self.assertTrue(mount["readOnly"])
        self.assertIn("-tls-cert=" + mount["mountPath"] + "/tls.crt", container["args"])
        self.assertIn("-tls-key=" + mount["mountPath"] + "/tls.key", container["args"])

    def test_release_selectors_are_disjoint_and_match_services(self):
        selectors = []
        for release in ("alpha", "beta"):
            documents = self.render(release=release)
            deployment = documents["Deployment"]["spec"]
            selector = deployment["selector"]["matchLabels"]
            self.assertEqual(selector, deployment["template"]["metadata"]["labels"])
            self.assertEqual(selector, documents["Service"]["spec"]["selector"])
            selectors.append(selector)
        self.assertNotEqual(selectors[0], selectors[1])

    def test_service_account_can_be_created_or_reused(self):
        documents = self.render()
        pod = documents["Deployment"]["spec"]["template"]["spec"]
        self.assertEqual(pod["serviceAccountName"], documents["ServiceAccount"]["metadata"]["name"])
        values = copy.deepcopy(PRODUCTION)
        values["serviceAccount"] = {"create": False, "name": "existing-control"}
        documents = self.render(values)
        self.assertNotIn("ServiceAccount", documents)
        self.assertEqual(documents["Deployment"]["spec"]["template"]["spec"]["serviceAccountName"], "existing-control")

    def test_embedding_token_is_optional_and_custom_secret_keys_work(self):
        pod = self.render()["Deployment"]["spec"]["template"]["spec"]
        self.assertNotIn("FENCED_EMBEDDING_TOKEN", [item["name"] for item in pod["containers"][0]["env"]])
        values = copy.deepcopy(PRODUCTION)
        values["embedding"].update(tokenSecret="embedding-auth", tokenSecretKey="bearer")
        values["security"]["tls"].update(certificateKey="cert.pem", privateKeyKey="key.pem")
        pod = self.render(values)["Deployment"]["spec"]["template"]["spec"]
        env = {item["name"]: item["valueFrom"]["secretKeyRef"] for item in pod["containers"][0]["env"]}
        self.assertEqual(env["FENCED_EMBEDDING_TOKEN"], {"name": "embedding-auth", "key": "bearer"})
        self.assertEqual(pod["volumes"][0]["secret"]["items"], [{"key": "cert.pem", "path": "tls.crt"}, {"key": "key.pem", "path": "tls.key"}])

    def test_removed_development_and_bundled_database_options_are_rejected(self):
        for legacy in ("devTenant", "postgresql"):
            values = copy.deepcopy(PRODUCTION)
            if legacy == "devTenant":
                values["security"]["devTenant"] = "prod"
            else:
                values["postgresql"] = {"enabled": True}
            result = self.invoke(values)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(legacy, result.stderr)

    def test_configured_chart_lints(self):
        result = self.invoke(PRODUCTION, lint=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
