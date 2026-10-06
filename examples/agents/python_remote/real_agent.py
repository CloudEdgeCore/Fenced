"""The v1.1 real reference agent: model + tools + memory via the runtime's
loopback MCP endpoint. Launch with the environment the adapter worker
provides:

    FENCED_MCP_URL=http://127.0.0.1:<mcp-port>     FENCED_MODEL_REF=fake/agent-model     python examples/agents/python_remote/real_agent.py --port 8089
"""

from __future__ import annotations

import argparse

from fenced_runtime import RealAgent, apply_legacy_compat, serve


def main() -> None:
    apply_legacy_compat()
    parser = argparse.ArgumentParser(description="Fenced v1.1 real reference agent")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8089)
    parser.add_argument("--model-ref", default="", help="model reference (default: FENCED_MODEL_REF)")
    parser.add_argument("--memory-namespace", default="", help="memory namespace (default: FENCED_MEMORY_NAMESPACE)")
    args = parser.parse_args()
    agent = RealAgent(model_ref=args.model_ref or None, memory_namespace=args.memory_namespace or None)
    server = serve(agent, "python-remote-real", args.host, args.port)
    print(f"http://{server.server_address[0]}:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
