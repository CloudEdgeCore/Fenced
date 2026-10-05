import threading
import unittest
from unittest import mock

from fenced_runtime import realagent


class _FakeMCP:
    def __init__(self, _url, execution_id):
        self.execution_id = execution_id

    def initialize(self):
        return None


class _SnapshotAgent(realagent.RealAgent):
    def __init__(self):
        super().__init__(mcp_url="http://mcp.invalid", model_ref="fake/model")
        self.second_turn_started = threading.Event()
        self.release_second_turn = threading.Event()
        self.turn = 0

    def _discover_tools(self, _mcp):
        return ["weather.lookup"]

    def _recall(self, _mcp, _goal):
        return []

    def _invoke_model(self, _mcp, _messages, _tool_names):
        self.turn += 1
        if self.turn == 1:
            return {
                "content": "",
                "toolCalls": [{"id": "call-1", "name": "weather.lookup", "arguments": {"city": "paris"}}],
                "usage": {},
            }
        self.second_turn_started.set()
        assert self.release_second_turn.wait(2)
        return {"content": "sunny", "toolCalls": [], "usage": {}}

    def _call_tool(self, _mcp, _call):
        return {"temperature": 21}

    def _remember(self, _mcp, _execution_id, _goal, _final):
        return "memory-1"


class RealAgentCheckpointTests(unittest.TestCase):
    def test_checkpoint_records_confirmed_tool_effect_before_next_model_turn(self):
        with mock.patch.object(realagent, "MCPClient", _FakeMCP):
            agent = _SnapshotAgent()
            output = {}
            failure = []

            def execute():
                try:
                    output.update(agent.run(
                        {"executionId": "attempt-1", "goal": "weather in paris"},
                        lambda _kind, _payload: None,
                        threading.Event(),
                    ))
                except Exception as error:  # pragma: no cover - surfaced below
                    failure.append(error)

            thread = threading.Thread(target=execute)
            thread.start()
            self.assertTrue(agent.second_turn_started.wait(2))

            checkpoint = agent.checkpoint("attempt-1")
            messages = checkpoint["state"]["messages"]
            self.assertTrue(any(message["role"] == "tool" for message in messages))

            agent.release_second_turn.set()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(failure, [])
            self.assertEqual(output["answer"], "sunny")

    def test_terminal_snapshot_survives_until_final_checkpoint(self):
        with mock.patch.object(realagent, "MCPClient", _FakeMCP):
            agent = _SnapshotAgent()
            agent.release_second_turn.set()

            output = agent.run(
                {"executionId": "attempt-final", "goal": "weather in paris"},
                lambda _kind, _payload: None,
                threading.Event(),
            )
            self.assertEqual(output["answer"], "sunny")

            final_checkpoint = agent.checkpoint("attempt-final")
            self.assertEqual(final_checkpoint["state"]["final"], "sunny")
            self.assertTrue(any(
                message["role"] == "tool"
                for message in final_checkpoint["state"]["messages"]
            ))

            # Delivering the terminal snapshot also releases its live state.
            self.assertEqual(agent.checkpoint("attempt-final")["state"], {})


if __name__ == "__main__":
    unittest.main()
