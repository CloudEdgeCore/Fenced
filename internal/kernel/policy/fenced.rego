# Agent OS v1alpha1 admission and tool policy (Rego v1).
#
# The kernel evaluates this module with default-deny semantics: `allow` is
# false unless a rule grants it, and `deny_reasons` explains every failed
# condition so decisions stay machine-readable without model inference.
#
# Input is constructed by trusted kernel code:
#   input.task.priority            int
#   input.tenant.max_priority      int
#   input.tenant.allowed_tools     [string]  tool names this tenant may call
#   input.tenant.approval_required_risk string  risk level requiring approval
#   input.tenant.allowed_peers     [string]  AgentVersion refs this tenant may message
#   input.peer.to                  string  receiving AgentVersion reference
#   input.tool.name                string
#   input.tool.version             string
#   input.tool.action              string
#   input.tool.resource            string
#   input.tool.risk                string  side-effect risk of the descriptor
package fenced.policy

import rego.v1

default allow := false

# An admitted task must respect the tenant's maximum priority.
allow if {
	input.task.priority <= input.tenant.max_priority
}

deny_reasons contains "TASK_PRIORITY_EXCEEDS_TENANT_MAX" if {
	input.task.priority > input.tenant.max_priority
}

# Tool decisions: a tool is callable only when the tenant's policy data names
# it; high-risk tools additionally require human approval. Missing or empty
# tenant data denies by default. The nested rules form the virtual document
# data.fenced.policy.tool = {allow, deny_reasons, requires_approval}.
tool_allowed_by_name if {
	input.tenant.allowed_tools[_] == input.tool.name
}

tool.allow if tool_allowed_by_name

tool.deny_reasons contains "TOOL_NOT_ALLOWED" if {
	not tool_allowed_by_name
}

tool.requires_approval contains input.tool.name if {
	tool_allowed_by_name
	input.tool.risk == input.tenant.approval_required_risk
}

# Model decisions: a model is callable only when the tenant's policy data
# names it. The nested rules form data.fenced.policy.model =
# {allow, deny_reasons}.
model_allowed_by_name if {
	input.tenant.allowed_models[_] == input.model.name
}

model.allow if model_allowed_by_name

model.deny_reasons contains "MODEL_NOT_ALLOWED" if {
	not model_allowed_by_name
}

# Peer decisions: an IPC send is allowed only when the tenant's policy data
# names the receiving AgentVersion reference. Matching is exact, the same
# contract the tool and model allowlists use; per-version wildcards are the
# AgentVersion capability grants' job, one layer down. Missing or empty tenant
# data denies by default. The nested rules form
# data.fenced.policy.peer = {allow, deny_reasons}.
peer_allowed_by_name if {
	input.tenant.allowed_peers[_] == input.peer.to
}

peer.allow if peer_allowed_by_name

peer.deny_reasons contains "PEER_NOT_ALLOWED" if {
	not peer_allowed_by_name
}
