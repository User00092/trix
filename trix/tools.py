from __future__ import annotations

from typing import Any

from trix.models import Agent
from trix.policies import MAX_TREE_DEPTH


def function(
    name: str, description: str, properties: dict[str, Any], required: list[str]
) -> dict[str, Any]:
    return {
        "type": "function",
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        },
    }


def tools_for(agent: Agent) -> list[dict[str, Any]]:
    tools = [
        function("list_agents", "List agents visible in this Trix session.", {}, []),
        function(
            "get_agent",
            "Get an agent's current task, state, children, and reports.",
            {"agent_id": {"type": "string"}},
            ["agent_id"],
        ),
        function(
            "inspect_changes",
            "Read the current repository diff and changed-file summary without modifying it.",
            {},
            [],
        ),
    ]
    if agent.depth < MAX_TREE_DEPTH:
        tools.extend(
            [
                function(
                    "spawn_agent",
                    "Delegate one bounded execution task to a new direct child Codex agent.",
                    {
                        "name": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 80,
                            "description": (
                                "Human-friendly agent title for the UI, such as "
                                "'Repository Analyst'; do not use a slug or task identifier."
                            ),
                        },
                        "role": {"type": "string", "minLength": 1, "maxLength": 80},
                        "task": {"type": "string", "minLength": 1, "maxLength": 50000},
                    },
                    ["name", "role", "task"],
                ),
                function(
                    "send_instruction",
                    "Send corrective or follow-up direction to a direct child.",
                    {
                        "agent_id": {"type": "string"},
                        "message": {"type": "string", "minLength": 1, "maxLength": 50000},
                    },
                    ["agent_id", "message"],
                ),
                function(
                    "review_report",
                    "Accept or reject a direct child's report after checking its actual work.",
                    {
                        "report_id": {"type": "string"},
                        "accepted": {"type": "boolean"},
                        "feedback": {"type": "string", "maxLength": 20000},
                    },
                    ["report_id", "accepted"],
                ),
            ]
        )
    if agent.depth > 0:
        tools.append(
            function(
                "submit_report",
                "Submit an evidence-backed completion report to your direct parent.",
                {
                    "summary": {"type": "string", "minLength": 1},
                    "requirements_completed": {"type": "array", "items": {"type": "string"}},
                    "files_changed": {"type": "array", "items": {"type": "string"}},
                    "commands_run": {"type": "array", "items": {"type": "string"}},
                    "verification_results": {"type": "object"},
                    "known_issues": {"type": "array", "items": {"type": "string"}},
                    "risks": {"type": "array", "items": {"type": "string"}},
                    "recommended_parent_checks": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                ["summary"],
            )
        )
    else:
        tools.append(
            function(
                "complete_session",
                "Complete the session after all work is accepted and final checks are satisfied.",
                {
                    "summary": {"type": "string", "minLength": 1},
                    "verification": {"type": "array", "items": {"type": "string"}},
                },
                ["summary", "verification"],
            )
        )
    return [
        {
            "type": "namespace",
            "name": "trix",
            "description": "Delegate, observe, and verify work in the current Trix hierarchy.",
            "tools": tools,
        }
    ]
