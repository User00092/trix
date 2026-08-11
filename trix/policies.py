from __future__ import annotations

from dataclasses import dataclass

from trix.models import ACTIVE_AGENT_STATUSES, Agent

MAX_TREE_DEPTH = 2
MAX_ACTIVE_CHILDREN_PER_AGENT = 2
MAX_GLOBAL_ACTIVE_AGENTS = 6


class PolicyViolation(ValueError):
    """Raised when a requested orchestration transition is not allowed."""


@dataclass(frozen=True)
class DelegationPolicy:
    max_tree_depth: int = MAX_TREE_DEPTH
    max_active_children: int = MAX_ACTIVE_CHILDREN_PER_AGENT
    max_global_active_agents: int = MAX_GLOBAL_ACTIVE_AGENTS

    def validate_spawn(self, parent: Agent, agents: list[Agent]) -> None:
        if parent.depth >= self.max_tree_depth:
            raise PolicyViolation(f"Agents at depth {parent.depth} cannot delegate")
        active = [
            item
            for item in agents
            if item.parent_id == parent.id and item.status in ACTIVE_AGENT_STATUSES
        ]
        if len(active) >= self.max_active_children:
            raise PolicyViolation("Parent already has the maximum number of active children")
        globally_active = [item for item in agents if item.status in ACTIVE_AGENT_STATUSES]
        if len(globally_active) >= self.max_global_active_agents:
            raise PolicyViolation("Global active-agent limit reached")
