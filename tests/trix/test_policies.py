from __future__ import annotations

import pytest

from trix.models import Agent, AgentStatus
from trix.policies import DelegationPolicy, PolicyViolation


def agent(depth: int = 0, parent_id: str | None = None) -> Agent:
    return Agent(
        session_id="session",
        parent_id=parent_id,
        depth=depth,
        name="Agent",
        role="Worker",
        task="Work",
    )


def test_leaf_cannot_delegate() -> None:
    leaf = agent(depth=2)
    with pytest.raises(PolicyViolation, match="cannot delegate"):
        DelegationPolicy().validate_spawn(leaf, [leaf])


def test_only_two_active_direct_children() -> None:
    parent = agent()
    children = [agent(depth=1, parent_id=parent.id), agent(depth=1, parent_id=parent.id)]
    with pytest.raises(PolicyViolation, match="maximum"):
        DelegationPolicy().validate_spawn(parent, [parent, *children])


def test_completed_child_releases_slot() -> None:
    parent = agent()
    children = [agent(depth=1, parent_id=parent.id), agent(depth=1, parent_id=parent.id)]
    children[0].status = AgentStatus.COMPLETED
    DelegationPolicy().validate_spawn(parent, [parent, *children])


def test_global_limit_is_separate() -> None:
    parent = agent()
    others = [agent(depth=1, parent_id=f"other-{number}") for number in range(5)]
    with pytest.raises(PolicyViolation, match="Global"):
        DelegationPolicy().validate_spawn(parent, [parent, *others])
