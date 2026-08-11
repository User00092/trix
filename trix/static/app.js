const $ = (selector) => document.querySelector(selector);
let state = {
  session: null,
  agents: [],
  events: [],
  selected: null,
  modalAgent: null,
  socket: null,
  socketSession: null,
  heartbeat: null,
  reconnect: null,
};
let graphScale = 1;
let graphPanX = 0;
let graphPanY = 0;
let graphDrag = null;
let graphSuppressClick = false;
let renderFrame = null;

const esc = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (character) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        character
      ],
  );

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || response.statusText);
  }
  return response.json();
}

function statusLabel(value) {
  return value.replaceAll("_", " ");
}

function friendlyName(agent) {
  if (!agent) return "Trix";
  const explicit = agent.friendly_name || agent.display_name;
  if (explicit) return explicit;
  const raw = String(agent.name || agent.role || "Agent").trim();
  if (!/[-_]/.test(raw)) return raw;
  const acronyms = new Map([
    ["ai", "AI"], ["api", "API"], ["ci", "CI"], ["cli", "CLI"],
    ["css", "CSS"], ["db", "DB"], ["html", "HTML"], ["qa", "QA"],
    ["sdk", "SDK"], ["ui", "UI"], ["ux", "UX"],
  ]);
  return raw
    .split(/[-_]+/)
    .filter(Boolean)
    .map((word) => acronyms.get(word.toLowerCase()) || `${word[0].toUpperCase()}${word.slice(1)}`)
    .join(" ");
}

function scheduleRender() {
  if (renderFrame !== null) return;
  renderFrame = requestAnimationFrame(() => {
    renderFrame = null;
    render();
  });
}

function mergeAgent(agent) {
  if (!agent) return;
  const index = state.agents.findIndex((existing) => existing.id === agent.id);
  if (index === -1) state.agents.push(agent);
  else state.agents[index] = agent;
}

function statusGroup(status) {
  if (["completed"].includes(status)) return "completed";
  if (["failed", "cancelled"].includes(status)) return "failed";
  if (["waiting_for_children", "awaiting_verification", "queued"].includes(status)) {
    return "waiting";
  }
  if (["verifying", "reporting"].includes(status)) return "verifying";
  return "working";
}

function selectAgent(agentId) {
  state.selected = agentId;
  render();
}

function openAgentModal(agentId) {
  state.modalAgent = agentId;
  renderAgentModal();
  const dialog = $("#agent-dialog");
  if (!dialog.open) dialog.showModal();
}

function render() {
  const session = state.session;
  $("#title").textContent = session ? session.title : "Build with a visible agent team.";
  $("#subtitle").textContent = session
    ? session.user_prompt
    : "Codex does the engineering. Trix makes ownership, progress, and verification explicit.";
  const status = $("#status");
  status.className = `pill ${session?.status || "idle"}`;
  status.textContent = `● ${session ? statusLabel(session.status) : "Idle"}`;
  $("#agent-count").textContent = state.agents.length;
  renderTree();
  renderStats();
  renderDetail();
  renderEvents();
  renderGraph();
  if ($("#agent-dialog").open) renderAgentModal();
}

function renderTree() {
  const root = $("#tree");
  if (!state.agents.length) {
    root.className = "tree empty";
    root.textContent = "Create a session to assemble your team.";
    return;
  }
  root.className = "tree";
  const byParent = new Map();
  state.agents.forEach((agent) => {
    const key = agent.parent_id || "root";
    byParent.set(key, [...(byParent.get(key) || []), agent]);
  });
  const branch = (key, depth) =>
    (byParent.get(key) || [])
      .map(
        (agent) =>
          `<button class="agent ${state.selected === agent.id ? "selected" : ""}" ` +
          `data-agent="${agent.id}" style="padding-left:${10 + depth * 20}px">` +
          `<div class="agent-row"><span class="dot ${agent.status}"></span>` +
          `<strong>${esc(friendlyName(agent))}</strong></div>` +
          `<div class="agent-meta">${esc(agent.role)} · ${statusLabel(agent.status)}</div>` +
          `</button>${branch(agent.id, depth + 1)}`,
      )
      .join("");
  root.innerHTML = branch("root", 0);
  root.querySelectorAll("[data-agent]").forEach((element) => {
    element.onclick = () => selectAgent(element.dataset.agent);
  });
}

function renderStats() {
  const completed = state.agents.filter((agent) => agent.status === "completed").length;
  const active = state.agents.filter((agent) => statusGroup(agent.status) === "working").length;
  const failed = state.agents.filter((agent) => statusGroup(agent.status) === "failed").length;
  const depth = Math.max(...state.agents.map((agent) => agent.depth), 0);
  $("#stats").innerHTML =
    `<div class="stat"><b>${active}</b><span>Active</span></div>` +
    `<div class="stat"><b>${completed}</b><span>Accepted</span></div>` +
    `<div class="stat"><b>${failed}</b><span>Failed</span></div>` +
    `<div class="stat"><b>${depth}</b><span>Max depth</span></div>`;
}

function renderDetail() {
  const node = $("#detail");
  const agent = state.agents.find((item) => item.id === state.selected);
  $("#instruction").classList.toggle("hidden", !agent?.codex_thread_id);
  if (!agent) {
    node.className = "empty";
    node.textContent = "Select an agent to inspect its task and evidence.";
    return;
  }
  node.className = "agent-card";
  const report = agent.reports?.at(-1);
  node.innerHTML =
    `<span class="badge">${statusLabel(agent.status)}</span>` +
    `<h2>${esc(friendlyName(agent))}</h2><div class="role">${esc(agent.role)} · Depth ${agent.depth}</div>` +
    `<div class="task">${esc(agent.task)}</div><div class="facts">` +
    `<div class="fact"><span>Current activity</span>${esc(agent.current_activity)}</div>` +
    `<div class="fact"><span>Codex thread</span>${esc(agent.codex_thread_id?.slice(0, 12) || "Not started")}</div>` +
    `<div class="fact"><span>Files changed</span>${report?.files_changed?.length || 0}</div>` +
    `<div class="fact"><span>Report</span>${report ? statusLabel(report.status) : "Not submitted"}</div>` +
    `</div>${report ? `<div class="task"><strong>Completion report</strong><br>${esc(report.summary)}</div>` : ""}`;
}

function renderEvents() {
  const node = $("#events");
  if (!state.events.length) {
    node.className = "event-list empty";
    node.textContent = "Activity will stream here in real time.";
    return;
  }
  node.className = "event-list";
  node.innerHTML = state.events
    .slice(-200)
    .reverse()
    .map((event) => {
      const agent = state.agents.find((item) => item.id === event.agent_id);
      const time = new Date(event.created_at).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
      return `<div class="event"><time>${time}</time><span class="who">${esc(friendlyName(agent))}</span><span>${esc(event.message)}</span></div>`;
    })
    .join("");
}

function listMarkup(items, emptyText) {
  if (!items?.length) return `<div class="evidence-empty">${emptyText}</div>`;
  return `<ul>${items.map((item) => `<li>${esc(item)}</li>`).join("")}</ul>`;
}

function timelineKind(eventType) {
  if (eventType.includes("failed") || eventType.includes("rejected")) return "failed";
  if (eventType.includes("completed") || eventType.includes("accepted")) return "completed";
  if (eventType.includes("verification") || eventType.includes("report")) return "verifying";
  if (eventType.includes("waiting")) return "waiting";
  return "working";
}

function renderAgentModal() {
  const agent = state.agents.find((item) => item.id === state.modalAgent);
  const content = $("#agent-modal-content");
  if (!agent) {
    content.innerHTML = "";
    return;
  }
  const parent = state.agents.find((item) => item.id === agent.parent_id);
  const events = state.events.filter((event) => event.agent_id === agent.id);
  const reports = agent.reports || [];
  const reportMarkup = reports.length
    ? reports
        .map(
          (report, index) =>
            `<section class="modal-section report-block">` +
            `<div class="modal-section-heading"><span>Completion report ${reports.length > 1 ? index + 1 : ""}</span>` +
            `<span class="report-state ${report.status}">${esc(statusLabel(report.status))}</span></div>` +
            `<p class="report-summary">${esc(report.summary)}</p>` +
            `<div class="evidence-grid">` +
            `<div class="evidence-card"><h4>Requirements completed</h4>${listMarkup(report.requirements_completed, "No requirements listed")}</div>` +
            `<div class="evidence-card"><h4>Files changed</h4>${listMarkup(report.files_changed, "No changed files reported")}</div>` +
            `<div class="evidence-card"><h4>Commands run</h4>${listMarkup(report.commands_run, "No commands reported")}</div>` +
            `<div class="evidence-card"><h4>Verification</h4><pre>${esc(JSON.stringify(report.verification_results, null, 2))}</pre></div>` +
            `</div>` +
            `${report.known_issues?.length ? `<div class="report-callout warning"><strong>Known issues</strong>${listMarkup(report.known_issues, "")}</div>` : ""}` +
            `${report.risks?.length ? `<div class="report-callout warning"><strong>Risks</strong>${listMarkup(report.risks, "")}</div>` : ""}` +
            `${report.parent_feedback ? `<div class="report-callout feedback"><strong>Parent feedback</strong><p>${esc(report.parent_feedback)}</p></div>` : ""}` +
            `</section>`,
        )
        .join("")
    : `<section class="modal-section"><div class="modal-section-heading">Outputs and evidence</div><div class="modal-empty">This agent has not submitted a completion report yet. Live activity appears above as work progresses.</div></section>`;

  const timeline = events.length
    ? events
        .map((event) => {
          const kind = timelineKind(event.event_type);
          const time = new Date(event.created_at).toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
          });
          return `<div class="transcript-entry ${kind}"><div class="transcript-marker"><span>⌘</span></div>` +
            `<div class="transcript-copy"><div class="transcript-label">${esc(statusLabel(event.event_type))}<time>${time}</time></div>` +
            `<p>${esc(event.message)}</p></div></div>`;
        })
        .join("")
    : `<div class="modal-empty">No activity has been recorded for this agent yet.</div>`;

  content.innerHTML =
    `<header class="agent-modal-header"><div class="agent-modal-identity">` +
    `<span class="modal-status ${statusGroup(agent.status)}"></span>` +
    `<div><h2>${esc(friendlyName(agent))}</h2><span>${esc(agent.id.slice(0, 8))}</span></div></div>` +
    `<button id="close-agent-modal" class="modal-close" type="button" aria-label="Close agent details">×</button></header>` +
    `<div class="agent-modal-scroll"><section class="modal-overview">` +
    `<div><span>Assigned task</span><p>${esc(agent.task)}</p></div>` +
    `<dl><div><dt>Status</dt><dd>${esc(statusLabel(agent.status))}</dd></div>` +
    `<div><dt>Role</dt><dd>${esc(agent.role)}</dd></div>` +
    `<div><dt>Reports to</dt><dd>${esc(parent ? friendlyName(parent) : "User")}</dd></div>` +
    `<div><dt>Depth</dt><dd>${agent.depth}</dd></div></dl>` +
    `<div class="current-output"><span>Current activity</span><p>${esc(agent.current_activity)}</p></div>` +
    `${agent.error ? `<div class="report-callout error"><strong>Error</strong><p>${esc(agent.error)}</p></div>` : ""}` +
    `</section><section class="modal-section"><div class="modal-section-heading"><span>Activity and outputs</span><span>${events.length} events</span></div>` +
    `<div class="transcript">${timeline}</div></section>${reportMarkup}</div>`;

  $("#close-agent-modal").onclick = () => $("#agent-dialog").close();
  $("#modal-instruction").classList.toggle("hidden", !agent.codex_thread_id);
}

function renderGraph() {
  const svg = $("#agent-graph");
  const empty = $("#graph-empty");
  $("#graph-count").textContent = `${state.agents.length} ${state.agents.length === 1 ? "agent" : "agents"}`;
  if (!state.agents.length) {
    svg.innerHTML = "";
    svg.classList.add("hidden");
    empty.classList.remove("hidden");
    return;
  }
  empty.classList.add("hidden");
  svg.classList.remove("hidden");

  const width = 1120;
  const nodeWidth = 228;
  const nodeHeight = 66;
  const top = 72;
  const levelGap = 154;
  const maxDepth = Math.max(...state.agents.map((agent) => agent.depth), 0);
  const height = Math.max(330, top + maxDepth * levelGap + nodeHeight + 72);
  const levels = new Map();
  state.agents.forEach((agent) => {
    levels.set(agent.depth, [...(levels.get(agent.depth) || []), agent]);
  });
  const positions = new Map();
  levels.forEach((agents, depth) => {
    agents.forEach((agent, index) => {
      positions.set(agent.id, {
        x: ((index + 1) * width) / (agents.length + 1),
        y: top + depth * levelGap,
      });
    });
  });

  const selected = state.agents.find((agent) => agent.id === state.selected);
  const focusSelection = selected && selected.depth > 0;
  const links = state.agents
    .filter((agent) => agent.parent_id && positions.has(agent.parent_id))
    .map((agent) => {
      const parent = positions.get(agent.parent_id);
      const child = positions.get(agent.id);
      const startY = parent.y + nodeHeight / 2;
      const endY = child.y - nodeHeight / 2;
      const middle = (startY + endY) / 2;
      const selectedLink = focusSelection && agent.id === state.selected ? " selected" : "";
      return `<path class="graph-link${selectedLink}" d="M ${parent.x} ${startY} C ${parent.x} ${middle}, ${child.x} ${middle}, ${child.x} ${endY}"/>`;
    })
    .join("");

  const nodes = state.agents
    .map((agent) => {
      const position = positions.get(agent.id);
      const selectedNode = focusSelection && agent.id === state.selected ? " selected" : "";
      const group = statusGroup(agent.status);
      const displayName = friendlyName(agent);
      const name = esc(displayName.length > 28 ? `${displayName.slice(0, 27)}…` : displayName);
      return `<g class="graph-node ${group}${selectedNode}" data-graph-agent="${agent.id}" ` +
        `transform="translate(${position.x - nodeWidth / 2} ${position.y - nodeHeight / 2})" ` +
        `role="treeitem" tabindex="0" aria-label="${esc(displayName)}, ${statusLabel(agent.status)}">` +
        `<rect width="${nodeWidth}" height="${nodeHeight}" rx="11"/>` +
        `<circle class="graph-status-ring" cx="20" cy="24" r="7"/>` +
        `<circle class="graph-status" cx="20" cy="24" r="4"/>` +
        `<text class="graph-node-name" x="34" y="28">${name}</text>` +
        `<text class="graph-node-meta" x="20" y="49">${esc(statusLabel(agent.status))} · depth ${agent.depth}</text>` +
        `</g>`;
    })
    .join("");

  const centerX = width / 2;
  const centerY = height / 2;
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.dataset.graphCenterX = centerX;
  svg.dataset.graphCenterY = centerY;
  svg.innerHTML = `<g class="graph-world" transform="translate(${graphPanX} ${graphPanY}) translate(${centerX} ${centerY}) scale(${graphScale}) translate(${-centerX} ${-centerY})">${links}${nodes}</g>`;
  svg.querySelectorAll("[data-graph-agent]").forEach((element) => {
    const choose = (event) => {
      if (event && graphSuppressClick) {
        event.preventDefault();
        return;
      }
      openAgentModal(element.dataset.graphAgent);
    };
    element.addEventListener("click", choose);
    element.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        choose();
      }
    });
  });
}

function updateGraphTransform() {
  const svg = $("#agent-graph");
  const world = svg.querySelector(".graph-world");
  if (!world) return;
  const centerX = Number(svg.dataset.graphCenterX);
  const centerY = Number(svg.dataset.graphCenterY);
  world.setAttribute(
    "transform",
    `translate(${graphPanX} ${graphPanY}) translate(${centerX} ${centerY}) scale(${graphScale}) translate(${-centerX} ${-centerY})`,
  );
}

async function load(id) {
  const data = await api(`/api/sessions/${id}`);
  state.session = data.session;
  state.agents = data.agents;
  state.events = data.events;
  state.selected = state.selected || state.session.root_agent_id;
  if (state.socketSession !== id || !state.socket || state.socket.readyState > 1) connect(id);
  render();
}

function connect(id) {
  if (state.socket) state.socket.close();
  if (state.heartbeat) clearInterval(state.heartbeat);
  if (state.reconnect) clearTimeout(state.reconnect);
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${scheme}://${location.host}/api/sessions/${id}/events`);
  state.socket = socket;
  state.socketSession = id;
  socket.onmessage = (event) => {
    if (event.data === "pong") return;
    const message = JSON.parse(event.data);
    if (message.type === "snapshot") {
      state.session = message.session;
      state.agents = message.agents || [];
      state.events = message.events || [];
    } else if (message.type === "event") {
      if (message.session) state.session = message.session;
      mergeAgent(message.agent);
      if (message.event && !state.events.some((item) => item.id === message.event.id)) {
        state.events.push(message.event);
        if (state.events.length > 1000) state.events.splice(0, state.events.length - 1000);
      }
    }
    scheduleRender();
  };
  socket.onopen = () => {
    state.heartbeat = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) socket.send("ping");
    }, 25000);
  };
  socket.onclose = () => {
    if (state.socket === socket) {
      clearInterval(state.heartbeat);
      state.heartbeat = null;
      state.socket = null;
      state.reconnect = setTimeout(() => {
        if (state.session?.id === id) connect(id);
      }, 1500);
    }
  };
}

$("#new-session").onclick = () => $("#create-dialog").showModal();
$("#create-form").onsubmit = async (event) => {
  event.preventDefault();
  if (event.submitter?.value === "cancel") {
    $("#create-dialog").close();
    return;
  }
  const form = new FormData(event.currentTarget);
  try {
    const session = await api("/api/sessions", {
      method: "POST",
      body: JSON.stringify(Object.fromEntries(form)),
    });
    $("#create-dialog").close();
    history.replaceState(null, "", `/?session=${session.id}`);
    await load(session.id);
    state.session = await api(`/api/sessions/${session.id}/start`, { method: "POST" });
    scheduleRender();
  } catch (error) {
    alert(error.message);
  }
};

$("#instruction").onsubmit = async (event) => {
  event.preventDefault();
  const input = $("#instruction-text");
  const message = input.value.trim();
  if (!message || !state.modalAgent) return;
  try {
    const agent = await api(`/api/agents/${state.modalAgent}/instructions`, {
      method: "POST",
      body: JSON.stringify({ message }),
    });
    input.value = "";
    mergeAgent(agent);
    scheduleRender();
  } catch (error) {
    alert(error.message);
  }
};

$("#modal-instruction").onsubmit = async (event) => {
  event.preventDefault();
  const input = $("#modal-instruction-text");
  const message = input.value.trim();
  if (!message || !state.selected) return;
  try {
    const agent = await api(`/api/agents/${state.selected}/instructions`, {
      method: "POST",
      body: JSON.stringify({ message }),
    });
    input.value = "";
    mergeAgent(agent);
    scheduleRender();
  } catch (error) {
    alert(error.message);
  }
};

$("#agent-dialog").addEventListener("click", (event) => {
  if (event.target === event.currentTarget) event.currentTarget.close();
});

$("#agent-dialog").addEventListener("close", () => {
  state.modalAgent = null;
});

const graphSvg = $("#agent-graph");
graphSvg.addEventListener("pointerdown", (event) => {
  if (event.button < 0 || event.button > 2) return;
  const rect = graphSvg.getBoundingClientRect();
  const viewBox = graphSvg.viewBox.baseVal;
  graphDrag = {
    pointerId: event.pointerId,
    startX: event.clientX,
    startY: event.clientY,
    panX: graphPanX,
    panY: graphPanY,
    scaleX: viewBox.width / rect.width,
    scaleY: viewBox.height / rect.height,
    moved: false,
  };
  graphSvg.setPointerCapture(event.pointerId);
  graphSvg.classList.add("dragging");
});
graphSvg.addEventListener("pointermove", (event) => {
  if (!graphDrag || graphDrag.pointerId !== event.pointerId) return;
  const dx = event.clientX - graphDrag.startX;
  const dy = event.clientY - graphDrag.startY;
  if (Math.hypot(dx, dy) >= 4) graphDrag.moved = true;
  graphPanX = graphDrag.panX + dx * graphDrag.scaleX;
  graphPanY = graphDrag.panY + dy * graphDrag.scaleY;
  updateGraphTransform();
});
const endGraphDrag = (event) => {
  if (!graphDrag || graphDrag.pointerId !== event.pointerId) return;
  graphSuppressClick = graphDrag.moved;
  graphDrag = null;
  graphSvg.classList.remove("dragging");
  if (graphSvg.hasPointerCapture(event.pointerId)) graphSvg.releasePointerCapture(event.pointerId);
  setTimeout(() => { graphSuppressClick = false; }, 0);
};
graphSvg.addEventListener("pointerup", endGraphDrag);
graphSvg.addEventListener("pointercancel", endGraphDrag);
graphSvg.addEventListener("contextmenu", (event) => {
  if (graphSuppressClick) event.preventDefault();
});

$("#graph-zoom-in").onclick = () => {
  graphScale = Math.min(1.35, graphScale + 0.1);
  renderGraph();
};
$("#graph-zoom-out").onclick = () => {
  graphScale = Math.max(0.7, graphScale - 0.1);
  renderGraph();
};
$("#graph-fit").onclick = () => {
  graphScale = 1;
  graphPanX = 0;
  graphPanY = 0;
  renderGraph();
};

const id = new URLSearchParams(location.search).get("session");
if (id) load(id).catch(() => history.replaceState(null, "", "/"));
else api("/api/sessions").then((items) => items[0] && load(items[0].id));
render();
