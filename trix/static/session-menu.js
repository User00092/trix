(() => {
  const encode = (value) => String(value ?? "").replace(/[&<>"']/g, (character) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[character]);
  const dialog = document.querySelector("#sessions-dialog");
  const list = document.querySelector("#sessions-list");

  async function populate() {
    try {
      const response = await fetch("/api/sessions");
      if (!response.ok) throw new Error("Could not load sessions");
      const sessions = await response.json();
      const activeId = new URLSearchParams(location.search).get("session");
      list.innerHTML = sessions.length ? sessions.map((session) => {
        const selected = session.id === activeId ? " selected" : "";
        const created = new Date(session.created_at).toLocaleString([], {
          dateStyle: "medium", timeStyle: "short",
        });
        return `<button class="session-list-item${selected}" data-session="${encode(session.id)}" type="button">` +
          `<span class="session-list-status ${encode(session.status)}"></span>` +
          `<span class="session-list-copy"><strong>${encode(session.title)}</strong>` +
          `<small>${encode(created)} · ${encode(session.status.replaceAll("_", " "))}</small>` +
          `<span>${encode(session.user_prompt)}</span></span></button>`;
      }).join("") : `<div class="empty">No sessions have been created yet.</div>`;
      list.querySelectorAll("[data-session]").forEach((button) => {
        button.onclick = () => {
          const id = button.dataset.session;
          history.pushState({ session: id }, "", `/?session=${encodeURIComponent(id)}`);
          dialog.close();
          if (typeof load === "function") load(id);
          else location.reload();
        };
      });
    } catch (error) {
      list.innerHTML = `<div class="empty">${encode(error.message)}</div>`;
    }
  }

  document.querySelector("#sessions-menu").onclick = () => {
    dialog.showModal();
    populate();
  };
  document.querySelector("#close-sessions").onclick = () => dialog.close();
})();
