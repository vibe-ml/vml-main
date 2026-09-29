const overview = document.querySelector("#overview");
const research = document.querySelector("#research");
const navOverview = document.querySelector("#nav-overview");
const navResearch = document.querySelector("#nav-research");
const catalog = document.querySelector("#catalog");
const readiness = document.querySelector("#readiness");
const form = document.querySelector("#query-form");
const start = document.querySelector("#start");
const formError = document.querySelector("#form-error");
const activity = document.querySelector("#activity");
const report = document.querySelector("#report");
const runList = document.querySelector("#run-list");

let source = null;
let seenReport = false;

function show(name) {
  const onOverview = name === "overview";
  overview.hidden = !onOverview;
  research.hidden = onOverview;
  if (onOverview) {
    navOverview.setAttribute("aria-current", "page");
    navResearch.removeAttribute("aria-current");
  } else {
    navResearch.setAttribute("aria-current", "page");
    navOverview.removeAttribute("aria-current");
  }
  history.replaceState(null, "", onOverview ? "#project" : "#research");
}

navOverview.addEventListener("click", () => show("overview"));
navResearch.addEventListener("click", () => show("research"));

function escapeHtml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderMarkdown(sourceText) {
  const escaped = escapeHtml(sourceText);
  const lines = escaped.split("\n");
  const html = [];
  let inList = false;
  const flushList = () => {
    if (inList) {
      html.push("</ul>");
      inList = false;
    }
  };
  for (const line of lines) {
    if (line.startsWith("### ")) {
      flushList();
      html.push(`<h3>${inline(line.slice(4))}</h3>`);
    } else if (line.startsWith("## ")) {
      flushList();
      html.push(`<h2>${inline(line.slice(3))}</h2>`);
    } else if (line.startsWith("# ")) {
      flushList();
      html.push(`<h1>${inline(line.slice(2))}</h1>`);
    } else if (line.startsWith("- ")) {
      if (!inList) {
        html.push("<ul>");
        inList = true;
      }
      html.push(`<li>${inline(line.slice(2))}</li>`);
    } else if (line.trim() === "") {
      flushList();
    } else {
      flushList();
      html.push(`<p>${inline(line)}</p>`);
    }
  }
  flushList();
  return html.join("");
}

function inline(text) {
  return text
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" rel="noreferrer">$1</a>')
    .replace(/(^|\s)(https?:\/\/[^\s<]+)/g, '$1<a href="$2" rel="noreferrer">$2</a>');
}

function setReport(text, fresh) {
  if (!text) {
    report.classList.remove("is-fresh");
    report.innerHTML =
      '<p class="empty">The report lists technology candidates, the sources behind each claim, and what this search did not cover. A candidate is not a confirmed weak signal until the evidence says so.</p>';
    return;
  }
  report.innerHTML = renderMarkdown(text);
  if (fresh && !seenReport) {
    report.classList.add("is-fresh");
    seenReport = true;
  }
}

function addActivity(event) {
  if (event.kind === "report" || event.kind === "status") {
    return;
  }
  const item = document.createElement("li");
  const when = document.createElement("time");
  when.dateTime = event.at || "";
  when.textContent = (event.at || "").slice(11, 19);
  const scope = document.createElement("span");
  scope.className = "scope";
  scope.textContent = event.scope ? ` ${event.scope}` : "";
  const body = document.createElement("p");
  const label = event.kind === "result" && event.name ? `${event.name}: ` : "";
  body.textContent = `${label}${event.text || ""}`;
  item.append(when, scope, body);
  activity.append(item);
  item.scrollIntoView({ block: "nearest" });
}

function applyEvent(event) {
  if (event.kind === "report") {
    setReport(event.text, true);
    return;
  }
  if (event.kind === "status") {
    start.disabled = event.status === "running";
    if (event.status === "failed") {
      showError(event.text || "Research failed.");
    }
    if (event.status === "completed" || event.status === "failed") {
      loadRuns();
    }
    return;
  }
  addActivity(event);
}

function showError(message) {
  formError.hidden = false;
  formError.textContent = message;
}

function clearError() {
  formError.hidden = true;
  formError.textContent = "";
}

async function loadCatalog() {
  const response = await fetch("/api/catalog");
  const body = await response.json();
  catalog.replaceChildren();
  for (const component of body.components) {
    const term = document.createElement("dt");
    term.textContent = `${component.name} · ${component.repo}`;
    const detail = document.createElement("dd");
    detail.textContent = component.role;
    catalog.append(term, detail);
  }
}

async function loadStatus() {
  const response = await fetch("/api/status");
  const body = await response.json();
  if (body.ready) {
    const model = body.model_base_url || body.model;
    readiness.textContent = `Ready. The agent uses ${model} and web search.`;
    start.disabled = false;
    return;
  }
  readiness.textContent = body.message;
  start.disabled = true;
}

async function loadRuns() {
  const response = await fetch("/api/runs");
  const body = await response.json();
  const current = runList.value;
  runList.replaceChildren();
  const empty = document.createElement("option");
  empty.value = "";
  empty.textContent = body.runs.length ? "Select a run" : "None yet";
  runList.append(empty);
  for (const run of body.runs) {
    const option = document.createElement("option");
    option.value = run.id;
    option.textContent = `${run.created_at.slice(11, 19)} ${run.query.slice(0, 72)}`;
    runList.append(option);
  }
  if (current) {
    runList.value = current;
  }
}

function resetView() {
  if (source) {
    source.close();
    source = null;
  }
  activity.replaceChildren();
  seenReport = false;
  setReport("", false);
  clearError();
}

function watch(runId) {
  if (source) {
    source.close();
  }
  source = new EventSource(`/api/runs/${runId}/events`);
  source.onmessage = (message) => applyEvent(JSON.parse(message.data));
  source.onerror = () => {
    source.close();
  };
}

async function openRun(runId) {
  resetView();
  const response = await fetch(`/api/runs/${runId}`);
  if (!response.ok) {
    showError("That run is no longer available.");
    return;
  }
  const run = await response.json();
  document.querySelector("#query").value = run.query;
  document.querySelector("#exclusions").value = run.exclusions;
  if (run.status === "running" || run.status === "queued") {
    start.disabled = true;
    watch(run.id);
    return;
  }
  for (const event of run.events) {
    applyEvent(event);
  }
  if (run.report) {
    setReport(run.report, false);
  }
  if (run.status === "failed" && run.error) {
    showError(run.error);
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  clearError();
  const query = document.querySelector("#query").value.trim();
  const exclusions = document.querySelector("#exclusions").value.trim();
  start.disabled = true;
  const response = await fetch("/api/runs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, exclusions }),
  });
  const body = await response.json();
  if (!response.ok) {
    start.disabled = false;
    showError(body.detail || "Research did not start.");
    return;
  }
  resetView();
  start.disabled = true;
  runList.value = "";
  watch(body.id);
  loadRuns();
});

runList.addEventListener("change", () => {
  if (runList.value) {
    openRun(runList.value);
  }
});

if (location.hash === "#research") {
  show("research");
}

loadCatalog().catch(() => {
  readiness.textContent = "The project list did not load.";
});
loadStatus().catch(() => {
  readiness.textContent = "The agent status did not load.";
});
loadRuns();
