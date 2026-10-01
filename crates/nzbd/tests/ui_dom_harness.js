// UI DOM harness: proves the embedded page's renderer obeys the five
// rendering laws in docs/UI_V2_PLAN.md §3, using a ~100-line fake of the
// `dom` adapter instead of jsdom. No npm, no browser.
//
// The adapter interface exists precisely so a fake this small is enough:
// the reconciler and every row builder may touch the DOM ONLY through it,
// so counting calls on the fake counts every write the renderer makes.
//
// What this catches, concretely (field report 2026-07-25): a renderer that
// rebuilds rows every tick destroys the button you are pressing between
// mousedown and mouseup, so the click never fires. `identity across ticks`
// below is that bug's regression test.
"use strict";
const fs = require("fs");
const vm = require("vm");

const htmlPath = process.argv[2];
const html = fs.readFileSync(htmlPath, "utf8");
const scriptMatch = html.match(/<script>([\s\S]*?)<\/script>/);
if (!scriptMatch) { console.error("no inline <script> found"); process.exit(2); }

// ---------------------------------------------------------------------------
// The fake DOM adapter. Nodes are plain objects with a children array; every
// mutating call bumps a counter so the tests can assert "writes only what
// changed".
// ---------------------------------------------------------------------------
const counts = { create: 0, insert: 0, remove: 0, text: 0, cls: 0, style: 0, prop: 0, attr: 0, data: 0 };
function resetCounts() { for (const k of Object.keys(counts)) counts[k] = 0; }

function node(tag, cls) {
  return {
    tag, className: cls || "", children: [], parent: null,
    dataset: {}, style: {}, attrs: {}, textContent: "",
    title: "", disabled: false, hidden: false,
    scrollTop: 0, clientHeight: 0, scrollHeight: 0,
  };
}
const fake = {
  create(tag, cls) { counts.create++; return node(tag, cls); },
  append(parent, n) { n.parent = parent; parent.children.push(n); return n; },
  insertBefore(parent, n, ref) {
    counts.insert++;
    if (n.parent) {
      const i = n.parent.children.indexOf(n);
      if (i >= 0) n.parent.children.splice(i, 1);
    }
    const at = ref ? parent.children.indexOf(ref) : -1;
    if (at >= 0) parent.children.splice(at, 0, n); else parent.children.push(n);
    n.parent = parent;
  },
  remove(parent, n) {
    counts.remove++;
    const i = parent.children.indexOf(n);
    if (i >= 0) parent.children.splice(i, 1);
    n.parent = null;
  },
  first(parent) { return parent.children[0] || null; },
  next(n) {
    if (!n.parent) return null;
    return n.parent.children[n.parent.children.indexOf(n) + 1] || null;
  },
  text(n, s) { counts.text++; n.textContent = s; },
  cls(n, s) { counts.cls++; n.className = s; },
  toggle(n, c, on) { counts.cls++; n[c] = !!on; },
  style(n, k, v) { counts.style++; n.style[k] = v; },
  prop(n, k, v) { counts.prop++; n[k] = v; },
  attr(n, k, v) { counts.attr++; n.attrs[k] = v; },
  data(n, k, v) { counts.data++; n.dataset[k] = v; },
};

// ---------------------------------------------------------------------------
// Minimal sandbox: enough for the page script to reach its own bottom, where
// it publishes `window.__nzbd_test`.
// ---------------------------------------------------------------------------
// Elements the page reaches by id are a mini-DOM (real child lists), so the
// live renderer can run end to end against them — that is how the toast
// stack and the pending overlay get exercised for real.
const failures = [];
const ids = new Set([...html.matchAll(/id="([^"]+)"/g)].map((m) => m[1]));
// Checkboxes start where the MARKUP says they start. The scope toggles ship
// with per-file off, and the Logs backfill now asks only for the scopes
// that are ticked — a harness that defaulted every box to false would have
// the page fetching nothing and call it a pass.
const checkedIds = new Set(
  [...html.matchAll(/<input\b[^>]*>/g)]
    .filter((m) => /\bchecked\b/.test(m[0]))
    .map((m) => (m[0].match(/id="([^"]+)"/) || [])[1])
    .filter(Boolean),
);
function stubEl(id) {
  const t = {
    id, style: {}, dataset: {}, attrs: {}, hidden: false, disabled: false,
    value: "", textContent: "", innerHTML: "", className: "", title: "",
    checked: checkedIds.has(id), scrollTop: 0, clientHeight: 0, scrollHeight: 0,
    children: [], parentNode: null,
    classList: { toggle() {}, add() {}, remove() {} },
    querySelectorAll: () => [], querySelector: () => null,
    addEventListener() {}, replaceWith() {},
    appendChild(n) { if (n.parentNode) n.parentNode.removeChild(n); t.children.push(n); n.parentNode = t; return n; },
    removeChild(n) { const i = t.children.indexOf(n); if (i >= 0) t.children.splice(i, 1); n.parentNode = null; },
    insertBefore(n, ref) {
      if (n.parentNode) n.parentNode.removeChild(n);
      const at = ref ? t.children.indexOf(ref) : -1;
      if (at >= 0) t.children.splice(at, 0, n); else t.children.push(n);
      n.parentNode = t;
    },
    get firstChild() { return t.children[0] || null; },
    get nextSibling() {
      if (!t.parentNode) return null;
      const s = t.parentNode.children;
      return s[s.indexOf(t) + 1] || null;
    },
    select() {}, click() {}, focus() { t.focused = true; }, matches: () => false,
    reportValidity: () => true,
    setAttribute(k, v) { t.attrs[k] = v; }, removeAttribute() {}, getAttribute: () => null,
    closest: () => null,
  };
  return t;
}
const elCache = new Map();
// Selector-routed queryAll, for the handful of places the page reaches for
// a *set* of elements. `.wrap > *` is the one that matters: the first-run
// wizard hides the app through it.
const qsa = new Map();
function selMatch(el, sel) {
  return sel.split(",").map((x) => x.trim()).filter(Boolean).some((one) => {
    if (one.startsWith("#")) return el.id === one.slice(1);
    if (one.startsWith(".")) return (" " + el.className + " ").includes(" " + one.slice(1) + " ");
    return el.tag === one;
  });
}
function wrapChild(tag, id, cls) {
  // Reuse the id-addressed stub so the page and the test see one object.
  const el = id ? (elCache.get(id) || (elCache.set(id, stubEl(id)), elCache.get(id))) : stubEl("_" + tag);
  el.tag = tag;
  if (cls) el.className = cls;
  el.matches = (sel) => selMatch(el, sel);
  return el;
}
// Scripted daemon. `routes` maps a URL substring to {status, body}; the
// default is a dead daemon, which is what the boot path should survive.
const routes = new Map();
const seen = [];
const scheduledTimeouts = [];
async function routeFetch(url, init) {
  seen.push({
    url,
    method: (init && init.method) || "GET",
    headers: (init && init.headers) || {},
    body: init && init.body,
  });
  for (const [frag, entry] of routes) {
    if (!String(url).includes(frag)) continue;
    // A route may be a function when a test needs consecutive calls to
    // answer differently (a page that empties out under the reader).
    const res = typeof entry === "function" ? await entry(url, init) : entry;
    return { ok: res.status < 400, status: res.status, json: async () => res.body, text: async () => "" };
  }
  return { ok: false, status: 503, json: async () => ({}), text: async () => "" };
}
const sandbox = {
  console,
  __nzbd_test_enable: true,
  document: {
    getElementById(id) {
      if (!ids.has(id)) failures.push(`$("${id}") — no element with that id in the markup`);
      if (!elCache.has(id)) elCache.set(id, stubEl(id));
      return elCache.get(id);
    },
    querySelectorAll: (sel) => qsa.get(sel) || [],
    createElement: (t) => stubEl("_" + t),
    documentElement: stubEl("_root"),
    addEventListener() {},
  },
  navigator: { serviceWorker: { register: () => Promise.resolve() } },
  localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
  location: { reload() {} },
  // Swappable so the action tests can script the daemon's answers.
  fetch: async (url, init) => routeFetch(url, init),
  EventSource: class {
    constructor(u) { this.url = u; this.readyState = 0; this.listeners = {}; }
    addEventListener(n, f) { this.listeners[n] = f; }
    close() { this.readyState = 2; }
  },
  setInterval: () => 0, clearInterval() {},
  setTimeout: (_fn, delay) => { scheduledTimeouts.push(delay); return scheduledTimeouts.length; },
  clearTimeout() {},
  AbortController: class {
    constructor() { this.signal = { aborted: false }; }
    abort() { this.signal.aborted = true; }
  },
  confirm: () => true, alert() {},
  URL: { createObjectURL: () => "blob:x", revokeObjectURL() {} },
  Blob: class {}, Date, Math, JSON, Promise, Number, String, Array, Object, Set, Map,
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
process.on("unhandledRejection", (e) => {
  console.error("UI DOM HARNESS: unhandled rejection: " + (e && e.stack ? e.stack : e));
  process.exit(1);
});

vm.createContext(sandbox);
try {
  vm.runInContext(scriptMatch[1], sandbox, { filename: "index.html<script>" });
} catch (e) {
  console.error("script threw at load: " + e.message);
  process.exit(1);
}
const T = sandbox.window.__nzbd_test;
if (!T) { console.error("page did not expose window.__nzbd_test"); process.exit(1); }

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------
let checks = 0;
function ok(cond, what) {
  checks++;
  if (!cond) failures.push(what);
}
function eq(a, b, what) { ok(a === b, `${what}: expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`); }

function job(id, over) {
  return Object.assign({
    id, name: "job " + id, status: "downloading", category: "tv", priority: 0,
    size_bytes: 1000, downloaded_bytes: 100, failed_bytes: 0, remaining_bytes: 900,
    total_articles: 10, done_articles: 1, failed_articles: 0,
    files_total: 2, files_done: 0, health: 1000, critical_health: 850,
    rate_bps: 1024, retried_articles: 0, assigned_node: null, pp_done: false,
    dupe_key: "", dupe_score: 0, stages: [],
  }, over || {});
}
const models = (jobs) => jobs.map((j, i) => T.rowModel(j, { idx: i, count: jobs.length }));

// --- 0. display choices preserve the shipped layout as Classic ------------
{
  const d = T.readDisplay();
  eq(d.layout, "classic", "Classic is the layout fallback");
  eq(d.palette, "classic", "Classic is the palette fallback");
  eq(d.appearance, "auto", "appearance follows the system by default");
  ok(T.DISPLAY_LAYOUTS.includes("plex") && T.DISPLAY_LAYOUTS.includes("theater"),
    "both opt-in layouts are registered");
  ok(T.DISPLAY_PALETTES.includes("terminal") && T.DISPLAY_PALETTES.includes("tide") &&
    T.DISPLAY_PALETTES.includes("panoptic") && T.DISPLAY_PALETTES.includes("redline") &&
    T.DISPLAY_PALETTES.includes("panovic") && T.DISPLAY_PALETTES.includes("copper"),
    "the palette catalogue is registered");
  eq(T.displayMode("void", "light"), "dark", "Void stays midnight-only");
  eq(T.displayMode("vhs", "light"), "dark", "VHS stays midnight-only");
  eq(T.displayMode("panoptic", "light"), "dark", "Panoptic stays midnight-only");
  eq(T.displayMode("redline", "light"), "dark", "Redline stays midnight-only");
  eq(T.displayMode("panovic", "light"), "dark", "Burnt Pumpkin stays midnight-only");
  eq(T.displayMode("copper", "light"), "dark", "Copper stays midnight-only");
  ok(/:root\[data-palette="panovic"\]\s*\{[^}]*--bg:#000000;[^}]*--accent:#e8871e;--accent2:#81420b;[^}]*--on-accent:#150b00;/s.test(html),
    "Burnt Pumpkin keeps the shared true-black cockpit palette");
  ok(/:root\[data-palette="copper"\]\s*\{[^}]*--bg:#000000;[^}]*--accent:#cf7643;--accent2:#70402b;[^}]*--on-accent:#160b06;/s.test(html),
    "Copper keeps the shared true-black cockpit palette");
  ok(!/:root:is\(\[data-palette="panoptic"\],\[data-palette="redline"\],\[data-palette="panovic"\]\)/.test(html),
    "Burnt Pumpkin and Copper receive every cockpit-family rule");
}

// --- 1. rowModel is pure: no DOM, strings and flags only -------------------
{
  const m = T.rowModel(job(7, { downloaded_bytes: 500 }), { idx: 0, count: 2 });
  eq(m.key, "j7", "row key is the job id");
  eq(m.pct, "50%", "percent from downloaded/size");
  eq(m.st, "DOWNLOADING", "status uppercased");
  eq(m.upDisabled, true, "first row cannot move up");
  eq(m.downDisabled, false, "not the last row");
  for (const [k, v] of Object.entries(m))
    ok(v === null || ["string", "number", "boolean"].includes(typeof v),
      `rowModel.${k} is a scalar (got ${typeof v})`);
}

// --- 2. identity across ticks (THE regression test) ------------------------
{
  const tbody = node("tbody");
  const jobs = [job(1), job(2), job(3)];
  T.reconcileRows(tbody, models(jobs), fake);
  const before = tbody.children.slice();
  eq(before.length, 3, "three rows built");
  const delBtn = before[0].children[5].children[0].children[5];
  eq(delBtn.dataset.action, "delete", "last action button is delete");

  for (let tick = 0; tick < 50; tick++) {
    jobs.forEach((j, i) => { j.downloaded_bytes = 100 + tick * 10 + i; j.rate_bps = 1000 + tick; });
    T.reconcileRows(tbody, models(jobs), fake);
  }
  const after = tbody.children;
  eq(after.length, 3, "still three rows after 50 ticks");
  for (let i = 0; i < 3; i++)
    ok(before[i] === after[i], `row ${i} is the SAME node after 50 ticks`);
  ok(before[0].children[5].children[0].children[5] === delBtn,
    "the delete button survives 50 ticks — a click across a tick still fires");
}

// --- 3. law #3: a tick writes only the cells that changed ------------------
{
  const tbody = node("tbody");
  const jobs = [job(1)];
  T.reconcileRows(tbody, models(jobs), fake);
  // Nothing changed at all: zero writes.
  resetCounts();
  T.reconcileRows(tbody, models(jobs), fake);
  eq(counts.text + counts.cls + counts.style + counts.prop + counts.attr + counts.data, 0,
    "an unchanged job costs zero DOM writes");
  eq(counts.create, 0, "an unchanged job creates no nodes");
  // Only progress moved: the fill width, the percent and the detail line.
  jobs[0].downloaded_bytes = 200;
  jobs[0].remaining_bytes = 800;
  resetCounts();
  T.reconcileRows(tbody, models(jobs), fake);
  eq(counts.create, 0, "progress change creates no nodes");
  eq(counts.style, 1, "one style write (the bar width)");
  // Two of six cells: the percent and the bold "downloaded" figure. The
  // rest of the detail line (size, rate, ETA) rendered identically, so it
  // is not written — that is law #3 doing its job.
  eq(counts.text, 2, "two text writes (percent, bold bytes)");
  ok(counts.text + counts.style + counts.cls + counts.prop + counts.attr + counts.data <= 5,
    `a 1 Hz progress tick writes a handful of cells, not the row (was ${JSON.stringify(counts)})`);
}

// --- 4. keyed reorder moves nodes, it does not rebuild them ---------------
{
  const tbody = node("tbody");
  const jobs = [job(1), job(2), job(3)];
  T.reconcileRows(tbody, models(jobs), fake);
  const [n1, n2, n3] = tbody.children;
  resetCounts();
  T.reconcileRows(tbody, models([jobs[2], jobs[0], jobs[1]]), fake);
  eq(counts.create, 0, "a reorder creates nothing");
  ok(tbody.children[0] === n3 && tbody.children[1] === n1 && tbody.children[2] === n2,
    "reorder preserves every node, in the new order");
  eq(tbody.children[0].dataset.jobId, "3", "moved row keeps its identity");
}

// --- 5. add and remove ----------------------------------------------------
{
  const tbody = node("tbody");
  const jobs = [job(1), job(2)];
  T.reconcileRows(tbody, models(jobs), fake);
  const keep = tbody.children[1];
  T.reconcileRows(tbody, models([jobs[1]]), fake);
  eq(tbody.children.length, 1, "removed job's row is gone");
  ok(tbody.children[0] === keep, "the surviving row is the same node");
  T.reconcileRows(tbody, models([job(9), jobs[1]]), fake);
  eq(tbody.children.length, 2, "new job inserted");
  eq(tbody.children[0].dataset.jobId, "9", "inserted at its model position");
  ok(tbody.children[1] === keep, "the existing row still is not rebuilt");
}

// --- 6. foreign boot markup is swept once, then never again ---------------
{
  const tbody = node("tbody");
  fake.append(tbody, node("tr")); // the page's "Loading the queue…" row
  T.reconcileRows(tbody, models([job(1)]), fake);
  eq(tbody.children.length, 1, "boot placeholder replaced");
  resetCounts();
  T.reconcileRows(tbody, models([job(1)]), fake);
  eq(counts.remove, 0, "the sweep does not run again");
}

// --- 7. the empty placeholder is just another key -------------------------
{
  const tbody = node("tbody");
  T.reconcileRows(tbody, [{ key: "__empty", kind: "empty", span: 6, text: "nothing here" }], fake);
  eq(tbody.children.length, 1, "placeholder row present");
  eq(tbody.children[0].children[0].textContent, "nothing here", "placeholder text set");
  eq(tbody.children[0].children[0].attrs.colspan, "6", "placeholder spans the table");
  T.reconcileRows(tbody, models([job(1)]), fake);
  eq(tbody.children.length, 1, "placeholder swapped for the real row");
  eq(tbody.children[0].dataset.jobId, "1", "…and it is the job row");
}

// --- 8. status semantics the queue leans on ------------------------------
{
  eq(T.statusName({ post: { stage: "unpack" } }), "extracting", "post stage reads as words");
  const f = T.rowModel(job(1, { status: "fetching", size_bytes: 0, downloaded_bytes: 0 }), { idx: 0, count: 1 });
  eq(f.detail, "fetching the NZB from the indexer…", "a URL job says what it is doing");
  eq(f.size, "—", "no size until the NZB lands");
  eq(f.pauseHidden, true, "nothing to pause while fetching");
  const doomed = T.rowModel(job(1, { health: 700 }), { idx: 0, count: 1, healthAbortArmed: true });
  eq(doomed.hNote, "unrepairable · aborting", "armed health-abort is stated on the row");
  const doomed2 = T.rowModel(job(1, { health: 700 }), { idx: 0, count: 1, healthAbortArmed: false });
  eq(doomed2.hNote, "unrepairable · will fail at end", "…and so is the un-armed case");
  const held = job(1, { status: "paused", control: {
    lifecycle: "held", cause: "identity_conflict", retry_policy: "review",
    message: "conflicting yEnc declared size",
  } });
  const heldRow = T.rowModel(held, { mixedSection: false });
  eq(heldRow.st, "HELD", "a hold is distinct from a manual pause");
  eq(heldRow.stHidden, false, "the hold remains visible in a single-state section");
  ok(heldRow.dRest.includes("conflicting yEnc declared size"), "the hold reason appears in the row");
  eq(heldRow.pauseHidden, true, "review holds do not offer a resume that cannot work");
  const capacityRow = T.rowModel({ ...held, control: { ...held.control,
    cause: "capacity", retry_policy: "resume_same_job", message: "storage full" } });
  eq(capacityRow.pauseHidden, false, "resource holds retain the resume probe");
  eq(capacityRow.pauseAction, "resume", "resource holds can request recovery");
}

// --- 8b. storage paths: capacity, warning levels, stable rows ------------
{
  const GiB = 1024 ** 3;
  const storage = T.storageModels({ storage: [
    { label: "working", path: "/data/working", available_bytes: 25 * GiB, total_bytes: 100 * GiB },
    { label: "downloads", path: "/data/downloads", available_bytes: 10 * GiB, total_bytes: 100 * GiB },
    { label: "failed", path: "/data/failed", available_bytes: null, total_bytes: null },
  ] });
  eq(storage.length, 3, "every configured storage path gets a row");
  eq(storage[0].pct, "75%", "fullness is used capacity, not free capacity");
  eq(storage[0].used, "75.0 GiB / 100 GiB", "used and total capacity are both stated");
  eq(storage[0].free, "25.0 GiB free", "remaining capacity is stated too");
  ok(storage[1].cls.includes("warn"), "a filesystem at 90% reads as a warning");
  eq(storage[2].pct, "measuring…", "the initial probe does not pretend zero usage");

  const list = node("div");
  T.reconcileRows(list, storage, fake);
  const first = list.children[0];
  eq(first.children[0].children[0].textContent, "working", "the path role is visible");
  eq(first.children[0].children[1].textContent, "/data/working", "the actual path is visible");
  eq(first.children[1].children[0].style.width, "75.0%", "the bar shows the same fullness");
  resetCounts();
  T.reconcileRows(list, storage, fake);
  eq(counts.text + counts.cls + counts.style + counts.prop, 0,
    "unchanged storage readings do not rewrite their rows");
}

// --- 9. detail panel is a stable subtree ---------------------------------
{
  const tbody = node("tbody");
  const j = job(1);
  T.store.jobFiles = { job: 1, files: [
    { id: 1, filename: "a.rar", size_bytes: 10, done_segments: 1, total_segments: 2, failed_segments: 0, paused: false, is_par2: false, assembled: false },
    { id: 2, filename: "b.par2", size_bytes: 20, done_segments: 2, total_segments: 2, failed_segments: 0, paused: false, is_par2: true, assembled: true },
  ] };
  T.store.jobLogs = { job: 1, entries: [{ id: 5, kind: "INFO", time_unix: 1, text: "hello" }] };
  const withDetail = () => [T.rowModel(j, { idx: 0, count: 1 }), T.detailModel(j)];
  T.reconcileRows(tbody, withDetail(), fake);
  eq(tbody.children.length, 2, "detail row sits under its job row");
  const detail = tbody.children[1];
  // wrap children: head, pipeline, recovery, meta, files <details>, activity
  const wrap = detail.children[0].children[0];
  const recovery = detail.__c.recovery;
  eq(recovery.hidden, true, "recovery controls stay out of ordinary downloads");
  const filesBox = detail.__c.filesBox;
  eq(filesBox.tag, "details", "the file list is foldable");
  eq(filesBox.open, true, "…and open by default in the queue");
  const filesBody = filesBox.children[1].children[1];
  eq(filesBody.children.length, 2, "one row per file");
  const fileRow = filesBody.children[0];
  const logsBox = detail.__c.logs;
  eq(logsBox.children.length, 1, "one activity line");
  const logLine = logsBox.children[0];

  // Folding it yourself must survive the next tick: the model writes
  // `open` only when the DEFAULT changes, and it never does mid-panel.
  filesBox.open = false;
  resetCounts();
  T.reconcileRows(tbody, withDetail(), fake);
  eq(filesBox.open, false, "a tick does not re-open a list you folded");

  // A tick with more segments done must not rebuild the panel.
  T.store.jobFiles.files[0].done_segments = 2;
  T.store.jobLogs.entries.push({ id: 6, kind: "INFO", time_unix: 2, text: "world" });
  resetCounts();
  T.reconcileRows(tbody, withDetail(), fake);
  ok(tbody.children[1] === detail, "the detail row is the same node across a tick");
  ok(filesBody.children[0] === fileRow, "a file row is mutated, not rebuilt");
  ok(logsBox.children[0] === logLine, "the activity tail is appended to, not replaced");
  eq(logsBox.children.length, 2, "the new activity line was appended");
  eq(counts.remove, 0, "nothing is torn down — scroll position survives");
}

// --- 10. history rows -----------------------------------------------------
{
  const e = {
    job: 4, name: "old job", category: "tv", final_dir: "/dest/x", status: "SUCCESS",
    size: 2048, completed_at_unix: 1000, hidden: false, seen_count: 0,
    last_seen_at_unix: null, removed_at_unix: null, picked_up_by: null,
  };
  const m = T.histModel(e);
  eq(m.key, "h4", "history rows are keyed by job");
  eq(m.visAction, "h-hide", "a visible entry offers hide");
  eq(T.histModel(Object.assign({}, e, { hidden: true })).visAction, "h-restore",
    "a hidden entry offers restore");
  eq(T.histModel(Object.assign({}, e, { status: "DELETED" })).stCls, "st dim",
    "DELETED is already a styled history status");
  const tbody = node("tbody");
  T.reconcileRows(tbody, [m], fake);
  const acts = tbody.children[0].children[5].children[0];
  const byAction = (a) => [...acts.children].find(b => b.dataset.action === a);
  eq(acts.children.length, 5, "details / requeue / hide / forget / delete-files");
  ok(byAction("h-delete-files"), "destructive action is data-driven");
  eq(acts.children[acts.children.length - 1].dataset.action, "h-delete-files",
    "…and it is last, where a misaimed click is least likely to land on it");
  eq(byAction("h-requeue").hidden, true, "requeue is hidden unless the entry is parked");
  T.reconcileRows(tbody, [T.histModel(Object.assign({}, e, { status: "DELETED", can_requeue: true }))], fake);
  eq(byAction("h-requeue").hidden, false, "a parked entry can go back to the queue");
}

// --- 10b. the job record travels with the job into history ---------------
// Field report 2026-07-29: "I want all of that info kept with the job as it
// moves to history, so it can be looked back upon if needed." The detail
// panel needs no fetch — a record you have to ask the queue for is one that
// stops existing when the queue forgets the job.
{
  const withRecord = {
    job: 182, name: "Some.Movie.2024.1080p-GRP", category: "monarr",
    status: "SUCCESS", size: 5_153_960_755, health: 1000, hidden: false,
    completed_at_unix: 1_800_003_600, final_dir: "/working/monarr/completed/Some.Movie",
    stages: [], can_requeue: true,
    record: {
      client: "monarr",
      url: "https://drunkenslug.com/getnzb/cc310b99.nzb",
      original_name: "cc310b9901757996b0bdfd880c666e3812e6531d",
      total_articles: 6449, success_articles: 6448, failed_articles: 1,
      par_size: 210_000_000, queued_at_unix: 1_800_000_000,
      files: [
        { name: "movie.part01.rar", size: 1000, segments_total: 10, segments_done: 10, segments_failed: 0, par2: false },
        { name: "movie.part02.rar", size: 1000, segments_total: 10, segments_done: 9, segments_failed: 1, par2: false },
        { name: "movie.par2", size: 500, segments_total: 2, segments_done: 2, segments_failed: 0, par2: true },
      ],
    },
  };
  const d = T.histDetailModel(withRecord);
  // Field report 2026-07-31: "why can't the job in history just be exactly
  // like it was in the queue?" It is — same kind, same panel, same widget.
  eq(d.kind, "detail", "history reuses the queue's own detail panel");
  eq(d.scope, "history", "…and says which tab is holding it open");
  ok(d.meta.includes("added by monarr"), `who asked (got ${d.meta})`);
  ok(d.meta.includes("drunkenslug"), "where it came from");
  ok(d.meta.includes("cc310b99017"), "the name the *arr added it under stays findable");
  ok(d.articles.includes("6448/6449"), `article counts survive the trip (got ${d.articles})`);
  ok(d.articles.includes("1 failed"), "…including what did not arrive");
  ok(d.meta.includes("took "), "queued-to-finished duration is recoverable");
  eq(d.metaHidden, true, "the old prose line is replaced when structured facts exist");
  eq(d.metaClient, "monarr", "requesting client has its own labeled field");
  eq(d.metaElapsed, "1h", "total time has its own labeled field");
  ok(d.metaPar.includes("MiB"), `par2 size has its own labeled field (got ${d.metaPar})`);
  ok(d.metaSource.includes("drunkenslug"), "source has its own labeled field");
  eq(d.metaOriginal, withRecord.record.original_name,
    "the original name is no longer buried in prose");
  eq(d.metaDestination, withRecord.final_dir,
    "the output path is no longer buried in prose");
  eq(d.health, "health 100.0%", "health reads the same as it did in the queue");
  eq(d.nzbHidden, true, "no NZB link: history has no endpoint behind it");
  eq(d.files.length, 3, "the file table survives");
  eq(d.files[0].kind, "file", "recorded files render as the queue's file rows");
  eq(d.files[1].state, "SHORT", "a file that lost segments is marked");
  eq(d.files[2].state, "PAR2", "par2 files are distinguishable");

  // The one deliberate difference between the two tabs.
  eq(d.filesOpen, false, "history folds the file list…");
  eq(T.detailModel(job(1)).filesOpen, true, "…and the queue does not");

  // An entry from before records existed says so, rather than rendering an
  // empty panel that looks like a bug.
  const old = T.histDetailModel({ job: 7, name: "x", status: "SUCCESS", completed_at_unix: 1, stages: [] });
  ok(old.meta.includes("predates"), `pre-upgrade entries say so (got ${old.meta})`);
  eq(old.files.length, 0);

  // The panel is a toggle, and opening one does not fetch anything.
  routes.clear(); seen.length = 0;
  T.store.history = [withRecord];
  T.toggleHist(182);
  eq(T.getOpenHist(), 182, "details open");
  eq(seen.length, 0, "no request — the record is already in hand");
  const body = sandbox.document.getElementById("history-body");
  eq(body.children.length, 2, "the detail row renders under its own row");
  const dc = body.children[1].__c;
  eq(dc.meta.hidden, true, "the concatenated history paragraph is not rendered");
  eq(dc.facts.hidden, false, "the labeled history fact grid is visible");
  eq(dc.metaClient.textContent, "monarr", "fact values reach the stable detail subtree");
  eq(dc.metaSource.title, withRecord.record.url,
    "an ellipsized source keeps its full value in the tooltip");
  eq(dc.close.hidden, true, "history has one close control, not two adjacent ones");
  T.toggleHist(182);
  eq(T.getOpenHist(), null, "and closes");
  T.store.history = null;
  routes.clear(); seen.length = 0;
}

// --- 11. laws #1 and #4 as grep-able properties of the source -------------
// The live-rows renderer runs from the store declaration to the settings
// editor. Inside that span: no `innerHTML` (law #1) and no generated
// `onclick=` strings (law #4 — one delegated listener, `data-action` only).
// The settings/setup forms below it are template-built by design: they are
// re-rendered on user intent, never on a tick, and hold no live rows.
{
  const src = scriptMatch[1];
  const from = src.indexOf("// ---- the store ---");
  const to = src.indexOf("// ---- settings: a real form over nzbd.toml");
  ok(from > 0 && to > from, "renderer span located in the page source");
  const renderer = src.slice(from, to);
  const assigns = (renderer.match(/\.innerHTML\s*=/g) || []).length;
  eq(assigns, 0, "the live-rows renderer never assigns innerHTML");
  const onclicks = (renderer.match(/onclick\s*=\s*["'`]/g) || []).length;
  eq(onclicks, 0, "no generated onclick= strings in rendered rows");
  for (const container of ["queue-body", "history-body", "logbox", "badges", "clients-strip", "storage-list"])
    ok(!new RegExp(`\\$\\("${container}"\\)\\.innerHTML`).test(src),
      `#${container} is never rebuilt with innerHTML`);
}

// --- 12. toasts: stack, cap, dismiss, action ------------------------------
{
  const stack = sandbox.document.getElementById("toasts");
  stack.children.length = 0;
  const t1 = T.toast({ text: "one" });
  eq(stack.children.length, 1, "a toast lands in the stack");
  eq(t1.el.children[0].textContent, "one", "…carrying its message");
  T.toast({ text: "two", kind: "error" });
  ok(stack.children[1].className.includes("bad"), "an error toast is styled as one");
  T.toast({ text: "three" });
  T.toast({ text: "four" });
  eq(stack.children.length, 3, "the stack is capped at three");
  eq(stack.children[0].children[0].textContent, "two", "the oldest is the one dropped");
  let ran = 0;
  const t5 = T.toast({ text: "undo me", action: { label: "Undo", fn: () => ran++ } });
  const undoBtn = t5.el.children[1];
  eq(undoBtn.textContent, "Undo", "the action button carries its label");
  undoBtn.onclick();
  eq(ran, 1, "clicking the action runs it");
  ok(!stack.children.includes(t5.el), "…and dismisses the toast");
  const t6 = T.toast({ text: "bye" });
  t6.el.children[1].onclick(); // the × button
  ok(!stack.children.includes(t6.el), "the dismiss button removes the toast");
  stack.children.length = 0;
}

// --- 13. pending overlay: apply hides the row, confirm drops the op -------
{
  const jobs = [job(1), job(2)];
  T.store.jobs = jobs;
  T.store.jobsLoaded = true;
  T.pending.clear();
  eq(T.queueModels().filter(m => m.kind === "job").length, 2, "both jobs visible");
  T.pending.apply({ key: T.pending.key(1, "delete"), kind: "delete", jobId: 1, label: "Deleting job 1" });
  const after = T.queueModels().filter(m => m.kind === "job");
  eq(after.length, 1, "the deleted row is gone before the server answers");
  eq(after[0].id, 2, "…and it is the right one");
  // The tick that still lists job 1 must NOT flash it back.
  T.pending.reconcile(jobs);
  eq(T.queueModels().filter(m => m.kind === "job").length, 1,
    "a tick mid-flight cannot flash the deleted row back");
  // Now the server agrees it is gone: the op retires.
  T.pending.reconcile([jobs[1]]);
  eq(T.pending.ops.size, 0, "server state matching the intent drops the op");
  T.store.jobs = [jobs[1]];
  eq(T.queueModels().filter(m => m.kind === "job").length, 1, "and the view agrees");
}

// --- 14. pending overlay: pause overrides the chip, then retires ----------
{
  const j = job(1, { status: "downloading" });
  T.store.jobs = [j];
  T.pending.clear();
  T.pending.apply({ key: T.pending.key(1, "pause"), kind: "pause", jobId: 1, label: "Pausing job 1" });
  const m = T.queueModels().find(x => x.kind === "job");
  eq(m.st, "PAUSED", "the chip flips the instant the button is clicked");
  eq(m.pauseLabel, "resume", "…and the button offers the opposite action");
  ok(m.rowCls.includes("pending"), "the row reads as in-flight");
  T.pending.reconcile([j]); // server still says downloading
  eq(T.pending.ops.size, 1, "an unconfirmed pause stays applied");
  T.pending.reconcile([job(1, { status: "paused" })]);
  eq(T.pending.ops.size, 0, "the server agreeing retires the op");
}

// --- 15. pending overlay: timeout reverts and says so --------------------
{
  const stack = sandbox.document.getElementById("toasts");
  stack.children.length = 0;
  T.store.jobs = [job(1)];
  T.pending.clear();
  const op = T.pending.apply({ key: T.pending.key(1, "delete"), kind: "delete", jobId: 1, label: "Deleting job 1" });
  op.at = Date.now() - (T.PENDING_TTL_MS + 1000);
  T.pending.sweep();
  eq(T.pending.ops.size, 0, "an op nothing ever confirmed is dropped");
  eq(T.queueModels().filter(m => m.kind === "job").length, 1, "the row springs back");
  eq(stack.children.length, 1, "…and the user is told");
  ok(stack.children[0].children[0].textContent.includes("didn't take"),
    "the message names the failure, not a generic error");
  // An op whose POST is still in flight is NOT swept out from under it…
  const op2 = T.pending.apply({ key: T.pending.key(1, "pause"), kind: "pause", jobId: 1 });
  op2.inflight = true;
  op2.at = Date.now() - (T.PENDING_TTL_MS + 1000);
  T.pending.sweep();
  eq(T.pending.ops.size, 1, "a POST still in flight is given its time");
  // …but it gets a longer leash, not an exemption. A daemon that accepts
  // the connection and then never answers must not leave a row invisible
  // until the page is reloaded.
  op2.at = Date.now() - (T.PENDING_HARD_MS + 1000);
  T.pending.sweep();
  eq(T.pending.ops.size, 0, "even an in-flight op is eventually reverted");
  T.pending.clear();
  stack.children.length = 0;
}

// --- 15b. logs are a live tail, not a poll ------------------------------
// Re-polling over a healthy stream would replace the ring every 5 s,
// discarding the lines the stream just delivered (and any skipped-line
// markers) and re-downloading 300 entries to do it.
{
  const logPolls = () => seen.filter(r => r.url.includes("/api/v1/logs")).length;
  routes.clear();
  seen.length = 0;
  routes.set("/api/v1/logs", { status: 200, body: { entries: [] } });
  T.setActiveTab("logs");

  // Nothing loaded yet: backfill, whatever the stream is doing.
  T.store.logs = null;
  T.connState("live");
  T.refreshTab();
  eq(logPolls(), 1, "an empty Logs tab backfills once");

  // Loaded and the stream is healthy: the tail carries it, no poll.
  seen.length = 0;
  T.store.logs = [{ id: 1, scope: "system", kind: "INFO", time_unix: 1, text: "live line" }];
  T.refreshTab();
  T.refreshTab();
  eq(logPolls(), 0, "a live stream is not second-guessed every 5 s");

  // Stream down: the poll is the only thing carrying the page, so it runs.
  T.connState("reconnecting");
  T.refreshTab();
  eq(logPolls(), 1, "…but a dropped stream falls back to polling");

  T.setActiveTab("queue");
  T.connState("live");
  routes.clear();
  seen.length = 0;
}

// --- 16. pending overlay: explicit revert on a failed POST ---------------
{
  const stack = sandbox.document.getElementById("toasts");
  stack.children.length = 0;
  T.store.jobs = [job(1)];
  T.pending.clear();
  T.pending.apply({ key: T.pending.key(1, "delete"), kind: "delete", jobId: 1, label: "Deleting job 1" });
  T.pending.revert(T.pending.key(1, "delete"), "Deleting job 1 failed — job not found");
  eq(T.pending.ops.size, 0, "revert drops the op");
  eq(T.queueModels().filter(m => m.kind === "job").length, 1, "the row is back immediately");
  eq(stack.children[0].children[0].textContent, "Deleting job 1 failed — job not found",
    "the toast carries the daemon's own error string");
  stack.children.length = 0;
}

// --- 17. move ops reorder locally, and retire on the server's order ------
{
  const jobs = [job(1), job(2), job(3)];
  T.store.jobs = jobs;
  T.pending.clear();
  T.pending.apply({ key: T.pending.key(3, "move"), kind: "move", jobId: 3, wantIdx: 0, label: "Moving job 3" });
  const ids2 = T.queueModels().filter(m => m.kind === "job").map(m => m.id);
  eq(ids2.join(","), "3,1,2", "the row is where the user put it, immediately");
  T.pending.reconcile(jobs);
  eq(T.pending.ops.size, 1, "the old order does not satisfy the move");
  T.pending.reconcile([jobs[2], jobs[0], jobs[1]]);
  eq(T.pending.ops.size, 0, "the server's new order retires the move");
  T.pending.clear();
}

// --- 18. satisfied() is the whole resolution rule, in one place ----------
{
  const byId = new Map([[1, job(1, { status: "paused" })]]);
  ok(T.satisfied({ kind: "pause", jobId: 1 }, byId), "paused satisfies a pause");
  ok(!T.satisfied({ kind: "resume", jobId: 1 }, byId), "paused does not satisfy a resume");
  ok(!T.satisfied({ kind: "delete", jobId: 1 }, byId), "a present job does not satisfy a delete");
  ok(T.satisfied({ kind: "delete", jobId: 9 }, byId), "an absent job does");
  ok(T.satisfied({ kind: "pause", jobId: 9 }, byId),
    "a job that left the queue stops being overridden");
}

// --- 19. connection states, including the one the old UI could not see ---
{
  const conn = sandbox.document.getElementById("conn");
  const off = sandbox.document.getElementById("offline");
  T.connState("live");
  eq(T.conn(), "live", "live is live");
  eq(off.className, "", "no banner while connected");
  T.connState("reconnecting");
  ok(conn.className.includes("warn"), "reconnecting reads as a warning");
  eq(off.className, "", "…but still no page-wide banner: polls are carrying us");
  // One failed poll is a race with a restart; two is a dead daemon.
  T.pollResult(false);
  ok(T.conn() !== "unreachable", "a single miss is not a verdict");
  T.pollResult(false);
  eq(T.conn(), "unreachable", "two misses in a row is");
  eq(off.className, "show", "…and that gets a banner, not a gray dot");
  ok(off.textContent.includes("Can't reach the daemon"), "the banner says what is wrong");
  T.pollResult(true);
  ok(T.conn() !== "unreachable", "an answered poll clears it");
  eq(off.className, "", "banner gone");
}

// --- 19b. hb is liveness, not freshness ----------------------------------
// The daemon heartbeats a stream whose ticks it deduplicated. While the
// wire was moving, deduplicated ticks are impossible — missing ticks mean
// the ENGINE stopped publishing (wedged on slow storage). The old page fed
// hb into the same clock as ticks, told the user "● live updates", and
// never engaged the poll fallback: frozen page, green dot. That is the
// "dash goes long times without any update" field report (2026-07-26).
{
  const fullStatus = (over) => Object.assign({
    version: "0.1.0", up_since_unix: 1, download_rate_bps: 10 * 1048576,
    remaining_bytes: 1e9, session_downloaded_bytes: 5e7,
    download_paused: false, disk_low: false, quota_reached: false,
    blocked_servers: [], health_abort: false, speed_limit_bps: null,
    jobs_queued: 1, jobs_downloading: 1, jobs_finished: 0, servers: [],
  }, over || {});
  routes.clear(); seen.length = 0;
  routes.set("/api/v1/status", { status: 200, body: fullStatus() });
  routes.set("/api/v1/jobs", { status: 200, body: { jobs: [] } });
  T.store.status = fullStatus();
  const now = Date.now();

  // Ticks stale, hb fresh, wire active -> the engine is wedged: say so.
  T.connState("live");
  T.__setClocks(now - 20000, now - 1000);
  T.pollPass();
  eq(T.conn(), "stalled", "hb without ticks while downloading reads as stalled, not live");
  ok(seen.some(r => r.url.includes("/api/v1/status")),
    "…and the poll fallback engages instead of trusting the heartbeat");

  // A real tick is the one thing that clears it.
  T.applyTick({ status: fullStatus(), jobs: [] });
  eq(T.conn(), "live", "fresh data clears stalled");

  // Same gaps with an idle wire: a quiet stream is healthy, not stalled.
  seen.length = 0;
  T.store.status = fullStatus({ download_rate_bps: 0, jobs_downloading: 0 });
  T.__setClocks(now - 20000, now - 1000);
  T.pollPass();
  eq(T.conn(), "live", "an idle queue's deduplicated ticks stay live on hb alone");
  ok(!seen.some(r => r.url.includes("/api/v1/status")),
    "…without burning a status poll on a healthy idle stream");

  // hb stale too: the stream itself is gone.
  T.__setClocks(now - 20000, now - 20000);
  T.pollPass();
  eq(T.conn(), "reconnecting", "no ticks and no hb is a dead stream");

  // And hb may repair connection-level states, but never a stall.
  T.store.status = fullStatus();
  T.__setClocks(now - 20000, now);
  T.connState("reconnecting");
  T.applyHb();
  eq(T.conn(), "stalled", "hb upgrades reconnecting only as far as honesty allows");
  T.connState("stalled");
  T.applyHb();
  eq(T.conn(), "stalled", "hb alone never clears a stall — only a tick does");

  T.__setClocks(Date.now(), Date.now());
  T.connState("live");
  routes.clear(); seen.length = 0;
}

// --- 19c. a fatally-closed EventSource is rebuilt, not mourned -----------
// EventSource retries by itself only after clean network failures. Any
// non-200 answer (a 503 mid-restart, a proxy error page) closes it for
// good — the spec's "fail the connection" — and the old page would sit on
// "polling — reconnecting…" forever with a stream that was never coming
// back. The page owns that retry now.
{
  T.startStream();
  const s1 = T.__stream();
  ok(s1.es && s1.es.url.includes("/api/v1/events"), "stream points at the daemon");
  eq(s1.retryPending, false, "no rebuild pending while it is healthy");

  s1.es.readyState = 2; // the fatal close: non-200 answer, no built-in retry
  s1.es.onerror();
  const s2 = T.__stream();
  eq(s2.retryPending, true, "a fatal close schedules our own rebuild");
  eq(T.conn(), "reconnecting", "…and the page says what is happening");
  ok(s2.retryMs > 1000, "the retry backs off instead of hammering a restarting daemon");

  // The 5 s pass is the belt to that suspender: a CLOSED stream with no
  // rebuild pending (however it got that way) is given one.
  T.startStream();
  T.__stream().es.readyState = 2;
  T.__setClocks(Date.now(), Date.now()); // fresh clocks: pollPass must act on readyState alone
  T.pollPass();
  eq(T.__stream().retryPending, true, "ensureStream rebuilds a closed stream found by the timer");

  T.startStream(); // leave a healthy stream behind for later tests
  T.connState("live");
}

// --- 19c². the version identifies the build, from the footer --------------
// Field request 2026-07-26: a version pinned at the top that never changes
// identifies nothing. It lives in the footer now and renderStatus feeds it
// the daemon's build identity (version+hash, compile stamp in the tooltip).
{
  ok(html.indexOf("<footer>") >= 0 && html.indexOf("<footer>") < html.indexOf('id="ver"'),
    "the version element lives in the footer, not the header");
  const ver = sandbox.document.getElementById("ver");
  ver.textContent = ""; ver.__t = undefined; ver.title = "";
  T.store.status = {
    version: "0.1.0+gdeadbee42", built: "2026-07-26 21:14 UTC",
    download_paused: false, download_rate_bps: 0, remaining_bytes: 0,
    session_downloaded_bytes: 0, jobs_downloading: 0, jobs_queued: 0,
    blocked_servers: [], servers: [], health_abort: false,
    speed_limit_bps: null, disk_low: false, quota_reached: false,
  };
  T.renderStatus();
  eq(ver.textContent, "v0.1.0+gdeadbee42", "footer shows version + git hash");
  ok(ver.title.includes("built 2026-07-26 21:14 UTC"),
    "…and the tooltip carries the compile stamp");
}

// --- 19d. the sparkline survives on polls, sampled ------------------------
// The old page fed the sparkline only from SSE ticks: on a dead stream the
// rate number kept polling while the shape froze. Poll data feeds it now,
// through a ~1 Hz sampler so a nudge-burst cannot smear the time axis.
{
  T.spark.n = 0; T.spark.buf.fill(0);
  T.__setRateClock(0); // rewind the sampler (earlier tests just pushed)
  T.pushRateSampled(1000);
  T.pushRateSampled(2000); // within the sampling window: dropped
  eq(T.rateSeries().length, 1, "burst pushes collapse to one sample");
  eq(T.rateSeries()[0], 1000, "…keeping the first, not smearing the axis");
  T.spark.n = 0; T.spark.buf.fill(0);
}

// --- 20. no blocking dialogs anywhere in the page ------------------------
// The acceptance line for M4: a `confirm(` or `alert(` call, anywhere, is a
// regression. Comments are allowed to talk about them; code is not.
{
  const calls = [];
  scriptMatch[1].split("\n").forEach((line, i) => {
    const code = line.replace(/^\s*\/\/.*$/, "");
    if (/(^|[^.\w])(confirm|alert)\s*\(/.test(code)) calls.push(`line ${i + 1}: ${line.trim()}`);
  });
  eq(calls.length, 0, `no confirm()/alert() calls remain (${calls.join(" | ")})`);
}

// --- 21. "delete files" arms in place instead of opening a dialog --------
{
  const e = {
    job: 4, name: "old job", category: "tv", final_dir: "/dest/x", status: "SUCCESS",
    size: 2048, completed_at_unix: 1000, hidden: false, seen_count: 0,
    last_seen_at_unix: null, removed_at_unix: null, picked_up_by: null,
  };
  T.disarmButton();
  eq(T.histModel(e).delLabel, "delete files", "unarmed: the plain label");
  eq(T.histModel(e).delCls, "del", "…and the plain class");
  T.armButton(T.histKey(4));
  ok(T.armIsSet(T.histKey(4)), "the button is armed");
  const armedModel = T.histModel(e);
  eq(armedModel.delLabel, "sure?", "armed: the button says what the next click does");
  eq(armedModel.delCls, "del armed", "…and looks like it");
  ok(armedModel.delTip.includes("cannot be undone"),
    "the tooltip is explicit that this one is the irreversible action");
  // Arming lives outside the DOM, so it survives a reconcile — a `sure?`
  // written straight onto the node would be wiped by the next tick.
  const tbody = node("tbody");
  T.reconcileRows(tbody, [T.histModel(e)], fake);
  // Located by data-action, not index: the action set grows over time and
  // a positional lookup silently starts asserting about a different button.
  const btn = [...tbody.children[0].children[5].children[0].children]
    .find(b => b.dataset.action === "h-delete-files");
  eq(btn.textContent, "sure?", "armed state renders");
  T.reconcileRows(tbody, [T.histModel(e)], fake);
  eq(btn.textContent, "sure?", "…and survives a re-render");
  // Arming a different button disarms the first: only one at a time.
  T.armButton(T.histKey(9));
  ok(!T.armIsSet(T.histKey(4)), "arming elsewhere disarms the previous button");
  T.disarmButton();
  T.reconcileRows(tbody, [T.histModel(e)], fake);
  eq(btn.textContent, "delete files", "disarming restores the plain label");
}

// --- 23. rate ring: three minutes, wrapping correctly --------------------
{
  T.spark.n = 0;
  T.spark.buf.fill(0);
  eq(T.rateSeries().length, 0, "empty until something ticks");
  for (let i = 1; i <= 5; i++) T.pushRate(i * 100);
  eq(T.rateSeries().join(","), "100,200,300,400,500", "oldest first while filling");
  // Overflow: the ring must keep the NEWEST SPARK_N, in order.
  for (let i = 6; i <= 250; i++) T.pushRate(i * 100);
  const s = T.rateSeries();
  eq(s.length, T.SPARK_N, "capped at the window size");
  eq(s[0], (250 - T.SPARK_N + 1) * 100, "…starting at the oldest surviving sample");
  eq(s[s.length - 1], 25000, "…ending at the newest");
  ok(s.every((v, i) => i === 0 || v > s[i - 1]), "and in order across the wrap");
  T.spark.n = 0;
  T.spark.buf.fill(0);
}

// --- 24. title ticker ----------------------------------------------------
{
  eq(T.titleFor(null), "Runner", "no status yet: a plain title");
  eq(T.titleFor({ download_paused: true, download_rate_bps: 0 }), "⏸ paused — Runner",
    "paused says so");
  const busy = T.titleFor({ download_paused: false, download_rate_bps: 1048576, remaining_bytes: 10485760 });
  ok(busy.startsWith("▼ 1.0 MiB/s"), `rate leads the title (got ${busy})`);
  ok(busy.endsWith("— Runner"), "…and the app name still ends it");
  ok(busy.includes("10s"), "…with the time left, which is the other half of the question");
  eq(T.titleFor({ download_paused: false, download_rate_bps: 0 }), "Runner",
    "idle resets — no stale number left in the tab");
}

// --- 25. per-server chips ------------------------------------------------
{
  const s = {
    blocked_servers: [1],
    servers: [
      { server: 0, name: "eweka", rate_bps: 2048, day_bytes: 100, total_bytes: 900 },
      { server: 1, name: "blocknews", rate_bps: 0, day_bytes: 0, total_bytes: 5 },
      { server: 2, name: "idle-fill", rate_bps: 0, day_bytes: 0, total_bytes: 0 },
    ],
  };
  const chips = T.serverChipModels(s);
  eq(chips.length, 3, "one chip per configured server, including the quiet one");
  eq(chips[0].name, "eweka", "named, not numbered");
  eq(chips[0].rate, " 2.0 KiB/s", "carrying its share of the wire rate");
  ok(chips[0].cls.includes("live"), "a delivering server reads as live");
  eq(chips[1].rate, " blocked", "a blocked server says so instead of showing 0");
  ok(chips[1].cls.includes("on-bad"), "…and reads as a problem");
  ok(!chips[2].cls.includes("live"), "a quiet server is not marked live");
  ok(chips[0].tip.includes("add up to it"),
    "the tooltip states the same-measurement invariant these numbers rely on");
}

// --- 26. the log ring: bounded, and honest about what it missed ----------
{
  const rec = (id, scope, text) => ({ id, scope, kind: "INFO", time_unix: 1, text });
  T.store.logs = [];
  T.appendLogs([rec(1, "system", "boot"), rec(2, "job", "added")], 0);
  eq(T.store.logs.length, 2, "entries append");
  T.appendLogs([rec(3, "job", "finished")], 7);
  eq(T.store.logs.length, 4, "a skipped-lines marker is inserted with the batch");
  eq(T.store.logs[2].skipped, 7, "…carrying the count the server reported");
  // Filtering happens client-side; markers are never filtered away, because
  // "you are missing lines" is true regardless of which scopes you picked.
  ok(!T.logMatches(rec(9, "file", "x"), ["system", "job"]), "per-file lines filter out");
  ok(T.logMatches(rec(9, "file", "x"), ["system", "job", "file"]), "…and back in when ticked");
  ok(T.logMatches({ id: "s1", skipped: 3 }, []), "a skipped marker survives every filter");
  // Bounded: an all-night download must not grow the ring without limit.
  T.store.logs = [];
  for (let i = 0; i < T.LOG_RING_MAX + 250; i++) T.appendLogs([rec(i, "job", "line " + i)], 0);
  eq(T.store.logs.length, T.LOG_RING_MAX, "the ring is capped");
  eq(T.store.logs[T.store.logs.length - 1].text, "line " + (T.LOG_RING_MAX + 249),
    "…dropping the oldest, keeping the tail");
  // And it renders, marker and all.
  T.store.logs = [rec(1, "system", "hello"), { id: "s2", skipped: 4 }, rec(2, "system", "world")];
  for (const s of ["system", "job", "file"])
    sandbox.document.getElementById("lg-" + s).checked = true;
  const box = sandbox.document.getElementById("logbox");
  box.children.length = 0;
  delete box.__rows;
  T.renderLogs();
  eq(box.children.length, 3, "every line rendered");
  ok(box.children[1].textContent.includes("4 lines skipped"),
    "the gap is stated in the log itself, where it happened");
  T.store.logs = [];
}

// --- 27. paging ----------------------------------------------------------
// The tick carries the whole queue, so paging is a view concern. The thing
// that must not break: "move up" is a QUEUE operation, so a row's arrows
// have to reflect its global position, not its position on the page.
{
  const many = (n) => Array.from({ length: n }, (_, i) => job(i + 1));
  const bar = sandbox.document.getElementById("queue-pager");
  const info = sandbox.document.getElementById("pager-info");
  const rows = () => T.queueModels().filter(m => m.kind === "job");

  T.store.jobsLoaded = true;
  T.pending.clear();
  T.setPageSize(20);
  T.setPage(0);
  eq(T.PAGE_SIZE_DEFAULT, 20, "20 per page by default");
  ok(T.PAGE_SIZES.includes(0), "…and 'all' is one of the options");

  // 45 jobs, 20 to a page.
  T.store.jobs = many(45);
  let p = T.pageSlice(T.store.jobs);
  eq(p.pages, 3, "45 jobs at 20/page is three pages");
  eq(p.shown.length, 20, "the first page holds 20");
  eq(p.start, 0, "…starting at the top");
  eq(rows().length, 20, "and that is what renders");
  eq(rows()[0].id, 1, "first job on page 1");
  eq(rows()[0].upDisabled, true, "the very first row cannot move up");
  eq(bar.hidden, false, "the pager appears once it could do something");
  ok(info.textContent.includes("1–20 of 45"), `range is stated (got ${info.textContent})`);
  ok(info.textContent.includes("page 1/3"), "…and so is the position");

  // Page 2: the arrows must know these rows are NOT at the queue's edges.
  T.setPage(1);
  eq(rows()[0].id, 21, "page 2 starts at job 21");
  eq(rows()[0].upDisabled, false,
    "the first row of page 2 can still move up — paging is a view, not the queue");
  eq(rows()[19].downDisabled, false, "…and its last row can still move down");

  // Last page: partial, and the true last row is pinned.
  T.setPage(2);
  p = T.pageSlice(T.store.jobs);
  eq(p.shown.length, 5, "the last page holds the remainder");
  eq(rows()[4].id, 45, "…ending at the last job");
  eq(rows()[4].downDisabled, true, "the very last row cannot move down");
  ok(info.textContent.includes("41–45 of 45"), "the range follows");

  // The queue shrinking under you (jobs finishing) must not strand the view.
  T.store.jobs = many(5);
  eq(rows().length, 5, "a shrunk queue renders");
  eq(T.pageState().page, 0, "…and the page clamps instead of showing nothing");

  // "All" really is all.
  T.store.jobs = many(137);
  T.setPageSize(0);
  p = T.pageSlice(T.store.jobs);
  eq(p.pages, 1, "'all' is a single page");
  eq(rows().length, 137, "…holding every job");
  eq(rows()[136].downDisabled, true, "the last row is still the last row");

  // Changing the page size keeps you near where you were looking, rather
  // than dumping you back at the top.
  T.setPageSize(20);
  T.setPage(4); // rows 81–100
  T.setPageSize(50);
  eq(T.pageState().page, 1, "80 rows in => page 2 of 50 (rows 51–100), not page 1");
  T.setPageSize(20);
  T.setPage(0);

  // The detail panel belongs to its row, so it lives on that row's page.
  T.store.jobs = many(45);
  T.store.jobFiles = null;
  T.store.jobLogs = null;
  T.setOpenJob(25);
  eq(T.queueModels().some(m => m.kind === "detail"), false,
    "the open job's panel is not on a page that does not contain it");
  T.setPage(1);
  eq(T.queueModels().some(m => m.kind === "detail"), true,
    "…and comes back with it");
  T.setOpenJob(null);
  T.setPage(0);

  // A queue that fits gets no chrome at all.
  T.store.jobs = many(7);
  T.queueModels();
  eq(bar.hidden, true, "no pager for a queue that fits on one page");
  // …unless the reader chose a non-default size, which they need a way back from.
  T.setPageSize(0);
  T.queueModels();
  eq(bar.hidden, false, "the control stays reachable once it has been used");
  T.setPageSize(20);
  T.store.jobs = [];
  T.pending.clear();
}

// --- 22. the delete -> Undo state machine, end to end -------------------
// This is the whole point of M4: one click deletes, the toast offers Undo
// for as long as the server says the job is parked, and Undo requeues it.
(async () => {
  const stack = sandbox.document.getElementById("toasts");
  const reset = () => { stack.children.length = 0; routes.clear(); seen.length = 0; T.pending.clear(); };

  // --- 21b. BitTorrent intake is reachable from the dashboard ------------
  // The backend and Settings toggle shipped before an add surface did. Pin
  // both wire forms here: typed JSON for magnet/URL and raw metainfo bytes.
  reset();
  const addPanel = sandbox.document.getElementById("torrent-add-form");
  addPanel.hidden = true;
  T.showTorrentAdd();
  eq(addPanel.hidden, false, "+ add torrent reveals the intake form");
  T.showTorrentAdd(false);
  eq(addPanel.hidden, true, "the torrent form closes without changing the queue");
  eq(sandbox.document.getElementById("btn-addtorrent").focused, true,
    "closing the form returns keyboard focus to its toggle");
  T.setTorrentBusy(true);
  eq(sandbox.document.getElementById("btn-torrent-uri").disabled, true,
    "a pending admission disables duplicate submits");
  eq(sandbox.document.getElementById("btn-torrent-uri").textContent, "Adding…",
    "the pending button says what it is waiting on");
  T.setTorrentBusy(false);
  const magnetRequest = T.torrentRequest(" magnet:?xt=urn:btih:abc ", {
    category: " tv ", priority: "50", paused: true,
  });
  eq(magnetRequest.source.type, "magnet",
    "magnet links use typed magnet admission");
  eq(magnetRequest.category, "tv", "typed admission trims the category");
  eq(magnetRequest.priority, 50, "typed admission carries priority");
  eq(magnetRequest.paused, true, "typed admission carries the paused intent");
  eq(T.torrentRequest("https://tracker.example/file.torrent").source.type, "torrent_url",
    ".torrent URLs use typed URL admission");
  eq(T.torrentRequest("   "), null, "an empty source never reaches the daemon");

  routes.set("/api/v1/jobs", { status: 201, body: { id: 41, info_hash: "abc" } });
  scheduledTimeouts.length = 0;
  await T.addTorrentUri("magnet:?xt=urn:btih:abc", { category: "tv", priority: 50, paused: true });
  let add = seen.find(r => r.url === "/api/v1/jobs" && r.method === "POST");
  ok(add, "a magnet submits to the native jobs endpoint");
  eq(add.headers["content-type"], "application/json", "a magnet is typed JSON");
  eq(JSON.parse(add.body).source.type, "magnet", "the JSON names the source kind");
  eq(JSON.parse(add.body).category, "tv", "the JSON carries add options");
  eq(scheduledTimeouts.includes(45000), false,
    "magnet admission waits for the server-owned 120 s deadline and cleanup result");

  scheduledTimeouts.length = 0;
  await T.addTorrentUri("https://tracker.example/file.torrent", {});
  ok(scheduledTimeouts.includes(45000),
    "server-bounded remote metainfo fetches retain a client response deadline");

  seen.length = 0;
  const bytes = new Uint8Array([100, 3, 102, 111, 111, 51, 98, 97, 114, 101]).buffer;
  const uploaded = await T.addTorrentFiles(
    [{ name: "example.torrent", arrayBuffer: async () => bytes }],
    { category: "movies & tv", priority: -50, paused: true });
  add = seen.find(r => r.url.startsWith("/api/v1/jobs?") && r.method === "POST");
  eq(uploaded.added, 1, "a .torrent file is reported as added");
  eq(uploaded.failed, 0, "a successful .torrent upload is not reported as failed");
  eq(add.headers["content-type"], "application/x-bittorrent", "a .torrent sends raw metainfo");
  eq(add.body, bytes, "the uploaded bytes are not rewritten in the browser");
  ok(add.url.includes("priority=-50") && add.url.includes("paused=true"),
    "raw metainfo carries priority and paused options in the query");
  ok(add.url.includes("category=movies%20%26%20tv"), "the raw category is URL encoded");

  routes.set("/api/v1/jobs", { status: 503, body: { error: "BitTorrent is disabled" } });
  sandbox.document.getElementById("torrent-uri").value = "magnet:?xt=urn:btih:def";
  await T.addTorrentUri("magnet:?xt=urn:btih:def", {});
  const torrentMsg = sandbox.document.getElementById("addtorrent-msg");
  ok(torrentMsg.textContent.includes("BitTorrent is disabled"),
    "torrent intake shows the daemon's actionable rejection");
  eq(torrentMsg.className, "bad", "a rejected torrent is visibly an error");
  eq(sandbox.document.getElementById("torrent-uri").value, "magnet:?xt=urn:btih:def",
    "a rejected magnet remains available for correction and retry");

  // (a) a parked delete offers Undo
  reset();
  T.store.jobs = [job(1, { name: "big movie" })];
  T.store.jobsLoaded = true;
  routes.set("/actions/delete", { status: 200, body: { ok: true, parked: true } });
  await T.deleteJob(1, "big movie");
  eq(seen.filter(r => r.url.includes("/jobs/1/actions/delete") && r.method === "POST").length, 1,
    "exactly one delete POST, fired without a dialog");
  eq(stack.children.length, 1, "the user is told the job is gone");
  const t = stack.children[0];
  ok(t.children[0].textContent.includes("big movie"), "…by name");
  eq(t.children[1].textContent, "Undo", "…with an Undo on offer");
  eq(T.UNDO_MS, 8000, "Undo stays available for 8 s");

  // (b) Undo requeues it
  routes.set("/actions/requeue", { status: 200, body: { id: 42 } });
  await t.children[1].onclick();
  eq(seen.filter(r => r.url.includes("/history/1/actions/requeue")).length, 1,
    "Undo goes through the requeue action");
  ok(stack.children.some(x => x.children[0].textContent.includes("back in the queue")),
    "…and says so when it worked");

  // (c) nothing parked -> no Undo is promised
  reset();
  T.store.jobs = [job(2, { name: "no undo" })];
  routes.set("/actions/delete", { status: 200, body: { ok: true, parked: false } });
  await T.deleteJob(2, "no undo");
  eq(stack.children.length, 1, "still reported");
  eq(stack.children[0].children.length, 2, "but with no action button — just the dismiss ×");
  ok(stack.children[0].children[0].textContent.includes("can't be undone"),
    "…and it says why");

  // (d) a failed delete reverts the row and quotes the daemon
  reset();
  T.store.jobs = [job(3, { name: "stubborn" })];
  routes.set("/actions/delete", { status: 503, body: { error: "engine is shutting down" } });
  await T.deleteJob(3, "stubborn");
  eq(T.pending.ops.size, 0, "the optimistic hide is rolled back");
  eq(T.queueModels().filter(m => m.kind === "job").length, 1, "the row is visible again");
  ok(stack.children[0].children[0].textContent.includes("engine is shutting down"),
    "the toast carries the daemon's own words, not 'something went wrong'");
  ok(stack.children[0].className.includes("bad"), "…and reads as an error");

  // (e) a failed Undo says so instead of pretending
  reset();
  routes.set("/actions/requeue", { status: 404, body: { error: "no parked NZB for this entry" } });
  await T.requeueJob(7, "vanished");
  ok(stack.children[0].children[0].textContent.includes("no parked NZB"),
    "a requeue that cannot work says why");

  // --- 23. the first-run wizard takes the page OVER -----------------------
  // Field report 2026-07-26: the wizard rendered on top of a live-looking
  // dashboard — stat tiles, queue controls and tabs all still on screen,
  // all showing dashes, on a daemon that had no config at all. Two halves,
  // both pinned here.
  //
  // (a) the script hides every panel except the ones the wizard needs.
  const wrapKids = [
    wrapChild("header", null, ""),
    wrapChild("div", "cfgwarn", ""),
    wrapChild("section", "setup", ""),
    wrapChild("div", "offline", ""),
    wrapChild("div", null, "stats"),
    wrapChild("div", null, "controls"),
    wrapChild("nav", null, "tabs"),
    wrapChild("section", "tab-queue", ""),
    wrapChild("footer", null, ""),
  ];
  qsa.set(".wrap > *", wrapKids);
  T.hideAppForSetup();
  const shown = (el) => el.hidden === false;
  ok(shown(wrapKids[0]), "the header stays — the wizard is still nzbd");
  ok(shown(wrapKids[1]), "the config-durability warning stays: it explains the wizard");
  ok(shown(wrapKids[2]), "the wizard itself is revealed");
  ok(shown(wrapKids[8]), "the footer stays (it carries the build identity)");
  for (const el of [wrapKids[3], wrapKids[4], wrapKids[5], wrapKids[6], wrapKids[7]])
    ok(el.hidden === true, `${el.id || el.className} is hidden behind the wizard`);

  // (b) and `hidden` actually wins in the stylesheet. This is the half that
  // broke: .stats is a grid and .controls/nav.tabs are flexes, so the UA's
  // `[hidden] { display: none }` lost on specificity and those three — and
  // exactly those three — stayed on screen. A global !important override is
  // the only fix that also protects every element added later.
  const styleBlock = (html.match(/<style>([\s\S]*?)<\/style>/) || [])[1] || "";
  ok(/(^|[^.\w\[])\[hidden\]\s*\{[^}]*display:\s*none\s*!important/m.test(styleBlock),
    "the stylesheet carries a global [hidden] { display: none !important }");
  // Every rule that sets `display` on a class/tag is a candidate to shadow
  // it, so the override is not optional housekeeping — prove some exist.
  const displayRules = [...styleBlock.matchAll(/([^{}]+)\{[^}]*display:\s*(grid|flex|block|inline[\w-]*)/g)]
    .map((m) => m[1].trim()).filter((sel) => !sel.includes("[hidden]"));
  ok(displayRules.length > 0, "…and there are display: rules it has to outrank");

  // --- 24. the config-durability banner ----------------------------------
  // The root cause behind the wizard: a config directory that is writable
  // but ephemeral. Saves work, the operator believes the install is
  // configured, and the next container recreate serves this wizard again.
  const warn = sandbox.document.getElementById("cfgwarn");

  // (a) healthy mounts say nothing at all
  eq(T.showConfigDurability({ config_path: "/etc/nzbd/nzbd.toml", durable: true }), false,
    "a durable config directory raises no banner");
  ok(warn.hidden === true, "…and the banner stays out of the page");

  // (b) unknown (non-Linux, no /proc) is not an accusation
  eq(T.showConfigDurability({ config_path: "/etc/nzbd/nzbd.toml", durable: null }), false,
    "an unknown durability is not reported as a fault");

  // (c) ephemeral warns BEFORE anything is lost, and names the fix
  ok(T.showConfigDurability({
    config_path: "/etc/nzbd/nzbd.toml", durable: false,
    mirror_path: "/data/queue/nzbd.toml.saved",
  }), "an ephemeral config directory raises the banner");
  ok(warn.hidden === false, "…which is visible");
  ok(warn.innerHTML.includes("/etc/nzbd/nzbd.toml"), "…names the path at risk");
  ok(warn.innerHTML.includes("./config:/etc/nzbd"), "…shows the volume line that fixes it");
  ok(warn.innerHTML.includes("/data/queue/nzbd.toml.saved"), "…and where the durable copy lives");

  // (c2) in the WIZARD there is no config yet, so there is no state dir to
  // name — the banner must promise the behaviour instead of printing a
  // mirror path derived from a main_dir the operator has not chosen. Live
  // check 2026-07-26 against nuc3: setup mode reports writable=true,
  // container=true and no config at all, which is exactly this state.
  ok(T.showConfigDurability({ config_path: "/etc/nzbd/nzbd.toml", durable: false }),
    "the wizard still warns about an ephemeral config directory");
  ok(!/nzbd\.toml\.saved/.test(warn.innerHTML),
    "…without inventing a path for a config that does not exist yet");
  ok(warn.innerHTML.includes("as soon as you save"),
    "…promising the durable copy from the moment there is something to copy");

  // (d) after a recovery it reports what happened, not just what might
  ok(T.showConfigDurability({
    config_path: "/etc/nzbd/nzbd.toml", durable: false,
    recovered_from: "/data/queue/nzbd.toml.saved",
  }), "a recovered boot raises the banner");
  ok(warn.innerHTML.includes("recovered"), "…says the config was recovered");
  ok(warn.innerHTML.includes("/data/queue/nzbd.toml.saved"), "…from where");
  ok(warn.innerHTML.includes("./config:/etc/nzbd"), "…and still shows the mount to add");

  // --- 25. the masked TOML must not masquerade as a backup ---------------
  // Field report 2026-07-26: "it imported the config file but lost my
  // password". The settings editor shows every secret as ***unchanged***,
  // and its Download button handed that text back named `nzbd.toml` — the
  // exact filename you drop into /etc/nzbd. Restored later, the daemon
  // authenticated with the placeholder and the provider looked broken.
  const plain = "[api]\nbind = \"0.0.0.0:6789\"\n";
  const dPlain = T.advDownloadFile(plain);
  eq(dPlain.name, "nzbd.toml", "a config with no secrets downloads as the real thing");
  eq(dPlain.body, plain, "…unaltered");

  const maskedToml = "[[server]]\nhost = \"news\"\npassword = \"***unchanged***\"\n";
  const dMasked = T.advDownloadFile(maskedToml);
  eq(dMasked.name, "nzbd-masked.toml", "a masked config does NOT download as nzbd.toml");
  ok(dMasked.body.startsWith("# NOT A USABLE CONFIG"), "…and says so on line one");
  ok(dMasked.body.includes("refuses to start"), "…naming what happens if you use it anyway");
  ok(dMasked.body.includes(maskedToml), "…while still containing the config itself");

  // --- 26. a client chip is a glance, not a User-Agent dump --------------
  // Field report 2026-07-27: the API-clients strip rendered each agent
  // verbatim, so an open browser tab pushed a ~130-character
  // "Mozilla/5.0 (Macintosh; …) AppleWebKit/… Chrome/… Safari/…" chip
  // across the full width of the page and buried the two chips that
  // actually answer the question (is the *arr connected?).
  const CHROME_MAC = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36";
  eq(T.clientLabel(CHROME_MAC), "Chrome 150 (macOS)",
    "a browser agent collapses to browser + major version + platform");
  ok(T.clientLabel(CHROME_MAC).length < 20, "…short enough to sit beside its siblings");
  eq(T.clientLabel("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"),
    "Firefox 128 (Windows)", "Firefox is not mistaken for anything else");
  eq(T.clientLabel("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"),
    "Edge 149 (Windows)", "Edge claims Chrome in its own agent — specificity wins");
  eq(T.clientLabel("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
    + "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"),
    "Safari 17 (iOS)", "…and Chrome's Safari token does not make Safari read as Chrome");
  eq(T.clientLabel("Mozilla/5.0 (compatible; SomethingNew/1.0)"), "browser",
    "an unrecognised Mozilla agent still gets a short name, not the raw string");

  // A product token IS the short name — never mangle the one client whose
  // identity the operator is actually looking for.
  eq(T.clientLabel("Monarr/0.7.0"), "Monarr/0.7.0", "an *arr keeps its own name and version");
  eq(T.clientLabel("Sonarr/4.0.0.748"), "Sonarr/4.0.0.748", "…however long its version is");
  eq(T.clientLabel(""), "unknown", "a client that sent no agent is named, not blank");
  eq(T.clientLabel(null), "unknown", "…including a missing one");
  const longToken = "X".repeat(100);
  eq(T.clientLabel(longToken).length, T.UA_MAX, "an absurd non-browser token is still capped");
  ok(T.clientLabel(longToken).endsWith("…"), "…and says it was cut");

  {
    const now = 1_700_000_000;
    const client = (over) => Object.assign({
      user_agent: CHROME_MAC, calls: 12, first_seen_unix: now - 600,
      last_seen_unix: now - 1, last_method: "GET /api/v1/status",
      api: "native", event_subscriptions: 1,
    }, over || {});

    const [chip] = T.clientChipModels([client()], now);
    ok(chip.text.startsWith("Chrome 150 (macOS) · subscribed · "),
      "the chip leads with the short label, then how it is attached");
    ok(!chip.text.includes("AppleWebKit"), "the raw agent is gone from the chip text");
    ok(chip.tip.startsWith(CHROME_MAC), "…and is the first thing in the tooltip");
    ok(chip.tip.includes("12 calls since"), "the tooltip keeps everything it used to say");
    ok(chip.tip.includes("receiving events live"), "…including the subscription");
    eq(chip.key, "c" + CHROME_MAC, "identity is the raw agent, not the collapsed label");
    ok(chip.cls.includes("live"), "a subscriber is live");

    // Two browsers that collapse to the same label must remain two rows.
    const CHROME_WIN = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      + "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36";
    const both = T.clientChipModels(
      [client(), client({ user_agent: CHROME_WIN })], now);
    ok(both[0].key !== both[1].key, "same-browser clients keep distinct keys");
    const strip = node("div");
    T.reconcileRows(strip, both, fake);
    eq(strip.children.length, 2, "…and therefore distinct chips");

    const quiet = T.clientChipModels(
      [client({ event_subscriptions: 0, last_seen_unix: now - 9999 })], now)[0];
    ok(quiet.text.includes("· quiet ·"), "a long-silent poller reads as quiet");
    ok(!quiet.cls.includes("live"), "…and is not styled live");

    const [none] = T.clientChipModels([], now);
    ok(none.text.length < 30, "the empty-state chip is a chip, not a paragraph");
    ok(none.tip.includes("/api/v1/events"), "…with the explanation moved to its tooltip");
  }

  // Wire fixtures are shared with the native renderer and engine serialization.
  {
    reset();
    const fixtures = JSON.parse(fs.readFileSync(require("path").join(__dirname,
      "../../nzbd-types/fixtures/mobile-queue-parity.json"), "utf8"));
    for (const f of fixtures) {
      eq(T.sectionOf(f.job), f.section, f.name + " section parity");
      if (f.job.kind !== "torrent") continue;
      eq(T.torrentStatus(f.job), f.statusLabel, f.name + " label parity");
      const action = T.torrentPrimaryAction(f.job);
      eq(action ? action.action : null, f.action, f.name + " action parity");
      eq(action ? action.label : null, f.actionLabel, f.name + " action label parity");
      const row = T.rowModel(f.job);
      eq(row.pauseHidden, !action, f.name + " rendered action visibility");
      if (f.phase === "failed") eq(row.moveHidden, true, "failed cannot move");
      if (f.resume404) {
        T.store.jobs = [f.job];
        routes.set("/actions/resume", { status: 404, body: { error: "job not found" } });
        routes.set("/api/v1/jobs", { status: 200, body: { jobs: [f.job] } });
        await T.jobAction(f.job.id, "resume");
        eq(T.store.jobs.length, 1, "refused seed resume retains row");
        ok(T.store.jobs[0].ready, "refused seed retains authoritative readiness");
        routes.clear();
      }
    }
  }

  // Torrent lifecycle, section paging, and the stable seeding editor.
  {
    reset();
    T.collapsedSections.clear();
    const seed = job(501, { kind: "torrent", status: "queued", ready: true,
      torrent_phase: "seeding", torrent_control_intent: "running", upload_rate_bps: 840000,
      uploaded_bytes: 640, ratio: .64, seeding_seconds: 3600, useful_peers: 3,
      ready_at_unix: 1000, seed_policy: { stop_on_complete: false, ratio_limit: 2, time_limit_secs: 172800 } });
    eq(T.sectionOf(seed), "seeding", "ready torrents never fall back to generic queued status");
    eq(T.torrentStatus({ ...seed, upload_rate_bps: 0 }), "seeding · idle", "zero upload remains seeding");
    eq(T.sectionOf({ ...seed, torrent_phase: "missing_files" }), "waiting", "missing files override readiness");
    eq(T.sectionOf({ ...seed, status: "failed" }), "waiting", "failure cannot be hidden by a ready flag");
    eq(T.sectionOf({ ...seed, torrent_phase: "checking" }), "checking", "piece verification has its own section");
    eq(T.sectionOf({ ...seed, ready: false, torrent_phase: "fetching_metadata" }), "torrent_metadata", "magnet metadata is not fetching an NZB");
    const held = { ...seed, ready: false, status: "paused", torrent_phase: "paused_download", torrent_error: "storage full", seed_stop_reason: "storage_full" };
    eq(T.torrentStatus(held), "waiting for disk space", "storage holds are not queued or manually paused");
    ok(T.rowModel(held).dRest.includes("storage full"), "the row explains the storage hold");
    const stopped = { ...seed, id: 502, status: "paused", torrent_control_intent: "paused", seed_stop_reason: "manual" };
    eq(T.sectionOf(stopped), "completed", "accepted stop intent moves a ready torrent before backend acknowledgement");
    const row = T.rowModel(seed);
    eq(row.barHidden, true, "seeds have no misleading download bar");
    eq(row.moveHidden, true, "seeds have no download queue arrows");
    eq(row.pauseLabel, "stop seeding", "the action states exactly what it stops");
    ok(row.dRest.includes("uploaded") && row.dRest.includes("3 peers"), "seeding rows expose upload and peer metrics");
    eq(T.rowModel(stopped).pauseLabel, "start seeding", "manual stops can be reversed");
    eq(T.rowModel({ ...stopped, ratio: 2 }).pauseAction, "seed-options", "reached limits need a policy change before restart");

    T.store.jobsLoaded = true;
    T.store.jobs = [job(1), ...Array.from({ length: 100 }, (_, i) => ({ ...seed, id: i + 2 })), stopped,
      ...Array.from({ length: 30 }, (_, i) => job(600 + i, { status: "queued" }))];
    T.setPageSize(20); T.setPage(0);
    let qm = T.queueModels();
    eq(qm.filter(m => m.kind === "job").length, 20, "expanded sections share the row budget");
    eq(qm.find(m => m.key === "sec:seeding").count, "100", "section counts include off-page torrents");
    T.toggleSection("seeding");
    qm = T.queueModels();
    ok(!qm.some(m => m.kind === "job" && m.rowCls.includes("job-seeding")), "collapsed seeds use no row slots");
    ok(qm.some(m => m.kind === "job" && m.rowCls.includes("job-waiting")), "waiting jobs surface past a large hidden seed collection");
    ok(qm.find(m => m.key === "sec:seeding").summary.includes("uploaded"), "collapsed sections retain upload instrumentation");
    const body = node("tbody");
    T.reconcileRows(body, qm, fake);
    const section = body.children.find(n => n.__c && n.__c.toggle && n.__c.toggle.dataset.section === "seeding");
    eq(section.__c.toggle.attrs["aria-expanded"], "false", "collapsed state is accessible");
    T.toggleSection("completed"); T.toggleSection("waiting");
    eq(T.queueModels().filter(m => m.kind === "job").length, 1, "all large sections can fold independently");
    T.toggleSection("seeding");
    eq(T.queueModels().filter(m => m.kind === "job").length, 20, "reopening seeds restores pagination");
    T.collapsedSections.clear();

    const detail = T.detailModel(seed);
    eq(detail.torPeers, "3", "inspector exposes peer count");
    eq(detail.torRatio, "0.64", "inspector exposes share ratio");
    eq(detail.torTime, "1h", "inspector exposes cumulative seeding time");
    ok(detail.torPolicy.includes("first limit reached"), "the stopping rule is explicit");
    const panel = node("tbody");
    T.seedDrafts.set(seed.id, { mode: "limits", ratio: "3.5", hours: "" });
    T.reconcileRows(panel, [T.detailModel(seed)], fake);
    const input = panel.children[0].__c.seedRatio;
    T.reconcileRows(panel, [T.detailModel({ ...seed, uploaded_bytes: 900, ratio: .9 })], fake);
    ok(panel.children[0].__c.seedRatio === input, "live upload ticks preserve the editor node");
    eq(input.value, "3.5", "live ticks preserve an unsaved policy edit");
    eq(T.seedBody(T.seedDraft(seed)).ratio_limit, 3.5, "the edited ratio reaches the wire");
    eq(T.seedBody({ mode: "stop" }).stop_on_complete, true, "stop after download is explicit, not a zero ratio");
    let invalid = false;
    try { T.seedBody({ mode: "limits", ratio: "0", hours: "" }); } catch { invalid = true; }
    ok(invalid, "zero cannot silently mean stop after download");
    const query = T.torrentQuery({ stop_seeding_on_complete: true, seed_ratio_limit: 0, seed_time_limit_secs: 0 });
    ok(query.includes("stop_seeding_on_complete=true"), "raw torrent uploads carry completion policy");
    eq(T.torrentRequest("magnet:?xt=urn:btih:x", { stop_seeding_on_complete: true }).stop_seeding_on_complete,
      true, "typed admissions carry completion policy");
    T.seedDrafts.clear();
    T.store.jobs = [];
    T.setPage(0);
    reset();
  }

  // --- sections: what it is doing, above what is waiting ------------------
  {
    const post = (stage, over) =>
      job(0, Object.assign({ status: { post: { stage } } }, over || {}));

    eq(T.sectionOf(job(1, { status: "downloading" })), "downloading", "");
    eq(T.sectionOf(job(1, { status: "queued" })), "waiting", "");
    eq(T.sectionOf(job(1, { status: "paused" })), "waiting",
      "a paused job is still waiting its turn, not doing something");
    eq(T.sectionOf(post("par_repair")), "repairing", "");
    eq(T.sectionOf(post("unpack")), "extracting", "");
    eq(T.sectionOf(post("par_rename")), "renaming", "");
    eq(T.sectionOf(post("rar_rename")), "renaming",
      "both rename stages read as one thing to a person");
    // Delayed PAR fetching used to overwrite the wire status with
    // `completed` while leaving the verify span open. The timeline is the
    // durable activity fact, so an upgraded/restored row must still live in
    // post-processing and say what it is doing.
    const delayedParRace = job(402, {
      status: "completed",
      stages: [
        { stage: "par_rename", started_at_unix: 1000, ms: 2000 },
        { stage: "par_verify", started_at_unix: 1002 },
      ],
    });
    eq(T.currentPostStage(delayedParRace), "par_verify",
      "an open timeline span recovers the active post stage");
    eq(T.sectionOf(delayedParRace), "verifying",
      "an active verify never falls into Waiting");
    const delayedParModel = T.rowModel(delayedParRace,
      { section: T.sectionOf(delayedParRace), nowMs: 1_542_000, movable: false });
    eq(delayedParModel.st, "CHECKING INTEGRITY",
      "the recovered row names its actual work");
    eq(delayedParModel.barHidden, true,
      "the recovered post-processing row does not show download progress");
    eq(T.detailModel(delayedParRace, 1_542_000).st, "CHECKING INTEGRITY",
      "the row and its detail panel agree on the active stage");
    const fetchingDelayedPar = job(403, {
      status: "downloading",
      stages: [{ stage: "par_verify", started_at_unix: 1002 }],
    });
    eq(T.sectionOf(fetchingDelayedPar), "verifying",
      "the download interlude remains in the post-processing section");
    eq(T.detailModel(fetchingDelayedPar, 1_542_000).recoverHidden, true,
      "recovery controls stay hidden while the backend rejects restarts");
    // An unknown stage from a newer daemon must land in a visible section
    // rather than vanish between the buckets.
    eq(T.sectionOf(post("teleporting")), "post_queued",
      "an unrecognised stage is still shown somewhere");

    // Every section a status can produce has a heading defined for it.
    const keys = new Set(T.SECTIONS.map(x => x.key));
    for (const st of ["queued", "downloading", "paused", "fetching", "post_queued"])
      ok(keys.has(T.sectionOf(job(1, { status: st }))), "section exists for " + st);
    for (const st of ["par_rename", "par_verify", "par_repair", "rar_rename",
                      "unpack", "cleanup", "move", "post_unpack_rename", "script"])
      ok(keys.has(T.sectionOf(post(st))), "section exists for stage " + st);

    // Only the download queue keeps its move controls: off it, the arrows
    // would be attached to nothing.
    ok(T.ORDERED.has("waiting"), "waiting jobs can be reordered");
    ok(T.ORDERED.has("downloading"), "so can downloading ones — same queue");
    ok(!T.ORDERED.has("repairing"), "a repairing job has no queue position");
    const fixed = T.rowModel(post("par_repair"), { idx: 0, count: 3, movable: false });
    eq(fixed.moveHidden, true, "…so its move buttons are hidden");
    const movable = T.rowModel(job(1), { idx: 1, count: 3, movable: true });
    eq(movable.moveHidden, false, "a queued job keeps them");
    eq(T.rowModel(job(1), { idx: 0, count: 1 }).moveHidden, false,
      "a caller that says nothing about movability keeps the old controls");

    // Waiting stays visually neutral; active work carries a stable color
    // hook specific to what it is doing.
    const waiting = T.rowModel(job(1, { status: "queued" }), { section: "waiting" });
    const downloading = T.rowModel(job(2), { section: "downloading" });
    const repairing = T.rowModel(post("par_repair"), { section: "repairing" });
    const extracting = T.rowModel(post("unpack"), { section: "extracting" });
    ok(waiting.rowCls.includes("waiting-job") && waiting.rowCls.includes("job-waiting"),
      "waiting rows carry the neutral waiting treatment");
    ok(downloading.rowCls.includes("active-job") && downloading.rowCls.includes("job-downloading"),
      "a download stands out as active and as a download");
    ok(repairing.rowCls.includes("job-repairing"), "repairing has its own color hook");
    ok(extracting.rowCls.includes("job-extracting"), "extracting has a different color hook");
    ok(repairing.rowCls !== extracting.rowCls, "active job types remain distinguishable");
  }

  // --- the stage timer -----------------------------------------------------
  {
    const running = job(1, {
      status: { post: { stage: "par_repair" } },
      stages: [
        { stage: "par_verify", started_at_unix: 1000, ms: 12000 },
        { stage: "par_repair", started_at_unix: 1012 },
      ],
    });
    eq(T.stageElapsedMs(running, 1_312_000), 300_000, "elapsed comes off the open span");
    eq(T.stageElapsedMs(job(1, { stages: [] })), null, "no timeline, no timer");
    eq(T.stageElapsedMs(job(1, {
      stages: [{ stage: "move", started_at_unix: 1000, ms: 5 }],
    }), 9_000_000), null, "a closed span is not still running");
    // A clock that disagrees with the server must not print a negative age.
    eq(T.stageElapsedMs(running, 1_000_000), 0, "a skewed clock clamps at zero");

    const m = T.rowModel(running, { idx: 0, count: 1, movable: false, nowMs: 1_312_000 });
    ok(m.detail.includes("5m in repairing"),
      "the row says how long it has been repairing, got: " + m.detail);

    eq(T.fmtDur(0), "<1s", "a stage that just started is not '0s'");
    eq(T.fmtDur(4200), "4s", "");
    eq(T.fmtDur(65000), "1m 5s", "");
    eq(T.fmtDur(120000), "2m", "no dangling '0s'");
    eq(T.fmtDur(3600000), "1h", "");
    eq(T.fmtDur(5460000), "1h 31m", "");
    eq(T.fmtDur(null), "", "nothing to say about a stage with no duration");

    // The bytes are all in, so the bar would sit at a full 100% under a
    // heading that says REPAIRING — reading as "done" for a job with
    // nineteen minutes left. A stage with no byte progress draws no bar.
    eq(m.barHidden, true, "a post-processing row has no progress bar");
    eq(m.pct, "", "…and no percentage either");
    const dl = T.rowModel(job(1, { downloaded_bytes: 500 }), { idx: 0, count: 1 });
    eq(dl.barHidden, false, "a downloading row keeps its bar");
    eq(dl.pct, "50%", "…and its percentage");

    // A job waiting for a post slot has no stage running yet, so there is
    // no timer — and the daemon's own token must not stand in for one.
    const pq = T.rowModel(job(1, { status: "post_queued", downloaded_bytes: 1000 }),
      { idx: 0, count: 1, movable: false });
    ok(!pq.detail.includes("post_queued"),
      "the wire token does not reach the screen, got: " + pq.detail);
    eq(pq.st, "WAITING TO POST-PROCESS", "the pill says it in words");
    eq(pq.barHidden, true, "its download is finished, so its bar says nothing");
    eq(T.statusLabel("post_queued"), "waiting to post-process", "");
    eq(T.statusLabel("completed", null, false), "download complete",
      "completed names the download phase, not payload readiness");
    eq(T.statusLabel("completed", null, true), "ready",
      "durable post-processing completion is labeled as readiness");
    eq(T.rowModel(job(8, { status: "completed", ready: false }), {}).st,
      "DOWNLOAD COMPLETE", "the queue row names which phase completed");
    eq(T.rowModel(job(9, { status: "completed", ready: true }), {}).st,
      "READY", "a ready queue row does not understate final completion");
    eq(T.statusName("post_queued"), "post_queued",
      "…while the wire name is unchanged, because pending-ops compare on it");
    eq(T.statusLabel({ post: { stage: "par_repair" } }), "repairing", "");
  }

  // --- the job-detail pipeline --------------------------------------------
  {
    const j = job(1, {
      status: { post: { stage: "unpack" } },
      stages: [
        { stage: "par_verify", started_at_unix: 1000, ms: 12000 },
        { stage: "par_repair", started_at_unix: 1012, ms: 2400000 },
        { stage: "unpack", started_at_unix: 3412 },
      ],
    });
    const pipe = T.pipelineModels(j, 3_442_000);
    eq(pipe.length, 3, "one entry per stage that ran");
    eq(pipe[0].label, "checking integrity", "stages read in words, not enum names");
    eq(pipe[1].dur, "40m", "a closed stage shows what it cost");
    ok(pipe[2].cls.includes("running"), "the current stage is marked running");
    eq(pipe[2].dur, "30s", "…and its figure is live, not banked");
    ok(pipe[1].cls.includes("slow"), "the longest closed stage is called out");
    ok(!pipe[0].cls.includes("slow"), "…and only that one");
    const dm = T.detailModel(j, 3_442_000);
    eq(dm.recoverHidden, false, "a live post-processing job exposes recovery controls");
    const tbody = node("tbody");
    T.reconcileRows(tbody, [dm], fake);
    const recovery = tbody.children[0].__c.recovery;
    eq(recovery.hidden, false, "the recovery row is visible while post-processing");
    eq(recovery.children.length, 7, "label plus six safe restart boundaries");
    eq(recovery.children[3].dataset.action, "post-restart-unpack",
      "the extraction button targets the unpack recovery action");
    eq(recovery.children[3].disabled, false, "the current extraction phase can be restarted");
    eq(recovery.children[4].disabled, true,
      "cleanup cannot skip past an extraction that has not completed");
    // Distinct keys, so two visits to the same stage are two rows.
    const twice = T.pipelineModels(job(1, { stages: [
      { stage: "par_repair", started_at_unix: 1, ms: 10 },
      { stage: "unpack", started_at_unix: 2, ms: 10 },
      { stage: "par_repair", started_at_unix: 3, ms: 10 },
    ] }), 9000);
    eq(new Set(twice.map(x => x.key)).size, 3,
      "a stage entered twice gets two rows, not one that overwrites itself");
    // Only what ran.
    ok(!pipe.some(x => x.label === "moving"), "stages that never ran are not listed");
    eq(T.pipelineModels(job(1, { stages: [] }), 0).length, 0,
      "a job that never post-processed shows no pipeline");
    // A single closed stage has nothing to be slower than.
    const one = T.pipelineModels(job(1, { stages: [
      { stage: "unpack", started_at_unix: 1, ms: 500 },
    ] }), 9000);
    ok(!one[0].cls.includes("slow"), "one stage is not 'the slow one'");
  }

  // --- history says where the post-processing time went -------------------
  {
    const s = (stage, ms) => ({ stage, started_at_unix: 1000, ms });
    const a = T.ppSummary([s("par_verify", 12000), s("par_repair", 2400000), s("unpack", 63000)]);
    eq(a.note, "40m repairing", "the row names the stage that took the time");
    ok(a.tip.includes("post-processing took 41m"), "the tooltip has the total: " + a.tip);
    ok(a.tip.includes("checking integrity 12s"), "…and every stage: " + a.tip);

    eq(T.ppSummary([]).note, "", "no timeline, no note");
    eq(T.ppSummary(undefined).note, "", "an entry from an older nzbd is silent");
    eq(T.ppSummary([s("move", 40)]).note, "",
      "a job whose post-processing was instant explains nothing by saying so");
    // A still-open span cannot be part of a finished job's accounting.
    eq(T.ppSummary([s("unpack", 9000), { stage: "move", started_at_unix: 1, ms: null }]).note,
      "9s extracting", "an unclosed span is skipped rather than counted as zero");

    const m = T.histModel({
      job: 3, name: "x", status: "SUCCESS", size: 10, completed_at_unix: 1,
      stages: [s("par_repair", 300000), s("unpack", 5000)],
    });
    eq(m.ppNote, "5m repairing", "the history row carries it");
    const bare = T.histModel({
      job: 4, name: "y", status: "SUCCESS", size: 10, completed_at_unix: 1,
    });
    eq(bare.ppNote, "", "a pre-upgrade history row renders without one");
  }

  // --- history paging: the request IS the page ---------------------------
  // Server-side, unlike the queue's: `total` comes off the wire because the
  // page in hand cannot say how long the list is.
  {
    const bar = sandbox.document.getElementById("history-pager");
    const info = sandbox.document.getElementById("history-pager-info");
    const hist = (n, base) => Array.from({ length: n }, (_, i) => ({
      job: base + i, name: "h" + (base + i), status: "SUCCESS", size: 10,
      completed_at_unix: 1000 - i, stages: [],
    }));

    eq(T.HISTORY_PAGE_SIZE_DEFAULT, 20, "20 history rows per page by default");
    ok(!T.HISTORY_PAGE_SIZES.includes(0),
      "no 'all' option — 'all' is the thing this exists to stop asking for");

    routes.clear();
    seen.length = 0;
    routes.set("/api/v1/clients", { status: 200, body: { clients: [] } });
    routes.set("/api/v1/history", { status: 200, body: { entries: hist(20, 1), total: 179 } });
    T.setHistoryPagingForTest(0, 20, 0);
    await T.refreshHistory();

    const asked = seen.filter(r => r.url.includes("/api/v1/history")).map(r => r.url);
    eq(asked.length, 1, "one history request per page");
    ok(asked[0].includes("limit=20"), `the page size is on the wire (got ${asked[0]})`);
    ok(asked[0].includes("offset=0"), "…and so is the offset");
    eq(T.getHistoryPaging().total, 179, "total comes from the server, not from the page");
    eq(T.historyPages(), 9, "179 rows at 20/page is nine pages");
    eq(bar.hidden, false, "the pager appears once there is more than one page");
    ok(info.textContent.includes("1–20 of 179"),
      `the range names the whole list (got ${info.textContent})`);
    ok(info.textContent.includes("page 1/9"), "…and the position in it");

    // Page 3 asks for offset 40 — the browser never sees rows 1–40 at all.
    seen.length = 0;
    routes.set("/api/v1/history", { status: 200, body: { entries: hist(20, 41), total: 179 } });
    await T.setHistoryPage(2);
    const p3 = seen.filter(r => r.url.includes("/api/v1/history")).map(r => r.url)[0];
    ok(p3.includes("offset=40"), `page 3 asks the server for offset 40 (got ${p3})`);
    ok(info.textContent.includes("41–60 of 179"), "the range follows the request");

    // Changing the size keeps the row you were looking at in view.
    seen.length = 0;
    routes.set("/api/v1/history", { status: 200, body: { entries: hist(50, 41), total: 179 } });
    await T.setHistoryPageSize(50);
    eq(T.getHistoryPaging().page, 0, "row 41 lives on page 1 at 50/page");

    // History shrinking under you — a retention trim is exactly this — must
    // not strand the view on a page that no longer exists.
    seen.length = 0;
    let call = 0;
    routes.set("/api/v1/history", () => {
      call++;
      return call === 1
        ? { status: 200, body: { entries: [], total: 30 } }
        : { status: 200, body: { entries: hist(30, 1), total: 30 } };
    });
    T.setHistoryPagingForTest(5, 50, 400);
    await T.refreshHistory();
    eq(call, 2, "an empty page past the end is retried, not shown");
    eq(T.getHistoryPaging().page, 0, "…from the last page that exists");

    // A daemon too old to send `total` must not make the pager invent one.
    routes.set("/api/v1/history", { status: 200, body: { entries: hist(20, 1) } });
    T.setHistoryPagingForTest(1, 20, 0);
    await T.refreshHistory();
    eq(T.getHistoryPaging().total, 40,
      "with no total, the pager describes what it can reach and no more");

    T.setHistoryPagingForTest(0, 20, 0);
    routes.clear();
    seen.length = 0;
  }

  // Navigation must own its response even when the older fetch finishes last.
  {
    routes.clear(); seen.length = 0;
    routes.set("/api/v1/clients", { status: 200, body: { clients: [] } });
    let release;
    routes.set("/api/v1/history", url => {
      if (url.includes("offset=0")) return new Promise(resolve => { release = resolve; });
      return { status: 200, body: { total: 60, entries: [{ job: 99, name: "latest", status: "SUCCESS", size: 1, completed_at_unix: 100 }] } };
    });
    T.setHistoryPagingForTest(0, 20, 60);
    const old = T.refreshHistory();
    const coalesced = T.refreshHistory();
    eq(old, coalesced, "timer refresh shares the in-flight request for the same page");
    await T.setHistoryPage(1);
    eq(T.store.history[0].job, 99, "new page renders without waiting for old page");
    release({ status: 200, body: { total: 60, entries: [{ job: 1, name: "stale", status: "SUCCESS", size: 1, completed_at_unix: 100 }] } });
    await old;
    eq(T.store.history[0].job, 99, "older response cannot overwrite new page");
    eq(T.getHistoryPaging().page, 1, "pager remains on selected page");
    eq(sandbox.document.getElementById("history-loading").hidden, true, "old response cannot revive loading state");
    T.setHistoryPagingForTest(0, 20, 0);
    routes.clear(); seen.length = 0;
  }

  // A successful action must bypass a poll that began before the mutation.
  {
    routes.clear(); seen.length = 0;
    routes.set("/api/v1/clients", { status: 200, body: { clients: [] } });
    routes.set("/api/v1/history/1/actions/delete", { status: 200, body: { ok: true } });
    let release, requests = 0;
    routes.set("/api/v1/history", () => {
      if (++requests === 1) return new Promise(resolve => { release = resolve; });
      return { status: 200, body: { total: 0, entries: [] } };
    });
    T.setHistoryPagingForTest(0, 20, 1);
    const old = T.refreshHistory();
    await T.histAction(1, "delete");
    await T.refreshHistory();
    ok(requests >= 2, "acknowledged deletion starts a fresh history request");
    eq(T.store.history.length, 0, "post-delete snapshot is displayed");
    release({ status: 200, body: { total: 1, entries: [{ job: 1, name: "deleted", status: "SUCCESS", size: 1, completed_at_unix: 100 }] } });
    await old;
    eq(T.store.history.length, 0, "pre-delete response cannot restore a deleted row");
    T.setHistoryPagingForTest(0, 20, 0);
    routes.clear(); seen.length = 0;
  }

  // --- the log ring keeps a budget per class ------------------------------
  // The defect: one shared 500-line ring means a per-file flood evicts every
  // system and job line, so the Logs tab goes blank exactly when a download
  // is running — the one time anyone opens it.
  {
    const line = (id, scope, text) => ({
      id, scope, kind: "INFO", time_unix: 1_700_000_000, text,
      job: scope === "system" ? undefined : 7,
    });
    T.store.logs = [];
    T.appendLogs([line(1, "system", "nzbd starting"), line(2, "job", "job finished")], 0);
    for (let i = 0; i < T.LOG_RING_MAX * 3; i++)
      T.appendLogs([line(100 + i, "file", "file finished " + i)], 0);

    const kept = T.store.logs;
    const mains = kept.filter(e => T.logClass(e) === "main");
    const files = kept.filter(e => T.logClass(e) === "file");
    eq(mains.length, 2, "the boot banner and the job line survived the flood");
    eq(mains[0].text, "nzbd starting", "…in order");
    eq(files.length, T.LOG_RING_MAX, "per-file lines rolled against their OWN budget");
    eq(files[files.length - 1].text, "file finished " + (T.LOG_RING_MAX * 3 - 1),
      "…keeping the newest");
    ok(kept.every((e, i) => i === 0 || kept[i - 1].id <= e.id),
      "the ring stays in arrival order across both budgets");

    // A skip marker is a statement about the stream, so it rides with main
    // and a file flood cannot evict it either.
    T.store.logs = [];
    T.appendLogs([line(1, "system", "boot")], 0);
    T.appendLogs([line(2, "job", "job")], 9);
    for (let i = 0; i < T.LOG_RING_MAX + 50; i++) T.appendLogs([line(500 + i, "file", "f")], 0);
    ok(T.store.logs.some(e => e.skipped === 9), "the skipped-line marker survived");

    // The rendered line is memoised, not rebuilt per frame.
    const rec = line(1, "system", "hello");
    const first = T.logLineText(rec);
    rec.text = "changed underneath";
    eq(T.logLineText(rec), first, "a record's rendered line is computed once");
    ok(first.includes("INFO"), "…and carries the level");

    T.store.logs = [];
  }

  // --- the Logs backfill asks for what is shown ---------------------------
  // It used to fetch all three scopes and throw two thirds away: per-file
  // lines outnumber the rest ~2:1 even on an idle daemon, and the default
  // view hides them.
  {
    const doc = sandbox.document;
    routes.clear();
    seen.length = 0;
    routes.set("/api/v1/logs", { status: 200, body: { entries: [] } });

    // The default lives in the markup, and earlier blocks have ticked
    // boxes since — assert the markup, then set the state it describes.
    const boxChecked = (id) =>
      /\bchecked\b/.test((html.match(new RegExp(`<input\\b[^>]*id="${id}"[^>]*>`)) || [""])[0]);
    ok(boxChecked("lg-system"), "system is on by default");
    ok(boxChecked("lg-job"), "jobs are on by default");
    ok(!boxChecked("lg-file"), "per-file is off by default — it is the noisy one");
    for (const s of ["system", "job", "file"])
      doc.getElementById("lg-" + s).checked = boxChecked("lg-" + s);

    await T.refreshLogs();
    let url = seen.filter(r => r.url.includes("/api/v1/logs")).map(r => r.url)[0];
    ok(url.includes("scope=system,job"), `only the ticked scopes are asked for (got ${url})`);
    ok(!url.includes("file"), "…and the noisy one the view hides is not fetched");

    seen.length = 0;
    doc.getElementById("lg-file").checked = true;
    await T.refreshLogs();
    url = seen.filter(r => r.url.includes("/api/v1/logs")).map(r => r.url)[0];
    ok(url.includes("scope=system,job,file"), `ticking per-file widens the fetch (got ${url})`);

    // Every box off means there is nothing to show, so nothing is fetched.
    seen.length = 0;
    for (const s of ["system", "job", "file"]) doc.getElementById("lg-" + s).checked = false;
    await T.refreshLogs();
    eq(seen.filter(r => r.url.includes("/api/v1/logs")).length, 0,
      "no ticked scopes, no request");

    doc.getElementById("lg-system").checked = true;
    doc.getElementById("lg-job").checked = true;
    doc.getElementById("lg-file").checked = false;
    T.store.logs = [];
    routes.clear();
    seen.length = 0;
  }

  // Settings saves must submit the enabled flag, retain defaults, and leave
  // visible feedback after the form is refreshed from the saved config.
  {
    routes.clear();
    const doc = sandbox.document;
    const form = doc.getElementById("cfg-form");
    const save = doc.getElementById("cfg-save");
    const msg = doc.getElementById("cfg-msg");
    const config = process.env.NZBD_UI_CONFIG ? JSON.parse(process.env.NZBD_UI_CONFIG) : {
      paths: {}, queue: {}, post: { failure_action: "park" }, history: {}, server: [], category: [],
      torrent: { enabled: false, listen_port: 6881, pex: true },
    };
    let stored = config, submitted, rejectSave = false, reloadFailure = null;
    routes.set("/api/v1/config", (_url, init) => {
      if (init && init.method === "PUT") {
        submitted = init.body;
        if (rejectSave) return { status: 422, body: { error: "invalid torrent settings" } };
        if (init.headers["content-type"] === "application/json") stored = JSON.parse(init.body);
        return { status: 200, body: {
          applied_live: [], restart_required: ["torrent"], connection_notes: [],
        } };
      }
      if (reloadFailure === "network") throw new Error("network unavailable");
      if (reloadFailure === "http") return { status: 503, body: {} };
      return { status: 200, body: {
        config: stored, path: "/tmp/nzbd.toml", writable: true,
        toml: "[torrent]\nenabled = true", pending_restart: stored.torrent.enabled ? ["torrent"] : [],
      } };
    });
    let syncState = { mode: "local_only", state: "paused", paused: true,
      placement: "network_or_fuse", index_path: "/processing/history.sqlite",
      repair_pending: true, last_duration_ms: 0, last_bytes_read: 0 };
    routes.set("/api/v1/history-sync/resume", () => {
      syncState = { ...syncState, paused: false, state: "idle" };
      return { status: 200, body: syncState };
    });
    routes.set("/api/v1/history-sync/pause", () => {
      syncState = { ...syncState, paused: true, state: "paused" };
      return { status: 200, body: syncState };
    });
    routes.set("/api/v1/history-sync", () => ({ status: 200, body: syncState }));
    await vm.runInContext("loadSettings(true)", sandbox);
    ok(form.innerHTML.includes('data-settings-view="dev"><h3>Enable history synchronization'),
      "history enable control lives in Dev settings");
    ok(doc.getElementById("cfg-history-sync-advisory").textContent.includes("unmet"),
      "network storage readiness is advisory and visible");
    eq(doc.getElementById("cfg-history-sync-enable").disabled, false,
      "unmet storage readiness does not disable enablement");
    await vm.runInContext("toggleHistorySync()", sandbox);
    eq(syncState.paused, false, "Dev enable resumes synchronization on network storage");
    ok(doc.getElementById("history-sync-info").textContent.includes("idle"),
      "successful resume updates displayed synchronization state");
    eq(doc.getElementById("cfg-history-sync-enable").textContent, "Pause history synchronization",
      "successful resume offers the inverse operation");
    await vm.runInContext("toggleHistorySync()", sandbox);
    eq(syncState.paused, true, "the next click pauses instead of resuming again");
    ok(doc.getElementById("history-sync-info").textContent.includes("paused"),
      "successful pause renders the new state");

    eq(vm.runInContext("cfgDirty", sandbox), false, "live enable does not dirty config form");
    eq(doc.getElementById("cfg-history-sync-enable").disabled, false,
      "live enable is available after completion");

    ok(form.innerHTML.includes('data-path="torrent.listen_port" data-type="num" value="6881"'),
      "the BitTorrent form renders the API's default listen port");
    // Read controls from the renderer's markup instead of supplying a lone
    // checkbox: previously that stub missed an obsolete field elsewhere in
    // the form, which made the real PUT fail with a duplicate-field error.
    const unescape = text => text.replace(/&(amp|quot|#39|lt|gt);/g,
      (_, entity) => ({ amp: "&", quot: '"', "#39": "'", lt: "<", gt: ">" })[entity]);
    const controls = [...form.innerHTML.matchAll(/<input\b[^>]*>|<select\b[^>]*>[\s\S]*?<\/select>/g)]
      .filter(([markup]) => markup.includes('data-path="'))
      .map(([markup]) => {
        const tag = markup.match(/^<[^>]+>/)[0];
        const attr = name => unescape((tag.match(new RegExp(`${name}="([^"]*)"`)) || ["", ""])[1]);
        const options = [...markup.matchAll(/<option value="([^"]*)"([^>]*)>/g)];
        const selected = options.find(option => /\bselected\b/.test(option[2])) || options[0];
        return {
          dataset: { path: attr("data-path"), type: attr("data-type") },
          value: selected ? unescape(selected[1]) : attr("value"),
          checked: /\bchecked\b/.test(tag),
        };
      });
    if (process.env.NZBD_UI_CONFIG) {
      for (const control of controls) {
        const value = control.dataset.path.split(".").reduce((obj, key) => obj?.[key], config);
        ok(value !== undefined, `rendered setting ${control.dataset.path} exists in the API config`);
      }
    }
    const enable = controls.find(control => control.dataset.path === "torrent.enabled");
    const failure = controls.find(control => control.dataset.path === "post.failure_action");
    ok(!!failure, "the form uses the canonical failure_action field");
    eq(failure?.value, config.post.failure_action, "the form shows the configured failure policy");
    ok(form.innerHTML.includes('data-settings-view="dev"'), "settings renders a Dev view");
    const advice = T.enableAdvisory({ torrent: { listen_port: 6881, dht: true, socks_proxy_url: "socks5://localhost:1080" }, cluster: { enabled: true } });
    ok(advice.includes("unmet"), "incompatible configuration is advisory");
    ok(advice.includes("unknown"), "unverified runtime readiness is explicit");
    ok(advice.includes("review") && advice.includes("unknown magnet hash"),
      "DHT hash exposure is an explicit advisory when selected");
    ok(T.enableAdvisory({ torrent: { listen_port: 6881, dht: false }, cluster: {} })
      .includes("trackerless magnets have no discovery source"),
      "DHT-off readiness explains the trackerless-magnet consequence");
    ok(!advice.includes("disabled"), "readiness never disables an enable switch");
    for (const [port, state] of [[65534, "met"], [65535, "unmet"]]) {
      ok(T.enableAdvisory({ torrent: { listen_port: port } }).includes(`<b>${state}</b> · A peer TCP port`),
        `port ${port} readiness matches configuration validation`);
    }
    const generalCard = { dataset: { settingsView: "general" }, hidden: false };
    const devCard = { dataset: { settingsView: "dev" }, hidden: true };
    const queryBeforeViews = form.querySelectorAll;
    form.querySelectorAll = sel => sel === "[data-settings-view]" ? [generalCard, devCard] : [];
    doc.getElementById("cfg-dev").onclick();
    ok(generalCard.hidden && !devCard.hidden, "Dev shows feature controls");
    doc.getElementById("cfg-general").onclick();
    ok(!generalCard.hidden && devCard.hidden, "General restores normal settings");
    form.querySelectorAll = queryBeforeViews;
    enable.checked = true;
    const originalQuery = form.querySelectorAll;
    const edited = process.env.NZBD_UI_CONFIG ? controls : controls.filter(control =>
      control.dataset.path === "torrent.enabled" || control.dataset.path.startsWith("post."));
    form.querySelectorAll = sel => sel === "[data-path]" ? edited : [];
    form.oninput();
    eq(save.disabled, false, "editing enables Save changes");
    await save.onclick();
    eq(JSON.parse(submitted).torrent.enabled, true, "Save changes submits BitTorrent enabled");
    eq(JSON.parse(submitted).torrent.listen_port, 6881, "save preserves the default listen port");
    eq(JSON.parse(submitted).post.failure_action, config.post.failure_action, "save preserves the configured failure policy");
    ok(!("health_action" in JSON.parse(submitted).post), "save does not add the legacy alias");
    if (process.env.NZBD_UI_SAVED_CONFIG_PATH) fs.writeFileSync(process.env.NZBD_UI_SAVED_CONFIG_PATH, submitted);
    eq(msg.textContent, "saved", "save confirmation survives the settings reload");
    eq(doc.getElementById("restart-banner").hidden, false, "enabling BitTorrent requests a restart");
    eq(save.disabled, true, "a successful save clears the dirty state");

    rejectSave = true;
    enable.checked = false;
    form.oninput();
    await save.onclick();
    eq(msg.textContent, "invalid torrent settings", "validation failure stays visible");
    eq(save.disabled, false, "a rejected save remains retryable");
    eq(enable.checked, false, "a rejected save retains the edit");

    submitted = null;
    enable.dataset.path = "missing.enabled";
    await save.onclick();
    eq(submitted, null, "a collection error does not send a partial config");
    ok(msg.textContent.startsWith("Could not read settings:"), "collection errors are visible");
    eq(save.disabled, false, "a collection error leaves Save changes enabled");
    enable.dataset.path = "torrent.enabled";

    rejectSave = false;
    await doc.getElementById("adv-save").onclick();
    eq(msg.textContent, "TOML saved", "advanced save confirmation survives reload");

    for (const failure of ["network", "http"]) {
      reloadFailure = failure;
      for (const id of ["cfg-save", "adv-save"]) {
        form.oninput();
        doc.getElementById("restart-banner").hidden = true;
        await doc.getElementById(id).onclick();
        const prefix = id === "cfg-save" ? "saved" : "TOML saved";
        eq(msg.textContent, prefix + " · could not reload settings; refresh the page",
          `${id}: a ${failure} reload failure preserves success and explains recovery`);
        eq(msg.className, "warn", `${id}: a ${failure} reload failure is advisory`);
        eq(doc.getElementById("restart-banner").hidden, false,
          `${id}: a ${failure} reload failure preserves restart advice`);
        eq(save.disabled, true, `${id}: a ${failure} reload failure clears the dirty state`);
      }
    }
    form.querySelectorAll = originalQuery;
    routes.clear();
  }

  // --- Files tab (field report 2026-09-28) ------------------------------------
  // After a scan the old list showed "0 entries · 0 B" for folders nobody had
  // walked, Inspect rendered its panel at the foot of the page, and the queued
  // walk waited for a 30 s tick. Each of those is pinned here.
  {
    const doc = sandbox.document;
    const art = (id, extra) => ({
      id, generation: "g", revision: 1, job: null,
      path: "/working/monarr/completed/" + id, root: "/working/monarr/completed",
      root_identity: {}, identity: {}, state: "unknown", owned: false, keep: true, hold: "review",
      created_at: 1790636080, updated_at: 1790636080, retention_seconds: 0, deadline: null,
      eligible_seconds: 0, files: [], error: null, inspected_at: null, ...extra,
    });
    const entry = (id, extra, files = 0, bytes = 0, measured = false) =>
      ({ artifact: art(id, extra), files, bytes, measured, earliest_expiry: null });

    // 1. A folder that has not been walked is "—", never a zero.
    const raw = T.fileRowModel(entry("Fright.Night.1985"));
    eq(raw.files, "—", "unmeasured file count is a dash");
    eq(raw.size, "—", "unmeasured size is a dash");
    eq(raw.measured, false, "…and the model says why");
    eq(raw.name, "Fright.Night.1985", "the row shows the folder name");
    eq(raw.sub, "/working/monarr/completed", "…with its parent underneath");
    eq(raw.st, "unknown", "state pill");
    eq(raw.stCls, "st warn", "an unknown folder needs a person, so it is amber");
    eq(raw.hold, "on review hold", "the hold is spelled out");
    eq(raw.ret, "never deleted automatically", "retention explains what will NOT happen");
    eq(raw.owner, "not owned", "…and whose it is");
    eq(raw.inspectLabel, "inspect", "a fresh folder offers inspect");
    const walked = T.fileRowModel(entry("Archer.S03E04", { inspected_at: 1790636100 }, 2, 6347221425, true));
    eq(walked.files, "2", "measured count");
    eq(walked.size, "5.9 GiB", "measured size");
    eq(walked.inspectLabel, "re-inspect", "a measured folder offers a re-walk");
    const empty = T.fileRowModel(entry("Empty.Dir", { inspected_at: 1790636100 }, 0, 0, true));
    eq(empty.files, "0", "a measured empty folder is honestly zero");
    const owned = T.fileRowModel({ ...entry("Just.Friends.2018", { state: "completed", owned: true, keep: false, hold: null }, 2, 100, true), earliest_expiry: 1790700000 });
    eq(owned.stCls, "st ok", "an owned completed payload is green");
    ok(owned.ret.startsWith("expires "), `retention names the expiry (got ${owned.ret})`);
    const kept = T.fileRowModel(entry("Kept", { state: "retained", owned: true, keep: true, hold: null }, 1, 1, true));
    eq(kept.ret, "keep indefinitely", "keep reads as keep");
    const active = T.fileRowModel(entry("Live", { state: "active" }));
    eq(active.inspectHidden, true, "a folder still being written cannot be walked");
    eq(active.st, "downloading", "active reads as downloading");
    const gone = T.fileRowModel(entry("Gone", { state: "source_gone", updated_at: Math.floor(Date.now() / 1000) - 120 }));
    eq(gone.rowCls, "hidden-row", "cleared records are dimmed");
    ok(gone.ret.startsWith("gone "), "…and say when they went");
    const broken = T.fileRowModel(entry("Odd", { error: "special file: ctl.sock" }));
    eq(broken.err, "special file: ctl.sock", "a walk failure rides on the row instead of a zero");

    // 2. Rendering: rows keep identity across refreshes, and the panel opens
    //    UNDER its row, not at the end of the list.
    const body = doc.getElementById("files-body");
    body.children.length = 0; body.__rows = undefined;
    const entries = ["a", "b", "c", "d"].map(id => entry(id, { inspected_at: 1 }, 1, 1024, true));
    T.store.files = { entries, total: 4, counts: { live: 4, attention: 4, owned: 0, cleared: 0, all: 4 }, discovery: null };
    T.filesView.total = 4; T.filesView.page = 0;
    T.renderFiles();
    eq(body.children.length, 4, "one row per folder");
    eq(body.children[0].children[2].className, "num", "a measured count keeps its numeric column class");
    const rowsBefore = body.children.slice();
    resetCounts();
    T.renderFiles();
    eq(body.children.length, 4, "a second render adds nothing");
    ok(rowsBefore.every((n, i) => body.children[i] === n), "rows keep identity across refreshes (clicks survive)");
    T.setFilesDetailForTest("b", { artifact: art("b", { inspected_at: 1 }), files: [{ path: "ep.mkv", identity: { bytes: 1024, directory: false } }], total: 1, offset: 0, events: [], preview: null });
    T.renderFiles();
    eq(body.children.length, 5, "the open folder adds exactly one panel row");
    eq(body.children[1].dataset.artifact, "b", "…under the row you clicked");
    eq(body.children[2].className, "detail-tr", "…as a detail row, not at the foot of the page");
    ok(body.children[1].className.includes("f-open"), "the open row is lit");
    const panel = body.children[2].children[0];
    ok(panel.innerHTML.includes("adopt &amp; keep"), "a measured unowned folder can be adopted");
    ok(panel.innerHTML.includes("ep.mkv"), "the file list is in the panel");
    const html1 = panel.innerHTML;
    T.renderFiles();
    eq(panel.innerHTML, html1, "an unchanged panel is not re-rendered (checkboxes survive the 5 s tick)");
    ok(body.children[2] === panel.parentNode, "…and it is the same node");
    T.setFilesDetailForTest(null, null);
    T.renderFiles();
    eq(body.children.length, 4, "closing removes the panel");

    // 3. The panel model: what is offered depends on state.
    const unmeasuredPanel = T.fileDetailModel({ artifact: art("u"), files: [], total: 0, offset: 0, events: [], preview: null });
    ok(!unmeasuredPanel.buttons.some(b => b.action === "f-adopt"), "an unmeasured folder cannot be adopted yet");
    eq(unmeasuredPanel.noteCls, "warn", "…and the panel says so");
    ok(unmeasuredPanel.buttons.some(b => b.action === "f-inspect"), "…but it can be inspected");
    const ownedPanel = T.fileDetailModel({ artifact: art("o", { owned: true, state: "retained", hold: null, keep: true, inspected_at: 5 }), files: [], total: 0, offset: 0, events: [], preview: null });
    const acts = ownedPanel.buttons.map(b => b.action);
    ok(acts.includes("f-delete") && acts.includes("f-keep"), `an owned folder can be kept or deleted (got ${acts})`);
    ok(!acts.includes("f-adopt"), "…and not adopted twice");
    T.filesSelections.set("o", new Set(["x.mkv"]));
    const selPanel = T.fileDetailModel({ artifact: art("o", { owned: true, state: "retained", hold: null, keep: true, inspected_at: 5 }), files: [{ path: "x.mkv", identity: { bytes: 5, directory: false } }], total: 1, offset: 0, events: [], preview: null });
    eq(selPanel.selected, 1, "a ticked file counts as selected");
    ok(T.fileDetailHtml(selPanel).includes('value="x.mkv" ' + ' checked'), "…and re-renders ticked");
    T.filesSelections.clear();
    const paged = T.fileDetailModel({ artifact: art("p", { inspected_at: 5 }), files: Array.from({ length: 200 }, (_, i) => ({ path: "f" + i, identity: { bytes: 1, directory: false } })), total: 450, offset: 200, events: [], preview: null });
    ok(T.fileDetailHtml(paged).includes("201–400 of 450"), "the file list pages inside the panel");

    // Select all covers the folder, skips directories, and invalidates an old
    // recovery preview. A changing manifest must never commit a partial set.
    const bulkFiles = Array.from({ length: 59 }, (_, i) => ({ path: `part${i}.rar`, identity: { bytes: 5, directory: false } }));
    bulkFiles.push({ path: "subfolder", identity: { bytes: 0, directory: true } });
    const bulk = { artifact: art("bulk", { owned: true, state: "retained", hold: null, inspected_at: 5 }),
      files: bulkFiles, total: 60, fileCount: 59, offset: 0, events: [], preview: { text: "old preview" }, previewRequest: {} };
    T.setFilesDetailForTest("bulk", bulk);
    await T.filesSelectionChange({ classList: { contains: c => c === "recovery-all" }, checked: true,
      closest: () => ({ dataset: { artifact: "bulk" } }) });
    eq(T.filesSelections.get("bulk").size, 59, "one checkbox selects all 59 files");
    ok(!T.filesSelections.get("bulk").has("subfolder"), "directories are never selected for recovery");
    eq(bulk.preview, null, "bulk selection clears the previous preview");
    eq(bulk.previewRequest, null, "…and its old recovery request");
    const bulkModel = T.fileDetailModel(bulk);
    eq(bulkModel.allSelected, true, "the master checkbox reflects full selection");
    ok(T.fileDetailHtml(bulkModel).includes("Select all files"), "bulk selection has a visible label");
    ok(bulkModel.archiveNote.includes("does not unpack"), "archives explain the limits of recovery copying");
    ok(bulkModel.retainedNote.includes("does not mean processing completed"), "retained does not imply a finished download");
    await T.filesClick("f-select-none", { dataset: { artifact: "bulk" }, closest: () => null });
    eq(T.filesSelections.get("bulk").size, 0, "clear selection clears the entire folder");
    T.filesSelections.set("bulk", new Set(["part0.rar"]));
    eq(T.fileDetailModel(bulk).someSelected, true, "partial selection makes the master checkbox indeterminate");
    eq(T.filesStateModel({ state: "retained", error: "move failed" }).cls, "st bad", "retained with an error is not green success");

    const firstPage = bulkFiles.slice(0, 30), secondPage = bulkFiles.slice(30);
    bulk.files = secondPage; bulk.offset = 30;
    routes.set("/api/v1/artifacts/bulk/files?offset=0", { status: 200, body: { revision: 1, total: 60, files: firstPage } });
    routes.set("/api/v1/artifacts/bulk/files?offset=30", { status: 200, body: { revision: 1, total: 60, files: secondPage } });
    routes.set("/api/v1/artifacts/bulk", { status: 200, body: bulk.artifact });
    await T.selectAllFiles("bulk", true);
    eq(T.filesSelections.get("bulk").size, 59, "select all from page two includes files from both pages");
    T.filesSelections.set("bulk", new Set(["part0.rar"]));
    routes.set("/api/v1/artifacts/bulk/files?offset=30", { status: 200, body: { revision: 2, total: 60, files: secondPage } });
    await T.selectAllFiles("bulk", true);
    eq(T.filesSelections.get("bulk").size, 1, "a revision change preserves the previous selection without a partial update");
    T.filesSelections.clear();
    T.setFilesDetailForTest(null, null);
    routes.clear();

    // 3b. Staging is offered only when the server would accept it
    //     (owned · unheld · settled state). Field report 2026-09-28 #2: a
    //     folder held by an open recovery handoff offered "Preview recovery
    //     copy" and earned "stale selection, unowned or held source".
    const mediaFile = [{ path: "x.mkv", identity: { bytes: 5, directory: false } }];
    T.store.recoveries = [{ id: "70a9015f567db76ce0183b149c371c84", artifact: "h", state: "published", files: [{}], error: null }];
    const held = T.fileDetailModel({ artifact: art("h", { owned: true, state: "retained", keep: true, hold: "recovery:70a9015f567db76ce0183b149c371c84", inspected_at: 5 }), files: mediaFile, total: 1, offset: 0, events: [], preview: null });
    eq(held.stageable, false, "a folder held by a handoff cannot be staged");
    ok((held.stageBlock || "").includes("already staged as handoff 70a9015f…") && (held.stageBlock || "").includes("(published)"),
      `…and the panel names the handoff and its state (got ${held.stageBlock})`);
    eq(held.handoffs.length, 1, "the folder's own handoff is listed in its panel");
    eq(held.handoffs[0].cancellable, true, "…with a cancel");
    const heldHtml = T.fileDetailHtml(held);
    ok(!heldHtml.includes("f-stage"), "no stage button on a held folder");
    ok(!heldHtml.includes("recovery-file"), "…and no checkboxes to tick for nothing");
    ok(heldHtml.includes('data-action="f-rec-cancel"') && heldHtml.includes("70a9015f567db76ce0183b149c371c84"), "cancel handoff is offered in place");
    // The holding handoff may be off the listed page: the panel fetched it by
    // id (dt.holding) and still lists it. A failed one says cancel, not import.
    T.store.recoveries = [];
    const offPage = T.fileDetailModel({ artifact: art("h", { owned: true, state: "retained", hold: "recovery:70a9015f567db76ce0183b149c371c84", inspected_at: 5 }), files: mediaFile, total: 1, offset: 0, events: [], preview: null,
      holding: { id: "70a9015f567db76ce0183b149c371c84", artifact: "h", state: "failed", files: [{}], error: "filesystem: write /processing/recovery/.staging/x/payload/a.mkv: Invalid argument (os error 22)" } });
    eq(offPage.handoffs.length, 1, "a handoff fetched by id is listed even when the page does not carry it");
    ok((offPage.stageBlock || "").startsWith("Staging handoff 70a9015f… failed — filesystem: write"), `a failed handoff says so and says cancel (got ${offPage.stageBlock})`);
    ok(!(offPage.stageBlock || "").includes("Claim and import"), "…not import");
    const pending = T.fileDetailModel({ artifact: art("h", { owned: true, state: "retained", hold: "recovery:70a9015f567db76ce0183b149c371c84", inspected_at: 5 }), files: mediaFile, total: 1, offset: 0, events: [], preview: null,
      holding: { id: "70a9015f567db76ce0183b149c371c84", artifact: "h", state: "cancel_pending", files: [{}] } });
    eq(pending.handoffs[0].cancellable, false, "a cancelling handoff cannot be cancelled twice");
    ok(T.fileDetailHtml(pending).includes("cancelling…"), "…and says it is cancelling");
    const unowned = T.fileDetailModel({ artifact: art("u2", { inspected_at: 5 }), files: mediaFile, total: 1, offset: 0, events: [], preview: null });
    ok(unowned.stageBlock.startsWith("Only an owned folder"), "an unowned folder says adopt first");
    const reviewHeld = T.fileDetailModel({ artifact: art("r", { owned: true, state: "retained", hold: "review", inspected_at: 5 }), files: mediaFile, total: 1, offset: 0, events: [], preview: null });
    ok(reviewHeld.stageBlock.includes("release the hold"), "a review hold says release it");
    const fine = T.fileDetailModel({ artifact: art("ok", { owned: true, state: "retained", hold: null, inspected_at: 5 }), files: mediaFile, total: 1, offset: 0, events: [], preview: null });
    eq(fine.stageable, true, "owned · unheld · retained can be staged");
    ok(T.fileDetailHtml(fine).includes('data-action="f-stage"') && T.fileDetailHtml(fine).includes("recovery-file"), "…with checkboxes and the button");
    routes.set("/api/v1/recoveries/70a9015f567db76ce0183b149c371c84/cancel", { status: 200, body: { id: "70a9015f567db76ce0183b149c371c84", state: "cancelled" } });
    routes.set("/api/v1/recoveries", { status: 200, body: [] });
    routes.set("/api/v1/artifacts?", { status: 200, body: { entries: [], total: 0, counts: {}, discovery: null } });
    doc.getElementById("toasts").children.length = 0;
    await T.filesClick("f-rec-cancel", { dataset: { recovery: "70a9015f567db76ce0183b149c371c84" }, closest: () => null });
    ok(seen.some(r => r.url.endsWith("/recoveries/70a9015f567db76ce0183b149c371c84/cancel") && r.method === "POST"), "cancel posts to the handoff");
    ok(doc.getElementById("toasts").children.some(t => t.children[0].textContent.includes("cancelled")), "…and says so");
    routes.clear(); seen.length = 0;

    // 4. The scan pill: never "queued" forever.
    const now = Date.now();
    eq(T.filesScanModel(null, now).text, "never scanned", "no discovery yet");
    ok(T.filesScanModel({ state: "queued", created_at: 1 }, now).busy, "a queued scan is busy");
    const failed = T.filesScanModel({ state: "failed", created_at: Math.floor(now / 1000) - 60, error: "/x: not found" }, now);
    ok(failed.cls.includes("bad") && failed.tip === "/x: not found", "a failed scan is red and carries its reason");
    T.filesWatch.set("op1", { kind: "scan", artifact: null, since: now - 12000 });
    eq(T.filesScanModel({ state: "succeeded", created_at: 1 }, now).text, "scanning… 12 s", "a watched scan counts up");
    T.filesWatch.set("op2", { kind: "inspect", artifact: "a", since: now });
    const measuring = T.fileRowModel(entry("a"));
    eq(measuring.inspectLabel, "measuring…", "a watched inspect shows on its row");
    eq(measuring.inspectDisabled, true, "…and cannot be double-clicked");
    T.filesWatch.clear();

    // 5. The list is server-paged: filter, sort, search and size are on the wire.
    routes.clear(); seen.length = 0;
    const page = (n, first, total) => ({ status: 200, body: {
      entries: Array.from({ length: n }, (_, i) => entry("row" + (first + i), { inspected_at: 1 }, 1, 1, true)),
      total, counts: { live: total, attention: 0, owned: total, cleared: 0, all: total }, discovery: { state: "succeeded", created_at: 1 },
    } });
    routes.set("/api/v1/recoveries", { status: 200, body: [] });
    routes.set("/api/v1/artifacts?", page(50, 1, 186));
    T.setFilesDetailForTest(null, null);
    T.filesView.page = 0; T.filesView.filter = "live"; T.filesView.sort = "updated"; T.filesView.q = "";
    await T.setFilesPageSize(50);
    let asked = seen.filter(r => r.url.includes("/api/v1/artifacts?")).map(r => r.url);
    eq(asked.length, 1, "one list request");
    ok(asked[0].includes("limit=50") && asked[0].includes("offset=0") && asked[0].includes("filter=live") && asked[0].includes("sort=updated"),
      `size, offset, filter and sort are on the wire (got ${asked[0]})`);
    eq(T.filesView.total, 186, "total comes from the server");
    eq(T.filesPages(), 4, "186 at 50/page is four pages");
    eq(body.children.length, 50, "fifty rows, no more");
    const cnt = doc.getElementById("files-cnt-live");
    eq(cnt.textContent, "186", "the filter chip carries the server's count");
    seen.length = 0;
    routes.set("/api/v1/artifacts?", page(50, 101, 186));
    await T.setFilesPage(2);
    asked = seen.filter(r => r.url.includes("/api/v1/artifacts?")).map(r => r.url);
    ok(asked[0].includes("offset=100"), `page 3 asks for offset 100 (got ${asked[0]})`);
    seen.length = 0;
    await T.setFilesFilter("attention");
    asked = seen.filter(r => r.url.includes("/api/v1/artifacts?")).map(r => r.url);
    ok(asked[0].includes("filter=attention") && asked[0].includes("offset=0"), "a filter change restarts at page 1");
    seen.length = 0;
    await T.setFilesQuery("bates motel");
    asked = seen.filter(r => r.url.includes("/api/v1/artifacts?")).map(r => r.url);
    ok(asked[0].includes("q=bates%20motel"), `the search is on the wire, encoded (got ${asked[0]})`);
    seen.length = 0;
    await T.setFilesSort("size");
    asked = seen.filter(r => r.url.includes("/api/v1/artifacts?")).map(r => r.url);
    ok(asked[0].includes("sort=size"), "sort is on the wire");
    // A page that empties under you (the last folder on it was deleted) steps back.
    let call = 0;
    routes.set("/api/v1/artifacts?", () => { call++; return call === 1 ? page(0, 0, 30) : page(30, 1, 30); });
    T.filesView.page = 5;
    await T.refreshFiles(true);
    eq(call, 2, "an empty page past the end is retried");
    eq(T.filesView.page, 0, "…from the last page that exists");
    // Nothing is inspected in a way that lets a stale click through: the
    // page never asks for more than one page of manifests.
    ok(!seen.some(r => r.url.includes("/files?")), "the list never fetches manifests it is not showing");

    // Terminal recovery history is opt-in; a cancelled row must not look active.
    const terminalHistory = doc.getElementById("files-recoveries-terminal");
    terminalHistory.checked = false;
    seen.length = 0;
    await T.refreshFiles(true);
    ok(seen.some(r => r.url.includes("recoveries?include_terminal=false")), "active recovery view excludes terminal handoffs");
    terminalHistory.checked = true;
    seen.length = 0;
    await T.refreshFiles(true);
    ok(seen.some(r => r.url.includes("recoveries?include_terminal=true")), "history toggle includes completed and cancelled handoffs");
    terminalHistory.checked = false;

    // 5b. A daemon that answers the list with an error renders an empty
    //     state, not a TypeError on every 5 s poll.
    routes.set("/api/v1/artifacts?", { status: 503, body: { error: "restarting" } });
    T.store.files = null;
    await T.refreshFiles(true);
    eq(T.store.files, "off", "a non-OK list marks the inventory unavailable");
    eq(body.children.length, 1, "…as one empty row");
    ok(body.children[0].children[0].textContent.includes("unavailable"), "…that says so");
    // 5c. A slow answer to an older query never overwrites a newer one.
    let release;
    routes.set("/api/v1/artifacts?", (url) => url.includes("q=slow")
      ? new Promise(res => { release = () => res(page(1, 900, 1)); })
      : page(2, 1, 2));
    const slow = T.setFilesQuery("slow");
    await T.setFilesQuery("fast");
    eq(T.filesView.total, 2, "the newer query landed");
    release(); await slow;
    eq(T.filesView.total, 2, "…and the older, slower answer was dropped");
    eq(body.children.length, 2, "rows are the newer query's");

    // 6. Scan and inspect are watched to completion, not fired and forgotten.
    const timers = [];
    const realTimeout = sandbox.setTimeout;
    sandbox.setTimeout = (fn, ms) => { timers.push(fn); return timers.length; };
    let opState = "queued";
    routes.set("/api/v1/artifacts/scan", { status: 202, body: { id: "scan-1", kind: "scan", state: "queued" } });
    routes.set("/api/v1/artifact-operations/scan-1", () => ({ status: 200, body: { id: "scan-1", kind: "scan", state: opState } }));
    routes.set("/api/v1/artifacts?", page(3, 1, 3));
    seen.length = 0; timers.length = 0;
    await T.filesClick("f-scan", { dataset: {}, closest: () => null });
    ok(seen.some(r => r.url.endsWith("/api/v1/artifacts/scan") && r.method === "POST"), "scan is requested");
    eq(T.filesWatch.size, 1, "…and watched");
    eq(doc.getElementById("files-scan").disabled, true, "the scan button is held while a scan runs");
    ok(doc.getElementById("files-scan-state").textContent.startsWith("scanning"), "the pill says scanning");
    eq(timers.length, 1, "a poll is scheduled");
    await timers.shift()();
    eq(T.filesWatch.size, 1, "still queued: still watched");
    eq(timers.length, 1, "…and polled again");
    opState = "succeeded"; seen.length = 0;
    await timers.shift()();
    eq(T.filesWatch.size, 0, "a finished scan is no longer watched");
    ok(seen.some(r => r.url.includes("/api/v1/artifacts?")), "…and the list refreshes itself");
    eq(doc.getElementById("files-scan").disabled, false, "the button is released");
    const toasts = doc.getElementById("toasts");
    ok(toasts.children.some(t => t.children[0].textContent.startsWith("Scan finished")), "the result is announced");
    routes.set("/api/v1/artifacts/row1/inspect", { status: 202, body: { id: "insp-1", kind: "inspect", state: "queued" } });
    let inspPolls = 0;
    routes.set("/api/v1/artifact-operations/insp-1", () => ++inspPolls === 1
      ? { status: 503, body: {} }
      : { status: 200, body: { id: "insp-1", kind: "inspect", state: "failed", error: "payload identity changed; review required" } });
    timers.length = 0; toasts.children.length = 0;
    await T.filesClick("f-inspect", { dataset: { artifact: "row1" }, closest: () => null });
    eq(T.fileRowModel(entry("row1")).inspectLabel, "measuring…", "the row shows the walk in flight");
    await timers.shift()();
    eq(T.filesWatch.size, 1, "a transport error mid-poll keeps the watch alive");
    ok(!toasts.children.length, "…and does not report the task as failed");
    await timers.shift()();
    ok(toasts.children.some(t => t.children[0].textContent.includes("payload identity changed")),
      "a failed walk says why, in the daemon's words");
    eq(T.filesWatch.size, 0, "…and stops being watched");
    sandbox.setTimeout = realTimeout;
    routes.clear(); seen.length = 0;
    T.store.files = null; T.setFilesDetailForTest(null, null);
  }

  if (failures.length) {
    console.error("UI DOM FAILURES:");
    for (const f of failures) console.error("  - " + f);
    process.exit(1);
  }
  console.log(`ui dom ok: ${checks} assertions`);
  process.exit(0);
})();
