async function loadArchive() {
  const url = `archive.json?ts=${Date.now()}`;
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Kunde inte ladda archive.json (${r.status})`);
  return await r.json();
}
let A = null;
const $ = s => document.querySelector(s);
const $$ = s => [...document.querySelectorAll(s)];
const els = {
  th: $("#threadSelect"),
  exp: $("#exportBtn"),
  src: $("#sourceLine"),
  view: $$('input[name="voteView"]'),
  animBtn: $("#animateBtn"),
  delay: $("#liveDelayInput"),
  slider: $("#timeSlider"),
  ticks: $("#sliderTicks"),
  sliderLbl: $("#sliderTimeLabel"),
  summary: $("#summary"),
  tbody: $("#voteTable tbody"),
  fp: $("#playerFilter"),
  ths: $$("#voteTable thead th"),
  cv: $("#chart")
};
const st = {
  slug: "",
  votes: [],
  players: [],
  colors: {},
  fp: "",
  sort: "",
  animTimer: null,
  sliderIndex: null, // null => hoppa till max vid rebuild
  timeline: [],
  lim: null
};
const curView = () => els.view.find(r => r.checked)?.value || "latest";
const fmt = t => t ? new Date(t).toLocaleString("sv-SE", { dateStyle: "short", timeStyle: "short" }) : "–";
const enc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const mkColors = names => {
  const u = [...new Set(names)].sort((a, b) => a.localeCompare(b, "sv"));
  const m = {};
  u.forEach((n, i) => m[n] = `hsl(${Math.round(i * 360 / u.length)},70%,60%)`);
  return m;
};
const getLatest = vs => {
  const m = {};
  vs.slice().sort((a, b) => +new Date(a.ts) - +new Date(b.ts)).forEach(v => m[v.from] = v);
  return Object.values(m);
};
const urlp = () => new URLSearchParams(location.search);
function readURL() {
  const p = urlp();
  const view = p.get("view") === "all" ? "all" : "latest";
  const delay = parseInt(p.get("delay") || "200", 10);
  const slider = parseInt(p.get("slider") || "", 10);
  // tar bort mellanslag och slashes från thread
  const thread = (p.get("thread") || "").trim().replace(/\/+$/, "");
  return {
    thread,
    view,
    delay: isNaN(delay) ? 200 : delay,
    slider: isNaN(slider) ? null : slider, // saknas => max
    fp: p.get("fp") || "",
    sort: p.get("sort") || ""
  };
}
function applyURL() {
  const p = new URLSearchParams();
  if (st.slug) p.set("thread", st.slug);
  p.set("view", curView());
  p.set("delay", String(parseInt(els.delay.value || "200", 10) || 200));
  if (st.sliderIndex != null) p.set("slider", String(st.sliderIndex));
  if (st.fp) p.set("fp", st.fp);
  if (st.sort) p.set("sort", st.sort);
  const qs = p.toString();
  history.replaceState(null, "", qs ? `${location.pathname}?${qs}` : location.pathname);
}
function fillThreads() {
  els.th.innerHTML = '<option value="">Välj...</option>';
  A.threads.forEach(t => {
    const o = document.createElement("option");
    o.value = t.slug;
    o.textContent = t.name;
    els.th.appendChild(o);
  });
}
// Slider ticks = ALLA rösttider (st.votes), oavsett view/filter
function rebuildSlider(skipURL) {
  const base = st.votes
    .filter(v => v.ts && !isNaN(+new Date(v.ts)))
    .slice()
    .sort((a, b) => +new Date(a.ts) - +new Date(b.ts));
  st.timeline = base.map(v => new Date(v.ts));
  els.ticks.innerHTML = "";
  if (!st.timeline.length) {
    st.lim = null;
    st.sliderIndex = 0;
    els.slider.min = "0";
    els.slider.max = "0";
    els.slider.value = "0";
    els.sliderLbl.textContent = "–";
    if (!skipURL) { render(); applyURL(); }
    return;
  }
  const max = st.timeline.length - 1;
  els.slider.min = "0";
  els.slider.max = String(max);
  st.timeline.forEach((_, i) => {
    const o = document.createElement("option");
    o.value = String(i);
    els.ticks.appendChild(o);
  });
  // null eller utanför => max
  if (st.sliderIndex == null || st.sliderIndex > max) st.sliderIndex = max;
  if (st.sliderIndex < 0) st.sliderIndex = 0;
  els.slider.value = String(st.sliderIndex);
  st.lim = st.timeline[st.sliderIndex];
  els.sliderLbl.textContent = `${st.sliderIndex + 1}/${st.timeline.length} röster (${fmt(st.lim)})`;
  if (!skipURL) { render(); applyURL(); }
}
function onSlider(skipURL) {
  if (st.animTimer) { clearTimeout(st.animTimer); st.animTimer = null; }
  st.sliderIndex = parseInt(els.slider.value || "0", 10) || 0;
  if (!st.timeline.length) {
    st.lim = null;
    els.sliderLbl.textContent = "–";
    if (!skipURL) { render(); applyURL(); }
    return;
  }
  const max = st.timeline.length - 1;
  if (st.sliderIndex < 0) st.sliderIndex = 0;
  if (st.sliderIndex > max) st.sliderIndex = max;
  st.lim = st.timeline[st.sliderIndex];
  els.sliderLbl.textContent = `${st.sliderIndex + 1}/${st.timeline.length} röster (${fmt(st.lim)})`;
  if (skipURL) return;
  render();
  applyURL();
}
function loadThread(slug, skipURL) {
  if (st.animTimer) { clearTimeout(st.animTimer); st.animTimer = null; }
  const j = A.bySlug[slug];
  if (!j) return;
  st.slug = slug;
  st.votes = (j.votes || []).map(v => ({ ...v }));
  st.players = (j.players && j.players.length) ? j.players : [...new Set(st.votes.flatMap(v => [v.from, v.to]))];
  st.colors = mkColors(st.players);
  els.th.value = st.slug;
  if (st.slug) {
    const urlname = `https://www.rollspel.nu/threads/${st.slug}/`;
    const href = `https://www.rollspel.nu/threads/${encodeURI(st.slug)}/`;
    els.src.innerHTML = `Källa: <a target="_blank" href="${href}">${urlname}</a><br><br><p style="font-size: 12pt;"><b>För SL, kopiera: https://varulvsspel.github.io/kalkylator/?thread=${st.slug}</b></p>`;
  } else {
    els.src.innerHTML = "";
  }
  els.fp.innerHTML = '<option value="">Alla</option>';
  st.players.slice().sort((a, b) => a.localeCompare(b, "sv")).forEach(n => {
    const o = document.createElement("option");
    o.value = n;
    o.textContent = n;
    o.style.color = st.colors[n] || "#000";
    o.style.fontWeight = "bold";
    els.fp.appendChild(o);
  });
  // Återställ filter och slider när man byter tråd (minskar förvirring)
  if (!skipURL) {
    st.fp = "";
    els.fp.value = "";
    st.sliderIndex = null; // => max
  }
  rebuildSlider(skipURL);
  if (skipURL) render();
  if (!skipURL) applyURL();
}
function subset() {
  let vs = st.votes;
  if (st.lim) vs = vs.filter(v => +new Date(v.ts) <= +st.lim);
  if (curView() === "latest") vs = getLatest(vs);
  if (st.fp) vs = vs.filter(v => v.from === st.fp);
  return vs;
}
function bars(entries) {
  const ctx = els.cv.getContext("2d"), W = els.cv.width, H = els.cv.height;
  ctx.clearRect(0, 0, W, H);
  const pad = 10, left = 160;
  const lab = entries.map(e => e[0]), dat = entries.map(e => e[1]);
  const mx = Math.max(1, ...dat);
  ctx.font = "18px Arial";
  lab.forEach((name, i) => {
    const barH = Math.max(12, Math.floor((H - 2 * pad) / Math.max(1, lab.length)) - 2);
    const y = pad + i * (barH + 2);
    const w = Math.floor((W - left - pad - 10) * dat[i] / mx);
    const c = st.colors[name] || "#999";
    const val = "" + dat[i];
    const tw = ctx.measureText(val).width;
    ctx.fillStyle = c;
    ctx.fillText(name, pad, y + barH - 2);
    ctx.fillRect(left, y, w, barH);
    ctx.fillStyle = "#fff";
    ctx.fillText(val, Math.max(left + 4, left + w - tw - 4), y + barH - 2);
  });
}
function sortApply() {
  if (!st.sort) return;
  const rows = $$("#voteTable tbody tr");
  const asc = !st.sort.endsWith("-desc");
  const k = st.sort.split("-")[0];
  rows.sort((a, b) => {
    if (k === "from") {
      const A = a.children[0].textContent.trim();
      const B = b.children[0].textContent.trim();
      return asc ? A.localeCompare(B, "sv") : B.localeCompare(A, "sv");
    }
    const A = a.dataset.ts || "";
    const B = b.dataset.ts || "";
    return asc ? A.localeCompare(B) : B.localeCompare(A);
  });
  els.tbody.innerHTML = "";
  rows.forEach(r => els.tbody.appendChild(r));
}
function render(vsOverride = null) {
  const tableVotes = vsOverride ?? subset();
  // Staplar: alltid objektivt röstläge (ignorera fp), men följer slider-lim.
  // Under animation: använd override för att staplarna ska “leva”.
  let chartVotes;
  if (vsOverride) {
    chartVotes = getLatest(vsOverride);
  } else {
    chartVotes = st.votes;
    if (st.lim) chartVotes = chartVotes.filter(v => +new Date(v.ts) <= +st.lim);
    chartVotes = getLatest(chartVotes);
  }
  if (!chartVotes.length) {
    els.summary.textContent = "Inga röster att visa.";
    els.tbody.innerHTML = "";
    els.cv.getContext("2d").clearRect(0, 0, els.cv.width, els.cv.height);
    return;
  }
  const cnt = {}, first = {};
  chartVotes.slice().sort((a, b) => +new Date(a.ts) - +new Date(b.ts)).forEach(v => {
    cnt[v.to] = (cnt[v.to] || 0) + 1;
    if (!first[v.to] || +new Date(v.ts) < +new Date(first[v.to])) first[v.to] = v.ts;
  });
  const ord = Object.entries(cnt).sort((a, b) =>
    b[1] - a[1] || (+new Date(first[a[0]]) - +new Date(first[b[0]]))
  );
  const [danger, dCnt] = ord[0] || ["Ingen", 0];
  const last = tableVotes.length
    ? tableVotes.reduce((acc, v) => !acc || +new Date(v.ts) > +new Date(acc) ? v.ts : acc, null)
    : null;
  els.summary.textContent =
    `⚠️ Risk för utröstning: ${danger} (${dCnt} röster, sedan ${fmt(first[danger])}). Senast röst lagd ${fmt(last)}.`;
  els.tbody.innerHTML = "";
  const hist = {}, run = {}, GC = n => st.colors[n] || "#000";
  tableVotes.slice().sort((a, b) => +new Date(a.ts) - +new Date(b.ts)).forEach(v => {
    run[v.to] = (run[v.to] || 0) + 1;
    const stand = Object.entries(run).sort((x, y) => y[1] - x[1]);
    const leader = stand[0] ? `${stand[0][0]} (${stand[0][1]})` : "–";
    const runner = stand[1] ? `${stand[1][0]} (${stand[1][1]})` : "–";
    hist[v.from] = hist[v.from] || [];
    if (hist[v.from][hist[v.from].length - 1] !== v.to) hist[v.from].push(v.to);
    const chain = hist[v.from].map((n, i, a) => {
      const c = GC(n), safe = enc(n);
      if (i === a.length - 1) {
        const href = `https://www.rollspel.nu/threads/${encodeURI(st.slug)}/post-${v.post}`;
        return `<a target="_blank" href="${href}" style="color:${c};font-weight:bold">${safe}</a>`;
      }
      return `<span style="color:${c}">${safe}</span>`;
    }).join(" → ");
    const tr = document.createElement("tr");
    tr.dataset.from = v.from;
    tr.dataset.ts = v.ts || "";
    tr.innerHTML =
      `<td style="color:${GC(v.from)};font-weight:bold">${enc(v.from)}</td>` +
      `<td>${chain}</td>` +
      `<td>${fmt(v.ts)}</td>` +
      `<td>${leader}</td>` +
      `<td>${runner}</td>`;
    els.tbody.appendChild(tr);
  });
  sortApply();
  bars(ord);
}
function play() {
  if (st.animTimer) { clearTimeout(st.animTimer); st.animTimer = null; }
  const d = parseInt(els.delay.value || "200", 10);
  const lim = st.lim;
  const all = st.votes
    .filter(v => !lim || +new Date(v.ts) <= +lim)
    .sort((a, b) => +new Date(a.ts) - +new Date(b.ts));
  let i = 0;
  (function step() {
    if (i > all.length) { st.animTimer = null; return; }
    const sub = all.slice(0, i);
    let show = (curView() === "all") ? sub : getLatest(sub);
    if (st.fp) show = show.filter(v => v.from === st.fp);
    render(show);
    i++;
    st.animTimer = setTimeout(step, d);
  })();
}
function exportCSV() {
  const rows = subset();
  const csv = ["Röstgivare,Röst,Tidpunkt,Post,Page"];
  rows.forEach(v => csv.push(`"${v.from}","${v.to}","${v.ts}","${v.post}","${v.page}"`));
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([csv.join("\n")], { type: "text/csv" }));
  a.download = "rostdata.csv";
  a.click();
}
document.addEventListener("DOMContentLoaded", async () => {
  A = await loadArchive();
  fillThreads();
  const init = readURL();
  els.delay.value = String(init.delay || 200);
  st.fp = init.fp || "";
  st.sort = init.sort || "";
  st.sliderIndex = init.slider;
  const rv = els.view.find(r => r.value === init.view);
  if (rv) rv.checked = true;
  els.th.addEventListener("change", () => {
    const s = els.th.value || "";
    if (s) loadThread(s, false);
  });
  els.exp.addEventListener("click", exportCSV);
  // Byt view => hoppa till max
  els.view.forEach(r => r.addEventListener("change", () => {
    if (st.animTimer) {
      clearTimeout(st.animTimer);
      st.animTimer = null;
    }
    st.sliderIndex = null;
    rebuildSlider(false);
  }));
  els.animBtn.addEventListener("click", () => { if (!st.slug) return; play(); });
  els.delay.addEventListener("input", applyURL);
  els.slider.addEventListener("input", () => onSlider(false));
  // Byt filter => hoppa till max
  els.fp.addEventListener("change", () => {
    if (st.animTimer) {
      clearTimeout(st.animTimer);
      st.animTimer = null;
    }
    st.fp = els.fp.value || "";
    st.sliderIndex = null;
    rebuildSlider(false);
  });
  els.ths.forEach(th => {
    const k = th.dataset.sort;
    if (!k) return;
    th.addEventListener("click", () => {
      const cur = st.sort || `${k}-asc`;
      const desc = cur.startsWith(k) && cur.endsWith("-asc");
      st.sort = `${k}-${desc ? "desc" : "asc"}`;
      render();
      applyURL();
    });
  });
  if (init.thread && A.bySlug[init.thread]) {
    // Behåll URL-läget vid direktlänkad load:
    // - fp/sort/view/sliderIndex från URL respekteras
    // - men rebuild klämmer sliderIndex till max om den saknas/är för stor
    loadThread(init.thread, true);
    applyURL();
  } else {
    applyURL();
  }
});
