/* PipelineSketch app: canvas editor, panels and dialogs. Everything runs in the browser. MIT. */
(function () {
  'use strict';
  var PS = window.PS, C = PS.core, N = PS.nodes, D = PS.docs, HL = PS.highlight, IMP = PS.importer, TPL = PS.templates, ZIP = PS.zip, CRON = window.CronCore;
  var GEN = { airflow: PS.airflow, dagster: PS.dagster, prefect: PS.prefect, dbt: PS.dbt };
  var KEY = 'pipelinesketch:v1:', GRID = 20, MAX_UNDO = 150;
  var REPO_URL = 'https://github.com/shaikbasha1437/pipelinesketch';

  /* ---------------- small helpers ---------------- */
  function $(s, r) { return (r || document).querySelector(s); }
  function $$(s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); }
  var esc = HL.esc;
  function h(tag, attrs, kids) {
    var e = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (k) {
      var v = attrs[k];
      if (v == null || v === false) return;
      if (k === 'class') e.className = v;
      else if (k === 'text') e.textContent = v;
      else if (k === 'html') e.innerHTML = v;
      else if (k.slice(0, 2) === 'on' && typeof v === 'function') e.addEventListener(k.slice(2), v);
      else if (k === 'value') e.value = v;
      else if (k === 'checked') e.checked = !!v;
      else e.setAttribute(k, v === true ? '' : v);
    });
    (kids || []).forEach(function (c) { if (c != null && c !== false) e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c); });
    return e;
  }
  function isMobile() { return window.matchMedia('(max-width: 760px)').matches; }
  function isTyping(t) { t = t || document.activeElement; return !!t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.isContentEditable); }
  function snapV(v, on) { return on ? Math.round(v / GRID) * GRID : Math.round(v); }
  function rafThrottle(fn) { var q = false; return function () { if (q) return; q = true; requestAnimationFrame(function () { q = false; fn(); }); }; }
  function plural(n, w) { return n + ' ' + w + (n === 1 ? '' : 's'); }
  function fmtDate(t) { try { return new Date(t).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }); } catch (e) { return new Date(t).toISOString(); } }

  /* ---------------- toasts ---------------- */
  function toast(msg, kind, action) {
    var t = h('div', { class: 'toast ' + (kind || ''), role: 'status' }, [h('span', { text: msg })]);
    if (action) t.appendChild(h('button', { text: action.label, onclick: function () { action.run(); t.remove(); } }));
    $('#toasts').appendChild(t);
    setTimeout(function () { t.remove(); }, action ? 7000 : 3600);
    while ($('#toasts').children.length > 3) $('#toasts').firstChild.remove();
    return t;
  }

  /* ---------------- storage ---------------- */
  var store = {
    get: function (k) { try { return JSON.parse(localStorage.getItem(KEY + k)); } catch (e) { return null; } },
    set: function (k, v) { try { localStorage.setItem(KEY + k, JSON.stringify(v)); return true; } catch (e) { toast('Couldn\u2019t save to this browser (storage full or blocked). Use Share \u2192 Backup.', 'err'); return false; } },
    del: function (k) { try { localStorage.removeItem(KEY + k); } catch (e) { /* ignore */ } }
  };
  function listPipelines() { var x = store.get('index'); return Array.isArray(x) ? x : []; }
  function writePipeline(p) {
    p.updatedAt = Date.now();
    if (!store.set('p:' + p.id, p)) return false;
    var idx = listPipelines().filter(function (x) { return x.id !== p.id; });
    idx.unshift({ id: p.id, name: p.settings.name, nodes: p.nodes.length, updatedAt: p.updatedAt });
    store.set('index', idx); store.set('current', p.id);
    return true;
  }
  function readPipeline(id) { var raw = store.get('p:' + id); if (!raw) return null; try { return C.normalize(raw); } catch (e) { return null; } }
  function deletePipeline(id) { store.del('p:' + id); store.set('index', listPipelines().filter(function (x) { return x.id !== id; })); }

  /* ---------------- state ---------------- */
  var S = {
    p: null, sel: {}, selEdge: null, selGroups: {}, view: { x: 40, y: 40, k: 1 },
    theme: document.documentElement.getAttribute('data-theme') || 'dark',
    snap: store.get('snap') !== false, boxMode: false, undo: [], redo: [], issues: [], clip: null, pasteN: 0
  };
  var saveTimer = null;
  function scheduleSave() {
    $('#status-save').textContent = 'Saving\u2026';
    clearTimeout(saveTimer);
    saveTimer = setTimeout(flushSave, 300);
  }
  function flushSave() {
    clearTimeout(saveTimer); saveTimer = null;
    if (S.p && writePipeline(S.p)) $('#status-save').textContent = 'Saved locally \u00b7 ' + new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }

  /* ---------------- undo / redo ---------------- */
  // Call checkpoint() right BEFORE mutating S.p, then changed() after.
  function checkpoint() {
    S.undo.push(JSON.stringify(S.p));
    if (S.undo.length > MAX_UNDO) S.undo.shift();
    S.redo = [];
  }
  function restore(json) {
    S.p = C.normalize(JSON.parse(json));
    pruneSelection();
  }
  function undo() {
    if (!S.undo.length) { toast('Nothing to undo'); return; }
    S.redo.push(JSON.stringify(S.p)); restore(S.undo.pop()); changed(); refreshInspector();
  }
  function redo() {
    if (!S.redo.length) { toast('Nothing to redo'); return; }
    S.undo.push(JSON.stringify(S.p)); restore(S.redo.pop()); changed(); refreshInspector();
  }
  function pruneSelection() {
    Object.keys(S.sel).forEach(function (id) { if (!C.nodeById(S.p, id)) delete S.sel[id]; });
    Object.keys(S.selGroups).forEach(function (id) { if (!groupById(id)) delete S.selGroups[id]; });
    if (S.selEdge && !edgeById(S.selEdge)) S.selEdge = null;
  }
  function groupById(id) { return (S.p.groups || []).filter(function (g) { return g.id === id; })[0] || null; }
  function edgeById(id) { return S.p.edges.filter(function (e) { return e.id === id; })[0] || null; }
  function selectedIds() { return Object.keys(S.sel).filter(function (id) { return C.nodeById(S.p, id); }); }
  function clearSelection() { S.sel = {}; S.selEdge = null; S.selGroups = {}; }

  /* ---------------- rendering ---------------- */
  var svg, scene, viewport, wrap;
  function changed(opts) {
    S.issues = C.validate(S.p);
    render();
    if (!(opts && opts.noSave)) scheduleSave();
  }
  function issueMap() {
    var m = {};
    S.issues.forEach(function (i) {
      if (i.level === 'info') return;
      (i.nodes || (i.nodeId ? [i.nodeId] : [])).forEach(function (id) { if (m[id] !== 'error') m[id] = i.level; });
    });
    return m;
  }
  function render() {
    scene.innerHTML = D.svgBody(S.p, { theme: S.theme, selected: S.sel, issues: issueMap(), selectedEdge: S.selEdge, selectedGroups: S.selGroups, interactive: true });
    applyView();
    renderChrome();
    renderMinimap();
    if (!$('#issues').hidden) renderIssues();
  }
  var renderSoon = rafThrottle(function () { render(); });
  function renderChrome() {
    var s = S.p.settings, info = C.scheduleInfo(C.effectiveSchedule ? C.effectiveSchedule(S.p) : s.schedule, { timeZone: s.timezone, count: 1 });
    $('#pipe-name').textContent = s.name;
    $('#pipe-sched').textContent = (info.ok ? info.text : 'Invalid schedule') + ' \u00b7 ' + C.WAREHOUSES[s.warehouse].label;
    document.title = s.name + ' \u00b7 PipelineSketch';
    $('#btn-undo').disabled = !S.undo.length; $('#btn-redo').disabled = !S.redo.length;
    var e = S.issues.filter(function (i) { return i.level === 'error'; }).length, w = S.issues.filter(function (i) { return i.level === 'warning'; }).length;
    $('#status-issues').innerHTML = !e && !w ? '<span class="ok">\u2713 No issues</span>' :
      (e ? '<span class="e">\u2716 ' + plural(e, 'error') + '</span> ' : '') + (w ? '<span class="w">\u26a0 ' + plural(w, 'warning') + '</span>' : '');
    $('#status-count').textContent = plural(S.p.nodes.length, 'node') + ' \u00b7 ' + plural(S.p.edges.length, 'connection');
    $('#empty-hint').hidden = S.p.nodes.length > 0 || (S.p.groups || []).length > 0;
    $('#tool-snap').setAttribute('aria-pressed', String(S.snap));
    $('#tool-select').setAttribute('aria-pressed', String(S.boxMode));
  }
  function setTheme(t) {
    S.theme = t === 'light' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', S.theme);
    try { localStorage.setItem('pipelinesketch:theme', S.theme); } catch (e) { /* ignore */ }
    $('meta[name="theme-color"]').setAttribute('content', D.THEMES[S.theme].bg);
    $('#defs-holder').innerHTML = D.defs(S.theme);
    render();
  }

  /* ---------------- viewport ---------------- */
  function applyView() {
    var v = S.view;
    viewport.setAttribute('transform', 'translate(' + v.x + ',' + v.y + ') scale(' + v.k + ')');
    var g = GRID * v.k;
    wrap.style.backgroundSize = g + 'px ' + g + 'px';
    wrap.style.backgroundPosition = v.x + 'px ' + v.y + 'px';
    wrap.classList.toggle('no-grid', g < 6);
    $('#zoom-val').textContent = Math.round(v.k * 100) + '%';
    renderMinimapViewport();
  }
  function toWorld(cx, cy) { var r = svg.getBoundingClientRect(); return { x: (cx - r.left - S.view.x) / S.view.k, y: (cy - r.top - S.view.y) / S.view.k }; }
  function zoomAt(k, cx, cy) {
    var r = svg.getBoundingClientRect(), v = S.view;
    k = Math.max(0.15, Math.min(2.5, k));
    if (cx == null) { cx = r.left + r.width / 2; cy = r.top + r.height / 2; }
    var px = cx - r.left, py = cy - r.top;
    v.x = px - (px - v.x) * k / v.k; v.y = py - (py - v.y) * k / v.k; v.k = k;
    applyView();
  }
  function fitView(ids) {
    var r = svg.getBoundingClientRect(); if (!r.width) return;
    var b = ids && ids.length ? C.bounds(S.p, ids) : D.contentBounds(S.p);
    var pad = 60, bw = Math.max(b.x2 - b.x1, 1), bh = Math.max(b.y2 - b.y1, 1);
    var k = Math.max(0.15, Math.min(1.1, Math.min((r.width - pad * 2) / bw, (r.height - pad * 2 - 40) / bh)));
    S.view = { k: k, x: (r.width - bw * k) / 2 - b.x1 * k, y: (r.height - 40 - bh * k) / 2 - b.y1 * k };
    applyView();
  }
  function centerOn(id) {
    var n = C.nodeById(S.p, id); if (!n) return;
    var r = svg.getBoundingClientRect(), sz = C.nodeSize(n), v = S.view;
    if (v.k < 0.6) v.k = 0.9;
    var visibleW = r.width - (!isMobile() && !$('#inspector').hidden ? 0 : 0);
    v.x = visibleW / 2 - (n.x + sz.w / 2) * v.k;
    v.y = (isMobile() ? r.height * 0.3 : r.height / 2) - (n.y + sz.h / 2) * v.k;
    applyView();
  }
  function visibleCenter() { var r = svg.getBoundingClientRect(); return toWorld(r.left + r.width / 2, r.top + (isMobile() ? r.height * 0.35 : r.height / 2)); }

  /* ---------------- minimap ---------------- */
  var mm = { b: null, scale: 1, ox: 0, oy: 0 };
  function renderMinimap() {
    var box = $('#minimap'), m = $('#minimap-svg'), W = box.clientWidth, H = box.clientHeight;
    if (!W) return;
    var b = D.contentBounds(S.p), pad = 40;
    var bw = b.x2 - b.x1 + pad * 2, bh = b.y2 - b.y1 + pad * 2, sc = Math.min(W / bw, H / bh);
    mm = { b: b, scale: sc, ox: (W - bw * sc) / 2 - (b.x1 - pad) * sc, oy: (H - bh * sc) / 2 - (b.y1 - pad) * sc };
    var T = D.THEMES[S.theme], out = ['<g transform="translate(' + mm.ox + ',' + mm.oy + ') scale(' + sc + ')">'];
    (S.p.groups || []).forEach(function (g) { out.push('<rect x="' + g.x + '" y="' + g.y + '" width="' + g.w + '" height="' + g.h + '" fill="' + esc(g.color) + '" fill-opacity="0.15"/>'); });
    S.p.edges.forEach(function (e) { var d = D.edgePath(S.p, e); if (d) out.push('<path d="' + d + '" stroke="' + T.edge + '" stroke-width="' + (2 / sc) + '" fill="none"/>'); });
    S.p.nodes.forEach(function (n) { var s = C.nodeSize(n); out.push('<rect x="' + n.x + '" y="' + n.y + '" width="' + s.w + '" height="' + s.h + '" rx="8" fill="' + D.catColor(D.catOf(n)) + '" fill-opacity="' + (S.sel[n.id] ? 1 : 0.6) + '"/>'); });
    out.push('<rect class="vp" id="mm-vp" x="0" y="0" width="0" height="0"/></g>');
    m.innerHTML = out.join('');
    renderMinimapViewport();
  }
  function renderMinimapViewport() {
    var vp = $('#mm-vp'); if (!vp || !svg) return;
    var r = svg.getBoundingClientRect(), v = S.view;
    vp.setAttribute('x', -v.x / v.k); vp.setAttribute('y', -v.y / v.k);
    vp.setAttribute('width', r.width / v.k); vp.setAttribute('height', r.height / v.k);
  }
  function minimapMove(ev) {
    var r = $('#minimap').getBoundingClientRect(), sr = svg.getBoundingClientRect();
    var wx = (ev.clientX - r.left - mm.ox) / mm.scale, wy = (ev.clientY - r.top - mm.oy) / mm.scale;
    S.view.x = sr.width / 2 - wx * S.view.k; S.view.y = sr.height / 2 - wy * S.view.k;
    applyView();
  }

  /* ---------------- modal stack: dialogs and panels close via X, Cancel, Esc, backdrop and Back ---------------- */
  var stack = PS.modal.createModalStack({ history: window.history, confirm: function (msg) { return window.confirm(msg); } });
  var PANELS = { inspector: 1, issues: 1, palette: 1 };
  function syncBackdrop() {
    var open = Object.keys(PANELS).some(function (id) { return stack.isOpen(id); });
    $('#panel-backdrop').hidden = !(open && isMobile());
  }
  function topPanel() { var t = stack.top(); return t && PANELS[t.id] ? t.id : null; }
  function openPalette() {
    if (!isMobile()) { $('#palette-search').focus(); return; }
    stack.open({ id: 'palette', show: function () { $('#palette').classList.add('open'); syncBackdrop(); }, hide: function () { $('#palette').classList.remove('open'); syncBackdrop(); } });
  }
  function openIssues() {
    if (stack.isOpen('issues')) { renderIssues(); return; }
    stack.open({ id: 'issues', show: function () { $('#issues').hidden = false; renderIssues(); syncBackdrop(); }, hide: function () { $('#issues').hidden = true; syncBackdrop(); } });
  }
  function toggleIssues() { if (stack.isOpen('issues')) stack.requestClose('issues'); else openIssues(); }

  /* ---------------- validation panel ---------------- */
  function renderIssues() {
    var list = $('#issue-list'), real = S.issues;
    list.innerHTML = '';
    var e = real.filter(function (i) { return i.level === 'error'; }).length, w = real.filter(function (i) { return i.level === 'warning'; }).length;
    $('#issues-sum').textContent = e || w ? plural(e, 'error') + ', ' + plural(w, 'warning') : 'All good';
    if (!real.length) { list.appendChild(h('li', { class: 'ok' }, [h('span', { class: 'lv info', text: '\u2713' }), h('span', { text: 'No problems found. The exports should run as generated.' })])); return; }
    real.forEach(function (i) {
      var n = i.nodeId ? C.nodeById(S.p, i.nodeId) : null;
      var icon = i.level === 'error' ? '\u2716' : i.level === 'warning' ? '\u26a0' : '\u2139';
      list.appendChild(h('li', { tabindex: '0', role: 'button', 'data-issue': i.code, onclick: function () { focusIssue(i); }, onkeydown: function (ev) { if (ev.key === 'Enter') focusIssue(i); } }, [
        h('span', { class: 'lv ' + i.level, text: icon }),
        n ? h('span', { class: 'where', text: n.name }) : (i.fix === 'settings' ? h('span', { class: 'where', text: 'settings' }) : null),
        h('span', { text: i.message })
      ]));
    });
  }
  function focusIssue(i) {
    if (i.nodeId && C.nodeById(S.p, i.nodeId)) {
      clearSelection();
      (i.nodes || [i.nodeId]).forEach(function (id) { S.sel[id] = true; });
      if (isMobile()) stack.close('issues');
      render(); centerOn(i.nodeId); openInspector('node', i.nodeId, i.field);
    } else if (i.edgeId && edgeById(i.edgeId)) {
      clearSelection(); S.selEdge = i.edgeId; render(); openInspector('edge', i.edgeId);
    } else if (i.fix === 'settings') openSettings();
  }

  /* ---------------- canvas interactions ---------------- */
  var drag = null, pointers = {}, pinch = null, spaceDown = false;
  var ghost, marquee;
  function nodeIdsInGroups(gids) {
    var ids = {};
    gids.forEach(function (gid) {
      var g = groupById(gid); if (!g) return;
      S.p.nodes.forEach(function (n) { var s = C.nodeSize(n), cx = n.x + s.w / 2, cy = n.y + s.h / 2; if (cx >= g.x && cx <= g.x + g.w && cy >= g.y && cy <= g.y + g.h) ids[n.id] = 1; });
    });
    return Object.keys(ids);
  }
  function startMove(ev, w) {
    var nodeIds = selectedIds(), gids = Object.keys(S.selGroups);
    nodeIdsInGroups(gids).forEach(function (id) { if (nodeIds.indexOf(id) === -1) nodeIds.push(id); });
    var orig = {};
    nodeIds.forEach(function (id) { var n = C.nodeById(S.p, id); orig[id] = { x: n.x, y: n.y }; });
    gids.forEach(function (id) { var g = groupById(id); orig['g:' + id] = { x: g.x, y: g.y }; });
    drag = { kind: 'move', nodeIds: nodeIds, gids: gids, orig: orig, start: w, sx: ev.clientX, sy: ev.clientY, moved: false };
  }
  function onPointerDown(ev) {
    if (ev.button === 2) return;
    pointers[ev.pointerId] = { x: ev.clientX, y: ev.clientY };
    var ids = Object.keys(pointers);
    if (ids.length === 2) {
      var a = pointers[ids[0]], b = pointers[ids[1]];
      if (drag && drag.kind === 'move' && drag.moved) changed();
      drag = null; hideGhosts();
      pinch = { d: Math.hypot(a.x - b.x, a.y - b.y), k: S.view.k, mx: (a.x + b.x) / 2, my: (a.y + b.y) / 2, vx: S.view.x, vy: S.view.y };
      return;
    }
    if (ids.length > 2) return;
    try { svg.focus({ preventScroll: true }); } catch (e) { /* ignore */ }
    var t = ev.target, w = toWorld(ev.clientX, ev.clientY);
    var nodeEl = t.closest('[data-node]'), portOut = t.closest('[data-port-out]'), portIn = t.closest('[data-port-in]');
    var edgeEl = t.closest('[data-edge]'), grpRes = t.closest('[data-group-resize]'), grpEl = t.closest('[data-group]');
    var additive = ev.shiftKey || ev.ctrlKey || ev.metaKey;
    svg.setPointerCapture(ev.pointerId);
    if (ev.button === 1 || spaceDown) { drag = { kind: 'pan', sx: ev.clientX, sy: ev.clientY, vx: S.view.x, vy: S.view.y, moved: true }; svg.classList.add('panning'); return; }
    if (portOut && nodeEl) {
      var n = C.nodeById(S.p, nodeEl.getAttribute('data-node'));
      drag = { kind: 'connect', from: n.id, port: portOut.getAttribute('data-port-out'), p0: C.portPos(n, portOut.getAttribute('data-port-out'), 'out') };
      svg.classList.add('connecting');
      return;
    }
    if (portIn && nodeEl) {
      var tid = nodeEl.getAttribute('data-node'), tport = portIn.getAttribute('data-port-in');
      var ex = S.p.edges.filter(function (e) { return e.to === tid && e.toPort === tport; });
      if (ex.length) { // pick up the last connection into this port and re-route it
        var e0 = ex[ex.length - 1], src = C.nodeById(S.p, e0.from);
        drag = { kind: 'connect', from: src.id, port: e0.fromPort, p0: C.portPos(src, e0.fromPort, 'out'), detach: e0.id };
        svg.classList.add('connecting');
        return;
      }
    }
    if (nodeEl) {
      var id = nodeEl.getAttribute('data-node');
      if (additive) {
        if (S.sel[id]) delete S.sel[id]; else S.sel[id] = true;
        S.selEdge = null; render();
        if (!S.sel[id]) { drag = null; return; }
      } else if (!S.sel[id]) { S.sel = {}; S.sel[id] = true; S.selGroups = {}; S.selEdge = null; render(); }
      startMove(ev, w); drag.clickNode = additive ? null : id;
      return;
    }
    if (grpRes) {
      var g = groupById(grpRes.getAttribute('data-group-resize'));
      drag = { kind: 'resize', gid: g.id, w0: g.w, h0: g.h, start: w, moved: false, sx: ev.clientX, sy: ev.clientY };
      return;
    }
    if (edgeEl) {
      clearSelection(); S.selEdge = edgeEl.getAttribute('data-edge'); render();
      openInspector('edge', S.selEdge);
      drag = null; return;
    }
    if (grpEl) {
      var gid = grpEl.getAttribute('data-group');
      if (additive) { if (S.selGroups[gid]) delete S.selGroups[gid]; else S.selGroups[gid] = true; }
      else if (!S.selGroups[gid]) { clearSelection(); S.selGroups[gid] = true; }
      S.selEdge = null; render();
      startMove(ev, w); drag.clickGroup = additive ? null : gid;
      return;
    }
    if (ev.shiftKey || S.boxMode) { drag = { kind: 'marquee', start: w, add: ev.shiftKey, sx: ev.clientX, sy: ev.clientY, moved: false }; return; }
    drag = { kind: 'pan', sx: ev.clientX, sy: ev.clientY, vx: S.view.x, vy: S.view.y, moved: false };
    svg.classList.add('panning');
  }
  function onPointerMove(ev) {
    if (pointers[ev.pointerId]) pointers[ev.pointerId] = { x: ev.clientX, y: ev.clientY };
    if (pinch) {
      var ids = Object.keys(pointers); if (ids.length < 2) return;
      var a = pointers[ids[0]], b = pointers[ids[1]], d = Math.hypot(a.x - b.x, a.y - b.y);
      var mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2;
      S.view.x = pinch.vx + (mx - pinch.mx); S.view.y = pinch.vy + (my - pinch.my); S.view.k = pinch.k;
      zoomAt(pinch.k * d / Math.max(pinch.d, 1), mx, my);
      return;
    }
    if (!drag) return;
    var w = toWorld(ev.clientX, ev.clientY), far = Math.hypot(ev.clientX - (drag.sx || 0), ev.clientY - (drag.sy || 0)) > 4;
    if (drag.kind === 'pan') {
      if (far) drag.moved = true;
      S.view.x = drag.vx + ev.clientX - drag.sx; S.view.y = drag.vy + ev.clientY - drag.sy; applyView();
    } else if (drag.kind === 'move') {
      if (!drag.moved) { if (!far) return; drag.moved = true; checkpoint(); }
      var dx = w.x - drag.start.x, dy = w.y - drag.start.y, sn = S.snap && !ev.altKey;
      drag.nodeIds.forEach(function (id) { var n = C.nodeById(S.p, id), o = drag.orig[id]; n.x = snapV(o.x + dx, sn); n.y = snapV(o.y + dy, sn); });
      drag.gids.forEach(function (id) { var g = groupById(id), o = drag.orig['g:' + id]; g.x = snapV(o.x + dx, sn); g.y = snapV(o.y + dy, sn); });
      renderSoon();
    } else if (drag.kind === 'resize') {
      if (!drag.moved) { if (!far) return; drag.moved = true; checkpoint(); }
      var g = groupById(drag.gid), sn2 = S.snap && !ev.altKey;
      g.w = Math.max(160, snapV(drag.w0 + w.x - drag.start.x, sn2)); g.h = Math.max(90, snapV(drag.h0 + w.y - drag.start.y, sn2));
      renderSoon();
    } else if (drag.kind === 'connect') {
      ghost.setAttribute('d', D.curve(drag.p0.x, drag.p0.y, w.x, w.y));
      $$('.port.target', scene).forEach(function (x) { x.classList.remove('target'); });
      var tgt = connectTarget(ev.clientX, ev.clientY);
      if (tgt && tgt.el) tgt.el.classList.add('target');
    } else if (drag.kind === 'marquee') {
      if (far) drag.moved = true;
      var x = Math.min(w.x, drag.start.x), y = Math.min(w.y, drag.start.y);
      marquee.setAttribute('x', x); marquee.setAttribute('y', y);
      marquee.setAttribute('width', Math.abs(w.x - drag.start.x)); marquee.setAttribute('height', Math.abs(w.y - drag.start.y));
    }
  }
  function connectTarget(cx, cy) {
    var el = document.elementFromPoint(cx, cy); if (!el) return null;
    var nodeEl = el.closest && el.closest('[data-node]'); if (!nodeEl) return null;
    var n = C.nodeById(S.p, nodeEl.getAttribute('data-node')), t = N.get(n.type);
    if (!t.inPorts.length) return { node: n, port: null };
    var pin = el.closest('[data-port-in]'), port = pin ? pin.getAttribute('data-port-in') : null;
    if (!port) { // dropped on the card: first free input port, else the first one
      port = t.inPorts.filter(function (pt) { return !S.p.edges.some(function (e) { return e.to === n.id && e.toPort === pt; }); })[0] || t.inPorts[0];
    }
    return { node: n, port: port, el: nodeEl.querySelector('[data-port-in="' + port + '"]') };
  }
  function hideGhosts() { ghost.setAttribute('d', ''); marquee.setAttribute('width', 0); marquee.setAttribute('height', 0); svg.classList.remove('connecting', 'panning'); }
  function onPointerUp(ev) {
    delete pointers[ev.pointerId];
    if (pinch) { if (Object.keys(pointers).length < 2) pinch = null; return; }
    var d = drag; drag = null;
    hideGhosts();
    if (!d) return;
    if (d.kind === 'pan') {
      if (!d.moved) { // a click on empty canvas works like a backdrop: deselect and close the inspector
        clearSelection(); render();
        if (stack.isOpen('inspector')) stack.requestClose('inspector');
      }
    } else if (d.kind === 'move') {
      if (d.moved) changed();
      else if (d.clickNode) {
        if (Object.keys(S.sel).length > 1) { S.sel = {}; S.sel[d.clickNode] = true; render(); }
        openInspector('node', d.clickNode);
      } else if (d.clickGroup) { S.sel = {}; render(); openInspector('group', d.clickGroup); }
    } else if (d.kind === 'resize') { if (d.moved) changed(); }
    else if (d.kind === 'connect') finishConnect(d, ev.clientX, ev.clientY);
    else if (d.kind === 'marquee') {
      var w = toWorld(ev.clientX, ev.clientY);
      var r = { x: Math.min(w.x, d.start.x), y: Math.min(w.y, d.start.y), w: Math.abs(w.x - d.start.x), h: Math.abs(w.y - d.start.y) };
      if (!d.add) clearSelection();
      if (d.moved) {
        C.nodesInRect(S.p, r).forEach(function (id) { S.sel[id] = true; });
        (S.p.groups || []).forEach(function (g) { if (g.x >= r.x && g.y >= r.y && g.x + g.w <= r.x + r.w && g.y + g.h <= r.y + r.h) S.selGroups[g.id] = true; });
      }
      render();
      var k = selectedIds().length; if (k > 1) toast(plural(k, 'node') + ' selected');
    }
  }
  function finishConnect(d, cx, cy) {
    var tgt = connectTarget(cx, cy);
    if (!tgt) {
      if (d.detach) { checkpoint(); S.p.edges = S.p.edges.filter(function (e) { return e.id !== d.detach; }); changed(); toast('Connection removed', '', { label: 'Undo', run: undo }); }
      return;
    }
    if (!tgt.port) { toast(tgt.node.name + ' has no inputs', 'warn'); return; }
    if (d.detach) { var old = edgeById(d.detach); if (old && old.to === tgt.node.id && old.toPort === tgt.port) return; }
    var res = C.canConnect(S.p, d.from, d.port, tgt.node.id, tgt.port);
    if (!res.ok) { toast(res.reason, 'warn'); return; }
    checkpoint();
    if (d.detach) S.p.edges = S.p.edges.filter(function (e) { return e.id !== d.detach; });
    C.addEdge(S.p, d.from, d.port, tgt.node.id, tgt.port);
    changed();
    if (res.createsCycle) toast('That connection creates a cycle. Pipelines must be acyclic.', 'err', { label: 'Show', run: openIssues });
  }
  function onWheel(ev) {
    ev.preventDefault();
    var dy = ev.deltaMode === 1 ? ev.deltaY * 16 : ev.deltaY;
    if (ev.shiftKey && !ev.ctrlKey) { S.view.x -= dy; applyView(); return; }
    zoomAt(S.view.k * Math.exp(-dy * (ev.ctrlKey ? 0.01 : 0.0015)), ev.clientX, ev.clientY);
  }

  /* ---------------- palette ---------------- */
  function buildPalette() {
    var list = $('#palette-list'), q = ($('#palette-search').value || '').toLowerCase().trim();
    list.innerHTML = '';
    N.CATEGORIES.forEach(function (cat) {
      var types = N.TYPES.filter(function (t) { return t.category === cat.id && (!q || (t.label + ' ' + t.type + ' ' + cat.label).toLowerCase().indexOf(q) !== -1); });
      if (!types.length) return;
      var det = h('details', { class: 'pal-cat', open: true }, [h('summary', {}, [h('span', { class: 'dot', style: 'background:' + cat.color }), cat.label])]);
      types.forEach(function (t) {
        det.appendChild(h('button', { class: 'pal-item', 'data-type': t.type, title: t.label + ': drag onto the canvas or click to add' }, [
          h('span', { class: 'bar', style: 'background:' + cat.color }), h('span', { class: 'ic', text: t.icon }), h('span', { text: t.label })]));
      });
      list.appendChild(det);
    });
    if (!list.children.length) list.appendChild(h('p', { class: 'muted', style: 'padding:8px', text: 'No node matches \u201c' + q + '\u201d.' }));
  }
  var palDrag = null;
  function onPalDown(ev) {
    var item = ev.target.closest('.pal-item'); if (!item || ev.button !== 0) return;
    ev.preventDefault();
    palDrag = { type: item.getAttribute('data-type'), sx: ev.clientX, sy: ev.clientY, started: false, el: null, id: ev.pointerId };
  }
  function onPalMove(ev) {
    if (!palDrag || ev.pointerId !== palDrag.id) return;
    if (!palDrag.started) {
      if (Math.hypot(ev.clientX - palDrag.sx, ev.clientY - palDrag.sy) < 6) return;
      palDrag.started = true;
      var t = N.get(palDrag.type);
      palDrag.el = h('div', { class: 'drag-ghost', text: t.icon + ' ' + t.label });
      document.body.appendChild(palDrag.el);
      if (isMobile() && stack.isOpen('palette')) { $('#palette').classList.remove('open'); }
    }
    palDrag.el.style.left = (ev.clientX + 8) + 'px'; palDrag.el.style.top = (ev.clientY + 8) + 'px';
  }
  function onPalUp(ev) {
    if (!palDrag || ev.pointerId !== palDrag.id) return;
    var d = palDrag; palDrag = null;
    if (d.el) d.el.remove();
    if (!d.started) { addNodeClick(d.type); return; }
    var r = wrap.getBoundingClientRect();
    if (isMobile() && stack.isOpen('palette')) stack.close('palette');
    if (ev.clientX < r.left || ev.clientX > r.right || ev.clientY < r.top || ev.clientY > r.bottom) return;
    var w = toWorld(ev.clientX, ev.clientY), sz = { w: C.NODE_W, h: 64 };
    addNodeAt(d.type, w.x - sz.w / 2, w.y - sz.h / 2);
  }
  function addNodeAt(type, x, y, after) {
    checkpoint();
    var n = C.addNode(S.p, type, snapV(x, S.snap), snapV(y, S.snap));
    if (after) {
      var res = C.canConnect(S.p, after.id, N.get(after.type).outPorts[0], n.id, N.get(n.type).inPorts[0]);
      if (N.get(after.type).outPorts.length && N.get(n.type).inPorts.length && res.ok && !res.createsCycle) C.addEdge(S.p, after.id, N.get(after.type).outPorts[0], n.id, N.get(n.type).inPorts[0]);
      else after = null;
    }
    clearSelection(); S.sel[n.id] = true;
    changed();
    toast('Added ' + n.name + (after ? ' after ' + after.name : ''));
    if (!isMobile()) openInspector('node', n.id);
    return n;
  }
  function addNodeClick(type) {
    var ids = selectedIds(), after = ids.length === 1 ? C.nodeById(S.p, ids[0]) : null, x, y;
    if (after && N.get(after.type).outPorts.length && N.get(type).inPorts.length) {
      x = after.x + C.NODE_W + 80; y = after.y;
      while (S.p.nodes.some(function (m) { return Math.abs(m.x - x) < 40 && Math.abs(m.y - y) < 40; })) y += 100;
    } else {
      after = null;
      var c = visibleCenter(); x = c.x - C.NODE_W / 2; y = c.y - 32;
      while (S.p.nodes.some(function (m) { return Math.abs(m.x - x) < 30 && Math.abs(m.y - y) < 30; })) { x += 30; y += 30; }
    }
    if (isMobile() && stack.isOpen('palette')) stack.close('palette');
    var n = addNodeAt(type, x, y, after);
    var r = svg.getBoundingClientRect(), sx = n.x * S.view.k + S.view.x, sy = n.y * S.view.k + S.view.y;
    if (sx < 0 || sy < 0 || sx > r.width - 100 || sy > r.height - 60) centerOn(n.id);
  }

  /* ---------------- editing actions ---------------- */
  function deleteSelection() {
    var ids = selectedIds(), gids = Object.keys(S.selGroups), eid = S.selEdge;
    if (!ids.length && !gids.length && !eid) return;
    checkpoint();
    if (ids.length) C.removeNodes(S.p, ids);
    if (gids.length) S.p.groups = S.p.groups.filter(function (g) { return !S.selGroups[g.id]; });
    if (eid) S.p.edges = S.p.edges.filter(function (e) { return e.id !== eid; });
    var what = [ids.length ? plural(ids.length, 'node') : '', gids.length ? plural(gids.length, 'group') : '', eid ? '1 connection' : ''].filter(Boolean).join(', ');
    clearSelection();
    if (stack.isOpen('inspector')) stack.close('inspector');
    changed();
    toast('Deleted ' + what, '', { label: 'Undo', run: undo });
  }
  function copySelection(cut) {
    var ids = selectedIds(); if (!ids.length) { toast('Select nodes to copy'); return false; }
    var frag = C.copyFragment(S.p, ids);
    S.clip = frag; S.pasteN = 0;
    try { if (navigator.clipboard && window.isSecureContext) navigator.clipboard.writeText(JSON.stringify(frag)).catch(function () {}); } catch (e) { /* internal clipboard still works */ }
    if (cut) { deleteSelection(); } else toast('Copied ' + plural(ids.length, 'node'));
    return true;
  }
  function pasteFrag(frag) {
    if (!frag || !frag.nodes || !frag.nodes.length) { toast('Nothing to paste'); return; }
    S.pasteN++;
    checkpoint();
    var added = C.pasteFragment(S.p, frag, 40 * S.pasteN, 40 * S.pasteN);
    clearSelection(); added.forEach(function (id) { S.sel[id] = true; });
    changed();
    toast('Pasted ' + plural(added.length, 'node'));
  }
  function duplicateSelection() { if (copySelection(false)) pasteFrag(S.clip); }
  function selectAll() { clearSelection(); S.p.nodes.forEach(function (n) { S.sel[n.id] = true; }); render(); }
  function groupSelection() {
    var ids = selectedIds(), b, g;
    checkpoint();
    if (ids.length) { b = C.bounds(S.p, ids); g = { id: C.newId('g'), title: 'Group', text: '', x: b.x1 - 20, y: b.y1 - 44, w: b.x2 - b.x1 + 40, h: b.y2 - b.y1 + 64, color: '#3b82f6' }; }
    else { var c = visibleCenter(); g = { id: C.newId('g'), title: 'Note', text: 'Double-check the source contract', x: snapV(c.x - 160, true), y: snapV(c.y - 80, true), w: 320, h: 160, color: '#eab308' }; }
    S.p.groups.unshift(g); // groups render beneath nodes
    clearSelection(); S.selGroups[g.id] = true;
    changed();
    openInspector('group', g.id);
  }
  function nudge(dx, dy) {
    var ids = selectedIds(), gids = Object.keys(S.selGroups); if (!ids.length && !gids.length) return;
    checkpoint();
    ids.forEach(function (id) { var n = C.nodeById(S.p, id); n.x += dx; n.y += dy; });
    gids.forEach(function (id) { var g = groupById(id); g.x += dx; g.y += dy; });
    changed();
  }
  function autoLayout() {
    if (!S.p.nodes.length) return;
    checkpoint();
    var members = {};
    (S.p.groups || []).forEach(function (g) { members[g.id] = nodeIdsInGroups([g.id]); });
    var pos = C.layout(S.p);
    S.p.nodes.forEach(function (n) { if (pos[n.id]) { n.x = pos[n.id].x; n.y = pos[n.id].y; } });
    (S.p.groups || []).forEach(function (g) {
      if (!members[g.id].length) return;
      var b = C.bounds(S.p, members[g.id]);
      g.x = b.x1 - 20; g.y = b.y1 - 44; g.w = b.x2 - b.x1 + 40; g.h = b.y2 - b.y1 + 64;
    });
    changed(); fitView();
    toast('Auto-layout applied', '', { label: 'Undo', run: undo });
  }

  /* ---------------- inspector ---------------- */
  var insp = null; // { kind, id, undoLen, snap, redo, editing }
  function openInspector(kind, id, focusField) {
    insp = { kind: kind, id: id, undoLen: S.undo.length, snap: JSON.stringify(S.p), redo: S.redo.slice(), editing: null };
    if (!stack.isOpen('inspector')) {
      stack.open({ id: 'inspector', show: function () { $('#inspector').hidden = false; syncBackdrop(); },
        hide: function () { $('#inspector').hidden = true; insp = null; syncBackdrop(); setTimeout(renderMinimap, 0); } });
      setTimeout(renderMinimap, 0);
    }
    buildInspector(focusField);
  }
  function cancelInspector() {
    if (insp && S.undo.length > insp.undoLen) {
      S.p = C.normalize(JSON.parse(insp.snap)); S.undo.length = insp.undoLen; S.redo = insp.redo;
      pruneSelection(); changed();
      toast('Changes discarded');
    }
    stack.close('inspector');
  }
  function refreshInspector() {
    if (!insp) return;
    var gone = insp.kind === 'node' ? !C.nodeById(S.p, insp.id) : insp.kind === 'edge' ? !edgeById(insp.id) : !groupById(insp.id);
    if (gone) { stack.close('inspector'); return; }
    insp.undoLen = Math.min(insp.undoLen, S.undo.length);
    buildInspector();
  }
  // One undo step per field edit session (focus \u2192 change).
  function beginEditUndo(key) { if (insp && insp.editing !== key) { checkpoint(); insp.editing = key; } }
  function endEdit() { if (insp) insp.editing = null; }
  function liveChanged() { S.issues = C.validate(S.p); renderSoon(); scheduleSave(); updateInspectorIssues(); }

  function fieldWrap(label, input, opts) {
    opts = opts || {};
    return h('div', { class: 'field' + (opts.row ? ' check-row' : ''), 'data-field': opts.key || null }, opts.row ? [input, h('label', { for: input.id, text: label })] : [
      h('label', { for: input.id }, [label, opts.required ? h('span', { class: 'req', text: '*', title: 'required' }) : null]), input,
      opts.extra || null, opts.help ? h('div', { class: 'help', text: opts.help }) : null]);
  }
  var uid = 0;
  function inputFor(f, value, onInput) {
    var id = 'f' + (++uid), el, beginEdit = f.draft ? function () {} : beginEditUndo;
    if (f.type === 'select') {
      el = h('select', { id: id }, (f.options || []).map(function (o) { return h('option', { value: o, text: o }); }));
      el.value = value == null ? (f.default || f.options[0]) : value;
      el.addEventListener('change', function () { beginEdit(f.key); onInput(el.value); endEdit(); });
      return el;
    }
    if (f.type === 'bool') {
      el = h('input', { id: id, type: 'checkbox', checked: !!value });
      el.addEventListener('change', function () { beginEdit(f.key); onInput(el.checked); endEdit(); });
      return el;
    }
    if (f.type === 'code' || f.type === 'textarea') {
      el = h('textarea', { id: id, class: f.type === 'code' ? 'code' : '', spellcheck: 'false', rows: f.type === 'code' ? 8 : 3, placeholder: f.placeholder || '' });
      el.value = value == null ? '' : value;
      if (f.type === 'code') el.addEventListener('keydown', function (ev) {
        if (ev.key === 'Tab' && !ev.shiftKey) { ev.preventDefault(); var s = el.selectionStart; el.setRangeText('    ', s, el.selectionEnd, 'end'); el.dispatchEvent(new Event('input')); }
      });
    } else {
      el = h('input', { id: id, type: f.type === 'number' ? 'number' : 'text', placeholder: f.placeholder || '', spellcheck: 'false', autocomplete: 'off' });
      el.value = f.type === 'list' ? C.asList(value).join(', ') : (value == null ? '' : value);
    }
    el.addEventListener('input', function () {
      beginEdit(f.key);
      var v = el.value;
      if (f.type === 'number') v = v === '' ? '' : Number(v);
      if (f.type === 'list') v = C.asList(v);
      onInput(v);
    });
    el.addEventListener('change', endEdit);
    return el;
  }
  // Schedule input with presets, an English description and the next runs.
  function cronField(value, tz, onInput, label, draft) {
    var f = { key: 'cron', type: 'text', placeholder: '0 6 * * *  or  @daily', draft: !!draft };
    var preview = h('div', { class: 'cron-preview' });
    var input = inputFor(f, value, function (v) { onInput(v); paint(v); });
    function paint(v) {
      var tzNow = typeof tz === 'function' ? tz() : tz, info = C.scheduleInfo(v, { timeZone: tzNow || 'UTC', count: 5 });
      preview.className = 'cron-preview' + (info.ok ? '' : ' bad');
      preview.innerHTML = '';
      if (!info.ok) { preview.textContent = info.error; return; }
      preview.appendChild(h('div', { text: info.text }));
      if (info.next && info.next.length) {
        var fmt; try { fmt = new Intl.DateTimeFormat(undefined, { timeZone: tzNow || 'UTC', weekday: 'short', year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false }); } catch (e) { fmt = new Intl.DateTimeFormat(undefined, { timeZone: 'UTC', dateStyle: 'medium', timeStyle: 'short' }); }
        preview.appendChild(h('div', { class: 'muted', style: 'margin-top:4px', text: 'Next runs (' + (tzNow || 'UTC') + '):' }));
        preview.appendChild(h('ol', {}, info.next.map(function (t) { return h('li', { text: fmt.format(new Date(t)) }); })));
      }
      $$('.chip', chips).forEach(function (c) { c.classList.toggle('on', c.getAttribute('data-v') === String(v).trim()); });
    }
    var chips = h('div', { class: 'chips' }, C.PRESETS.map(function (pr) {
      return h('button', { type: 'button', class: 'chip', 'data-v': pr.value, title: pr.value, text: pr.label, onclick: function () { input.value = pr.value; input.dispatchEvent(new Event('input')); endEdit(); } });
    }));
    paint(value);
    var wrapEl = fieldWrap(label || 'Schedule', input, { key: 'cron', required: true, extra: h('div', {}, [chips, preview]), help: 'Cron (minute hour day month weekday), a preset like @daily, or None for manual runs.' });
    wrapEl.repaint = function () { paint(input.value); };
    return wrapEl;
  }
  function updateInspectorIssues() {
    var box = $('#insp-issues'); if (!box || !insp || insp.kind !== 'node') return;
    var list = S.issues.filter(function (i) { return i.nodeId === insp.id || (i.nodes && i.nodes.indexOf(insp.id) !== -1); });
    box.innerHTML = '';
    list.forEach(function (i) { box.appendChild(h('div', { class: 'notice ' + (i.level === 'error' ? 'err' : 'warn'), style: 'margin:0 0 8px', text: (i.level === 'error' ? '\u2716 ' : '\u26a0 ') + i.message })); });
    $$('#insp-body .field[data-field]').forEach(function (fe) {
      var k = fe.getAttribute('data-field'), bad = list.filter(function (i) { return i.field === k; })[0], m = $('.err-msg', fe);
      if (m) m.remove();
      if (bad) fe.appendChild(h('div', { class: 'err-msg', text: bad.message.replace(/^[^:]*: /, '') }));
    });
  }
  function connList(n) {
    var ul = h('ul', { class: 'conn-list' });
    var ins = C.upstreamEdges(S.p, n.id), outs = C.downstreamEdges(S.p, n.id);
    function row(e, dir) {
      var other = C.nodeById(S.p, dir === 'in' ? e.from : e.to); if (!other) return;
      var label = dir === 'in' ? '\u2190 ' + other.name + (e.fromPort !== 'out' ? ' (' + e.fromPort + ')' : '') + (e.toPort !== 'in' ? ' \u2192 ' + e.toPort : '') : '\u2192 ' + other.name + (e.fromPort !== 'out' ? ' from ' + e.fromPort : '') + (e.toPort !== 'in' ? ' (' + e.toPort + ')' : '');
      ul.appendChild(h('li', {}, [h('span', { text: label, title: label }), h('button', { class: 'x', title: 'Remove connection', 'aria-label': 'Remove connection', text: '\u2715', onclick: function () {
        checkpoint(); S.p.edges = S.p.edges.filter(function (x) { return x.id !== e.id; }); changed(); buildInspector(); } })]));
    }
    ins.forEach(function (e) { row(e, 'in'); }); outs.forEach(function (e) { row(e, 'out'); });
    if (!ins.length && !outs.length) ul.appendChild(h('li', { class: 'muted', text: 'Not connected. Drag from a right-hand port to another node.' }));
    return ul;
  }
  function sqlPreview(n) {
    var kind = C.kindOf(n);
    if (kind !== 'sql') return null;
    var pre = h('pre', { class: 'code sql-preview' });
    function paint() {
      try { pre.innerHTML = HL.highlight(C.compileSelect(S.p, n, function (x) { return C.relation(S.p, x); }, S.p.settings.warehouse), 'sql'); }
      catch (e) { pre.textContent = '-- ' + e.message; }
    }
    paint(); pre.repaint = paint;
    return h('div', { class: 'insp-section' }, [h('h4', { text: 'Compiled SQL (' + C.WAREHOUSES[S.p.settings.warehouse].label + ')' }), pre]);
  }
  function buildInspector(focusField) {
    var body = $('#insp-body'); body.innerHTML = '';
    if (!insp) return;
    if (insp.kind === 'node') {
      var n = C.nodeById(S.p, insp.id); if (!n) { stack.close('inspector'); return; }
      var t = N.get(n.type), color = t.color, preview = sqlPreview(n);
      $('#insp-title').textContent = n.name;
      body.appendChild(h('div', { class: 'type-chip', style: 'border-color:' + color }, [t.icon + ' ' + t.label]));
      body.appendChild(h('div', { id: 'insp-issues' }));
      var nameIn = h('input', { id: 'f-name', type: 'text', value: n.name, spellcheck: 'false', autocomplete: 'off' });
      nameIn.addEventListener('change', function () {
        var want = C.toIdent(nameIn.value, n.name);
        if (want === n.name) { nameIn.value = n.name; return; }
        checkpoint(); var got = C.renameNode(S.p, n.id, want); nameIn.value = got; $('#insp-title').textContent = got; changed();
        if (got !== nameIn.value.trim()) toast('Renamed to ' + got);
      });
      body.appendChild(fieldWrap('Name', nameIn, { key: 'name', help: 'Becomes the task id, table and model name: letters, numbers and underscores.' }));
      t.fields.forEach(function (f) {
        var onIn = function (v) { n.config[f.key] = v; liveChanged(); if (preview) preview.querySelector('pre').repaint(); };
        if (f.type === 'cron') { body.appendChild(cronField(n.config[f.key], function () { return S.p.settings.timezone; }, onIn, f.label)); return; }
        var input = inputFor(f, n.config[f.key], onIn);
        body.appendChild(fieldWrap(f.label, input, { key: f.key, required: f.required, help: f.help, row: f.type === 'bool' }));
      });
      if (['extract', 'wh_source', 'python'].indexOf(C.kindOf(n)) !== -1) {
        var colIn = inputFor({ key: 'columns', type: 'list', placeholder: 'order_id, amount, updated_at' }, n.columns, function (v) { n.columns = v; liveChanged(); });
        body.appendChild(fieldWrap('Known output columns', colIn, { key: 'columns', help: 'Optional. Used for docs, the dbt schema and column-aware checks.' }));
      }
      var notes = inputFor({ key: 'notes', type: 'textarea', placeholder: 'Why this step exists, owners, gotchas\u2026' }, n.notes, function (v) { n.notes = v; scheduleSave(); });
      body.appendChild(fieldWrap('Notes', notes, { key: 'notes', help: 'Shown in the Markdown doc and as the task docstring.' }));
      if (preview) body.appendChild(preview);
      body.appendChild(h('div', { class: 'insp-section' }, [h('h4', { text: 'Connections' }), connList(n)]));
      body.appendChild(h('div', { class: 'insp-section row' }, [
        h('button', { class: 'btn small', text: 'Duplicate', onclick: function () { S.sel = {}; S.sel[n.id] = true; duplicateSelection(); } }),
        h('button', { class: 'btn small danger', text: 'Delete node', onclick: function () { clearSelection(); S.sel[n.id] = true; deleteSelection(); } })]));
      updateInspectorIssues();
      if (focusField) { var fe = $('#insp-body .field[data-field="' + focusField + '"] input, #insp-body .field[data-field="' + focusField + '"] textarea, #insp-body .field[data-field="' + focusField + '"] select'); if (fe) setTimeout(function () { fe.focus(); }, 30); }
    } else if (insp.kind === 'edge') {
      var e = edgeById(insp.id); if (!e) { stack.close('inspector'); return; }
      var a = C.nodeById(S.p, e.from), b = C.nodeById(S.p, e.to);
      $('#insp-title').textContent = 'Connection';
      body.appendChild(h('p', {}, [h('code', { text: a.name + (e.fromPort !== 'out' ? '.' + e.fromPort : '') }), ' \u2192 ', h('code', { text: b.name + (e.toPort !== 'in' ? '.' + e.toPort : '') })]));
      var tb = N.get(b.type);
      if (tb.inPorts.length > 1) {
        var sel = h('select', { id: 'f-toport' }, tb.inPorts.map(function (pt) { return h('option', { value: pt, text: pt }); })); sel.value = e.toPort;
        sel.addEventListener('change', function () { checkpoint(); e.toPort = sel.value; changed(); });
        body.appendChild(fieldWrap('Into port', sel, {}));
      }
      body.appendChild(h('p', { class: 'muted', text: 'Tip: drag the arrow end off a node input port to re-route or remove it.' }));
      body.appendChild(h('button', { class: 'btn danger', text: 'Delete connection', onclick: function () { clearSelection(); S.selEdge = e.id; deleteSelection(); } }));
    } else if (insp.kind === 'group') {
      var g = groupById(insp.id); if (!g) { stack.close('inspector'); return; }
      $('#insp-title').textContent = 'Group / comment';
      var ti = inputFor({ key: 'title', type: 'text' }, g.title, function (v) { g.title = v; liveChanged(); });
      var tx = inputFor({ key: 'text', type: 'textarea', placeholder: 'A comment shown under the title' }, g.text, function (v) { g.text = v; liveChanged(); });
      var colors = ['#3b82f6', '#22c55e', '#eab308', '#a855f7', '#ef4444', '#64748b', '#14b8a6'];
      var cs = h('select', { id: 'f-gcolor' }, colors.map(function (c) { return h('option', { value: c, text: c }); })); cs.value = colors.indexOf(g.color) === -1 ? colors[0] : g.color;
      cs.addEventListener('change', function () { checkpoint(); g.color = cs.value; changed(); });
      body.appendChild(fieldWrap('Title', ti, { key: 'title' }));
      body.appendChild(fieldWrap('Comment', tx, { key: 'text', help: 'Groups also become subgraphs in the Mermaid export.' }));
      body.appendChild(fieldWrap('Colour', cs, {}));
      var inside = nodeIdsInGroups([g.id]);
      body.appendChild(h('p', { class: 'muted', text: inside.length ? 'Contains ' + plural(inside.length, 'node') + '. Dragging the group moves them too.' : 'Empty: drag nodes inside, or use it as a comment.' }));
      body.appendChild(h('div', { class: 'row' }, [
        inside.length ? h('button', { class: 'btn small', text: 'Fit to contents', onclick: function () { checkpoint(); var bb = C.bounds(S.p, inside); g.x = bb.x1 - 20; g.y = bb.y1 - 44; g.w = bb.x2 - bb.x1 + 40; g.h = bb.y2 - bb.y1 + 64; changed(); } }) : null,
        h('button', { class: 'btn small danger', text: 'Delete group (keep nodes)', onclick: function () { clearSelection(); S.selGroups[g.id] = true; deleteSelection(); } })]));
    }
  }

  /* ---------------- pipeline settings dialog ---------------- */
  function timezones() {
    try { if (Intl.supportedValuesOf) return ['UTC'].concat(Intl.supportedValuesOf('timeZone').filter(function (z) { return z !== 'UTC'; })); } catch (e) { /* older browsers */ }
    return ['UTC', 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles', 'Europe/London', 'Europe/Berlin', 'Asia/Kolkata', 'Asia/Singapore', 'Asia/Tokyo', 'Australia/Sydney'];
  }
  function openSettings() {
    var draft = C.clone(S.p.settings), body = h('div', {});
    function txt(key, label, help, type) {
      var f = { key: key, type: type || 'text', draft: true };
      var el = inputFor(f, draft[key], function (v) { draft[key] = v; });
      return fieldWrap(label, el, { key: key, help: help, row: type === 'bool' });
    }
    // Settings edits live in a draft, so they never touch undo until Save.
    var nameF = txt('name', 'Pipeline name (DAG id)', 'Used as the DAG id, Dagster job, Prefect flow and dbt project name.');
    body.appendChild(nameF);
    body.appendChild(txt('description', 'Description'));
    body.appendChild(h('div', { class: 'grid2' }, [txt('owner', 'Owner'), txt('tags', 'Tags', null, 'list')]));
    var tzIn = h('input', { id: 'f-tz', type: 'text', list: 'tz-list', value: draft.timezone, autocomplete: 'off', spellcheck: 'false' });
    var tzList = h('datalist', { id: 'tz-list' }, timezones().map(function (z) { return h('option', { value: z }); }));
    var cron = cronField(draft.schedule, function () { return draft.timezone; }, function (v) { draft.schedule = v; }, 'Schedule', true);
    tzIn.addEventListener('input', function () { draft.timezone = tzIn.value.trim() || 'UTC'; cron.repaint(); });
    body.appendChild(cron);
    if (S.p.nodes.some(function (n) { return n.type === 'schedule'; })) body.appendChild(h('div', { class: 'notice warn', style: 'margin:0 0 10px', text: 'A Schedule node on the canvas overrides this schedule.' }));
    body.appendChild(h('div', { class: 'grid2' }, [fieldWrap('Time zone', tzIn, { key: 'timezone', extra: tzList, help: 'IANA name, e.g. America/Chicago. Schedules run in this zone.' }), txt('startDate', 'Start date', 'YYYY-MM-DD')]));
    body.appendChild(h('div', { class: 'grid2' }, [txt('retries', 'Retries per task', null, 'number'), txt('retryDelayMin', 'Retry delay (minutes)', null, 'number')]));
    body.appendChild(txt('catchup', 'Catch up missed runs (Airflow catchup)', null, 'bool'));
    var whSel = h('select', { id: 'f-wh' }, Object.keys(C.WAREHOUSES).map(function (k) { return h('option', { value: k, text: C.WAREHOUSES[k].label }); }));
    whSel.value = draft.warehouse; whSel.addEventListener('change', function () { draft.warehouse = whSel.value; });
    body.appendChild(h('div', { class: 'insp-section' }, [h('h4', { text: 'Warehouse' })]));
    body.appendChild(h('div', { class: 'grid2' }, [fieldWrap('Warehouse', whSel, { key: 'warehouse', help: 'Sets the SQL dialect and the dbt adapter.' }), txt('dbtProjectDir', 'dbt project dir (in Airflow)')]));
    body.appendChild(h('div', { class: 'grid2' }, [txt('rawSchema', 'Raw schema', 'Extracts land here.'), txt('stagingSchema', 'Staging schema', 'SQL and Python steps write here.')]));
    body.appendChild(txt('failureEmail', 'Email on failure', 'Optional. Airflow emails this address when a task fails.'));
    body.appendChild(h('div', { class: 'insp-section' }, [h('h4', { text: 'Connection IDs' }), h('p', { class: 'muted', style: 'margin:0 0 8px', text: 'Airflow connection ids; Dagster and Prefect read the matching environment variables (see the export header).' })]));
    var cg = h('div', { class: 'grid2' });
    Object.keys(draft.conns).forEach(function (k) {
      var el = h('input', { id: 'f-conn-' + k, type: 'text', value: draft.conns[k] || '', placeholder: k === 'warehouse' ? C.WAREHOUSES[draft.warehouse].conn : '', spellcheck: 'false' });
      el.addEventListener('input', function () { draft.conns[k] = el.value.trim(); });
      cg.appendChild(fieldWrap(k === 'warehouse' ? 'Warehouse' : k.charAt(0).toUpperCase() + k.slice(1), el, {}));
    });
    body.appendChild(cg);
    openDialog('settings', { title: 'Pipeline settings', cls: 'mid', body: body, buttons: [
      { label: 'Cancel', cancel: true },
      { label: 'Save settings', primary: true, run: function () {
        var name = C.toIdent(draft.name || '', 'my_pipeline');
        var sched = C.scheduleInfo(draft.schedule, { timeZone: 'UTC' });
        if (!sched.ok) { toast('Fix the schedule first: ' + sched.error, 'err'); return false; }
        try { new Intl.DateTimeFormat('en-US', { timeZone: draft.timezone }); } catch (e) { toast('Unknown time zone ' + draft.timezone, 'err'); return false; }
        draft.name = name; draft.tags = C.asList(draft.tags);
        draft.retries = Number(draft.retries) || 0; draft.retryDelayMin = Number(draft.retryDelayMin) || 0;
        checkpoint(); S.p.settings = draft; changed();
        toast('Settings saved');
      } }] });
  }

  /* ---------------- generic dialog ---------------- */
  var lastFocus = null;
  function openDialog(id, o) {
    if (stack.isOpen(id)) return null;
    var root = h('div', { class: 'modal', 'data-modal': id, role: 'dialog', 'aria-modal': 'true', 'aria-label': o.title });
    var closeBtn = h('button', { class: 'x', 'aria-label': 'Close', title: 'Close (Esc)', text: '\u2715', 'data-role': 'x', onclick: function () { stack.requestClose(id); } });
    var head = h('div', { class: 'panel-head' }, [h('h3', { text: o.title }), o.headExtra || null, h('span', { class: 'spacer' }), closeBtn]);
    var foot = o.buttons ? h('div', { class: 'panel-foot' }, o.buttons.map(function (b) {
      if (b.node) return b.node;
      return h('button', { class: 'btn' + (b.primary ? ' primary' : '') + (b.danger ? ' danger' : ''), text: b.label, 'data-role': b.cancel ? 'cancel' : (b.role || null), style: b.left ? 'margin-right:auto' : null,
        onclick: function () {
          if (b.cancel) { stack.requestClose(id); return; }
          var r = b.run ? b.run() : undefined;
          if (r === false || b.keepOpen) return;
          stack.close(id).then(function () { if (b.after) b.after(); });
        } });
    })) : null;
    var sheet = h('div', { class: 'sheet ' + (o.cls || '') }, [head, o.raw ? o.body : h('div', { class: 'sheet-body' }, [o.body]), foot]);
    root.appendChild(h('div', { class: 'backdrop', 'data-role': 'backdrop', onclick: function () { stack.requestClose(id); } }));
    root.appendChild(sheet);
    stack.open({ id: id, isDirty: o.isDirty,
      show: function () {
        lastFocus = document.activeElement;
        $('#modal-root').appendChild(root);
        var f = o.focus ? $(o.focus, root) : $('.sheet-body input, .sheet-body textarea, .sheet-body select', root);
        try { (f || closeBtn).focus({ preventScroll: true }); } catch (e) { /* ignore */ }
        if (o.onShow) o.onShow(root);
      },
      hide: function () { root.remove(); if (o.onHide) o.onHide(); try { (lastFocus && document.contains(lastFocus) ? lastFocus : svg).focus({ preventScroll: true }); } catch (e) { /* ignore */ } } });
    return root;
  }
  function closeThen(id, fn) { return stack.close(id).then(fn); }
  function confirmDialog(msg, okLabel, danger) {
    return new Promise(function (resolve) {
      var done = false;
      openDialog('confirm', { title: 'Please confirm', body: h('p', { style: 'margin:0', text: msg }), onHide: function () { if (!done) resolve(false); },
        buttons: [{ label: 'Cancel', cancel: true }, { label: okLabel || 'OK', primary: !danger, danger: danger, run: function () { done = true; }, after: function () { resolve(true); } }] });
    });
  }

  /* ---------------- files ---------------- */
  function download(name, data, mime) {
    var blob = data instanceof Blob ? data : new Blob([data], { type: mime || 'text/plain;charset=utf-8' });
    var url = URL.createObjectURL(blob), a = h('a', { href: url, download: name });
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function () { URL.revokeObjectURL(url); }, 4000);
  }
  function copyText(text) {
    function fallback() {
      var ta = h('textarea', { style: 'position:fixed;left:-9999px;top:0' }); ta.value = text; document.body.appendChild(ta); ta.select();
      var ok = false; try { ok = document.execCommand('copy'); } catch (e) { ok = false; }
      ta.remove(); toast(ok ? 'Copied to clipboard' : 'Copy failed: select the text and copy it manually', ok ? '' : 'warn');
    }
    if (navigator.clipboard && window.isSecureContext) navigator.clipboard.writeText(text).then(function () { toast('Copied to clipboard'); }, fallback);
    else fallback();
  }
  function svgToPng(svgText, scale) {
    return new Promise(function (resolve, reject) {
      var m = /width="(\d+)" height="(\d+)"/.exec(svgText), w = m ? +m[1] : 800, hgt = m ? +m[2] : 600;
      scale = Math.min(scale || 2, 8000 / w, 8000 / hgt);
      var img = new Image();
      img.onload = function () {
        var c = document.createElement('canvas'); c.width = Math.round(w * scale); c.height = Math.round(hgt * scale);
        var ctx = c.getContext('2d'); ctx.scale(scale, scale); ctx.drawImage(img, 0, 0, w, hgt);
        c.toBlob(function (b) { if (b) resolve(b); else reject(new Error('PNG export failed')); }, 'image/png');
      };
      img.onerror = function () { reject(new Error('Couldn\u2019t render the diagram')); };
      img.src = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgText);
    });
  }
  function bytesOf(s) { return new TextEncoder().encode(s); }

  /* ---------------- export dialog ---------------- */
  function exportTargets(p) {
    var name = p.settings.name;
    return [
      { id: 'airflow', label: 'Airflow 2.x', lang: 'python', file: name + '.py', make: function () { return GEN.airflow.generate(p); }, note: 'Copy into your dags/ folder. Required provider packages are listed in the file header.' },
      { id: 'dbt', label: 'dbt project', multi: true, make: function () { return GEN.dbt.generate(p); }, note: 'Unzip, copy profiles.example.yml to ~/.dbt/profiles.yml, then run dbt build. Extract, Python and sink steps stay in the orchestrator.' },
      { id: 'dagster', label: 'Dagster', lang: 'python', file: name + '_dagster.py', make: function () { return GEN.dagster.generate(p); }, note: 'Run with dagster dev -f ' + name + '_dagster.py. Set WAREHOUSE_URL and the connection env vars from the header.' },
      { id: 'prefect', label: 'Prefect', lang: 'python', file: name + '_prefect.py', make: function () { return GEN.prefect.generate(p); }, note: 'python ' + name + '_prefect.py --once runs it now; without --once it serves on the schedule.' },
      { id: 'mermaid', label: 'Mermaid', lang: 'mermaid', file: name + '.mmd', make: function (o) { return D.mermaid(p, { hints: o.hints }); }, note: 'Paste into GitHub Markdown, Notion or mermaid.live. The %% ps: lines let PipelineSketch re-import it losslessly.' },
      { id: 'markdown', label: 'Docs', lang: 'markdown', file: name + '.md', make: function () { return D.markdown(p); }, note: 'A README for the pipeline: schedule, steps, checks, alerts and a Mermaid diagram.' },
      { id: 'image', label: 'PNG / SVG', image: true },
      { id: 'json', label: 'JSON', lang: 'json', file: name + '.pipeline.json', make: function () { return JSON.stringify(C.shareable(p), null, 2) + '\n'; }, note: 'The full design. Import it later or keep it in git next to the generated code.' }
    ];
  }
  function bundleFiles(p) {
    var name = p.settings.name, files = {}, errs = [];
    exportTargets(p).forEach(function (t) {
      if (t.image) { try { files[name + '/docs/' + name + '.svg'] = bytesOf(D.svg(p, { theme: 'light' })); } catch (e) { errs.push('SVG: ' + e.message); } return; }
      try {
        var out = t.make({ hints: true });
        if (t.multi) Object.keys(out).forEach(function (k) { files[name + '/dbt/' + k] = bytesOf(out[k]); });
        else {
          var dir = { airflow: 'airflow/dags/', dagster: 'dagster/', prefect: 'prefect/', mermaid: 'docs/', markdown: 'docs/', json: '' }[t.id];
          files[name + '/' + dir + t.file] = bytesOf(out);
        }
      } catch (e) { errs.push(t.label + ': ' + e.message); }
    });
    return { files: files, errs: errs };
  }
  function openExport(initial) {
    var p = S.p, targets = exportTargets(p), cur = null, dbtFile = null, hints = true, imgTheme = S.theme, cache = {};
    var tabs = h('div', { class: 'tabs', role: 'tablist' });
    var notice = h('div', {}), main = h('div', { class: 'export-body' });
    var errs = S.issues.filter(function (i) { return i.level === 'error'; });
    if (errs.length) notice.appendChild(h('div', { class: 'notice err' }, [
      '\u2716 ' + plural(errs.length, 'validation error') + ': the exported code may not run until they are fixed. ',
      h('button', { text: 'Show issues', onclick: function () { closeThen('export', openIssues); } })]));
    function gen(t) {
      var key = t.id + (t.id === 'mermaid' ? ':' + hints : '');
      if (!(key in cache)) { try { cache[key] = { ok: t.make({ hints: hints }) }; } catch (e) { cache[key] = { err: e }; } }
      return cache[key];
    }
    function codeView(fname, text, lang, extraBtns) {
      var pre = h('pre', { class: 'code', tabindex: '0', 'data-lang': lang });
      pre.innerHTML = HL.highlight(text, lang);
      return h('div', { class: 'code-wrap' }, [
        h('div', { class: 'code-bar' }, [h('span', { class: 'fname', text: fname, title: fname })].concat(extraBtns || []).concat([
          h('button', { class: 'btn small', 'data-act': 'copy', text: 'Copy', onclick: function () { copyText(text); } }),
          h('button', { class: 'btn small primary', 'data-act': 'download', text: 'Download', onclick: function () { download(fname.split('/').pop(), text); } })])),
        pre]);
    }
    function show(id) {
      cur = targets.filter(function (t) { return t.id === id; })[0] || targets[0];
      $$('.tab', tabs).forEach(function (b) { b.setAttribute('aria-selected', String(b.getAttribute('data-tab') === cur.id)); });
      main.innerHTML = '';
      var note = $('.tab-note', notice); if (note) note.remove();
      if (cur.note) notice.appendChild(h('div', { class: 'notice tab-note', text: cur.note }));
      if (cur.image) {
        var themeSel = h('select', { style: 'width:auto' }, [h('option', { value: 'dark', text: 'Dark' }), h('option', { value: 'light', text: 'Light' })]);
        themeSel.value = imgTheme;
        var img = h('img', { alt: 'Pipeline diagram' }), svgText = '';
        var paint = function () { svgText = D.svg(p, { theme: imgTheme }); img.src = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgText); };
        themeSel.addEventListener('change', function () { imgTheme = themeSel.value; paint(); });
        paint();
        main.appendChild(h('div', { class: 'code-wrap' }, [
          h('div', { class: 'code-bar' }, [h('span', { class: 'fname', text: p.settings.name + '.png / .svg' }), themeSel,
            h('button', { class: 'btn small', 'data-act': 'download-svg', text: 'Download SVG', onclick: function () { download(p.settings.name + '.svg', svgText, 'image/svg+xml'); } }),
            h('button', { class: 'btn small primary', 'data-act': 'download-png', text: 'Download PNG', onclick: function () {
              svgToPng(svgText, 2).then(function (b) { download(p.settings.name + '.png', b); }, function (e) { toast(e.message, 'err'); }); } })]),
          h('div', { class: 'img-preview' }, [img])]));
        return;
      }
      var r = gen(cur);
      if (r.err) { main.appendChild(h('div', { class: 'notice err', text: 'Couldn\u2019t generate ' + cur.label + ': ' + r.err.message })); return; }
      if (cur.multi) {
        var files = r.ok, names = Object.keys(files);
        if (!dbtFile || !files[dbtFile]) dbtFile = names.filter(function (n) { return /^models\/.+\.sql$/.test(n); })[0] || names[0];
        var list = h('div', { class: 'file-list', role: 'listbox', 'aria-label': 'dbt files' }, names.map(function (n) {
          return h('button', { class: n === dbtFile ? 'on' : '', text: n, title: n, onclick: function () { dbtFile = n; show('dbt'); } });
        }));
        var zipBtn = h('button', { class: 'btn small', 'data-act': 'download-zip', text: 'Download project (.zip)', onclick: function () {
          var z = {}; names.forEach(function (n) { z[p.settings.name + '/' + n] = bytesOf(files[n]); });
          download(p.settings.name + '_dbt.zip', new Blob([ZIP.build(z)], { type: 'application/zip' }));
        } });
        main.appendChild(list);
        main.appendChild(codeView(dbtFile, files[dbtFile], /\.ya?ml$/.test(dbtFile) ? 'yaml' : /\.md$/.test(dbtFile) ? 'markdown' : 'sql', [zipBtn]));
        return;
      }
      var extra = [];
      if (cur.id === 'mermaid') {
        var cb = h('input', { type: 'checkbox', id: 'mm-hints', checked: hints });
        cb.addEventListener('change', function () { hints = cb.checked; show('mermaid'); });
        extra.push(h('label', { for: 'mm-hints', class: 'muted', style: 'display:flex;gap:4px;align-items:center;font-size:12px' }, [cb, 'Re-import hints']));
      }
      main.appendChild(codeView(cur.file, r.ok, cur.lang, extra));
    }
    targets.forEach(function (t) { tabs.appendChild(h('button', { class: 'tab', role: 'tab', 'data-tab': t.id, text: t.label, onclick: function () { show(t.id); } })); });
    var body = h('div', { style: 'display:flex;flex-direction:column;flex:1;min-height:0' }, [tabs, notice, main]);
    openDialog('export', { title: 'Export ' + p.settings.name, cls: 'wide', raw: true, body: body, focus: '.tab[aria-selected="true"]',
      buttons: [
        { label: 'Download everything (.zip)', role: 'download-all', left: true, keepOpen: true, run: function () {
          var b = bundleFiles(p); if (b.errs.length) toast('Skipped: ' + b.errs.join('; '), 'warn');
          download(p.settings.name + '_pipelinesketch.zip', new Blob([ZIP.build(b.files)], { type: 'application/zip' }));
        } },
        { label: 'Close', cancel: true }] });
    show(initial || 'airflow');
  }

  /* ---------------- import dialog ---------------- */
  function seg(name, opts, value) {
    return h('div', { class: 'seg', role: 'radiogroup' }, opts.map(function (o) {
      return h('label', {}, [h('input', { type: 'radio', name: name, value: o[0], checked: o[0] === value }), o[1]]);
    }));
  }
  function segVal(root, name) { var x = $('input[name="' + name + '"]:checked', root); return x ? x.value : null; }
  function parseImport(text, fmt) {
    fmt = fmt === 'auto' ? IMP.detect(text) : fmt;
    if (!fmt) throw new Error('Couldn\u2019t tell the format. Pick JSON, Mermaid or Airflow.');
    if (fmt === 'json') return { fmt: fmt, pipelines: IMP.fromJSON(text).pipelines };
    if (fmt === 'mermaid') { var m = IMP.fromMermaid(text); return { fmt: fmt, pipelines: [m.pipeline], note: m.hinted ? 'with PipelineSketch hints (lossless)' : 'node types guessed from names and shapes' }; }
    var a = IMP.fromAirflow(text);
    return { fmt: fmt, pipelines: [a.pipeline], note: 'tasks and >> dependencies; check each node\u2019s config' };
  }
  function openImport() {
    var ta = h('textarea', { id: 'import-text', class: 'code', rows: 12, spellcheck: 'false', placeholder: 'Paste a PipelineSketch JSON file or backup, a Mermaid flowchart, or an Airflow DAG (tasks and >> dependencies)\u2026' });
    var file = h('input', { type: 'file', id: 'import-file', accept: '.json,.mmd,.mermaid,.md,.py,.txt', style: 'width:auto' });
    var status = h('div', { class: 'help', id: 'import-status', text: 'Nothing to import yet.' });
    var fmt = seg('imp-fmt', [['auto', 'Auto-detect'], ['json', 'JSON'], ['mermaid', 'Mermaid'], ['airflow', 'Airflow']], 'auto');
    var mode = seg('imp-mode', [['new', 'Open as new pipeline'], ['replace', 'Replace current canvas']], 'new');
    var body = h('div', {}, [
      h('div', { class: 'field' }, [h('label', { for: 'import-file', text: 'From a file' }), file]),
      h('div', { class: 'field' }, [h('label', { for: 'import-text', text: 'Or paste' }), ta, status]),
      h('div', { class: 'field' }, [h('div', { class: 'lbl', text: 'Format' }), fmt]),
      h('div', { class: 'field' }, [h('div', { class: 'lbl', text: 'Where' }), mode])]);
    function check() {
      if (!ta.value.trim()) { status.textContent = 'Nothing to import yet.'; return; }
      try {
        var r = parseImport(ta.value, segVal(body, 'imp-fmt'));
        var n = r.pipelines.reduce(function (a, p) { return a + p.nodes.length; }, 0), e = r.pipelines.reduce(function (a, p) { return a + p.edges.length; }, 0);
        status.textContent = '\u2713 ' + { json: 'JSON', mermaid: 'Mermaid', airflow: 'Airflow DAG' }[r.fmt] + ': ' + (r.pipelines.length > 1 ? plural(r.pipelines.length, 'pipeline') + ', ' : '') + plural(n, 'node') + ', ' + plural(e, 'connection') + (r.note ? ' (' + r.note + ')' : '');
        status.style.color = '';
      } catch (err) { status.textContent = '\u2716 ' + err.message; status.style.color = 'var(--err)'; }
    }
    ta.addEventListener('input', check); body.addEventListener('change', function (ev) { if (ev.target.name === 'imp-fmt') check(); });
    file.addEventListener('change', function () {
      var f = file.files && file.files[0]; if (!f) return;
      f.text().then(function (t) { ta.value = t; check(); });
    });
    openDialog('import', { title: 'Import', cls: 'mid', body: body, focus: '#import-text', isDirty: function () { return false; },
      buttons: [{ label: 'Cancel', cancel: true }, { label: 'Import', primary: true, role: 'import', run: function () {
        var r;
        try { r = parseImport(ta.value, segVal(body, 'imp-fmt')); } catch (err) { toast(err.message, 'err'); return false; }
        if (!r.pipelines.length || !r.pipelines.some(function (p) { return p.nodes.length; })) { toast('Nothing to import: no nodes found', 'err'); return false; }
        var where = r.pipelines.length > 1 ? 'new' : segVal(body, 'imp-mode');
        closeThen('import', function () { applyImport(r, where); });
        return false;
      } }] });
  }
  function applyImport(r, where) {
    if (where === 'replace') {
      var np = r.pipelines[0]; np.id = S.p.id;
      checkpoint(); S.p = np; clearSelection(); changed(); fitView();
      toast('Imported into this canvas', '', { label: 'Undo', run: undo });
      return;
    }
    flushSave();
    r.pipelines.forEach(function (p) { p.id = C.newId('p'); writePipeline(p); });
    setPipeline(r.pipelines[0]);
    toast(r.pipelines.length > 1 ? 'Restored ' + plural(r.pipelines.length, 'pipeline') : 'Imported ' + r.pipelines[0].settings.name);
  }

  /* ---------------- share & backup ---------------- */
  function backupJSON() {
    flushSave();
    var all = listPipelines().map(function (x) { return readPipeline(x.id); }).filter(Boolean);
    return JSON.stringify({ kind: 'pipelinesketch-backup', version: C.VERSION, exportedAt: new Date().toISOString(), pipelines: all }, null, 2) + '\n';
  }
  function downloadBackup() { download('pipelinesketch-backup-' + new Date().toISOString().slice(0, 10) + '.json', backupJSON(), 'application/json'); }
  function openShare() {
    var input = h('input', { id: 'share-link', type: 'text', readonly: true, value: 'Building link\u2026' });
    var info = h('div', { class: 'help', id: 'share-info' });
    var link = '';
    var copyBtn = h('button', { class: 'btn primary', 'data-act': 'copy-link', text: 'Copy link', disabled: true, onclick: function () { copyText(link); } });
    var nativeBtn = navigator.share ? h('button', { class: 'btn', text: 'Share\u2026', onclick: function () { navigator.share({ title: S.p.settings.name, url: link }).catch(function () {}); } }) : null;
    var body = h('div', {}, [
      h('p', { style: 'margin-top:0', text: 'The whole design is compressed into the link itself. Nothing is uploaded anywhere: whoever opens it gets their own copy.' }),
      h('div', { class: 'field' }, [h('label', { for: 'share-link', text: 'Share link' }), h('div', { class: 'row', style: 'flex-wrap:nowrap' }, [input, copyBtn, nativeBtn]), info]),
      h('div', { class: 'insp-section' }, [h('h4', { text: 'Files' }),
        h('div', { class: 'row' }, [
          h('button', { class: 'btn', 'data-act': 'download-json', text: 'Download this pipeline (.json)', onclick: function () { download(S.p.settings.name + '.pipeline.json', JSON.stringify(C.shareable(S.p), null, 2) + '\n', 'application/json'); } }),
          h('button', { class: 'btn', 'data-act': 'backup', text: 'Back up all pipelines (.json)', onclick: downloadBackup })]),
        h('p', { class: 'help', text: 'Pipelines are saved in this browser only. Keep a backup if you clear site data or switch devices; restore it from Pipelines or Import.' })])]);
    openDialog('share', { title: 'Share ' + S.p.settings.name, body: body, buttons: [{ label: 'Close', cancel: true }] });
    C.encodeShare(S.p).then(function (tok) {
      link = location.href.split('#')[0] + '#p=' + tok;
      input.value = link; copyBtn.disabled = false;
      info.textContent = link.length.toLocaleString() + ' characters' + (link.length > 8000 ? ': long links can get cut off in some chat apps; send the JSON file instead.' : '.');
      input.addEventListener('focus', function () { input.select(); });
    }, function (e) { input.value = ''; info.textContent = 'Couldn\u2019t build a link: ' + e.message; });
  }
  function openSharedToken(tok) {
    return C.decodeShare(tok).then(function (p) {
      flushSave();
      p.id = C.newId('p'); writePipeline(p); setPipeline(p);
      toast('Opened shared pipeline ' + p.settings.name + '. It\u2019s now saved in your browser.');
    }, function (e) { toast('That share link is broken: ' + e.message, 'err'); });
  }

  /* ---------------- switching pipelines ---------------- */
  function setPipeline(p) {
    S.p = p; S.undo = []; S.redo = []; clearSelection();
    if (stack.isOpen('inspector')) stack.close('inspector');
    writePipeline(p);
    changed({ noSave: true });
    fitView();
    $('#status-save').textContent = 'Saved locally';
  }
  function fromTemplate(t) { var p = t.build(); p.id = C.newId('p'); return p; }
  function newBlank() {
    var taken = listPipelines().map(function (x) { return x.name; });
    flushSave(); setPipeline(C.createPipeline(C.uniqueName('my_pipeline', taken)));
    toast('New blank pipeline');
  }
  function openPipelines() {
    var ul = h('ul', { class: 'pipe-list', id: 'pipe-list' });
    function paint() {
      flushSave();
      ul.innerHTML = '';
      listPipelines().forEach(function (x) {
        var isCur = x.id === S.p.id;
        var nameBox = h('div', { class: 'pname' }, [h('b', { text: x.name }), h('small', { text: (x.nodes != null ? plural(x.nodes, 'node') + ' \u00b7 ' : '') + 'edited ' + fmtDate(x.updatedAt) + (isCur ? ' \u00b7 open now' : '') })]);
        var li = h('li', { class: isCur ? 'current' : '', 'data-pid': x.id }, [nameBox,
          isCur ? null : h('button', { class: 'btn small primary', text: 'Open', onclick: function () { var p = readPipeline(x.id); if (!p) { toast('That pipeline is damaged', 'err'); return; } closeThen('pipelines', function () { setPipeline(p); }); } }),
          h('button', { class: 'btn small', text: 'Rename', onclick: function () {
            var inp = h('input', { type: 'text', value: x.name, style: 'width:auto;flex:1' });
            var save = function () {
              var p = readPipeline(x.id); if (!p) return;
              var nm = C.toIdent(inp.value, p.settings.name);
              if (x.id === S.p.id) { checkpoint(); S.p.settings.name = nm; changed(); flushSave(); } else { p.settings.name = nm; var curId = S.p.id; writePipeline(p); store.set('current', curId); }
              paint();
            };
            inp.addEventListener('keydown', function (ev) { if (ev.key === 'Enter') { ev.preventDefault(); save(); } if (ev.key === 'Escape') { ev.stopPropagation(); paint(); } });
            li.replaceChild(h('div', { class: 'row', style: 'flex:1' }, [inp, h('button', { class: 'btn small primary', text: 'Save', onclick: save })]), nameBox);
            inp.focus(); inp.select();
          } }),
          h('button', { class: 'btn small', text: 'Duplicate', onclick: function () {
            var p = readPipeline(x.id); if (!p) return;
            p.id = C.newId('p'); p.settings.name = C.uniqueName(p.settings.name + '_copy', listPipelines().map(function (y) { return y.name; }));
            var curId = S.p.id; writePipeline(p); store.set('current', curId); paint(); toast('Duplicated as ' + p.settings.name);
          } }),
          h('button', { class: 'btn small danger', text: 'Delete', onclick: function () {
            confirmDialog('Delete ' + x.name + '? This can\u2019t be undone (unless you have a backup).', 'Delete', true).then(function (ok) {
              if (!ok) return;
              deletePipeline(x.id);
              if (x.id === S.p.id) {
                var rest = listPipelines(), next = rest.length && readPipeline(rest[0].id);
                if (next) setPipeline(next); else setPipeline(C.createPipeline('my_pipeline'));
              }
              paint(); toast('Deleted ' + x.name);
            });
          } })]);
        ul.appendChild(li);
      });
    }
    paint();
    var restore = h('input', { type: 'file', accept: '.json', style: 'display:none', id: 'restore-file' });
    restore.addEventListener('change', function () {
      var f = restore.files && restore.files[0]; if (!f) return;
      f.text().then(function (t) {
        var r; try { r = IMP.fromJSON(t); } catch (e) { toast('Not a PipelineSketch file: ' + e.message, 'err'); return; }
        var curId = S.p.id;
        r.pipelines.forEach(function (p) { p.id = C.newId('p'); writePipeline(p); });
        store.set('current', curId); paint(); toast('Restored ' + plural(r.pipelines.length, 'pipeline'));
      });
    });
    var body = h('div', {}, [
      h('div', { class: 'row', style: 'margin-bottom:12px' }, [
        h('button', { class: 'btn primary', text: '+ New blank pipeline', onclick: function () { closeThen('pipelines', newBlank); } }),
        h('button', { class: 'btn', text: 'From a template\u2026', onclick: function () { closeThen('pipelines', openTemplates); } }),
        h('span', { class: 'spacer' }),
        h('button', { class: 'btn', text: 'Back up all', onclick: downloadBackup }),
        h('button', { class: 'btn', text: 'Restore backup', onclick: function () { restore.click(); } }), restore]),
      ul,
      h('p', { class: 'help', text: 'Saved automatically in this browser (localStorage).' })]);
    openDialog('pipelines', { title: 'Your pipelines', cls: 'mid', body: body, buttons: [{ label: 'Close', cancel: true }] });
  }

  /* ---------------- templates ---------------- */
  function openTemplates() {
    var grid = h('div', { class: 'tpl-grid' });
    TPL.LIST.forEach(function (t) {
      var p = t.build(), thumb = h('div', { class: 'thumb' });
      thumb.innerHTML = D.svg(p, { theme: S.theme }).replace(/ width="\d+" height="\d+"/, ' preserveAspectRatio="xMidYMid meet"');
      grid.appendChild(h('div', { class: 'tpl-card', 'data-template': t.id }, [thumb, h('div', { class: 'meta' }, [
        h('h4', { text: t.title }), h('p', { text: t.description }),
        h('div', { class: 'tags' }, t.tags.map(function (x) { return h('span', { class: 'tag', text: x }); })),
        h('div', { class: 'row' }, [h('span', { class: 'muted', style: 'flex:1', text: plural(p.nodes.length, 'node') + ' \u00b7 ' + C.WAREHOUSES[p.settings.warehouse].label }),
          h('button', { class: 'btn primary small', 'data-use': t.id, text: 'Use template', onclick: function () {
            closeThen('templates', function () { flushSave(); var np = fromTemplate(t); setPipeline(np); toast('Created ' + np.settings.name + ' from a template'); });
          } })])])]));
    });
    openDialog('templates', { title: 'Templates', cls: 'wide', body: h('div', {}, [h('p', { class: 'muted', style: 'margin-top:0', text: 'Each template opens as a new pipeline; your current one stays saved.' }), grid]),
      buttons: [{ label: 'Blank pipeline', left: true, run: function () { }, after: newBlank }, { label: 'Cancel', cancel: true }] });
  }

  /* ---------------- help & mobile menu ---------------- */
  function openHelp() {
    var keys = [['Drag palette item', 'Add a node (or click it; with one node selected it is added and connected after it)'], ['Drag right port \u2192 node', 'Connect'], ['Drag from an input port', 'Re-route or remove a connection'],
      ['Drag empty canvas', 'Pan'], ['Wheel / pinch', 'Zoom'], ['Shift + drag', 'Box select'], ['Shift/Ctrl + click', 'Add to selection'],
      ['Ctrl+Z / Ctrl+Shift+Z', 'Undo / redo'], ['Ctrl+C / X / V / D', 'Copy / cut / paste / duplicate'], ['Delete', 'Delete selection'], ['Ctrl+A', 'Select all'],
      ['Ctrl+G', 'Group selection (or add a comment box)'], ['Shift+L', 'Auto-layout'], ['F', 'Fit to screen'], ['+ / \u2212 / 0', 'Zoom in / out / 100%'], ['Arrows', 'Nudge (Shift = more)'],
      ['Alt while dragging', 'Ignore snap'], ['Enter', 'Edit the selected node'], ['Ctrl+E', 'Export'], ['Esc', 'Close the top panel or dialog']];
    var dl = h('dl', { class: 'kbd-list' }); keys.forEach(function (k) { dl.appendChild(h('dt', { text: k[0] })); dl.appendChild(h('dd', { text: k[1] })); });
    openDialog('help', { title: 'Shortcuts & tips', cls: 'mid', body: h('div', {}, [dl,
      h('p', { class: 'muted', html: 'PipelineSketch runs entirely in your browser: no account, no server, works offline. MIT licensed, source on <a href="' + REPO_URL + '" target="_blank" rel="noopener" style="color:var(--accent)">GitHub</a>.' })]),
      buttons: [{ label: 'Close', cancel: true }] });
  }
  function openMore() {
    var items = [['Pipelines', openPipelines], ['Templates', openTemplates], ['Import', openImport], ['Share & backup', openShare], ['Pipeline settings', openSettings],
      ['Auto-layout', autoLayout], ['Validation', openIssues], ['Group / comment', groupSelection], ['Toggle theme', function () { setTheme(S.theme === 'dark' ? 'light' : 'dark'); }], ['Shortcuts & help', openHelp]];
    openDialog('more', { title: 'Menu', body: h('div', { class: 'menu-list' }, items.map(function (it) {
      return h('button', { class: 'btn', text: it[0], onclick: function () { closeThen('more', it[1]); } });
    })), buttons: [{ label: 'Cancel', cancel: true }] });
  }

  /* ---------------- keyboard ---------------- */
  function onKey(ev) {
    var mod = ev.ctrlKey || ev.metaKey, k = ev.key, top = stack.top();
    if (k === 'Escape') {
      if (top) { ev.preventDefault(); stack.requestClose(top.id); return; }
      if (selectedIds().length || S.selEdge || Object.keys(S.selGroups).length) { clearSelection(); render(); }
      return;
    }
    if (top && !PANELS[top.id]) return; // a dialog is open: leave keys to it
    if (isTyping(ev.target)) return;
    var lower = k.toLowerCase();
    if (mod && lower === 'z') { ev.preventDefault(); if (ev.shiftKey) redo(); else undo(); return; }
    if (mod && lower === 'y') { ev.preventDefault(); redo(); return; }
    if (mod && lower === 'c') { if (selectedIds().length) { ev.preventDefault(); copySelection(false); } return; }
    if (mod && lower === 'x') { if (selectedIds().length) { ev.preventDefault(); copySelection(true); } return; }
    if (mod && lower === 'v') { if (S.clip) { ev.preventDefault(); pasteFrag(S.clip); } return; }
    if (mod && lower === 'd') { ev.preventDefault(); duplicateSelection(); return; }
    if (mod && lower === 'a') { ev.preventDefault(); selectAll(); return; }
    if (mod && lower === 'g') { ev.preventDefault(); groupSelection(); return; }
    if (mod && lower === 'e') { ev.preventDefault(); openExport(); return; }
    if (mod && lower === 's') { ev.preventDefault(); flushSave(); toast('Saved in this browser'); return; }
    if (mod || ev.altKey) return;
    if (k === 'Delete' || k === 'Backspace') { ev.preventDefault(); deleteSelection(); return; }
    if (k === 'L' && ev.shiftKey) { autoLayout(); return; }
    if (lower === 'f') { fitView(selectedIds().length > 1 ? selectedIds() : null); return; }
    if (k === '+' || k === '=') { zoomAt(S.view.k * 1.2); return; }
    if (k === '-' || k === '_') { zoomAt(S.view.k / 1.2); return; }
    if (k === '0') { zoomAt(1); return; }
    if (k === '?') { openHelp(); return; }
    if (k === 'Enter') { var ids = selectedIds(); if (ids.length === 1) { ev.preventDefault(); openInspector('node', ids[0]); } return; }
    var step = ev.shiftKey ? GRID * 5 : GRID;
    var arrows = { ArrowLeft: [-step, 0], ArrowRight: [step, 0], ArrowUp: [0, -step], ArrowDown: [0, step] };
    if (arrows[k] && (selectedIds().length || Object.keys(S.selGroups).length)) { ev.preventDefault(); nudge(arrows[k][0], arrows[k][1]); return; }
    if (k === ' ') { spaceDown = true; ev.preventDefault(); }
  }
  function onPaste(ev) {
    if (isTyping(ev.target) || (stack.top() && !PANELS[stack.top().id])) return;
    var t = (ev.clipboardData && ev.clipboardData.getData('text')) || '';
    try { var f = JSON.parse(t); if (f && f.kind === 'pipelinesketch-fragment') { ev.preventDefault(); pasteFrag(f); } } catch (e) { /* not ours */ }
  }

  /* ---------------- startup ---------------- */
  function bind() {
    svg = $('#canvas'); scene = $('#scene'); viewport = $('#viewport'); wrap = $('#canvas-wrap'); ghost = $('#ghost-edge'); marquee = $('#marquee');
    $('#defs-holder').innerHTML = D.defs(S.theme);
    svg.addEventListener('pointerdown', onPointerDown);
    svg.addEventListener('pointermove', onPointerMove);
    svg.addEventListener('pointerup', onPointerUp);
    svg.addEventListener('pointercancel', function (ev) { delete pointers[ev.pointerId]; pinch = null; if (drag && drag.kind === 'move' && drag.moved) changed(); drag = null; hideGhosts(); });
    svg.addEventListener('wheel', onWheel, { passive: false });
    svg.addEventListener('dblclick', function (ev) { if (!ev.target.closest('[data-node],[data-group],[data-edge]')) fitView(); });
    var mmDown = false, mmEl = $('#minimap');
    mmEl.addEventListener('pointerdown', function (ev) { mmDown = true; mmEl.setPointerCapture(ev.pointerId); minimapMove(ev); });
    mmEl.addEventListener('pointermove', function (ev) { if (mmDown) minimapMove(ev); });
    mmEl.addEventListener('pointerup', function () { mmDown = false; });
    $('#palette-list').addEventListener('pointerdown', onPalDown);
    document.addEventListener('pointermove', onPalMove);
    document.addEventListener('pointerup', onPalUp);
    document.addEventListener('pointercancel', function () { if (palDrag && palDrag.el) palDrag.el.remove(); palDrag = null; });
    $('#palette-list').addEventListener('keydown', function (ev) { var it = ev.target.closest('.pal-item'); if (it && (ev.key === 'Enter' || ev.key === ' ')) { ev.preventDefault(); addNodeClick(it.getAttribute('data-type')); } });
    $('#palette-search').addEventListener('input', buildPalette);
    var on = function (sel, fn) { $(sel).addEventListener('click', fn); };
    on('#btn-undo', undo); on('#btn-redo', redo); on('#btn-layout', autoLayout);
    on('#btn-settings', openSettings); on('#btn-pipelines', openPipelines); on('#btn-templates', openTemplates);
    on('#btn-import', openImport); on('#btn-share', openShare); on('#btn-export', function () { openExport(); });
    on('#btn-theme', function () { setTheme(S.theme === 'dark' ? 'light' : 'dark'); });
    on('#btn-help', openHelp); on('#btn-more', openMore); on('#btn-issues', toggleIssues);
    on('#btn-palette', openPalette); on('#fab-add', openPalette);
    on('#tool-select', function () { S.boxMode = !S.boxMode; renderChrome(); toast(S.boxMode ? 'Box select: drag on the canvas to select' : 'Drag on the canvas pans'); });
    on('#tool-snap', function () { S.snap = !S.snap; store.set('snap', S.snap); renderChrome(); toast(S.snap ? 'Snap to grid on' : 'Snap to grid off'); });
    on('#tool-group', groupSelection);
    on('#zoom-in', function () { zoomAt(S.view.k * 1.2); }); on('#zoom-out', function () { zoomAt(S.view.k / 1.2); });
    on('#zoom-val', function () { zoomAt(1); }); on('#zoom-fit', function () { fitView(); });
    on('#panel-backdrop', function () { var t = topPanel(); if (t) stack.requestClose(t); });
    document.addEventListener('click', function (ev) {
      var c = ev.target.closest('[data-close]'); if (c) { stack.requestClose(c.getAttribute('data-close')); return; }
      var x = ev.target.closest('[data-cancel="inspector"]'); if (x) { cancelInspector(); return; }
      var a = ev.target.closest('[data-action="templates"]'); if (a) openTemplates();
    });
    document.addEventListener('keydown', onKey);
    document.addEventListener('keyup', function (ev) { if (ev.key === ' ') spaceDown = false; });
    document.addEventListener('paste', onPaste);
    window.addEventListener('popstate', stack.onPopState);
    window.addEventListener('resize', rafThrottle(function () { renderMinimap(); syncBackdrop(); }));
    window.addEventListener('pagehide', flushSave);
    document.addEventListener('visibilitychange', function () { if (document.hidden) flushSave(); });
    window.addEventListener('hashchange', function () { var m = /^#p=(.+)$/.exec(location.hash); if (m) { history.replaceState(history.state, '', location.href.split('#')[0]); openSharedToken(m[1]); } });
    window.addEventListener('storage', function (ev) { if (ev.key === KEY + 'p:' + (S.p && S.p.id)) $('#status-save').textContent = 'Changed in another tab: last save wins'; });
  }
  function init() {
    bind();
    buildPalette();
    var first = false, p = null, cur = store.get('current');
    if (cur) p = readPipeline(cur);
    if (!p) { var idx = listPipelines(); for (var i = 0; i < idx.length && !p; i++) p = readPipeline(idx[i].id); }
    if (!p) { p = fromTemplate(TPL.LIST[0]); first = true; }
    setPipeline(p);
    var m = /^#p=(.+)$/.exec(location.hash);
    if (m) { history.replaceState(null, '', location.href.split('#')[0]); openSharedToken(m[1]); }
    else if (first) toast('Welcome! This is an example pipeline. Try Templates, or Pipelines \u2192 New blank.');
    if ('serviceWorker' in navigator && /^https?:$/.test(location.protocol)) navigator.serviceWorker.register('sw.js').catch(function () { /* offline support is optional */ });
    window.PipelineSketch = { state: S, undo: undo, redo: redo, setPipeline: setPipeline, render: render, fitView: fitView, version: C.VERSION };
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
