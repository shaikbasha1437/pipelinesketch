/* Mermaid, SVG and Markdown exporters. The SVG renderer is also used for PNG export. MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./core.js'), require('./nodes.js'), require('./cron.js'));
  else (root.PS = root.PS || {}).docs = factory(root.PS.core, root.PS.nodes, root.CronCore);
})(typeof self !== 'undefined' ? self : this, function (C, N, CRON) {
  'use strict';
  function esc(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
  function catOf(n) { var t = N.get(n.type); return t ? t.category : 'control'; }
  function catColor(cat) { var c = N.CATEGORIES.filter(function (x) { return x.id === cat; })[0]; return c ? c.color : '#94a3b8'; }
  function groupOf(p, n) {
    var sz = C.nodeSize(n), cx = n.x + sz.w / 2, cy = n.y + sz.h / 2;
    return (p.groups || []).filter(function (g) { return cx >= g.x && cx <= g.x + g.w && cy >= g.y && cy <= g.y + g.h; })[0] || null;
  }
  /* ---------------- Mermaid ---------------- */
  var SHAPES = { source: ['[(', ')]'], transform: ['[', ']'], quality: ['{{', '}}'], sink: ['[/', '/]'], control: ['([', '])'] };
  function mmLabel(s) { return String(s).replace(/"/g, '#quot;').replace(/[<>]/g, function (c) { return c === '<' ? '#lt;' : '#gt;'; }); }
  var MM_RESERVED = /^(end|graph|subgraph|flowchart|class|classdef|style|linkstyle|click|call|href|default|direction)$/i;
  function mmId(n) { return MM_RESERVED.test(n.name) ? 'n_' + n.name : n.name; }
  function mermaid(p, opts) {
    opts = opts || {};
    var L = ['flowchart LR'];
    if (opts.hints !== false) {
      L.push('    %% PipelineSketch export: the ps: comments let PipelineSketch re-import this diagram losslessly.');
      L.push('    %% ps:settings ' + JSON.stringify(p.settings));
      (p.groups || []).forEach(function (g) { L.push('    %% ps:group ' + JSON.stringify({ title: g.title, text: g.text, x: g.x, y: g.y, w: g.w, h: g.h, color: g.color })); });
    }
    var order = C.topoSort(p), ids = order.order.concat(order.cyclic);
    var byGroup = {}, loose = [];
    ids.forEach(function (id) { var n = C.nodeById(p, id), g = groupOf(p, n); if (g) (byGroup[g.id] = byGroup[g.id] || []).push(n); else loose.push(n); });
    function decl(n, ind) {
      var t = N.get(n.type), sh = SHAPES[catOf(n)], sum = N.summary(n);
      var label = '<b>' + n.name + '</b><br/>' + (t ? t.label : n.type) + (sum ? '<br/><small>' + sum + '</small>' : '');
      var out = [];
      if (opts.hints !== false) out.push(ind + '%% ps:node ' + mmId(n) + ' ' + n.type + ' ' + JSON.stringify({ name: n.name, config: n.config, columns: n.columns || [], notes: n.notes || '', x: Math.round(n.x), y: Math.round(n.y) }));
      out.push(ind + mmId(n) + sh[0] + '"' + mmLabel(label).replace(/#lt;(\/?)(b|br\/|small)#gt;/g, '<$1$2>') + '"' + sh[1]);
      return out;
    }
    loose.forEach(function (n) { L = L.concat(decl(n, '    ')); });
    (p.groups || []).forEach(function (g, gi) {
      if (!byGroup[g.id]) return;
      L.push('    subgraph ' + C.toIdent('grp_' + g.title, 'group') + '_' + (gi + 1) + '["' + mmLabel(g.title || 'Group') + '"]');
      byGroup[g.id].forEach(function (n) { L = L.concat(decl(n, '        ')); });
      L.push('    end');
    });
    p.edges.forEach(function (e) {
      var a = C.nodeById(p, e.from), b = C.nodeById(p, e.to); if (!a || !b) return;
      var lbl = e.fromPort !== 'out' ? e.fromPort : e.toPort !== 'in' ? e.toPort : '';
      L.push('    ' + mmId(a) + (lbl ? ' -->|' + lbl + '| ' : ' --> ') + mmId(b));
    });
    N.CATEGORIES.forEach(function (c) { L.push('    classDef ' + c.id + ' fill:' + c.color + '22,stroke:' + c.color + ',stroke-width:2px'); });
    N.CATEGORIES.forEach(function (c) {
      var ns = p.nodes.filter(function (n) { return catOf(n) === c.id; }).map(mmId);
      if (ns.length) L.push('    class ' + ns.join(',') + ' ' + c.id);
    });
    return L.join('\n') + '\n';
  }
  /* ---------------- SVG ---------------- */
  var THEMES = {
    dark: { bg: '#0f1117', grid: '#1c2030', node: '#181c27', nodeBorder: '#2c3346', text: '#e6e9f2', sub: '#98a2b8', edge: '#6b7690', port: '#0f1117', sel: '#60a5fa' },
    light: { bg: '#f7f8fb', grid: '#e4e7ef', node: '#ffffff', nodeBorder: '#cfd5e2', text: '#151a26', sub: '#5b6478', edge: '#8a93a8', port: '#ffffff', sel: '#2563eb' }
  };
  function trunc(s, px, size) { s = String(s || ''); var max = Math.floor(px / (size * 0.58)); return s.length > max ? s.slice(0, Math.max(1, max - 1)) + '\u2026' : s; }
  function edgePath(p, e) {
    var a = C.nodeById(p, e.from), b = C.nodeById(p, e.to); if (!a || !b) return null;
    var s = C.portPos(a, e.fromPort, 'out'), t = C.portPos(b, e.toPort, 'in');
    return curve(s.x, s.y, t.x, t.y);
  }
  function curve(x1, y1, x2, y2) {
    var dx = Math.max(40, Math.abs(x2 - x1) / 2);
    return 'M' + x1 + ',' + y1 + ' C' + (x1 + dx) + ',' + y1 + ' ' + (x2 - dx) + ',' + y2 + ' ' + x2 + ',' + y2;
  }
  // Inner SVG markup (no <svg> wrapper). opts: theme, selected {id:1}, issues {nodeId: 'error'|'warning'}, selectedEdge, selectedGroup
  function svgBody(p, opts) {
    opts = opts || {};
    var T = THEMES[opts.theme] || THEMES.dark, sel = opts.selected || {}, iss = opts.issues || {}, out = [];
    (p.groups || []).forEach(function (g) {
      var on = opts.selectedGroups && opts.selectedGroups[g.id];
      out.push('<g class="grp" data-group="' + esc(g.id) + '">' +
        '<rect x="' + g.x + '" y="' + g.y + '" width="' + g.w + '" height="' + g.h + '" rx="10" fill="' + esc(g.color) + '" fill-opacity="0.08" stroke="' + (on ? T.sel : esc(g.color)) + '" stroke-opacity="' + (on ? 1 : 0.6) + '" stroke-width="' + (on ? 2 : 1.5) + '" stroke-dasharray="6 4"/>' +
        '<text x="' + (g.x + 12) + '" y="' + (g.y + 22) + '" font-size="13" font-weight="600" fill="' + esc(g.color) + '" class="grp-title">' + esc(trunc(g.title || 'Group', g.w - 24, 13)) + '</text>' +
        (g.text ? '<text x="' + (g.x + 12) + '" y="' + (g.y + 40) + '" font-size="11" fill="' + T.sub + '">' + esc(trunc(g.text, g.w - 24, 11)) + '</text>' : '') +
        (opts.interactive ? '<rect class="grp-resize" data-group-resize="' + esc(g.id) + '" x="' + (g.x + g.w - 14) + '" y="' + (g.y + g.h - 14) + '" width="14" height="14" fill="' + esc(g.color) + '" fill-opacity="0.5"/>' : '') + '</g>');
    });
    p.edges.forEach(function (e) {
      var d = edgePath(p, e); if (!d) return;
      var on = opts.selectedEdge === e.id, lbl = e.fromPort !== 'out' ? e.fromPort : e.toPort !== 'in' ? e.toPort : '';
      out.push('<g class="edge" data-edge="' + esc(e.id) + '">' + (opts.interactive ? '<path d="' + d + '" stroke="transparent" stroke-width="12" fill="none"/>' : '') +
        '<path d="' + d + '" stroke="' + (on ? T.sel : T.edge) + '" stroke-width="' + (on ? 3 : 2) + '" fill="none" marker-end="url(#ps-arrow' + (on ? '-sel' : '') + ')"/>');
      if (lbl) { var a = C.nodeById(p, e.from), b = C.nodeById(p, e.to), s = C.portPos(a, e.fromPort, 'out'), t = C.portPos(b, e.toPort, 'in'); out.push('<text x="' + Math.round((s.x + t.x) / 2) + '" y="' + Math.round((s.y + t.y) / 2 - 6) + '" font-size="11" text-anchor="middle" fill="' + T.sub + '">' + esc(lbl) + '</text>'); }
      out.push('</g>');
    });
    p.nodes.forEach(function (n) {
      var t = N.get(n.type) || { label: n.type, icon: '?', inPorts: ['in'], outPorts: ['out'] }, sz = C.nodeSize(n), color = catColor(catOf(n));
      var on = !!sel[n.id], lvl = iss[n.id];
      var border = on ? T.sel : lvl === 'error' ? '#ef4444' : lvl === 'warning' ? '#f59e0b' : T.nodeBorder;
      var g = '<g class="node" data-node="' + esc(n.id) + '" transform="translate(' + n.x + ',' + n.y + ')">' +
        '<rect width="' + sz.w + '" height="' + sz.h + '" rx="8" fill="' + T.node + '" stroke="' + border + '" stroke-width="' + (on || lvl ? 2 : 1) + '"/>' +
        '<rect width="4" height="' + (sz.h - 12) + '" x="0" y="6" rx="2" fill="' + color + '"/>' +
        '<text x="14" y="22" font-size="14">' + esc(t.icon) + '</text>' +
        '<text x="36" y="22" font-size="13" font-weight="600" fill="' + T.text + '">' + esc(trunc(n.name, sz.w - 48, 13)) + '</text>' +
        '<text x="36" y="40" font-size="11" fill="' + T.sub + '">' + esc(trunc(t.label, sz.w - 48, 11)) + '</text>' +
        '<text x="14" y="' + (sz.h - 10) + '" font-size="10.5" fill="' + T.sub + '" font-family="ui-monospace, SFMono-Regular, Menlo, monospace">' + esc(trunc(N.summary(n), sz.w - 28, 10.5)) + '</text>';
      if (lvl) g += '<circle cx="' + (sz.w - 12) + '" cy="12" r="5" fill="' + (lvl === 'error' ? '#ef4444' : '#f59e0b') + '"/>';
      t.inPorts.forEach(function (pt) {
        var pp = C.portPos(n, pt, 'in');
        g += '<circle class="port in" data-port-in="' + esc(pt) + '" cx="0" cy="' + (pp.y - n.y) + '" r="6" fill="' + T.port + '" stroke="' + color + '" stroke-width="2"/>';
        if (t.inPorts.length > 1) g += '<text x="10" y="' + (pp.y - n.y + 4) + '" font-size="9.5" fill="' + T.sub + '">' + esc(pt) + '</text>';
      });
      t.outPorts.forEach(function (pt) {
        var pp = C.portPos(n, pt, 'out');
        g += '<circle class="port out" data-port-out="' + esc(pt) + '" cx="' + sz.w + '" cy="' + (pp.y - n.y) + '" r="6" fill="' + color + '" stroke="' + T.port + '" stroke-width="2"/>';
        if (t.outPorts.length > 1) g += '<text x="' + (sz.w - 10) + '" y="' + (pp.y - n.y + 4) + '" font-size="9.5" text-anchor="end" fill="' + T.sub + '">' + esc(pt) + '</text>';
      });
      out.push(g + '</g>');
    });
    return out.join('');
  }
  function defs(theme) {
    var T = THEMES[theme] || THEMES.dark;
    return '<defs><marker id="ps-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="' + T.edge + '"/></marker>' +
      '<marker id="ps-arrow-sel" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="' + T.sel + '"/></marker></defs>';
  }
  function contentBounds(p) {
    var b = C.bounds(p, p.nodes.map(function (n) { return n.id; }));
    (p.groups || []).forEach(function (g) { b.x1 = Math.min(b.x1, g.x); b.y1 = Math.min(b.y1, g.y); b.x2 = Math.max(b.x2, g.x + g.w); b.y2 = Math.max(b.y2, g.y + g.h); });
    if (!isFinite(b.x1)) b = { x1: 0, y1: 0, x2: 400, y2: 200 };
    return b;
  }
  function svg(p, opts) {
    opts = opts || {};
    var T = THEMES[opts.theme] || THEMES.dark, b = contentBounds(p), pad = 40;
    var x = b.x1 - pad, yy = b.y1 - pad - 30, w = b.x2 - b.x1 + pad * 2, h = b.y2 - b.y1 + pad * 2 + 30;
    return '<svg xmlns="http://www.w3.org/2000/svg" width="' + Math.round(w) + '" height="' + Math.round(h) + '" viewBox="' + [x, yy, w, h].map(Math.round).join(' ') + '" font-family="Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif">' +
      defs(opts.theme) + '<rect x="' + Math.round(x) + '" y="' + Math.round(yy) + '" width="' + Math.round(w) + '" height="' + Math.round(h) + '" fill="' + T.bg + '"/>' +
      '<text x="' + Math.round(x + pad) + '" y="' + Math.round(yy + 30) + '" font-size="15" font-weight="700" fill="' + T.text + '">' + esc(p.settings.name) + '</text>' +
      svgBody(p, opts) + '</svg>\n';
  }
  /* ---------------- Markdown ---------------- */
  function mdCell(s) { return String(s == null ? '' : s).replace(/\|/g, '\\|').replace(/\n/g, ' '); }
  function markdown(p, opts) {
    opts = opts || {};
    var s = p.settings, sched = C.scheduleInfo(C.effectiveSchedule(p)), issues = C.validate(p);
    var L = ['# ' + s.name, ''];
    if (s.description) L.push(s.description, '');
    L.push('| Setting | Value |', '| --- | --- |',
      '| Owner | ' + mdCell(s.owner || '-') + ' |',
      '| Schedule | `' + mdCell(C.effectiveSchedule(p) || 'None') + '`' + (sched.ok ? ' (' + mdCell(sched.text) + ')' : '') + ' |',
      '| Timezone | ' + mdCell(s.timezone) + ' |', '| Warehouse | ' + mdCell(C.WAREHOUSES[s.warehouse].label) + ' |',
      '| Raw / staging schema | `' + s.rawSchema + '` / `' + s.stagingSchema + '` |',
      '| Retries | ' + s.retries + ' (every ' + s.retryDelayMin + ' min) |', '| Start date | ' + s.startDate + (s.catchup ? ' (catch-up on)' : '') + ' |');
    if (s.tags && s.tags.length) L.push('| Tags | ' + mdCell(C.asList(s.tags).join(', ')) + ' |');
    if (s.failureEmail) L.push('| Failure email | ' + mdCell(s.failureEmail) + ' |');
    L.push('', '## Diagram', '', '```mermaid', mermaid(p, { hints: false }).trim(), '```', '');
    var order = C.topoSort(p), ids = order.order.concat(order.cyclic);
    L.push('## Steps', '', '| # | Step | Type | Details | Upstream |', '| --- | --- | --- | --- | --- |');
    ids.forEach(function (id, i) {
      var n = C.nodeById(p, id), t = N.get(n.type);
      var ups = C.upstreamEdges(p, id).map(function (e) { var u = C.nodeById(p, e.from); return u ? '`' + u.name + '`' + (e.fromPort !== 'out' ? ' (' + e.fromPort + ')' : '') : ''; }).join(', ');
      L.push('| ' + (i + 1) + ' | `' + n.name + '` | ' + mdCell(t ? t.icon + ' ' + t.label : n.type) + ' | ' + mdCell(N.summary(n)) + ' | ' + (ups || '-') + ' |');
    });
    var detail = ids.map(function (id) { return C.nodeById(p, id); }).filter(function (n) { return n.notes || ['sql', 'dbt_model', 'custom_check', 'python'].indexOf(n.type) !== -1 || C.relation(p, n); });
    if (detail.length) {
      L.push('', '## Step details', '');
      detail.forEach(function (n) {
        L.push('### `' + n.name + '`', '');
        if (n.notes) L.push(n.notes, '');
        var rel = C.relation(p, n); if (rel) L.push('Writes / reads: `' + rel + '`', '');
        var cols = C.columnsOf(p, n); if (cols && cols.length) L.push('Columns: ' + cols.map(function (c) { return '`' + c + '`'; }).join(', '), '');
        if (n.config.sql) L.push('```sql', String(n.config.sql).trim(), '```', '');
        if (n.config.code) L.push('```python', String(n.config.code).trim(), '```', '');
      });
    }
    var checks = p.nodes.filter(function (n) { return C.kindOf(n) === 'check'; });
    if (checks.length) {
      L.push('## Data quality checks', '');
      checks.forEach(function (n) { var prod = C.dataProducer(p, n.id); L.push('- **' + n.name + '** (' + N.get(n.type).label + ') on `' + (prod ? prod.name : '?') + '`: ' + N.summary(n)); });
      L.push('');
    }
    var alerts = p.nodes.filter(function (n) { return n.type === 'alert' || n.type === 'sla'; });
    if (alerts.length) { L.push('## Alerts and SLAs', ''); alerts.forEach(function (n) { L.push('- **' + n.name + '**: ' + N.summary(n)); }); L.push(''); }
    var real = issues.filter(function (i) { return i.level !== 'info'; });
    L.push('## Validation', '', real.length ? real.map(function (i) { return '- ' + (i.level === 'error' ? '\u274c' : '\u26a0\ufe0f') + ' ' + i.message; }).join('\n') : 'No issues found.', '');
    if (sched.ok && sched.kind === 'cron' && CRON) {
      try {
        var tz = s.timezone || 'UTC', runs = CRON.nextRuns(CRON.parse(sched.cron), { count: 5, timeZone: tz, from: opts.now != null ? opts.now : Date.now() });
        var fmt = new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23', weekday: 'short' });
        if (runs.length) L.push('## Next runs (' + tz + ')', '', runs.map(function (t) { return '- ' + fmt.format(new Date(t)); }).join('\n'), '');
      } catch (e) { /* ignore */ }
    }
    L.push('---', '', '_Generated by [PipelineSketch](https://shaikbasha1437.github.io/pipelinesketch/)._', '');
    return L.join('\n');
  }
  return { mermaid: mermaid, svg: svg, svgBody: svgBody, defs: defs, markdown: markdown, curve: curve, edgePath: edgePath, contentBounds: contentBounds, THEMES: THEMES, catColor: catColor, catOf: catOf };
});
