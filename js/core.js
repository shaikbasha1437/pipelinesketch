/* PipelineSketch core: pipeline model, graph algorithms, SQL compiler, validation, layout, share links. MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./nodes.js'), require('./cron.js'));
  else (root.PS = root.PS || {}).core = factory(root.PS.nodes, root.CronCore);
})(typeof self !== 'undefined' ? self : this, function (NODES, Cron) {
  'use strict';
  var VERSION = 1;
  var NODE_W = 220;
  var WAREHOUSES = {
    snowflake: { label: 'Snowflake', conn: 'snowflake_default' },
    bigquery: { label: 'BigQuery', conn: 'google_cloud_default' },
    postgres: { label: 'Postgres', conn: 'postgres_default' },
    redshift: { label: 'Redshift', conn: 'redshift_default' },
    duckdb: { label: 'DuckDB', conn: 'duckdb_default' }
  };

  /* ---------------- ids & identifiers ---------------- */
  var seq = 0;
  function newId(prefix) {
    seq = (seq + 1) % 1e6;
    return (prefix || 'n') + Date.now().toString(36).slice(-5) + Math.random().toString(36).slice(2, 6) + seq.toString(36);
  }
  var PY_KW = 'false none true and as assert async await break class continue def del elif else except finally for from global if import in is lambda nonlocal not or pass raise return try while with yield match case print'.split(' ');
  var SQL_RESERVED = 'all alter and any array as asc between both by case cast check column constraint create cross current current_date current_time current_timestamp current_user default delete desc distinct drop else end except exists false fetch for foreign from full grant group having in inner insert intersect interval into is join lateral leading left like limit natural not null offset on only or order outer primary qualify references right row rows select session_user some table then to trailing true union unique update user using values when where window with'.split(' ');
  var PY_NAMES = 'dag task pendulum datetime timedelta default_args pipeline pd os io json dg sa dt requests warehouse context'.split(' ');
  var RESERVED = {};
  PY_KW.concat(SQL_RESERVED, PY_NAMES).forEach(function (w) { RESERVED[w] = true; });
  function toIdent(s, fallback) {
    var t = String(s == null ? '' : s).normalize ? String(s == null ? '' : s).normalize('NFKD') : String(s);
    t = t.replace(/[\u0300-\u036f]/g, '').replace(/([a-z0-9])([A-Z])/g, '$1_$2').toLowerCase()
      .replace(/[^a-z0-9_]+/g, '_').replace(/_+/g, '_').replace(/^_+|_+$/g, '');
    if (!t) t = fallback || 'node';
    if (/^[0-9]/.test(t)) t = 't_' + t;
    if (t.length > 63) t = t.slice(0, 63).replace(/_+$/, '');
    if (RESERVED[t]) t = t + '_t';
    return t;
  }
  function isIdent(s) { return typeof s === 'string' && s === toIdent(s); }
  function uniqueName(base, taken) {
    var b = toIdent(base), n = b, i = 2;
    var has = typeof taken === 'function' ? taken : function (x) { return taken.indexOf(x) !== -1; };
    while (has(n)) { n = b.replace(/_\d+$/, '') + '_' + i; i++; }
    return n;
  }
  function asList(v) {
    if (Array.isArray(v)) return v.map(function (x) { return String(x).trim(); }).filter(Boolean);
    if (v == null || v === '') return [];
    return String(v).split(/[,\n]/).map(function (x) { return x.trim(); }).filter(Boolean);
  }
  function lines(v) { return String(v || '').split('\n').map(function (x) { return x.trim().replace(/,$/, ''); }).filter(Boolean); }

  /* ---------------- pipeline model ---------------- */
  function defaultSettings() {
    return {
      name: 'my_pipeline', description: '', owner: 'data-eng', schedule: '@daily', timezone: 'UTC',
      retries: 2, retryDelayMin: 5, startDate: '2026-01-01', catchup: false, tags: ['pipelinesketch'],
      warehouse: 'snowflake', rawSchema: 'raw', stagingSchema: 'staging', dbtProjectDir: '/opt/airflow/dbt',
      failureEmail: '',
      conns: { warehouse: '', postgres: 'postgres_default', mysql: 'mysql_default', aws: 'aws_default', gcp: 'google_cloud_default', kafka: 'kafka_default', slack: 'slack_default' }
    };
  }
  function createPipeline(name) {
    var s = defaultSettings();
    if (name) s.name = toIdent(name, 'my_pipeline');
    return { version: VERSION, id: newId('p'), settings: s, nodes: [], edges: [], groups: [], updatedAt: Date.now() };
  }
  function normalize(raw) {
    if (!raw || typeof raw !== 'object') throw new Error('Not a pipeline file');
    if (raw.pipeline && raw.pipeline.nodes) raw = raw.pipeline;
    if (!Array.isArray(raw.nodes)) throw new Error('Pipeline has no "nodes" list');
    var d = defaultSettings(), s = Object.assign({}, d, raw.settings || {});
    s.conns = Object.assign({}, d.conns, (raw.settings && raw.settings.conns) || {});
    s.tags = asList(s.tags);
    if (!WAREHOUSES[s.warehouse]) s.warehouse = 'snowflake';
    var ids = {};
    var nodes = raw.nodes.filter(function (n) { return n && NODES.get(n.type); }).map(function (n) {
      var id = n.id && !ids[n.id] ? String(n.id) : newId('n');
      ids[id] = true;
      return { id: id, type: n.type, name: String(n.name || n.type), x: +n.x || 0, y: +n.y || 0,
        config: Object.assign(NODES.defaults(n.type), n.config || {}), columns: n.columns ? asList(n.columns) : [], notes: n.notes || '' };
    });
    var edgeKeys = {};
    var edges = (raw.edges || []).filter(function (e) { return e && ids[e.from] && ids[e.to]; }).map(function (e) {
      return { id: e.id || newId('e'), from: String(e.from), fromPort: e.fromPort || 'out', to: String(e.to), toPort: e.toPort || 'in' };
    }).filter(function (e) { var k = e.from + e.fromPort + '>' + e.to + e.toPort; if (edgeKeys[k]) return false; edgeKeys[k] = 1; return true; });
    var groups = (raw.groups || []).map(function (g) {
      return { id: g.id || newId('g'), title: String(g.title || 'Group'), text: g.text || '', x: +g.x || 0, y: +g.y || 0, w: Math.max(120, +g.w || 300), h: Math.max(80, +g.h || 200), color: g.color || '#64748b' };
    });
    return { version: VERSION, id: raw.id || newId('p'), settings: s, nodes: nodes, edges: edges, groups: groups, updatedAt: raw.updatedAt || Date.now() };
  }
  function clone(o) { return JSON.parse(JSON.stringify(o)); }
  function nodeById(p, id) { for (var i = 0; i < p.nodes.length; i++) if (p.nodes[i].id === id) return p.nodes[i]; return null; }
  function nodeByName(p, name) { for (var i = 0; i < p.nodes.length; i++) if (p.nodes[i].name === name) return p.nodes[i]; return null; }
  function addNode(p, type, x, y, name) {
    var t = NODES.get(type);
    if (!t) throw new Error('Unknown node type ' + type);
    var n = { id: newId('n'), type: type, name: uniqueName(name || defaultName(type), p.nodes.map(function (m) { return m.name; })), x: x || 0, y: y || 0, config: NODES.defaults(type), columns: [], notes: '' };
    p.nodes.push(n);
    return n;
  }
  function defaultName(type) {
    var m = { pg_table: 'pg_source', mysql_table: 'mysql_source', s3_files: 's3_files', gcs_files: 'gcs_files', kafka_topic: 'kafka_events', rest_api: 'api_source',
      snowflake_table: 'sf_table', bigquery_table: 'bq_table', sql: 'sql_transform', warehouse_table: 'load_table', s3_parquet: 'export_s3', lake_table: 'lake_table',
      kafka_sink: 'publish_kafka', alert: 'notify', custom_check: 'custom_check', dbt_model: 'dbt_model', python: 'py_transform' };
    return m[type] || type;
  }
  function canConnect(p, from, fromPort, to, toPort) {
    var a = nodeById(p, from), b = nodeById(p, to);
    if (!a || !b) return { ok: false, reason: 'Missing node' };
    if (from === to) return { ok: false, reason: 'A node can\u2019t connect to itself' };
    if (NODES.get(a.type).outPorts.indexOf(fromPort) === -1) return { ok: false, reason: 'No output port ' + fromPort };
    if (NODES.get(b.type).inPorts.indexOf(toPort) === -1) return { ok: false, reason: b.name + ' has no input port ' + toPort };
    if (p.edges.some(function (e) { return e.from === from && e.to === to && e.fromPort === fromPort && e.toPort === toPort; })) return { ok: false, reason: 'Already connected' };
    return { ok: true, createsCycle: reachable(p, to, from) };
  }
  function addEdge(p, from, fromPort, to, toPort) {
    var c = canConnect(p, from, fromPort || 'out', to, toPort || 'in');
    if (!c.ok) return null;
    var e = { id: newId('e'), from: from, fromPort: fromPort || 'out', to: to, toPort: toPort || 'in' };
    p.edges.push(e);
    return e;
  }
  function removeNodes(p, ids) {
    var set = {}; ids.forEach(function (i) { set[i] = 1; });
    p.nodes = p.nodes.filter(function (n) { return !set[n.id]; });
    p.edges = p.edges.filter(function (e) { return !set[e.from] && !set[e.to]; });
  }
  function renameNode(p, id, newName) {
    var n = nodeById(p, id); if (!n) return null;
    var old = n.name;
    var name = uniqueName(newName, function (x) { return x !== old && p.nodes.some(function (m) { return m.name === x; }); });
    n.name = name;
    if (old !== name) {
      // keep {{ ref('old') }} references working
      var re = new RegExp("ref\\(\\s*(['\"])" + old.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + "\\1\\s*\\)", 'g');
      p.nodes.forEach(function (m) { ['sql'].forEach(function (k) { if (m.config && typeof m.config[k] === 'string') m.config[k] = m.config[k].replace(re, "ref('" + name + "')"); }); });
    }
    return name;
  }
  // Copy a set of nodes (and edges among them) into a portable fragment.
  function copyFragment(p, ids) {
    var set = {}; ids.forEach(function (i) { set[i] = 1; });
    return { kind: 'pipelinesketch-fragment', nodes: clone(p.nodes.filter(function (n) { return set[n.id]; })), edges: clone(p.edges.filter(function (e) { return set[e.from] && set[e.to]; })) };
  }
  function pasteFragment(p, frag, dx, dy) {
    var map = {}, added = [];
    var taken = p.nodes.map(function (n) { return n.name; });
    (frag.nodes || []).forEach(function (n) {
      if (!NODES.get(n.type)) return;
      var c = clone(n); c.id = newId('n'); c.x = (+n.x || 0) + (dx || 0); c.y = (+n.y || 0) + (dy || 0);
      c.name = uniqueName(n.name, taken); taken.push(c.name);
      c.config = Object.assign(NODES.defaults(n.type), c.config || {}); c.columns = asList(c.columns);
      map[n.id] = c.id; p.nodes.push(c); added.push(c.id);
    });
    (frag.edges || []).forEach(function (e) { if (map[e.from] && map[e.to]) p.edges.push({ id: newId('e'), from: map[e.from], fromPort: e.fromPort, to: map[e.to], toPort: e.toPort }); });
    return added;
  }

  /* ---------------- geometry ---------------- */
  function nodeSize(n) {
    var t = NODES.get(n.type) || { inPorts: ['in'], outPorts: ['out'] };
    var k = Math.max(t.inPorts.length, t.outPorts.length, 1);
    return { w: NODE_W, h: Math.max(64, 30 + 24 * k) };
  }
  function portPos(n, port, dir) {
    var t = NODES.get(n.type), list = dir === 'in' ? t.inPorts : t.outPorts, sz = nodeSize(n);
    var i = Math.max(0, list.indexOf(port)), k = list.length || 1;
    return { x: n.x + (dir === 'in' ? 0 : sz.w), y: n.y + Math.round(sz.h * (i + 1) / (k + 1)) };
  }
  function bounds(p, ids) {
    var set = ids ? {} : null; if (ids) ids.forEach(function (i) { set[i] = 1; });
    var b = { x1: Infinity, y1: Infinity, x2: -Infinity, y2: -Infinity };
    p.nodes.forEach(function (n) { if (set && !set[n.id]) return; var s = nodeSize(n); b.x1 = Math.min(b.x1, n.x); b.y1 = Math.min(b.y1, n.y); b.x2 = Math.max(b.x2, n.x + s.w); b.y2 = Math.max(b.y2, n.y + s.h); });
    if (!set) (p.groups || []).forEach(function (g) { b.x1 = Math.min(b.x1, g.x); b.y1 = Math.min(b.y1, g.y); b.x2 = Math.max(b.x2, g.x + g.w); b.y2 = Math.max(b.y2, g.y + g.h); });
    if (b.x1 === Infinity) return { x1: 0, y1: 0, x2: 0, y2: 0, empty: true };
    return b;
  }
  function nodesInRect(p, r) {
    return p.nodes.filter(function (n) { var s = nodeSize(n); return n.x >= r.x && n.y >= r.y && n.x + s.w <= r.x + r.w && n.y + s.h <= r.y + r.h; }).map(function (n) { return n.id; });
  }

  /* ---------------- graph ---------------- */
  function upstreamEdges(p, id, port) { return p.edges.filter(function (e) { return e.to === id && (!port || e.toPort === port); }); }
  function downstreamEdges(p, id, port) { return p.edges.filter(function (e) { return e.from === id && (!port || e.fromPort === port); }); }
  function reachable(p, from, to) {
    var seen = {}, stack = [from];
    while (stack.length) {
      var x = stack.pop(); if (x === to) return true;
      if (seen[x]) continue; seen[x] = 1;
      p.edges.forEach(function (e) { if (e.from === x) stack.push(e.to); });
    }
    return false;
  }
  // Kahn's algorithm, stable by canvas position (left to right, top to bottom).
  function topoSort(p) {
    var indeg = {}, adj = {}, pos = {};
    p.nodes.forEach(function (n, i) { indeg[n.id] = 0; adj[n.id] = []; pos[n.id] = n; });
    p.edges.forEach(function (e) { if (indeg[e.to] == null || indeg[e.from] == null) return; indeg[e.to]++; adj[e.from].push(e.to); });
    var cmp = function (a, b) { return (pos[a].x - pos[b].x) || (pos[a].y - pos[b].y) || (a < b ? -1 : a > b ? 1 : 0); };
    var ready = p.nodes.filter(function (n) { return indeg[n.id] === 0; }).map(function (n) { return n.id; }).sort(cmp);
    var order = [];
    while (ready.length) {
      var id = ready.shift(); order.push(id);
      adj[id].forEach(function (t) { if (--indeg[t] === 0) { ready.push(t); ready.sort(cmp); } });
    }
    var cyclic = p.nodes.filter(function (n) { return order.indexOf(n.id) === -1; }).map(function (n) { return n.id; });
    return { order: order, ok: cyclic.length === 0, cyclic: cyclic };
  }
  // Tarjan's SCC: every strongly connected component with a real cycle, each as an ordered path.
  function findCycles(p) {
    var index = 0, stack = [], on = {}, idx = {}, low = {}, out = [], adj = {};
    p.nodes.forEach(function (n) { adj[n.id] = []; });
    p.edges.forEach(function (e) { if (adj[e.from] && adj[e.to]) adj[e.from].push(e.to); });
    function strong(v) {
      idx[v] = low[v] = index++; stack.push(v); on[v] = true;
      adj[v].forEach(function (w) {
        if (idx[w] == null) { strong(w); low[v] = Math.min(low[v], low[w]); }
        else if (on[w]) low[v] = Math.min(low[v], idx[w]);
      });
      if (low[v] === idx[v]) {
        var comp = [], w;
        do { w = stack.pop(); on[w] = false; comp.push(w); } while (w !== v);
        if (comp.length > 1 || adj[v].indexOf(v) !== -1) out.push(cyclePath(comp, adj));
      }
    }
    p.nodes.forEach(function (n) { if (idx[n.id] == null) strong(n.id); });
    return out;
  }
  function cyclePath(comp, adj) {
    var inC = {}; comp.forEach(function (c) { inC[c] = 1; });
    var start = comp.slice().sort()[0], path = [start], seen = {}; seen[start] = 1;
    var cur = start;
    for (var guard = 0; guard < comp.length + 1; guard++) {
      var nxt = adj[cur].filter(function (w) { return inC[w]; });
      var back = nxt.indexOf(start) !== -1 && path.length > 1;
      var fresh = nxt.filter(function (w) { return !seen[w]; })[0];
      if (back && !fresh) break;
      if (!fresh) { if (nxt.indexOf(start) !== -1) break; break; }
      path.push(fresh); seen[fresh] = 1; cur = fresh;
      if (adj[cur].indexOf(start) !== -1 && path.length === comp.length) break;
    }
    return path;
  }
  function kindOf(n) { var t = NODES.get(n.type); return t ? t.kind : null; }
  var PRODUCES = { extract: 1, wh_source: 1, sql: 1, python: 1, dbt: 1 };
  // The node that physically holds the data flowing out of `id` (checks, sensors, branches and most sinks pass data through).
  function dataProducer(p, id, seen) {
    seen = seen || {};
    if (seen[id]) return null; seen[id] = 1;
    var n = nodeById(p, id); if (!n) return null;
    var k = kindOf(n);
    if (PRODUCES[k]) return n;
    if (n.type === 'warehouse_table') return n;
    if (n.type === 'schedule' || n.type === 'sla') return null;
    var ups = upstreamEdges(p, id);
    for (var i = 0; i < ups.length; i++) { var r = dataProducer(p, ups[i].from, seen); if (r) return r; }
    return null;
  }
  function dataInputs(p, id, port) {
    var out = [];
    upstreamEdges(p, id, port).forEach(function (e) {
      var prod = dataProducer(p, e.from);
      if (prod && out.indexOf(prod) === -1) out.push(prod);
    });
    return out;
  }
  function isTask(n) { return n.type !== 'schedule' && n.type !== 'sla' && kindOf(n) !== 'wh_source'; }
  // Nearest upstream tasks, skipping things that are not tasks (warehouse tables, schedule nodes).
  function taskUpstreams(p, id) {
    var res = [], seen = {};
    (function walk(x) {
      upstreamEdges(p, x).forEach(function (e) {
        if (seen[e.from]) return; seen[e.from] = 1;
        var u = nodeById(p, e.from); if (!u) return;
        if (isTask(u)) { if (res.indexOf(u.id) === -1) res.push(u.id); }
        else if (u.type !== 'schedule' && u.type !== 'sla') walk(u.id);
      });
    })(id);
    return res;
  }
  function descendants(p, id) {
    var seen = {}, stack = [id];
    while (stack.length) { var x = stack.pop(); p.edges.forEach(function (e) { if (e.from === x && !seen[e.to]) { seen[e.to] = 1; stack.push(e.to); } }); }
    return seen;
  }
  function branchDescendants(p) {
    var all = {};
    p.nodes.forEach(function (n) { if (n.type === 'branch') { var d = descendants(p, n.id); for (var k in d) all[k] = 1; } });
    return all;
  }
  function effectiveSchedule(p) {
    var s = p.nodes.filter(function (n) { return n.type === 'schedule' && n.config.cron; })[0];
    return s ? String(s.config.cron).trim() : String(p.settings.schedule || '').trim();
  }
  function slaFor(p, id) {
    var mins = null;
    downstreamEdges(p, id).forEach(function (e) { var t = nodeById(p, e.to); if (t && t.type === 'sla' && +t.config.minutes > 0) mins = mins == null ? +t.config.minutes : Math.min(mins, +t.config.minutes); });
    return mins;
  }

  /* ---------------- relations & SQL compiler ---------------- */
  function whConn(s) { return (s.conns && s.conns.warehouse) || WAREHOUSES[s.warehouse].conn; }
  function sqlLit(v) { return /^-?\d+(\.\d+)?$/.test(String(v)) ? String(v) : "'" + String(v).replace(/'/g, "''") + "'"; }
  function schemaOf(rel) { var parts = String(rel).replace(/`/g, '').split('.'); return parts.length === 2 ? parts[0] : null; }
  function tableOf(rel) { var parts = String(rel).replace(/`/g, '').split('.'); return parts[parts.length - 1]; }
  // Physical relation of a data-producing node inside the warehouse.
  function relation(p, n) {
    if (!n) return null;
    var s = p.settings, c = n.config || {}, k = kindOf(n);
    if (n.type === 'snowflake_table') return [c.database, c.schema, c.table].filter(Boolean).join('.');
    if (n.type === 'bigquery_table') return c.project ? '`' + [c.project, c.dataset, c.table].join('.') + '`' : [c.dataset, c.table].join('.');
    if (k === 'extract') return s.rawSchema + '.' + n.name;
    if (k === 'sql' || k === 'python' || k === 'dbt') return s.stagingSchema + '.' + n.name;
    if (n.type === 'warehouse_table') return c.table || n.name;
    return null;
  }
  function substituteSql(sql, ins, refFn) {
    return String(sql || '')
      .replace(/\{\{\s*input(?:_(\d+))?\s*\}\}/g, function (m, i) { var r = ins[i ? +i - 1 : 0]; return r || '/* missing input ' + (i || 1) + ' */'; })
      .replace(/\{\{\s*ref\(\s*['"]([A-Za-z0-9_]+)['"]\s*\)\s*\}\}/g, function (m, name) { var r = refFn(name); return r || m; });
  }
  // Compile a transform node to a SELECT. relOf(node) maps an upstream producer to how it is referenced.
  function compileSelect(p, n, relOf, dialect) {
    var c = n.config || {};
    var rel = function (x) { return x ? relOf(x) : null; };
    var ins = n.type === 'join' ? [] : dataInputs(p, n.id).map(rel);
    var first = ins[0] || '/* missing input */';
    var refFn = function (name) { var m = nodeByName(p, name); return m ? rel(dataProducer(p, m.id) || m) : null; };
    switch (n.type) {
      case 'sql': case 'dbt_model': return substituteSql(c.sql, ins, refFn).trim();
      case 'filter': return 'select *\nfrom ' + first + '\nwhere ' + (c.condition || 'true');
      case 'select': return 'select\n' + lines(c.columns).map(function (l) { return '    ' + l; }).join(',\n') + '\nfrom ' + first;
      case 'join': {
        var L = rel(dataInputs(p, n.id, 'left')[0]) || '/* missing left input */', R = rel(dataInputs(p, n.id, 'right')[0]) || '/* missing right input */';
        var how = { inner: 'inner join', left: 'left join', right: 'right join', full: 'full outer join' }[c.how || 'inner'] || 'inner join';
        var on = String(c.on || '').trim(), cond = /[=<>]|\band\b/i.test(on) ? '\n    on ' + on : '\n    using (' + asList(on).join(', ') + ')';
        var sel = lines(c.select || '*'); if (!sel.length) sel = ['*'];
        return 'select\n' + sel.map(function (l) { return '    ' + l; }).join(',\n') + '\nfrom ' + L + ' as l\n' + how + ' ' + R + ' as r' + cond;
      }
      case 'aggregate': {
        var g = asList(c.group_by), a = lines(c.aggregations);
        return 'select\n' + g.concat(a).map(function (l) { return '    ' + l; }).join(',\n') + '\nfrom ' + first + (g.length ? '\ngroup by ' + g.join(', ') : '');
      }
      case 'dedupe': {
        var keys = asList(c.keys).join(', '), ob = String(c.order_by || '').trim() || keys;
        if (dialect === 'postgres') return 'select distinct on (' + keys + ') *\nfrom ' + first + '\norder by ' + keys + ', ' + ob;
        return 'select *\nfrom ' + first + (dialect === 'bigquery' ? '\nwhere true' : '') + '\nqualify row_number() over (partition by ' + keys + ' order by ' + ob + ') = 1';
      }
      case 'union': {
        if (!ins.length) return 'select * from /* missing input */';
        var op = c.distinct ? (dialect === 'bigquery' ? 'union distinct' : 'union') : 'union all';
        return ins.map(function (r) { return 'select * from ' + r; }).join('\n' + op + '\n');
      }
      case 'pivot': {
        var idx = asList(c.index), agg = (c.agg || 'sum').toLowerCase();
        var cols = asList(c.values).map(function (v) {
          return '    ' + agg + '(case when ' + c.pivot_column + ' = ' + sqlLit(v) + ' then ' + c.value_column + ' end) as ' + toIdent(v, 'value');
        });
        return 'select\n' + idx.map(function (x) { return '    ' + x; }).concat(cols).join(',\n') + '\nfrom ' + first + '\ngroup by ' + idx.join(', ');
      }
    }
    return null;
  }
  function createSchema(schema) { return schema ? ['create schema if not exists ' + schema] : []; }
  // Statements that (re)build `rel` from a SELECT.
  function materialize(rel, select, dialect) {
    var pre = createSchema(schemaOf(rel));
    if (dialect === 'postgres' || dialect === 'redshift') return pre.concat(['drop table if exists ' + rel, 'create table ' + rel + ' as\n' + select]);
    return pre.concat(['create or replace table ' + rel + ' as\n' + select]);
  }
  function ensureTable(target, source, dialect) {
    if (dialect === 'redshift') return 'create table if not exists ' + target + ' (like ' + source + ')';
    return 'create table if not exists ' + target + ' as\nselect * from ' + source + ' where 1 = 0';
  }
  // Statements that write `source` into a warehouse table with the node's write mode.
  function sinkStatements(target, source, mode, keys, cols, dialect) {
    var pre = createSchema(schemaOf(target));
    if (mode === 'append') return pre.concat([ensureTable(target, source, dialect), 'insert into ' + target + '\nselect * from ' + source]);
    if (mode === 'merge') {
      keys = asList(keys);
      if (cols && cols.length && (dialect === 'snowflake' || dialect === 'bigquery' || dialect === 'postgres')) {
        var nonKey = cols.filter(function (c) { return keys.indexOf(c) === -1; });
        return pre.concat([ensureTable(target, source, dialect),
          'merge into ' + target + ' as t\nusing ' + source + ' as s\n    on ' + keys.map(function (k) { return 't.' + k + ' = s.' + k; }).join(' and ') +
          (nonKey.length ? '\nwhen matched then update set\n    ' + nonKey.map(function (c) { return c + ' = s.' + c; }).join(',\n    ') : '') +
          '\nwhen not matched then insert (' + cols.join(', ') + ')\n    values (' + cols.map(function (c) { return 's.' + c; }).join(', ') + ')']);
      }
      // Without a declared column list: delete matching keys, then insert (same result as a merge, in one transaction).
      var del = dialect === 'bigquery'
        ? 'delete from ' + target + ' as t\nwhere exists (\n    select 1 from ' + source + ' as s\n    where ' + keys.map(function (k) { return 's.' + k + ' = t.' + k; }).join(' and ') + '\n)'
        : 'delete from ' + target + '\nusing ' + source + ' as s\nwhere ' + keys.map(function (k) { return target + '.' + k + ' = s.' + k; }).join(' and ');
      return pre.concat([ensureTable(target, source, dialect), del, 'insert into ' + target + '\nselect * from ' + source]);
    }
    return materialize(target, 'select * from ' + source, dialect);
  }
  function freshnessExpr(col, hours, dialect) {
    var h = Math.max(1, Math.round(+hours || 24));
    if (dialect === 'snowflake') return 'max(' + col + ') >= dateadd(hour, -' + h + ', current_timestamp())';
    if (dialect === 'bigquery') return 'max(' + col + ') >= timestamp_sub(current_timestamp(), interval ' + h + ' hour)';
    if (dialect === 'redshift') return 'max(' + col + ') >= dateadd(hour, -' + h + ', getdate())';
    return 'max(' + col + ") >= now() - interval '" + h + " hours'";
  }

  /* ---------------- column inference ---------------- */
  function aliasOf(expr) {
    var m = /\bas\s+([A-Za-z_][A-Za-z0-9_]*)\s*$/i.exec(expr); if (m) return m[1].toLowerCase();
    var m2 = /^(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)$/.exec(expr.trim()); return m2 ? m2[1].toLowerCase() : null;
  }
  function columnsOf(p, n, seen) {
    if (!n) return null;
    seen = seen || {}; if (seen[n.id]) return null; seen[n.id] = 1;
    if (n.columns && n.columns.length) return n.columns.map(function (c) { return String(c).split(':')[0].trim().toLowerCase(); });
    var c = n.config || {};
    var inCols = function (port) { var u = upstreamEdges(p, n.id, port)[0]; return u ? columnsOf(p, nodeById(p, u.from), seen) : null; };
    switch (n.type) {
      case 'filter': case 'dedupe': case 'not_null': case 'unique': case 'accepted_values': case 'row_count': case 'freshness': case 'custom_check':
      case 'sensor': case 'branch': case 'warehouse_table': case 's3_parquet': case 'lake_table': case 'kafka_sink': case 'alert':
        return inCols();
      case 'union': return inCols();
      case 'select': { var out = lines(c.columns).map(aliasOf); return out.indexOf(null) === -1 && out.indexOf('*') === -1 ? out : null; }
      case 'aggregate': { var o = asList(c.group_by).map(aliasOf).concat(lines(c.aggregations).map(aliasOf)); return o.indexOf(null) === -1 ? o : null; }
      case 'pivot': return asList(c.index).map(aliasOf).concat(asList(c.values).map(function (v) { return toIdent(v, 'value'); }));
      case 'join': {
        var L = inCols('left'), R = inCols('right'), sel = lines(c.select || '*');
        if (sel.length === 1 && sel[0] === '*' && L && R) {
          var on = String(c.on || ''); var using = /[=<>]|\band\b/i.test(on) ? [] : asList(on).map(function (x) { return x.toLowerCase(); });
          return L.concat(R.filter(function (x) { return using.indexOf(x) === -1; }));
        }
        var res = sel.map(aliasOf); return res.indexOf(null) === -1 ? res : null;
      }
    }
    return null;
  }

  /* ---------------- cron / schedules ---------------- */
  var PRESETS = [
    { value: '@hourly', label: 'Hourly' }, { value: '@daily', label: 'Daily at midnight' }, { value: '0 6 * * *', label: 'Daily at 06:00' },
    { value: '0 6 * * 1-5', label: 'Weekdays at 06:00' }, { value: '*/15 * * * *', label: 'Every 15 minutes' }, { value: '@weekly', label: 'Weekly (Sunday)' },
    { value: '@monthly', label: 'Monthly' }, { value: '@once', label: 'Once' }, { value: 'None', label: 'Manual only' }
  ];
  function scheduleInfo(expr, opts) {
    opts = opts || {};
    var e = String(expr == null ? '' : expr).trim();
    if (!e || e === 'None' || e === 'none' || e === 'null') return { ok: true, kind: 'none', text: 'Manual only: runs when triggered', next: [], cron: null };
    if (e === '@once') return { ok: true, kind: 'once', text: 'Runs once, right after the start date', next: [], cron: null };
    if (e === '@continuous') return { ok: true, kind: 'continuous', text: 'Runs continuously, a new run as soon as the last one ends', next: [], cron: null };
    if (/^@/.test(e) && !Cron.MACROS[e.toLowerCase()]) return { ok: false, error: 'Unknown preset "' + e + '". Use @hourly, @daily, @weekly, @monthly, @yearly, @once or None.' };
    if (e.split(/\s+/).length === 6 && !/^@/.test(e)) return { ok: false, error: 'Airflow cron has 5 fields (minute hour day month weekday); a seconds field isn\u2019t supported.' };
    var parsed = Cron.parse(e);
    if (!parsed.ok) return { ok: false, error: parsed.errors.map(function (x) { return x.message; }).join('; ') };
    var text = Cron.describe(parsed, { h24: true });
    var next = Cron.nextRuns(parsed, { count: opts.count || 5, timeZone: opts.timeZone || 'UTC', from: opts.from });
    return { ok: true, kind: 'cron', text: text.charAt(0).toUpperCase() + text.slice(1), next: next, cron: parsed.expanded };
  }

  /* ---------------- validation ---------------- */
  function validate(p) {
    var issues = [];
    var add = function (level, code, message, nodeId, extra) { var i = { level: level, code: code, message: message }; if (nodeId) i.nodeId = nodeId; if (extra) Object.assign(i, extra); issues.push(i); };
    var s = p.settings, wh = s.warehouse;
    if (!p.nodes.length) { add('info', 'empty', 'The canvas is empty. Drag a node from the palette or start from a template.'); return issues; }
    // settings
    if (!isIdent(s.name)) add('error', 'pipeline-name', 'Pipeline name "' + s.name + '" isn\u2019t a valid identifier. Try "' + toIdent(s.name, 'my_pipeline') + '".', null, { fix: 'settings' });
    if (!String(s.owner || '').trim()) add('warning', 'owner', 'Pipeline has no owner.', null, { fix: 'settings' });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(String(s.startDate || '')) || isNaN(Date.parse(s.startDate))) add('error', 'start-date', 'Start date must be YYYY-MM-DD.', null, { fix: 'settings' });
    if (!(+s.retries >= 0)) add('error', 'retries', 'Retries must be 0 or more.', null, { fix: 'settings' });
    var sched = scheduleInfo(effectiveSchedule(p));
    if (!sched.ok) add('error', 'schedule', 'Schedule: ' + sched.error, (p.nodes.filter(function (n) { return n.type === 'schedule'; })[0] || {}).id, { fix: 'settings' });
    ['rawSchema', 'stagingSchema'].forEach(function (k) { if (!isIdent(s[k])) add('error', 'schema', 'Settings: "' + s[k] + '" isn\u2019t a valid schema name.', null, { fix: 'settings' }); });
    // edges
    p.edges.forEach(function (e) {
      var a = nodeById(p, e.from), b = nodeById(p, e.to);
      if (!a || !b) add('error', 'edge', 'An edge points at a missing node.', null, { edgeId: e.id });
      else if (NODES.get(a.type).outPorts.indexOf(e.fromPort) === -1 || NODES.get(b.type).inPorts.indexOf(e.toPort) === -1) add('error', 'edge-port', 'Edge ' + a.name + ' \u2192 ' + b.name + ' uses a port that doesn\u2019t exist.', b.id, { edgeId: e.id });
    });
    // cycles
    findCycles(p).forEach(function (cyc) {
      var names = cyc.map(function (id) { return nodeById(p, id).name; });
      add('error', 'cycle', 'Cycle: ' + names.concat(names[0]).join(' \u2192 ') + '. Pipelines must be acyclic (a DAG).', cyc[0], { nodes: cyc });
    });
    // names
    var seenNames = {};
    p.nodes.forEach(function (n) {
      if (!isIdent(n.name)) add('error', 'name', '"' + n.name + '" isn\u2019t a valid name (lowercase letters, digits and _; not a reserved word). Try "' + toIdent(n.name) + '".', n.id, { fix: 'rename', suggestion: toIdent(n.name) });
      if (seenNames[n.name]) add('error', 'duplicate-name', 'Two nodes are named "' + n.name + '". Names become task IDs and table names, so they must be unique.', n.id);
      seenNames[n.name] = 1;
    });
    var schedules = p.nodes.filter(function (n) { return n.type === 'schedule'; });
    if (schedules.length > 1) add('warning', 'schedules', 'More than one Schedule node; only "' + schedules[0].name + '" is used.', schedules[1].id);
    var many = p.nodes.length > 1;
    p.nodes.forEach(function (n) {
      var t = NODES.get(n.type), c = n.config || {};
      if (!t) { add('error', 'type', 'Unknown node type ' + n.type, n.id); return; }
      t.fields.forEach(function (f) {
        if (!f.required) return;
        var v = c[f.key], empty = v == null || (Array.isArray(v) ? !v.length : String(v).trim() === '');
        if (empty) add('error', 'required', n.name + ': "' + f.label + '" is required.', n.id, { field: f.key });
      });
      var ups = upstreamEdges(p, n.id), downs = downstreamEdges(p, n.id), data = dataInputs(p, n.id);
      if (many && !ups.length && !downs.length) add('warning', 'disconnected', n.name + ' isn\u2019t connected to anything.', n.id);
      if (t.category === 'source' && many && !downs.length && ups.length + downs.length > 0) add('warning', 'unused-source', 'Data from ' + n.name + ' isn\u2019t used by any downstream node.', n.id);
      if (t.category === 'source' && !downs.length && many && !ups.length) { /* covered by disconnected */ }
      if (t.category === 'sink' && !ups.length) add('error', 'sink-no-input', 'Sink ' + n.name + ' has nothing upstream to write.', n.id);
      else if (t.category === 'sink' && n.type !== 'alert' && !data.length) add('error', 'sink-no-data', 'Sink ' + n.name + ' has no data input (its upstream nodes don\u2019t produce a table).', n.id);
      if ((t.category === 'transform' || t.category === 'quality') && n.type !== 'join' && !data.length) add('error', 'no-input', n.name + ' needs an input.', n.id);
      if (n.type === 'join') {
        var L = dataInputs(p, n.id, 'left'), R = dataInputs(p, n.id, 'right');
        if (!L.length) add('error', 'join-left', 'Join ' + n.name + ' has nothing on its left input.', n.id);
        if (!R.length) add('error', 'join-right', 'Join ' + n.name + ' has nothing on its right input.', n.id);
        var lc = L[0] && columnsOf(p, L[0]), rc = R[0] && columnsOf(p, R[0]), on = String(c.on || '').trim();
        if (lc && rc && on) {
          if (/[=<>]|\band\b/i.test(on)) {
            var bad = [];
            on.replace(/\b([lr])\.([A-Za-z_][A-Za-z0-9_]*)/g, function (m, side, col) { if ((side === 'l' ? lc : rc).indexOf(col.toLowerCase()) === -1) bad.push(m); return m; });
            if (bad.length) add('error', 'join-schema', 'Join ' + n.name + ': ' + bad.join(', ') + ' not found in the input columns (left: ' + lc.join(', ') + '; right: ' + rc.join(', ') + ').', n.id);
            var sel = lines(c.select || '*');
            var dup = lc.filter(function (x) { return rc.indexOf(x) !== -1; });
            if (sel.length === 1 && sel[0] === '*' && dup.length) add('warning', 'join-dup', 'Join ' + n.name + ': both sides have ' + dup.join(', ') + '; "select *" with an ON join creates duplicate column names. List output columns or join with USING.', n.id);
          } else {
            asList(on).forEach(function (k) {
              var miss = []; if (lc.indexOf(k.toLowerCase()) === -1) miss.push('left'); if (rc.indexOf(k.toLowerCase()) === -1) miss.push('right');
              if (miss.length) add('error', 'join-schema', 'Join ' + n.name + ': key "' + k + '" is missing on the ' + miss.join(' and ') + ' side (left: ' + lc.join(', ') + '; right: ' + rc.join(', ') + ').', n.id);
            });
          }
        }
      }
      if (n.type === 'union') {
        if (data.length < 2) add('warning', 'union-inputs', 'Union ' + n.name + ' has ' + data.length + ' input; connect at least two.', n.id);
        var schemas = data.map(function (d) { return columnsOf(p, d); }).filter(Boolean);
        if (schemas.length > 1 && schemas.some(function (sc) { return sc.join(',') !== schemas[0].join(','); }))
          add('error', 'union-schema', 'Union ' + n.name + ': inputs have different columns (' + schemas.map(function (sc) { return '[' + sc.join(', ') + ']'; }).join(' vs ') + ').', n.id);
      }
      if (['filter', 'select', 'aggregate', 'dedupe', 'pivot'].indexOf(n.type) !== -1 && data.length > 1) add('warning', 'extra-inputs', n.name + ' uses only its first input; the others are ignored.', n.id);
      if ((n.type === 'sql' || n.type === 'dbt_model' || n.type === 'custom_check') && c.sql) {
        String(c.sql).replace(/\{\{\s*input_(\d+)\s*\}\}/g, function (m, i) { if (+i > data.length) add('error', 'sql-input', n.name + ' uses {{ input_' + i + ' }} but has ' + data.length + ' input(s).', n.id); return m; });
        String(c.sql).replace(/\{\{\s*ref\(\s*['"]([A-Za-z0-9_]+)['"]\s*\)\s*\}\}/g, function (m, name) { if (!nodeByName(p, name)) add('error', 'sql-ref', n.name + ' refers to unknown node "' + name + '".', n.id); return m; });
        if (n.type !== 'custom_check' && /;\s*\S/.test(String(c.sql).trim())) add('error', 'sql-multi', n.name + ' must be a single SELECT (found ";").', n.id);
      }
      if (t.category === 'quality' && data[0]) {
        var cols = columnsOf(p, data[0]);
        var want = n.type === 'not_null' || n.type === 'unique' ? asList(c.columns) : (c.column ? [c.column] : []);
        if (cols) want.forEach(function (w) { if (cols.indexOf(String(w).toLowerCase()) === -1) add('warning', 'check-column', n.name + ': column "' + w + '" isn\u2019t in ' + data[0].name + ' (' + cols.join(', ') + ').', n.id); });
      }
      if ((n.type === 'warehouse_table' || n.type === 'lake_table') && c.mode === 'merge') {
        var keys = asList(c.keys);
        if (!keys.length) add('error', 'merge-keys', n.name + ': merge needs at least one key column.', n.id, { field: 'keys' });
        var ic = data[0] && columnsOf(p, data[0]);
        if (ic) keys.forEach(function (k) { if (ic.indexOf(k.toLowerCase()) === -1) add('error', 'merge-key-missing', n.name + ': merge key "' + k + '" isn\u2019t an input column (' + ic.join(', ') + ').', n.id); });
      }
      if (n.type === 'warehouse_table' && c.table && !/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$/.test(c.table)) add('error', 'table-name', n.name + ': "' + c.table + '" isn\u2019t a valid table name (use schema.table).', n.id);
      if (n.type === 'python' && c.code && !/^def\s+transform\s*\(/m.test(c.code)) add('error', 'python-fn', n.name + ': code must define "def transform(df, **inputs):".', n.id);
      if (n.type === 'alert' && c.channel === 'email' && !asList(c.to).length) add('error', 'alert-to', n.name + ': email alerts need at least one recipient.', n.id);
      if (n.type === 'sensor' && c.storage !== 'local' && !String(c.bucket || '').trim()) add('error', 'sensor-bucket', n.name + ': a bucket is required for ' + (c.storage || 's3').toUpperCase() + '.', n.id);
      if (n.type === 'branch') ['yes', 'no'].forEach(function (port) { if (!downstreamEdges(p, n.id, port).length) add('warning', 'branch-port', 'Branch ' + n.name + ': nothing is connected to "' + port + '".', n.id); });
      if (n.type === 'sla' && !ups.length) add('warning', 'sla-input', 'SLA ' + n.name + ' applies to the tasks connected into it; connect at least one.', n.id);
      if (n.type === 'snowflake_table' && wh !== 'snowflake') add('warning', 'warehouse', n.name + ' is a Snowflake table but the pipeline warehouse is ' + WAREHOUSES[wh].label + '.', n.id, { fix: 'settings' });
      if (n.type === 'bigquery_table' && wh !== 'bigquery') add('warning', 'warehouse', n.name + ' is a BigQuery table but the pipeline warehouse is ' + WAREHOUSES[wh].label + '.', n.id, { fix: 'settings' });
      if (n.type === 'schedule' && c.cron) { var si = scheduleInfo(c.cron); if (!si.ok && schedules[0] !== n) add('error', 'schedule', n.name + ': ' + si.error, n.id); }
    });
    var order = { error: 0, warning: 1, info: 2 };
    issues.sort(function (a, b) { return order[a.level] - order[b.level]; });
    return issues;
  }

  /* ---------------- auto layout (layered, left to right) ---------------- */
  function layout(p, opts) {
    opts = opts || {};
    var gapX = opts.gapX || 290, gapY = opts.gapY || 40, x0 = opts.x0 == null ? 80 : opts.x0, y0 = opts.y0 == null ? 80 : opts.y0;
    var ids = p.nodes.map(function (n) { return n.id; });
    // drop back edges (DFS) so cycles can still be laid out
    var state = {}, keep = [], adj = {};
    ids.forEach(function (i) { adj[i] = []; });
    p.edges.forEach(function (e) { if (adj[e.from] && adj[e.to]) adj[e.from].push(e); });
    var sorted = p.nodes.slice().sort(function (a, b) { return a.x - b.x || a.y - b.y; }).map(function (n) { return n.id; });
    function dfs(v) { state[v] = 1; adj[v].forEach(function (e) { if (state[e.to] === 1) return; keep.push(e); if (!state[e.to]) dfs(e.to); }); state[v] = 2; }
    sorted.forEach(function (v) { if (!state[v]) dfs(v); });
    var preds = {}; ids.forEach(function (i) { preds[i] = []; });
    keep.forEach(function (e) { if (preds[e.to].indexOf(e.from) === -1) preds[e.to].push(e.from); });
    var layer = {}, sub = { nodes: p.nodes, edges: keep };
    topoSort(sub).order.forEach(function (id) { layer[id] = preds[id].reduce(function (m, u) { return Math.max(m, layer[u] + 1); }, 0); });
    // Pull sources right next to their first consumer.
    var succ = {}; ids.forEach(function (i) { succ[i] = []; }); keep.forEach(function (e) { succ[e.from].push(e.to); });
    ids.forEach(function (id) { if (!preds[id].length && succ[id].length) layer[id] = Math.max(0, Math.min.apply(null, succ[id].map(function (s) { return layer[s]; })) - 1); });
    var layers = [];
    ids.forEach(function (id) { var l = layer[id] || 0; (layers[l] = layers[l] || []).push(id); });
    var byId = {}; p.nodes.forEach(function (n) { byId[n.id] = n; });
    layers = layers.map(function (L) { return (L || []).sort(function (a, b) { return byId[a].y - byId[b].y; }); });
    var posIn = function () { var m = {}; layers.forEach(function (L) { L.forEach(function (id, i) { m[id] = i; }); }); return m; };
    for (var sweep = 0; sweep < 6; sweep++) {
      var pos = posIn(), down = sweep % 2 === 0;
      var range = down ? layers.map(function (_, i) { return i; }) : layers.map(function (_, i) { return layers.length - 1 - i; });
      range.forEach(function (li) {
        var L = layers[li]; if (!L) return;
        var bary = {};
        L.forEach(function (id) {
          var nb = down ? preds[id] : succ[id];
          bary[id] = nb.length ? nb.reduce(function (s, u) { return s + pos[u]; }, 0) / nb.length : pos[id];
        });
        L.sort(function (a, b) { return bary[a] - bary[b] || pos[a] - pos[b]; });
        L.forEach(function (id, i) { pos[id] = i; });
      });
    }
    var heights = layers.map(function (L) { return (L || []).reduce(function (h, id) { return h + nodeSize(byId[id]).h + gapY; }, -gapY); });
    var maxH = Math.max.apply(null, heights.concat([0]));
    var out = {};
    layers.forEach(function (L, li) {
      var y = y0 + (maxH - heights[li]) / 2;
      (L || []).forEach(function (id) { out[id] = { x: x0 + li * gapX, y: Math.round(y / 10) * 10 }; y += nodeSize(byId[id]).h + gapY; });
    });
    return out;
  }

  /* ---------------- share links ---------------- */
  function b64urlFromBytes(bytes) {
    var bin = ''; for (var i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
    var b = typeof btoa === 'function' ? btoa(bin) : Buffer.from(bin, 'binary').toString('base64');
    return b.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }
  function bytesFromB64url(s) {
    s = String(s).replace(/-/g, '+').replace(/_/g, '/'); while (s.length % 4) s += '=';
    var bin = typeof atob === 'function' ? atob(s) : Buffer.from(s, 'base64').toString('binary');
    var out = new Uint8Array(bin.length); for (var i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  function streamBytes(bytes, stream) {
    var rs = new Blob([bytes]).stream().pipeThrough(stream);
    return new Response(rs).arrayBuffer().then(function (b) { return new Uint8Array(b); });
  }
  function shareable(p) {
    var c = clone(p); delete c.updatedAt; delete c.id;
    c.nodes.forEach(function (n) { n.x = Math.round(n.x); n.y = Math.round(n.y); if (!n.notes) delete n.notes; if (!n.columns || !n.columns.length) delete n.columns; });
    return c;
  }
  function encodeShare(p) {
    var json = JSON.stringify(shareable(p)), bytes = new TextEncoder().encode(json);
    if (typeof CompressionStream === 'function') {
      try { return streamBytes(bytes, new CompressionStream('deflate-raw')).then(function (z) { return 'z.' + b64urlFromBytes(z); }); } catch (e) { /* fall through */ }
    }
    return Promise.resolve('j.' + b64urlFromBytes(bytes));
  }
  function decodeShare(token) {
    token = String(token || '').replace(/^#?(p=)?/, '');
    var kind = token.slice(0, 2), body = token.slice(2);
    var done = function (bytes) { return normalize(JSON.parse(new TextDecoder().decode(bytes))); };
    try {
      if (kind === 'j.') return Promise.resolve(done(bytesFromB64url(body)));
      if (kind === 'z.') {
        if (typeof DecompressionStream !== 'function') return Promise.reject(new Error('This browser can\u2019t open compressed links'));
        return streamBytes(bytesFromB64url(body), new DecompressionStream('deflate-raw')).then(done);
      }
    } catch (e) { return Promise.reject(e); }
    return Promise.reject(new Error('Not a PipelineSketch share link'));
  }

  /* ---------------- code-writing helpers (shared by generators) ---------------- */
  function pyStr(s) {
    s = String(s == null ? '' : s);
    if (s.indexOf('\n') === -1) return JSON.stringify(s);
    return '"""\n' + s.replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '\n"""';
  }
  function pyList(arr) { return '[' + arr.map(pyStr).join(', ') + ']'; }
  function indent(s, n) { var pad = new Array(n + 1).join(' '); return String(s).split('\n').map(function (l) { return l ? pad + l : l; }).join('\n'); }
  function yamlStr(s) { s = String(s == null ? '' : s); return /^[A-Za-z0-9_./@-][A-Za-z0-9_ ./@-]*$/.test(s) && !/^(true|false|yes|no|null|on|off|~)$/i.test(s) && !/^[\d.]+$/.test(s) ? s : JSON.stringify(s); }

  return {
    VERSION: VERSION, NODE_W: NODE_W, WAREHOUSES: WAREHOUSES, PRESETS: PRESETS,
    newId: newId, toIdent: toIdent, isIdent: isIdent, uniqueName: uniqueName, asList: asList, lines: lines,
    defaultSettings: defaultSettings, createPipeline: createPipeline, normalize: normalize, clone: clone,
    nodeById: nodeById, nodeByName: nodeByName, addNode: addNode, addEdge: addEdge, canConnect: canConnect, removeNodes: removeNodes, renameNode: renameNode,
    copyFragment: copyFragment, pasteFragment: pasteFragment,
    nodeSize: nodeSize, portPos: portPos, bounds: bounds, nodesInRect: nodesInRect,
    upstreamEdges: upstreamEdges, downstreamEdges: downstreamEdges, reachable: reachable, topoSort: topoSort, findCycles: findCycles,
    kindOf: kindOf, dataProducer: dataProducer, dataInputs: dataInputs, isTask: isTask, taskUpstreams: taskUpstreams, descendants: descendants,
    branchDescendants: branchDescendants, effectiveSchedule: effectiveSchedule, slaFor: slaFor,
    whConn: whConn, sqlLit: sqlLit, schemaOf: schemaOf, tableOf: tableOf, relation: relation, substituteSql: substituteSql, compileSelect: compileSelect,
    materialize: materialize, sinkStatements: sinkStatements, freshnessExpr: freshnessExpr, columnsOf: columnsOf,
    scheduleInfo: scheduleInfo, validate: validate, layout: layout,
    encodeShare: encodeShare, decodeShare: decodeShare, shareable: shareable,
    pyStr: pyStr, pyList: pyList, indent: indent, yamlStr: yamlStr
  };
});
