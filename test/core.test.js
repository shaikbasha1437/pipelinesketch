'use strict';
const test = require('node:test');
const { PS, assert } = require('./helpers');
const C = PS.core;

function chain(types) {
  const p = C.createPipeline('t');
  const ns = types.map((t, i) => C.addNode(p, t, i * 300, 0));
  for (let i = 0; i < ns.length - 1; i++) C.addEdge(p, ns[i].id, 'out', ns[i + 1].id, 'in');
  return { p, ns };
}

test('topological sort orders upstream first', () => {
  const { p, ns } = chain(['pg_table', 'sql', 'filter', 'warehouse_table']);
  const extra = C.addNode(p, 'not_null', 0, 200);
  C.addEdge(p, ns[1].id, 'out', extra.id, 'in');
  const r = C.topoSort(p);
  assert.deepEqual(r.cyclic, []);
  const pos = Object.fromEntries(r.order.map((id, i) => [id, i]));
  for (const e of p.edges) assert.ok(pos[e.from] < pos[e.to], 'edge order');
});

test('cycles are detected and reported by validation', () => {
  const { p, ns } = chain(['sql', 'filter', 'aggregate']);
  const c = C.canConnect(p, ns[2].id, 'out', ns[0].id, 'in');
  assert.equal(c.ok, true); assert.equal(c.createsCycle, true);
  C.addEdge(p, ns[2].id, 'out', ns[0].id, 'in');
  assert.ok(C.findCycles(p).length >= 1);
  const issues = C.validate(p).filter((i) => i.code === 'cycle');
  assert.equal(issues.length, 1);
  assert.equal(issues[0].level, 'error');
  assert.ok(issues[0].nodeId);
  assert.match(issues[0].message, /acyclic/);
});

test('connections: no self loops, unknown ports or duplicates', () => {
  const { p, ns } = chain(['sql', 'join']);
  assert.equal(C.canConnect(p, ns[0].id, 'out', ns[0].id, 'in').ok, false);
  assert.equal(C.canConnect(p, ns[0].id, 'out', ns[1].id, 'nope').ok, false);
  assert.equal(C.canConnect(p, ns[0].id, 'out', ns[1].id, 'left').ok, true);
  C.addEdge(p, ns[0].id, 'out', ns[1].id, 'left');
  assert.equal(C.canConnect(p, ns[0].id, 'out', ns[1].id, 'left').ok, false);
});

test('validation flags missing required config and passes templates', () => {
  const p = C.createPipeline('t');
  const n = C.addNode(p, 'pg_table', 0, 0);
  const req = C.validate(p).filter((i) => i.code === 'required' && i.nodeId === n.id);
  assert.ok(req.some((i) => i.field === 'table'));
  for (const t of PS.templates.LIST) {
    const errs = C.validate(t.build()).filter((i) => i.level === 'error');
    assert.deepEqual(errs, [], t.id + ' should have no errors');
  }
});

test('identifier sanitizing', () => {
  assert.equal(C.toIdent('Orders Raw'), 'orders_raw');
  assert.equal(C.toIdent('orderItems'), 'order_items');
  assert.equal(C.toIdent('2024 sales!'), 't_2024_sales');
  assert.equal(C.toIdent('Café-Ünits'), 'cafe_units');
  assert.equal(C.toIdent('class'), 'class_t');
  assert.equal(C.toIdent('   '), 'node');
  assert.equal(C.toIdent('x'.repeat(80)).length, 63);
  assert.ok(C.isIdent('ok_name'));
  assert.equal(C.uniqueName('stg', ['stg', 'stg_2']), 'stg_3');
});

test('rename keeps ref() references working', () => {
  const { p, ns } = chain(['sql', 'sql']);
  ns[1].config.sql = "select * from {{ ref('" + ns[0].name + "') }}";
  C.renameNode(p, ns[0].id, 'clean_orders');
  assert.equal(ns[0].name, 'clean_orders');
  assert.match(ns[1].config.sql, /ref\('clean_orders'\)/);
});

test('copy / paste keeps internal edges and makes names unique', () => {
  const { p, ns } = chain(['sql', 'filter']);
  const frag = C.copyFragment(p, ns.map((n) => n.id));
  const added = C.pasteFragment(p, JSON.parse(JSON.stringify(frag)), 40, 40);
  assert.equal(added.length, 2);
  assert.equal(p.edges.length, 2);
  assert.equal(new Set(p.nodes.map((n) => n.name)).size, 4);
});

test('auto-layout puts every node right of its inputs without overlaps', () => {
  for (const t of PS.templates.LIST) {
    const p = t.build(), pos = C.layout(p);
    for (const e of p.edges) assert.ok(pos[e.to].x > pos[e.from].x, t.id + ' left-to-right');
    const boxes = p.nodes.map((n) => ({ ...pos[n.id], ...C.nodeSize(n) }));
    for (let i = 0; i < boxes.length; i++) for (let j = i + 1; j < boxes.length; j++) {
      const a = boxes[i], b = boxes[j];
      assert.ok(a.x + a.w <= b.x || b.x + b.w <= a.x || a.y + a.h <= b.y || b.y + b.h <= a.y, t.id + ' overlap');
    }
  }
});

test('share link round-trips a pipeline', async () => {
  for (const t of PS.templates.LIST) {
    const p = t.build();
    const tok = await C.encodeShare(p);
    assert.match(tok, /^[zj]\.[A-Za-z0-9_-]+$/);
    const q = await C.decodeShare('#p=' + tok);
    assert.deepEqual(C.shareable(q), C.shareable(p));
  }
  await assert.rejects(C.decodeShare('nonsense'));
});
