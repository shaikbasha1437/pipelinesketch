'use strict';
const test = require('node:test');
const { PS, assert } = require('./helpers');
const C = PS.core, I = PS.importer;
const strip = (p) => ({
  settings: p.settings,
  nodes: p.nodes.map((n) => ({ type: n.type, name: n.name, config: n.config, columns: n.columns, notes: n.notes, x: n.x, y: n.y })).sort((a, b) => a.name.localeCompare(b.name)),
  edges: p.edges.map((e) => [C.nodeById(p, e.from).name, e.fromPort, C.nodeById(p, e.to).name, e.toPort].join(' ')).sort(),
  groups: p.groups.map((g) => [g.title, g.text, g.x, g.y, g.w, g.h, g.color].join('|'))
});

test('Mermaid export \u2192 import is lossless for every template', () => {
  for (const t of PS.templates.LIST) {
    const p = t.build();
    const r = I.fromMermaid(PS.docs.mermaid(p, { hints: true }));
    assert.equal(r.hinted, true);
    assert.deepEqual(strip(r.pipeline), strip(p), t.id);
  }
});

test('plain Mermaid flowcharts import with guessed types, links and subgraphs', () => {
  const r = I.fromMermaid(['graph TD', '  A[Postgres orders] --> B{is weekday}', '  B -->|yes| C[(fct_sales table)]', '  B -->|no| D[Slack alert]',
    '  subgraph Out["Outputs"]', '    C', '  end', '  A & D --> E[dedupe orders]'].join('\n'));
  const p = r.pipeline, by = (n) => p.nodes.find((x) => x.name === n);
  assert.equal(r.hinted, false);
  assert.equal(by('a').type, 'pg_table');
  assert.equal(by('b').type, 'branch');
  assert.equal(by('d').type, 'alert');
  assert.equal(p.edges.length, 5);
  assert.ok(p.edges.some((e) => e.from === by('b').id && e.fromPort === 'yes' && e.to === by('c').id));
  assert.equal(p.groups.length, 1);
  assert.equal(p.groups[0].title, 'Outputs');
  assert.throws(() => I.fromMermaid('sequenceDiagram\n A->>B: hi'), /flowchart/);
});

test('Airflow import recovers tasks and dependencies of generated DAGs', () => {
  for (const t of PS.templates.LIST) {
    const p = t.build();
    const r = I.fromAirflow(PS.airflow.generate(p));
    const want = p.nodes.filter((n) => C.isTask(n)).map((n) => n.name).sort();
    assert.deepEqual(r.pipeline.nodes.map((n) => n.name).sort(), want, t.id);
    assert.equal(r.pipeline.settings.name, p.settings.name);
    assert.equal(C.findCycles(r.pipeline).length, 0);
  }
});

test('Airflow import handles hand-written DAGs', () => {
  const src = `
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
with DAG(dag_id="nightly_sales", schedule="0 2 * * *") as dag:
    extract = BashOperator(task_id="extract_orders", bash_command="python extract.py")
    clean = SQLExecuteQueryOperator(task_id="clean_orders", sql="select 1")
    model = BashOperator(task_id="dbt_run", bash_command="dbt build --select marts")  # >> not a dep
    notify = BashOperator(task_id="notify", bash_command="echo done")
    extract >> clean >> [model, notify]
    notify.set_upstream(model)
`;
  const r = I.fromAirflow(src), p = r.pipeline, by = (n) => p.nodes.find((x) => x.name === n);
  assert.equal(p.settings.name, 'nightly_sales');
  assert.equal(p.settings.schedule, '0 2 * * *');
  assert.equal(by('clean_orders').type, 'sql');
  assert.equal(by('dbt_run').type, 'dbt_model');
  assert.equal(p.edges.length, 4);
});

test('JSON import accepts single pipelines and backups', () => {
  const a = PS.templates.LIST[0].build(), b = PS.templates.LIST[1].build();
  assert.equal(I.fromJSON(JSON.stringify(C.shareable(a))).pipelines.length, 1);
  const r = I.fromJSON(JSON.stringify({ kind: 'pipelinesketch-backup', pipelines: [a, b] }));
  assert.equal(r.pipelines.length, 2);
  assert.equal(r.pipelines[1].settings.name, b.settings.name);
  assert.throws(() => I.fromJSON('{"nope":1}'));
  assert.equal(I.detect('flowchart LR\n a-->b'), 'mermaid');
  assert.equal(I.detect('{"nodes":[]}'), 'json');
  assert.equal(I.detect('a >> b'), 'airflow');
});

test('zip writer produces a valid archive', () => {
  const z = PS.zip.build({ 'a/hello.txt': new TextEncoder().encode('hello'), 'a/b/c.sql': new TextEncoder().encode('select 1') });
  const dv = new DataView(z.buffer, z.byteOffset, z.byteLength);
  assert.equal(dv.getUint32(0, true), 0x04034b50);
  assert.equal(dv.getUint32(z.length - 22, true), 0x06054b50);
  assert.equal(dv.getUint16(z.length - 12, true), 2);
  assert.equal(PS.zip.crc32(new TextEncoder().encode('hello')) >>> 0, 0x3610a686);
});

test('modal stack closes on Back and from the UI', async () => {
  const states = [{}]; let idx = 0;
  const H = { get state() { return states[idx]; }, pushState(s) { states.splice(idx + 1); states.push(s); idx++; }, go(n) { idx += n; setTimeout(() => stack.onPopState(), 0); } };
  const log = [];
  const stack = PS.modal.createModalStack({ history: H, setTimeout });
  stack.open({ id: 'a', show: () => log.push('show a'), hide: () => log.push('hide a') });
  stack.open({ id: 'b', show: () => log.push('show b'), hide: () => log.push('hide b') });
  assert.equal(stack.size, 2);
  idx--; stack.onPopState(); // browser Back
  assert.equal(stack.size, 1);
  await stack.close('a');
  assert.equal(stack.size, 0);
  assert.deepEqual(log, ['show a', 'show b', 'hide b', 'hide a']);
});
