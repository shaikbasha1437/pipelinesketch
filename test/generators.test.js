'use strict';
const test = require('node:test');
const { PS, assert, snapshot } = require('./helpers');
const NOW = Date.UTC(2026, 9, 1, 12);

for (const t of PS.templates.LIST) {
  test('generator snapshots: ' + t.id, () => {
    const p = t.build(), name = p.settings.name;
    snapshot(name + '.airflow.py', PS.airflow.generate(p));
    snapshot(name + '.dagster.py', PS.dagster.generate(p));
    snapshot(name + '.prefect.py', PS.prefect.generate(p));
    snapshot(name + '.dbt.txt', PS.dbt.toText(PS.dbt.generate(p)));
    snapshot(name + '.mmd', PS.docs.mermaid(p, { hints: true }));
    snapshot(name + '.md', PS.docs.markdown(p, { now: NOW }));
  });
}

test('generated Airflow DAGs reference every task once and keep dependencies', () => {
  for (const t of PS.templates.LIST) {
    const p = t.build(), code = PS.airflow.generate(p);
    assert.match(code, new RegExp('dag_id="' + p.settings.name + '"'));
    for (const n of p.nodes.filter((x) => PS.core.isTask(x))) assert.ok(code.includes('"' + n.name + '"'), t.id + ' task ' + n.name);
    assert.ok(!/undefined|NaN|\[object Object\]/.test(code), t.id + ' has no JS leftovers');
  }
});

test('dbt project has the expected files and no JS leftovers', () => {
  for (const t of PS.templates.LIST) {
    const files = PS.dbt.generate(t.build());
    for (const f of ['dbt_project.yml', 'profiles.example.yml', 'models/sources.yml', 'macros/generate_schema_name.sql']) assert.ok(files[f], t.id + ' ' + f);
    for (const [k, v] of Object.entries(files)) assert.ok(!/undefined|NaN|\[object Object\]/.test(v), t.id + ' ' + k);
  }
});

test('svg export is standalone and escaped', () => {
  const p = PS.templates.LIST[0].build();
  p.nodes[0].name = 'a<b&c';
  const svg = PS.docs.svg(p, { theme: 'light' });
  assert.match(svg, /^<svg xmlns="http:\/\/www.w3.org\/2000\/svg"/);
  assert.ok(svg.includes('a&lt;b&amp;c'));
  assert.ok(!svg.includes('a<b&c'));
});

test('highlighter escapes HTML', () => {
  const out = PS.highlight.highlight('select "<script>" from t -- </x>', 'sql');
  assert.ok(!out.includes('<script>'));
  assert.match(out, /hl-kw/);
});
