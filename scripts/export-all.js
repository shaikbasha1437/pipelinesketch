#!/usr/bin/env node
// Writes every template's generated code to ./out (or the dir given) so it can be
// checked with real tools: python -m py_compile, Airflow DagBag, `dbt parse`.
'use strict';
const fs = require('fs');
const path = require('path');
const js = (f) => require(path.join(__dirname, '..', 'js', f));
const T = js('templates.js');
const GEN = { airflow: ['dags', '.py'], dagster: ['dagster', '_dagster.py'], prefect: ['prefect', '_prefect.py'] };
const out = path.resolve(process.argv[2] || 'out');
fs.rmSync(out, { recursive: true, force: true });
for (const t of T.LIST) {
  const p = t.build(), name = p.settings.name;
  for (const [k, [dir, ext]] of Object.entries(GEN)) {
    fs.mkdirSync(path.join(out, dir), { recursive: true });
    fs.writeFileSync(path.join(out, dir, name + ext), js('gen-' + k + '.js').generate(p));
  }
  const files = js('gen-dbt.js').generate(p);
  for (const [f, text] of Object.entries(files)) {
    const fp = path.join(out, 'dbt', name, f);
    fs.mkdirSync(path.dirname(fp), { recursive: true });
    fs.writeFileSync(fp, text);
  }
  // A local DuckDB profile so `dbt parse` works without warehouse credentials.
  fs.writeFileSync(path.join(out, 'dbt', name, 'profiles.yml'),
    name + ':\n  target: ci\n  outputs:\n    ci:\n      type: duckdb\n      path: ci.duckdb\n      schema: ' + p.settings.stagingSchema + '\n');
  fs.mkdirSync(path.join(out, 'mermaid'), { recursive: true });
  fs.writeFileSync(path.join(out, 'mermaid', name + '.mmd'), js('gen-docs.js').mermaid(p, { hints: true }));
}
console.log('wrote ' + T.LIST.length + ' templates to ' + out);
