'use strict';
const fs = require('fs');
const path = require('path');
const assert = require('node:assert/strict');
const ROOT = path.join(__dirname, '..');
const req = (f) => require(path.join(ROOT, 'js', f));
const PS = {
  cron: req('cron.js'), nodes: req('nodes.js'), core: req('core.js'), templates: req('templates.js'),
  airflow: req('gen-airflow.js'), dagster: req('gen-dagster.js'), prefect: req('gen-prefect.js'), dbt: req('gen-dbt.js'),
  docs: req('gen-docs.js'), importer: req('import.js'), zip: req('zip.js'), highlight: req('highlight.js'), modal: req('modal.js')
};
// Snapshot check: UPDATE_SNAPSHOTS=1 npm test rewrites them.
function snapshot(name, text) {
  const file = path.join(__dirname, 'snapshots', name);
  if (process.env.UPDATE_SNAPSHOTS || !fs.existsSync(file)) { fs.writeFileSync(file, text); return; }
  assert.equal(text, fs.readFileSync(file, 'utf8'), 'snapshot ' + name + ' changed (UPDATE_SNAPSHOTS=1 to accept)');
}
module.exports = { PS, snapshot, assert };
