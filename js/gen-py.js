/* Shared pieces for the plain-Python exporters (Dagster, Prefect). MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./core.js'), require('./gen-common.js'));
  else (root.PS = root.PS || {}).genpy = factory(root.PS.core, root.PS.gen);
})(typeof self !== 'undefined' ? self : this, function (C, G) {
  'use strict';
  var py = C.pyStr;
  // Task-level inputs of a node: [{node, port}] where port is the branch port when the edge leaves a branch.
  function taskInputs(p, id) {
    var res = [], seen = {};
    (function walk(x) {
      C.upstreamEdges(p, x).forEach(function (e) {
        var u = C.nodeById(p, e.from); if (!u) return;
        var key = e.from + ':' + (u.type === 'branch' ? e.fromPort : '');
        if (seen[key]) return; seen[key] = 1;
        if (C.isTask(u)) res.push({ node: u, port: u.type === 'branch' ? e.fromPort : null });
        else if (u.type !== 'schedule' && u.type !== 'sla') walk(u.id);
      });
    })(id);
    return res;
  }
  function needs(p) {
    var t = {}; p.nodes.forEach(function (n) { t[n.type] = 1; }); return t;
  }
  // Module docstring + imports + constants shared by both exporters.
  function prelude(p, title, frameworkImports, extraHeader) {
    var s = p.settings, pk = G.packages(p, 'plain');
    var head = G.header(p, title, ['', 'Install: pip install ' + frameworkImports.pip.concat(pk).join(' '),
      'Environment: WAREHOUSE_URL (SQLAlchemy URL, e.g. ' + G.urlExample(s.warehouse) + ')' + envList(p)].concat(extraHeader || []));
    var L = [G.docstring(head), 'import datetime as dt', 'import io', 'import json', 'import os', ''];
    L = L.concat(frameworkImports.lines);
    L.push('import pandas as pd', 'import sqlalchemy as sa', '');
    L.push('PIPELINE = ' + py(s.name), 'RAW_SCHEMA = ' + py(s.rawSchema || 'raw'), 'STAGING_SCHEMA = ' + py(s.stagingSchema || 'staging'),
      'DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", ' + py(s.dbtProjectDir || 'dbt') + ')', '', '_ENGINES: dict[str, sa.Engine] = {}', '');
    return L.join('\n');
  }
  function envList(p) {
    var env = {};
    p.nodes.forEach(function (n) {
      if (n.type === 'pg_table') env[G.connEnv(G.conn(p, n, 'postgres'))] = 1;
      if (n.type === 'mysql_table') env[G.connEnv(G.conn(p, n, 'mysql'))] = 1;
      if (n.type === 'kafka_topic' || n.type === 'kafka_sink') env.KAFKA_BOOTSTRAP_SERVERS = 1;
      if (n.type === 'alert' && n.config.channel !== 'email') env.SLACK_WEBHOOK_URL = 1;
      if (n.type === 'alert' && n.config.channel === 'email') env['SMTP_HOST/SMTP_PORT/SMTP_FROM'] = 1;
      if (n.type === 'rest_api' && n.config.token_env) env[n.config.token_env] = 1;
    });
    var k = Object.keys(env);
    return k.length ? ', ' + k.join(', ') : '';
  }
  function transforms(p) {
    var L = [];
    p.nodes.forEach(function (n) { if (n.type === 'python') L.push('# --- user code for ' + n.name + ' ---', G.renameTransform(n.config.code, n.name).trim(), '', ''); });
    return L.join('\n');
  }
  function steps(p) {
    return G.sortedTasks(p).filter(function (n) { return n && C.isTask(n); }).map(function (n) { return G.plainStep(p, n); }).join('\n\n');
  }
  return { taskInputs: taskInputs, prelude: prelude, transforms: transforms, steps: steps, needs: needs };
});
