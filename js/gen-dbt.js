/* dbt project exporter: sources, staging/mart models, incremental sinks, tests, exposures. MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./core.js'), require('./gen-common.js'));
  else (root.PS = root.PS || {}).dbt = factory(root.PS.core, root.PS.gen);
})(typeof self !== 'undefined' ? self : this, function (C, G) {
  'use strict';
  var yamlStr = C.yamlStr;
  // YAML strings in dbt are Jinja-rendered: strip template delimiters from free text.
  function y(v) { return yamlStr(String(v == null ? '' : v).replace(/\{\{\s*|\s*\}\}/g, '').replace(/\{%|%\}|\{#|#\}/g, '')); }
  function isModel(n) { var k = C.kindOf(n); return k === 'sql' || k === 'dbt' || n.type === 'warehouse_table'; }
  function sourceGroups(p) {
    // name -> {name, schema, database, tables:[{name, identifier, node}]}
    var g = {}, s = p.settings;
    function add(key, schema, database, desc, node, identifier) {
      if (!g[key]) g[key] = { name: key, schema: schema, database: database, description: desc, tables: [] };
      g[key].tables.push({ name: node.name, identifier: identifier, node: node });
    }
    p.nodes.forEach(function (n) {
      var k = C.kindOf(n), c = n.config || {};
      if (k === 'extract') add('raw', s.rawSchema || 'raw', null, 'Raw data landed by the orchestrator (' + s.name + ').', n, null);
      else if (k === 'python') add('pipeline_outputs', s.stagingSchema || 'staging', null, 'Tables written by Python steps in the orchestrator.', n, null);
      else if (n.type === 'snowflake_table') add(C.toIdent(('ext_' + (c.schema || 'public')).toLowerCase()), c.schema, c.database || null, 'Existing Snowflake tables.', n, c.table);
      else if (n.type === 'bigquery_table') add(C.toIdent(('ext_' + (c.dataset || 'dataset')).toLowerCase()), c.dataset, c.project || null, 'Existing BigQuery tables.', n, c.table);
    });
    return g;
  }
  function generate(p) {
    var s = p.settings, proj = s.name, files = {}, wh = s.warehouse;
    var groups = sourceGroups(p), srcOf = {};
    Object.keys(groups).forEach(function (k) { groups[k].tables.forEach(function (t) { srcOf[t.node.id] = k; }); });
    function ref(x) {
      if (!x) return '/* missing input */';
      if (srcOf[x.id]) return "{{ source('" + srcOf[x.id] + "', '" + x.name + "') }}";
      return "{{ ref('" + x.name + "') }}";
    }
    function depRef(x) { return srcOf[x.id] ? "source('" + srcOf[x.id] + "', '" + x.name + "')" : "ref('" + x.name + "')"; }
    var models = p.nodes.filter(isModel);
    var folder = {};
    models.forEach(function (n) {
      var f = n.type === 'warehouse_table' ? 'marts' : n.type === 'dbt_model' ? (/^(stg|base|int)_/.test(n.name) ? 'staging' : 'marts') : 'staging';
      folder[n.id] = f;
      var body, cfg = [];
      if (n.type === 'warehouse_table') {
        var c = n.config, parts = String(c.table || n.name).replace(/`/g, '').split('.'), alias = parts.pop(), schema = parts.pop(), db = parts.pop();
        var mode = c.mode || 'overwrite', keys = C.asList(c.keys);
        if (mode === 'overwrite') cfg.push("materialized='table'");
        else {
          cfg.push("materialized='incremental'");
          if (mode === 'merge') {
            cfg.push('unique_key=' + JSON.stringify(keys).replace(/"/g, "'"));
            cfg.push("incremental_strategy='" + (wh === 'snowflake' || wh === 'bigquery' ? 'merge' : 'delete+insert') + "'");
          } else if (wh !== 'bigquery') cfg.push("incremental_strategy='append'");
          if (wh !== 'bigquery') cfg.push("on_schema_change='append_new_columns'");
        }
        if (schema) cfg.push("schema='" + schema + "'");
        if (db) cfg.push("database='" + db + "'");
        cfg.push("alias='" + alias + "'");
        body = 'select * from ' + ref(C.dataInputs(p, n.id)[0]);
      } else {
        if (n.type === 'dbt_model' && n.config.materialized) cfg.push("materialized='" + n.config.materialized + "'");
        body = C.compileSelect(p, n, ref, wh);
      }
      var head = n.notes ? '-- ' + String(n.notes).split('\n').join('\n-- ') + '\n' : '';
      files['models/' + folder[n.id] + '/' + n.name + '.sql'] = head + (cfg.length ? '{{ config(' + cfg.join(', ') + ') }}\n\n' : '') + body.trim() + '\n';
    });
    // ---- tests
    var colTests = {}; // producerId -> {col -> [test yaml lines]}
    function addColTest(prod, col, lines) { colTests[prod.id] = colTests[prod.id] || {}; (colTests[prod.id][col] = colTests[prod.id][col] || []).push(lines); }
    p.nodes.forEach(function (n) {
      if (C.kindOf(n) !== 'check') return;
      var prod = C.dataProducer(p, n.id), c = n.config || {};
      if (!prod) return;
      var target = ref(prod);
      if (n.type === 'not_null') C.asList(c.columns).forEach(function (col) { addColTest(prod, col, ['- not_null']); });
      else if (n.type === 'unique') {
        var cols = C.asList(c.columns);
        if (cols.length === 1) addColTest(prod, cols[0], ['- unique']);
        else if (cols.length) files['tests/' + n.name + '.sql'] = '-- ' + cols.join(', ') + ' must be unique together in ' + prod.name + '\nselect ' + cols.join(', ') + ', count(*) as n\nfrom ' + target + '\ngroup by ' + cols.join(', ') + '\nhaving count(*) > 1\n';
      } else if (n.type === 'accepted_values') addColTest(prod, c.column, ['- accepted_values:', '    arguments:', '      values: [' + C.asList(c.values).map(function (v) { return JSON.stringify(String(v)); }).join(', ') + ']']);
      else if (n.type === 'row_count') files['tests/' + n.name + '.sql'] = '-- ' + prod.name + ' row count must satisfy: ' + G.rowCountCond(c) + '\nselect count(*) as row_count\nfrom ' + target + '\nhaving not (' + G.rowCountCond(c) + ')\n';
      else if (n.type === 'freshness') files['tests/' + n.name + '.sql'] = '-- newest ' + c.column + ' in ' + prod.name + ' must be at most ' + (c.max_age_hours || 24) + ' hours old\nselect max(' + c.column + ') as newest\nfrom ' + target + '\nhaving not (' + C.freshnessExpr(c.column, c.max_age_hours, wh) + ')\n';
      else if (n.type === 'custom_check') files['tests/' + n.name + '.sql'] = '-- returns failing rows\n' + C.substituteSql(c.sql, C.dataInputs(p, n.id).map(ref), function (nm) { var m = C.nodeByName(p, nm); return m ? ref(C.dataProducer(p, m.id) || m) : null; }).trim() + '\n';
    });
    function columnsYaml(n, ind) {
      var pad = new Array(ind + 1).join(' '), known = C.columnsOf(p, n) || [], tests = colTests[n.id] || {};
      var cols = known.slice(); Object.keys(tests).forEach(function (c) { if (cols.indexOf(c) === -1) cols.push(c); });
      if (!cols.length) return [];
      var L = [pad + 'columns:'];
      cols.forEach(function (col) {
        L.push(pad + '  - name: ' + col);
        if (tests[col]) { L.push(pad + '    data_tests:'); tests[col].forEach(function (t) { t.forEach(function (ln) { L.push(pad + '      ' + ln); }); }); }
      });
      return L;
    }
    // sources.yml
    var gk = Object.keys(groups);
    if (gk.length) {
      var S = ['version: 2', '', 'sources:'];
      gk.forEach(function (k) {
        var g = groups[k];
        S.push('  - name: ' + g.name);
        if (g.database) S.push('    database: ' + y(g.database));
        S.push('    schema: ' + y(g.schema), '    description: ' + y(g.description), '    tables:');
        g.tables.forEach(function (t) {
          S.push('      - name: ' + t.name);
          if (t.identifier) S.push('        identifier: ' + y(t.identifier));
          S.push('        description: ' + y(t.node.notes || summary(t.node)));
          S = S.concat(columnsYaml(t.node, 8));
        });
      });
      files['models/sources.yml'] = S.join('\n') + '\n';
    }
    ['staging', 'marts'].forEach(function (f) {
      var ms = models.filter(function (n) { return folder[n.id] === f; });
      if (!ms.length) return;
      var M = ['version: 2', '', 'models:'];
      ms.forEach(function (n) {
        M.push('  - name: ' + n.name, '    description: ' + y(n.notes || summary(n)));
        M = M.concat(columnsYaml(n, 4));
      });
      files['models/' + f + '/schema.yml'] = M.join('\n') + '\n';
    });
    // exposures for sinks that live outside the warehouse
    var exps = p.nodes.filter(function (n) { return n.type === 's3_parquet' || n.type === 'lake_table' || n.type === 'kafka_sink'; });
    if (exps.length) {
      var E = ['version: 2', '', 'exposures:'];
      exps.forEach(function (n) {
        var up = C.dataInputs(p, n.id)[0];
        E.push('  - name: ' + n.name, '    type: application', '    description: ' + y((n.notes ? n.notes + ' ' : '') + summary(n) + ' (written by the orchestrator).'),
          '    owner:', '      name: ' + y(s.owner || 'data team'));
        if (s.failureEmail) E.push('      email: ' + y(s.failureEmail));
        if (up) E.push('    depends_on:', '      - ' + y(depRef(up)));
      });
      files['models/exposures.yml'] = E.join('\n') + '\n';
    }
    files['macros/generate_schema_name.sql'] = '{#- Use custom schemas (e.g. analytics) as-is instead of prefixing them with the target schema. -#}\n{% macro generate_schema_name(custom_schema_name, node) -%}\n    {%- if custom_schema_name is none -%}\n        {{ target.schema }}\n    {%- else -%}\n        {{ custom_schema_name | trim }}\n    {%- endif -%}\n{%- endmacro %}\n';
    files['dbt_project.yml'] = ['name: ' + proj, 'version: "1.0.0"', 'config-version: 2', 'profile: ' + proj, '',
      'model-paths: ["models"]', 'test-paths: ["tests"]', 'macro-paths: ["macros"]', 'clean-targets: ["target", "dbt_packages"]', '',
      'models:', '  ' + proj + ':'].concat(['staging', 'marts'].filter(function (f) { return models.some(function (n) { return folder[n.id] === f; }); }).map(function (f) { return '    ' + f + ':\n      +materialized: table'; })).join('\n') + '\n';
    files['profiles.example.yml'] = profiles(p);
    files['README.md'] = readme(p, files);
    return files;
  }
  function profiles(p) {
    var s = p.settings, sch = s.stagingSchema || 'staging', L = ['# Copy to ~/.dbt/profiles.yml (or set DBT_PROFILES_DIR) and fill in the env vars.', s.name + ':', '  target: dev', '  outputs:', '    dev:'];
    var o = {
      snowflake: ['type: snowflake', "account: \"{{ env_var('SNOWFLAKE_ACCOUNT') }}\"", "user: \"{{ env_var('SNOWFLAKE_USER') }}\"", "password: \"{{ env_var('SNOWFLAKE_PASSWORD') }}\"", "role: \"{{ env_var('SNOWFLAKE_ROLE', 'TRANSFORMER') }}\"", "database: \"{{ env_var('SNOWFLAKE_DATABASE') }}\"", "warehouse: \"{{ env_var('SNOWFLAKE_WAREHOUSE') }}\"", 'schema: ' + sch, 'threads: 4'],
      bigquery: ['type: bigquery', 'method: oauth', "project: \"{{ env_var('GCP_PROJECT') }}\"", 'dataset: ' + sch, 'location: US', 'threads: 4'],
      postgres: ['type: postgres', "host: \"{{ env_var('PGHOST', 'localhost') }}\"", "user: \"{{ env_var('PGUSER') }}\"", "password: \"{{ env_var('PGPASSWORD') }}\"", 'port: 5432', "dbname: \"{{ env_var('PGDATABASE') }}\"", 'schema: ' + sch, 'threads: 4'],
      redshift: ['type: redshift', "host: \"{{ env_var('REDSHIFT_HOST') }}\"", "user: \"{{ env_var('REDSHIFT_USER') }}\"", "password: \"{{ env_var('REDSHIFT_PASSWORD') }}\"", 'port: 5439', "dbname: \"{{ env_var('REDSHIFT_DATABASE') }}\"", 'schema: ' + sch, 'threads: 4'],
      duckdb: ['type: duckdb', "path: \"{{ env_var('DUCKDB_PATH', 'warehouse.duckdb') }}\"", 'schema: ' + sch, 'threads: 4']
    }[s.warehouse];
    L = L.concat(o.map(function (l) { return '      ' + l; }));
    if (s.warehouse !== 'duckdb') L.push('    local:', '      type: duckdb', '      path: local.duckdb', '      schema: ' + sch, '      threads: 1');
    return L.join('\n') + '\n';
  }
  function readme(p, files) {
    var s = p.settings;
    return ['# ' + s.name + ' (dbt project)', '', (s.description || '') + '', '',
      'Generated by [PipelineSketch](https://shaikbasha1437.github.io/pipelinesketch/). Requires dbt-core 1.10 or newer and dbt-' + s.warehouse + '.', '',
      '```bash', 'pip install "dbt-core>=1.10" dbt-' + s.warehouse, 'cp profiles.example.yml ~/.dbt/profiles.yml   # then fill in the env vars', 'dbt parse', 'dbt build', '```', '',
      '- Raw tables in `' + s.rawSchema + '` are landed by the orchestrator (Airflow, Dagster or Prefect exports) and declared as sources.',
      '- Transform models build in the target schema (`' + s.stagingSchema + '`); warehouse-table sinks build as table or incremental models in their own schema.',
      '- Column checks are generic `data_tests`; row-count, freshness, multi-column unique and custom checks are singular tests in `tests/`.',
      '- Sinks outside the warehouse (files, lakes, Kafka) are documented as exposures.', '', '## Files', ''].concat(Object.keys(files).sort().map(function (f) { return '- `' + f + '`'; })).join('\n') + '\n';
  }
  function summary(n) {
    var T = (typeof module === 'object' && module.exports) ? require('./nodes.js') : self.PS.nodes;
    try { return T.summary(n) || T.get(n.type).label; } catch (e) { return n.type; }
  }
  function toText(files) {
    return Object.keys(files).sort().map(function (f) { return '# ===== ' + f + ' =====\n' + files[f]; }).join('\n');
  }
  return { generate: generate, toText: toText };
});
