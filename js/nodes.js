/* PipelineSketch node library: every node type, its ports and its config form. MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else (root.PS = root.PS || {}).nodes = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  var CATEGORIES = [
    { id: 'source', label: 'Sources', color: '#22c55e' },
    { id: 'transform', label: 'Transforms', color: '#3b82f6' },
    { id: 'quality', label: 'Quality checks', color: '#eab308' },
    { id: 'sink', label: 'Sinks', color: '#a855f7' },
    { id: 'control', label: 'Control', color: '#94a3b8' }
  ];
  var FORMAT = { key: 'format', label: 'File format', type: 'select', options: ['parquet', 'csv', 'json'], default: 'parquet' };
  var COLS = function (label, help) { return { key: 'columns', label: label || 'Columns', type: 'list', required: true, placeholder: 'id, email', help: help }; };

  // kind: what the node becomes when exported.
  //  extract   = pulls data from outside the warehouse and lands it in RAW_SCHEMA.<name>
  //  wh_source = a table already in the warehouse (no task, referenced directly)
  //  sql       = compiled to SQL and materialized as STAGING_SCHEMA.<name>
  //  python    = pandas function, result stored as STAGING_SCHEMA.<name>
  //  dbt       = a dbt model run with `dbt build --select`
  //  check     = data test on its input (passes data through)
  //  sink      = writes its input somewhere
  //  control   = scheduling / orchestration helpers
  var TYPES = [
    // ---------- sources ----------
    { type: 'pg_table', label: 'Postgres table', category: 'source', kind: 'extract', icon: '🐘', conn: 'postgres', fields: [
      { key: 'schema', label: 'Schema', type: 'text', default: 'public', required: true },
      { key: 'table', label: 'Table', type: 'text', required: true, placeholder: 'orders' },
      { key: 'incremental_column', label: 'Incremental column', type: 'text', placeholder: 'updated_at', help: 'Only rows changed in the run\u2019s data interval are copied. Leave empty for a full copy.' },
      { key: 'conn_id', label: 'Connection ID', type: 'text', placeholder: 'defaults to pipeline setting' }] },
    { type: 'mysql_table', label: 'MySQL table', category: 'source', kind: 'extract', icon: '🐬', conn: 'mysql', fields: [
      { key: 'schema', label: 'Database', type: 'text', required: true, placeholder: 'shop' },
      { key: 'table', label: 'Table', type: 'text', required: true, placeholder: 'customers' },
      { key: 'incremental_column', label: 'Incremental column', type: 'text', placeholder: 'updated_at' },
      { key: 'conn_id', label: 'Connection ID', type: 'text', placeholder: 'defaults to pipeline setting' }] },
    { type: 's3_files', label: 'S3 files', category: 'source', kind: 'extract', icon: '🪣', conn: 'aws', fields: [
      { key: 'bucket', label: 'Bucket', type: 'text', required: true, placeholder: 'acme-landing' },
      { key: 'prefix', label: 'Key prefix', type: 'text', required: true, placeholder: 'orders/{ds}/', help: '{ds} becomes the run date (YYYY-MM-DD).' },
      FORMAT, { key: 'conn_id', label: 'Connection ID', type: 'text', placeholder: 'defaults to pipeline setting' }] },
    { type: 'gcs_files', label: 'GCS files', category: 'source', kind: 'extract', icon: '☁️', conn: 'gcp', fields: [
      { key: 'bucket', label: 'Bucket', type: 'text', required: true },
      { key: 'prefix', label: 'Object prefix', type: 'text', required: true, placeholder: 'exports/{ds}/' },
      FORMAT, { key: 'conn_id', label: 'Connection ID', type: 'text', placeholder: 'defaults to pipeline setting' }] },
    { type: 'kafka_topic', label: 'Kafka topic', category: 'source', kind: 'extract', icon: '📨', conn: 'kafka', fields: [
      { key: 'topic', label: 'Topic', type: 'text', required: true, placeholder: 'events' },
      { key: 'max_messages', label: 'Max messages per run', type: 'number', default: 100000 },
      { key: 'conn_id', label: 'Connection ID', type: 'text', placeholder: 'defaults to pipeline setting', help: 'Messages are JSON. Offsets are committed only after the batch is loaded (at-least-once).' }] },
    { type: 'rest_api', label: 'REST API', category: 'source', kind: 'extract', icon: '🌐', fields: [
      { key: 'url', label: 'URL', type: 'text', required: true, placeholder: 'https://api.example.com/v1/orders' },
      { key: 'records_path', label: 'Records path', type: 'text', placeholder: 'data.items', help: 'Dot path to the list of records in the JSON response. Empty = the response itself.' },
      { key: 'next_field', label: 'Next-page field', type: 'text', placeholder: 'next', help: 'Dot path to the next page URL. Empty = single request.' },
      { key: 'token_env', label: 'Bearer token env var', type: 'text', placeholder: 'API_TOKEN' },
      { key: 'max_pages', label: 'Max pages', type: 'number', default: 50 }] },
    { type: 'snowflake_table', label: 'Snowflake table', category: 'source', kind: 'wh_source', icon: '❄️', fields: [
      { key: 'database', label: 'Database', type: 'text', placeholder: 'ANALYTICS' },
      { key: 'schema', label: 'Schema', type: 'text', required: true, placeholder: 'PUBLIC' },
      { key: 'table', label: 'Table', type: 'text', required: true }] },
    { type: 'bigquery_table', label: 'BigQuery table', category: 'source', kind: 'wh_source', icon: '🔷', fields: [
      { key: 'project', label: 'Project', type: 'text', placeholder: 'my-gcp-project' },
      { key: 'dataset', label: 'Dataset', type: 'text', required: true },
      { key: 'table', label: 'Table', type: 'text', required: true }] },
    // ---------- transforms ----------
    { type: 'sql', label: 'SQL', category: 'transform', kind: 'sql', icon: '🧮', fields: [
      { key: 'sql', label: 'SELECT statement', type: 'code', lang: 'sql', required: true, default: 'select *\nfrom {{ input }}', help: '{{ input }} is the first input, {{ input_2 }} the second, {{ ref(\'node_name\') }} any node.' }] },
    { type: 'filter', label: 'Filter', category: 'transform', kind: 'sql', icon: '🔎', fields: [
      { key: 'condition', label: 'WHERE condition', type: 'text', required: true, placeholder: "status = 'complete'" }] },
    { type: 'select', label: 'Select / rename', category: 'transform', kind: 'sql', icon: '✂️', fields: [
      { key: 'columns', label: 'Columns', type: 'textarea', required: true, placeholder: 'id\ncust_id as customer_id\ncast(amount as numeric) as amount', help: 'One per line: column, or expression as new_name.' }] },
    { type: 'join', label: 'Join', category: 'transform', kind: 'sql', icon: '🔗', inPorts: ['left', 'right'], fields: [
      { key: 'how', label: 'Join type', type: 'select', options: ['inner', 'left', 'right', 'full'], default: 'left' },
      { key: 'on', label: 'Join on', type: 'text', required: true, placeholder: 'customer_id   or   l.cust_id = r.id', help: 'Column names (same on both sides) or an ON condition using l. and r.' },
      { key: 'select', label: 'Output columns', type: 'textarea', default: '*', help: 'Use l. and r. prefixes. * keeps everything.' }] },
    { type: 'aggregate', label: 'Aggregate', category: 'transform', kind: 'sql', icon: '∑', fields: [
      { key: 'group_by', label: 'Group by', type: 'list', placeholder: 'customer_id, order_date' },
      { key: 'aggregations', label: 'Aggregations', type: 'textarea', required: true, placeholder: 'count(*) as orders\nsum(amount) as revenue' }] },
    { type: 'dedupe', label: 'Dedupe', category: 'transform', kind: 'sql', icon: '🧹', fields: [
      { key: 'keys', label: 'Unique key(s)', type: 'list', required: true, placeholder: 'id' },
      { key: 'order_by', label: 'Keep first by', type: 'text', placeholder: 'updated_at desc', help: 'Which row wins for each key.' }] },
    { type: 'union', label: 'Union', category: 'transform', kind: 'sql', icon: '⋃', fields: [
      { key: 'distinct', label: 'Remove duplicate rows', type: 'bool', default: false }] },
    { type: 'pivot', label: 'Pivot', category: 'transform', kind: 'sql', icon: '🔄', fields: [
      { key: 'index', label: 'Row keys', type: 'list', required: true, placeholder: 'customer_id' },
      { key: 'pivot_column', label: 'Pivot column', type: 'text', required: true, placeholder: 'channel' },
      { key: 'values', label: 'Pivot values', type: 'list', required: true, placeholder: 'web, store, app' },
      { key: 'value_column', label: 'Value column', type: 'text', required: true, placeholder: 'amount' },
      { key: 'agg', label: 'Aggregate', type: 'select', options: ['sum', 'count', 'avg', 'min', 'max'], default: 'sum' }] },
    { type: 'python', label: 'Python function', category: 'transform', kind: 'python', icon: '🐍', fields: [
      { key: 'code', label: 'pandas function', type: 'code', lang: 'python', required: true,
        default: 'def transform(df, **inputs):\n    """df is the first input; other inputs arrive by node name."""\n    return df',
        help: 'Must define transform(df, **inputs) and return a DataFrame.' },
      { key: 'requirements', label: 'Extra pip packages', type: 'list', placeholder: 'scikit-learn' }] },
    { type: 'dbt_model', label: 'dbt model', category: 'transform', kind: 'dbt', icon: '🟧', fields: [
      { key: 'sql', label: 'Model SQL', type: 'code', lang: 'sql', required: true, default: 'select *\nfrom {{ input }}', help: '{{ input }} becomes ref()/source() of the upstream node.' },
      { key: 'materialized', label: 'Materialization', type: 'select', options: ['view', 'table', 'incremental', 'ephemeral'], default: 'table' }] },
    // ---------- quality ----------
    { type: 'not_null', label: 'Not null', category: 'quality', kind: 'check', icon: '∅', fields: [COLS('Columns that must not be null')] },
    { type: 'unique', label: 'Unique', category: 'quality', kind: 'check', icon: '☝️', fields: [COLS('Columns that must be unique', 'Each column is checked on its own.')] },
    { type: 'accepted_values', label: 'Accepted values', category: 'quality', kind: 'check', icon: '✅', fields: [
      { key: 'column', label: 'Column', type: 'text', required: true },
      { key: 'values', label: 'Allowed values', type: 'list', required: true, placeholder: 'placed, shipped, returned' }] },
    { type: 'row_count', label: 'Row count range', category: 'quality', kind: 'check', icon: '#️⃣', fields: [
      { key: 'min', label: 'At least', type: 'number', default: 1 },
      { key: 'max', label: 'At most', type: 'number', placeholder: 'no limit' }] },
    { type: 'freshness', label: 'Freshness', category: 'quality', kind: 'check', icon: '⏱️', fields: [
      { key: 'column', label: 'Timestamp column', type: 'text', required: true, placeholder: 'updated_at' },
      { key: 'max_age_hours', label: 'Max age (hours)', type: 'number', required: true, default: 24 }] },
    { type: 'custom_check', label: 'Custom SQL check', category: 'quality', kind: 'check', icon: '🧪', fields: [
      { key: 'sql', label: 'Query returning failing rows', type: 'code', lang: 'sql', required: true, default: 'select *\nfrom {{ input }}\nwhere amount < 0', help: 'The check passes when this returns zero rows (same rule as a dbt test).' }] },
    // ---------- sinks ----------
    { type: 'warehouse_table', label: 'Warehouse table', category: 'sink', kind: 'sink', icon: '🏛️', fields: [
      { key: 'table', label: 'Target table', type: 'text', required: true, placeholder: 'analytics.fct_orders' },
      { key: 'mode', label: 'Write mode', type: 'select', options: ['overwrite', 'append', 'merge'], default: 'overwrite' },
      { key: 'keys', label: 'Merge keys', type: 'list', placeholder: 'order_id', help: 'Required for merge.' }] },
    { type: 's3_parquet', label: 'S3 Parquet', category: 'sink', kind: 'sink', icon: '📦', conn: 'aws', fields: [
      { key: 'bucket', label: 'Bucket', type: 'text', required: true },
      { key: 'prefix', label: 'Key prefix', type: 'text', required: true, placeholder: 'exports/orders/dt={ds}/' },
      { key: 'partition_by', label: 'Partition columns', type: 'list', placeholder: 'country' },
      { key: 'conn_id', label: 'Connection ID', type: 'text', placeholder: 'defaults to pipeline setting' }] },
    { type: 'lake_table', label: 'Delta / Iceberg table', category: 'sink', kind: 'sink', icon: '🧊', fields: [
      { key: 'format', label: 'Table format', type: 'select', options: ['delta', 'iceberg'], default: 'delta' },
      { key: 'location', label: 'Table URI / identifier', type: 'text', required: true, placeholder: 's3://lake/silver/orders  or  db.orders (Iceberg)' },
      { key: 'catalog', label: 'Iceberg catalog name', type: 'text', placeholder: 'default' },
      { key: 'mode', label: 'Write mode', type: 'select', options: ['append', 'overwrite', 'merge'], default: 'append' },
      { key: 'keys', label: 'Merge keys', type: 'list' },
      { key: 'partition_by', label: 'Partition columns (Delta)', type: 'list' }] },
    { type: 'kafka_sink', label: 'Kafka topic', category: 'sink', kind: 'sink', icon: '📤', conn: 'kafka', fields: [
      { key: 'topic', label: 'Topic', type: 'text', required: true },
      { key: 'key_column', label: 'Message key column', type: 'text' },
      { key: 'conn_id', label: 'Connection ID', type: 'text', placeholder: 'defaults to pipeline setting' }] },
    { type: 'alert', label: 'Slack / email alert', category: 'sink', kind: 'sink', icon: '🔔', fields: [
      { key: 'channel', label: 'Send via', type: 'select', options: ['slack', 'email'], default: 'slack' },
      { key: 'to', label: 'Email recipients', type: 'list', placeholder: 'data-team@example.com' },
      { key: 'when', label: 'Send when upstream', type: 'select', options: ['succeeds', 'fails', 'finishes'], default: 'succeeds' },
      { key: 'message', label: 'Message', type: 'textarea', required: true, default: 'Pipeline {{ dag.dag_id }} finished for {{ ds }}.', help: '{{ ds }}, {{ dag.dag_id }} and {{ run_id }} are filled in.' }] },
    // ---------- control ----------
    { type: 'schedule', label: 'Schedule', category: 'control', kind: 'control', icon: '🗓️', inPorts: [], fields: [
      { key: 'cron', label: 'Schedule', type: 'cron', required: true, default: '@daily', help: 'Overrides the pipeline schedule.' }] },
    { type: 'sensor', label: 'Wait for file', category: 'control', kind: 'control', icon: '⏳', fields: [
      { key: 'storage', label: 'Storage', type: 'select', options: ['s3', 'gcs', 'local'], default: 's3' },
      { key: 'bucket', label: 'Bucket', type: 'text', placeholder: 'not used for local files' },
      { key: 'path', label: 'Key / prefix / path', type: 'text', required: true, placeholder: 'orders/{ds}/_SUCCESS' },
      { key: 'poke_minutes', label: 'Check every (min)', type: 'number', default: 5 },
      { key: 'timeout_hours', label: 'Give up after (hours)', type: 'number', default: 6 }] },
    { type: 'branch', label: 'Branch', category: 'control', kind: 'control', icon: '🔀', outPorts: ['yes', 'no'], fields: [
      { key: 'condition', label: 'Python condition', type: 'text', required: true, default: 'run_date.weekday() < 5', help: 'run_date is a datetime.date. True follows "yes", False follows "no".' }] },
    { type: 'sla', label: 'SLA', category: 'control', kind: 'control', icon: '🚨', outPorts: [], fields: [
      { key: 'minutes', label: 'Must finish within (minutes)', type: 'number', required: true, default: 60, help: 'Applies to every task connected into this node.' }] }
  ];
  var BY_TYPE = {};
  TYPES.forEach(function (t) {
    if (!t.inPorts) t.inPorts = t.category === 'source' ? ['in'] : ['in'];
    if (!t.outPorts) t.outPorts = ['out'];
    t.color = CATEGORIES.filter(function (c) { return c.id === t.category; })[0].color;
    BY_TYPE[t.type] = t;
  });
  function get(type) { return BY_TYPE[type] || null; }
  function defaults(type) {
    var t = get(type), c = {};
    if (!t) return c;
    t.fields.forEach(function (f) { if (f.default !== undefined) c[f.key] = f.default; });
    return c;
  }
  // Short one-line summary shown on the canvas card.
  function summary(node) {
    var c = node.config || {}, j = function (v) { return Array.isArray(v) ? v.join(', ') : (v || ''); };
    switch (node.type) {
      case 'pg_table': case 'mysql_table': return (c.schema ? c.schema + '.' : '') + (c.table || '?') + (c.incremental_column ? ' \u0394 ' + c.incremental_column : '');
      case 's3_files': return 's3://' + (c.bucket || '?') + '/' + (c.prefix || '') + ' \u00b7 ' + (c.format || '');
      case 'gcs_files': return 'gs://' + (c.bucket || '?') + '/' + (c.prefix || '') + ' \u00b7 ' + (c.format || '');
      case 'kafka_topic': case 'kafka_sink': return 'topic ' + (c.topic || '?');
      case 'rest_api': return (c.url || '?').replace(/^https?:\/\//, '');
      case 'snowflake_table': return [c.database, c.schema, c.table].filter(Boolean).join('.');
      case 'bigquery_table': return [c.project, c.dataset, c.table].filter(Boolean).join('.');
      case 'sql': case 'dbt_model': case 'custom_check': return (c.sql || '').replace(/\s+/g, ' ').slice(0, 60);
      case 'filter': return 'where ' + (c.condition || '?');
      case 'select': return (c.columns || '').split('\n').filter(Boolean).length + ' columns';
      case 'join': return (c.how || 'inner') + ' join on ' + (c.on || '?');
      case 'aggregate': return 'group by ' + (j(c.group_by) || '(all)');
      case 'dedupe': return 'unique ' + j(c.keys) + (c.order_by ? ' by ' + c.order_by : '');
      case 'union': return c.distinct ? 'union distinct' : 'union all';
      case 'pivot': return c.pivot_column ? c.pivot_column + ' \u2192 ' + j(c.values) : '';
      case 'python': return 'pandas transform';
      case 'not_null': case 'unique': return j(c.columns);
      case 'accepted_values': return (c.column || '?') + ' in (' + j(c.values) + ')';
      case 'row_count': return (c.min != null && c.min !== '' ? c.min : 0) + ' \u2013 ' + (c.max != null && c.max !== '' ? c.max : '\u221e') + ' rows';
      case 'freshness': return (c.column || '?') + ' < ' + (c.max_age_hours || '?') + 'h old';
      case 'warehouse_table': return (c.table || '?') + ' \u00b7 ' + (c.mode || 'overwrite') + (c.mode === 'merge' && c.keys ? ' on ' + j(c.keys) : '');
      case 's3_parquet': return 's3://' + (c.bucket || '?') + '/' + (c.prefix || '');
      case 'lake_table': return (c.format || 'delta') + ' \u00b7 ' + (c.location || '?') + ' \u00b7 ' + (c.mode || 'append');
      case 'alert': return (c.channel || 'slack') + ' when ' + (c.when || 'succeeds');
      case 'schedule': return c.cron || '';
      case 'sensor': return (c.storage || 's3') + ': ' + (c.path || '?');
      case 'branch': return 'if ' + (c.condition || '?');
      case 'sla': return 'within ' + (c.minutes || '?') + ' min';
    }
    return '';
  }
  return { CATEGORIES: CATEGORIES, TYPES: TYPES, get: get, defaults: defaults, summary: summary };
});
