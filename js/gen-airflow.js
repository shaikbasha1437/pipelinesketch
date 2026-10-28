/* Airflow 2.x DAG exporter (TaskFlow API + common.sql / provider operators). MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./core.js'), require('./gen-common.js'));
  else (root.PS = root.PS || {}).airflow = factory(root.PS.core, root.PS.gen);
})(typeof self !== 'undefined' ? self : this, function (C, G) {
  'use strict';
  var py = C.pyStr;
  function jinjaDs(s) { return String(s || '').replace(/\{ds\}/g, '{{ ds }}'); }
  function pyDict(obj, ind) {
    var pad = new Array((ind || 0) + 1).join(' ');
    var keys = Object.keys(obj);
    if (!keys.length) return '{}';
    return '{\n' + keys.map(function (k) {
      var v = obj[k];
      return pad + '    ' + JSON.stringify(k) + ': ' + (v && typeof v === 'object' ? pyDict(v, (ind || 0) + 4) : JSON.stringify(v)) + ',';
    }).join('\n') + '\n' + pad + '}';
  }
  function hookFor(wh) {
    return {
      snowflake: ['from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook', 'SnowflakeHook(snowflake_conn_id=WAREHOUSE_CONN_ID)'],
      bigquery: ['from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook', 'BigQueryHook(gcp_conn_id=WAREHOUSE_CONN_ID, use_legacy_sql=False)'],
      postgres: ['from airflow.providers.postgres.hooks.postgres import PostgresHook', 'PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)'],
      redshift: ['from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook', 'RedshiftSQLHook(redshift_conn_id=WAREHOUSE_CONN_ID)'],
      duckdb: ['from duckdb_provider.hooks.duckdb_hook import DuckDBHook', 'DuckDBHook(duckdb_conn_id=WAREHOUSE_CONN_ID)']
    }[wh];
  }
  function helpers(wh, needLoad, needRead) {
    var h = hookFor(wh), out = [];
    out.push('def _warehouse_hook():', '    """Hook for the warehouse connection (imported lazily to keep DAG parsing fast)."""', '    ' + h[0], '', '    return ' + h[1], '', '');
    if (needRead) out.push('def _read_df(sql: str) -> pd.DataFrame:', '    return _warehouse_hook().get_pandas_df(sql)', '', '');
    if (needLoad) {
      out.push('def _load_df(df: pd.DataFrame, schema: str, table: str) -> None:', '    """Replace schema.table in the warehouse with the contents of df."""');
      if (wh === 'snowflake') out.push('    from snowflake.connector.pandas_tools import write_pandas', '',
        '    conn = _warehouse_hook().get_conn()', '    try:', '        conn.cursor().execute(f"create schema if not exists {schema}")',
        '        if df.empty:  # nothing new: empty the landing table instead of leaving old rows',
        '            conn.cursor().execute(f"truncate table if exists {schema}.{table}")', '            return',
        '        write_pandas(conn, df, table_name=table.upper(), schema=schema.upper(), auto_create_table=True, overwrite=True, quote_identifiers=False)',
        '    finally:', '        conn.close()');
      else if (wh === 'bigquery') out.push('    from google.cloud import bigquery', '',
        '    client = _warehouse_hook().get_client()', '    job = client.load_table_from_dataframe(', '        df, f"{schema}.{table}",',
        '        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),', '    )', '    job.result()');
      else if (wh === 'duckdb') out.push('    conn = _warehouse_hook().get_conn()', '    conn.execute(f"create schema if not exists {schema}")',
        '    conn.register("_incoming", df)', '    conn.execute(f"create or replace table {schema}.{table} as select * from _incoming")', '    conn.unregister("_incoming")');
      else out.push('    engine = _warehouse_hook().get_sqlalchemy_engine()', '    with engine.begin() as conn:',
        '        conn.exec_driver_sql(f"create schema if not exists {schema}")',
        '        df.to_sql(table, conn, schema=schema, if_exists="replace", index=False, chunksize=10_000' + (wh === 'redshift' ? ', method="multi"' : '') + ')');
      out.push('', '');
    }
    return out.join('\n');
  }
  function generate(p) {
    var s = p.settings, wh = s.warehouse, nodes = G.sortedTasks(p), tasks = nodes.filter(C.isTask);
    var imports = { 'from airflow.decorators import dag, task': 1 }, sqlImports = {}, consts = [], defs = [], calls = [], userFns = [];
    var needLoad = false, needRead = false, usesPd = false;
    var bd = C.branchDescendants(p), cycles = C.findCycles(p);
    function kw(n) {
      var k = [];
      if (n.type === 'alert') k.push(['trigger_rule', { succeeds: 'all_success', fails: 'one_failed', finishes: 'all_done' }[n.config.when || 'succeeds']]);
      else if (bd[n.id] && C.taskUpstreams(p, n.id).length > 1) k.push(['trigger_rule', 'none_failed_min_one_success']);
      var sla = C.slaFor(p, n.id); if (sla) k.push(['sla', 'timedelta(minutes=' + sla + ')']);
      return k;
    }
    function kwStr(n, sep) { return kw(n).map(function (x) { return sep + x[0] + '=' + (x[0] === 'sla' ? x[1] : JSON.stringify(x[1])); }).join(''); }
    function opCall(n, cls, args) {
      var all = ['task_id=' + JSON.stringify(n.name)].concat(args, kw(n).map(function (x) { return x[0] + '=' + (x[0] === 'sla' ? x[1] : JSON.stringify(x[1])); }));
      calls.push('    t_' + n.name + ' = ' + cls + '(\n' + all.map(function (a) { return '        ' + a + ','; }).join('\n') + '\n    )');
    }
    function taskDef(n, fn, params, doc, body) {
      defs.push('@task(task_id=' + JSON.stringify(n.name) + kwStr(n, ', ') + ')\ndef ' + fn + '(' + params + '):\n    ' + py(doc) + '\n' + C.indent(body, 4) + '\n\n');
      calls.push('    t_' + n.name + ' = ' + fn + '()');
    }
    function constSql(n, stmts) {
      var nm = 'SQL_' + G.upper(n.name);
      consts.push(nm + ' = [\n' + stmts.map(function (x) { return '    ' + py(x) + ','; }).join('\n') + '\n]\n');
      return nm;
    }
    tasks.forEach(function (n) {
      var c = n.config || {}, kind = C.kindOf(n), rel = G.inputRel(p, n) || '/* missing input */', L;
      if (n.type === 'pg_table' || n.type === 'mysql_table') {
        needLoad = true; usesPd = true;
        var isPg = n.type === 'pg_table', cid = G.conn(p, n, isPg ? 'postgres' : 'mysql');
        var hook = isPg ? ['from airflow.providers.postgres.hooks.postgres import PostgresHook', 'PostgresHook(postgres_conn_id=' + py(cid) + ')'] : ['from airflow.providers.mysql.hooks.mysql import MySqlHook', 'MySqlHook(mysql_conn_id=' + py(cid) + ')'];
        var q = 'SELECT * FROM ' + c.schema + '.' + c.table;
        L = [hook[0], ''];
        if (c.incremental_column) {
          L.push('# Incremental: only rows changed inside this run\'s data interval.',
            'sql = ' + py(q + ' WHERE ' + c.incremental_column + ' >= %(start)s AND ' + c.incremental_column + ' < %(end)s'),
            'df = ' + hook[1] + '.get_pandas_df(', '    sql, parameters={"start": data_interval_start, "end": data_interval_end}', ')');
        } else L.push('df = ' + hook[1] + '.get_pandas_df(' + py(q) + ')');
        L.push('_load_df(df, RAW_SCHEMA, ' + py(n.name) + ')', 'return {"rows": len(df)}');
        taskDef(n, 'extract_' + n.name, c.incremental_column ? 'data_interval_start=None, data_interval_end=None' : '', 'Copy ' + c.schema + '.' + c.table + ' from ' + (isPg ? 'Postgres' : 'MySQL') + ' into ' + C.relation(p, n) + '.', L.join('\n'));
      } else if (n.type === 's3_files' || n.type === 'gcs_files') {
        needLoad = true; usesPd = true;
        var s3 = n.type === 's3_files', dsParam = /\{ds\}/.test(c.prefix) ? 'ds=None' : '';
        L = ['import io', '', s3 ? 'from airflow.providers.amazon.aws.hooks.s3 import S3Hook' : 'from airflow.providers.google.cloud.hooks.gcs import GCSHook', '',
          'hook = ' + (s3 ? 'S3Hook(aws_conn_id=' : 'GCSHook(gcp_conn_id=') + py(G.conn(p, n, s3 ? 'aws' : 'gcp')) + ')', 'prefix = ' + G.withDs(c.prefix, 'ds')];
        if (s3) L.push('keys = [k for k in hook.list_keys(bucket_name=' + py(c.bucket) + ', prefix=prefix) or [] if k.endswith(' + py(G.extOf(c.format)) + ')]',
          'frames = [' + G.readerExpr(c.format, 'hook.get_key(k, bucket_name=' + py(c.bucket) + ').get()["Body"].read()') + ' for k in keys]');
        else L.push('keys = [k for k in hook.list(' + py(c.bucket) + ', prefix=prefix) if k.endswith(' + py(G.extOf(c.format)) + ')]',
          'frames = [' + G.readerExpr(c.format, 'hook.download(bucket_name=' + py(c.bucket) + ', object_name=k)') + ' for k in keys]');
        L.push('df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()', '_load_df(df, RAW_SCHEMA, ' + py(n.name) + ')', 'return {"files": len(keys), "rows": len(df)}');
        taskDef(n, 'extract_' + n.name, dsParam, 'Load ' + (c.format || 'parquet') + ' files from ' + (s3 ? 's3://' : 'gs://') + c.bucket + '/' + c.prefix + ' into ' + C.relation(p, n) + '.', L.join('\n'));
      } else if (n.type === 'kafka_topic') {
        needLoad = true; usesPd = true;
        L = ['import json', '', 'from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook', '',
          '# The Kafka connection must set group.id and "enable.auto.commit": false.',
          'consumer = KafkaConsumerHook(topics=[' + py(c.topic) + '], kafka_config_id=' + py(G.conn(p, n, 'kafka')) + ').get_consumer()',
          'records = []', 'try:', '    while len(records) < ' + (+c.max_messages || 100000) + ':', '        msg = consumer.poll(timeout=5.0)',
          '        if msg is None:', '            break', '        if msg.error():', '            raise RuntimeError(msg.error())', '        records.append(json.loads(msg.value()))',
          '    df = pd.json_normalize(records)', '    _load_df(df, RAW_SCHEMA, ' + py(n.name) + ')', '    if records:', '        consumer.commit(asynchronous=False)  # only after the batch is stored: at-least-once',
          'finally:', '    consumer.close()', 'return {"messages": len(records)}'];
        taskDef(n, 'extract_' + n.name, '', 'Consume up to ' + (+c.max_messages || 100000) + ' JSON messages from ' + c.topic + ' into ' + C.relation(p, n) + '.', L.join('\n'));
      } else if (n.type === 'rest_api') {
        needLoad = true; usesPd = true;
        L = [(c.token_env ? 'import os\n\n' : '') + G.restBody(c, n.name, '_load_df(df, RAW_SCHEMA, ' + py(n.name) + ')'), 'return {"rows": len(df), "pages": pages}'];
        taskDef(n, 'extract_' + n.name, '', 'Fetch ' + c.url + ' into ' + C.relation(p, n) + '.', L.join('\n'));
      } else if (kind === 'sql') {
        sqlImports.SQLExecuteQueryOperator = 1;
        opCall(n, 'SQLExecuteQueryOperator', ['conn_id=WAREHOUSE_CONN_ID', 'sql=' + constSql(n, G.buildStatements(p, n))].concat(wh === 'bigquery' ? ['hook_params={"use_legacy_sql": False}'] : []));
      } else if (n.type === 'python') {
        needLoad = true; needRead = true; usesPd = true;
        userFns.push('# --- user code for ' + n.name + ' ---\n' + G.renameTransform(c.code, n.name).trim() + '\n\n');
        taskDef(n, 'run_' + n.name, '', 'Run transform_' + n.name + '() with pandas and store the result in ' + C.relation(p, n) + '.', G.pythonBody(p, n, '_read_df', '_load_df') + '\nreturn {"rows": len(result)}');
      } else if (n.type === 'dbt_model') {
        imports['from airflow.operators.bash import BashOperator'] = 1;
        opCall(n, 'BashOperator', ['bash_command=f"dbt build --select ' + n.name + ' --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"']);
      } else if (kind === 'check') {
        var hp = wh === 'bigquery' ? ['hook_params={"use_legacy_sql": False}'] : [];
        if (n.type === 'not_null' || n.type === 'unique') {
          sqlImports.SQLColumnCheckOperator = 1;
          var map = {}; C.asList(c.columns).forEach(function (col) { map[col] = {}; map[col][n.type === 'unique' ? 'unique_check' : 'null_check'] = { equal_to: 0 }; });
          opCall(n, 'SQLColumnCheckOperator', ['conn_id=WAREHOUSE_CONN_ID', 'table=' + py(rel), 'column_mapping=' + pyDict(map, 8)].concat(hp));
        } else if (n.type === 'row_count') {
          sqlImports.SQLTableCheckOperator = 1;
          opCall(n, 'SQLTableCheckOperator', ['conn_id=WAREHOUSE_CONN_ID', 'table=' + py(rel), 'checks={"row_count_check": {"check_statement": ' + py(G.rowCountCond(c)) + '}}'].concat(hp));
        } else {
          sqlImports.SQLCheckOperator = 1;
          opCall(n, 'SQLCheckOperator', ['conn_id=WAREHOUSE_CONN_ID', 'sql=' + constSql(n, [G.checkQueries(p, n)[0].sql]) + '[0]'].concat(hp));
        }
      } else if (n.type === 'warehouse_table') {
        sqlImports.SQLExecuteQueryOperator = 1;
        var args = ['conn_id=WAREHOUSE_CONN_ID', 'sql=' + constSql(n, G.buildStatements(p, n))];
        if (wh === 'bigquery') args.push('hook_params={"use_legacy_sql": False}');
        opCall(n, 'SQLExecuteQueryOperator', args);
      } else if (n.type === 's3_parquet') {
        needRead = true; usesPd = true;
        var pc = C.asList(c.partition_by);
        L = ['import os', 'import tempfile', '', 'from airflow.providers.amazon.aws.hooks.s3 import S3Hook', '',
          'df = _read_df(' + py('select * from ' + rel) + ')', 'prefix = ' + G.withDs(c.prefix, 'ds') + '.rstrip("/")',
          'hook = S3Hook(aws_conn_id=' + py(G.conn(p, n, 'aws')) + ')', 'uploaded = 0', 'with tempfile.TemporaryDirectory() as tmp:',
          pc.length ? '    df.to_parquet(tmp, partition_cols=' + C.pyList(pc) + ', index=False)' : '    df.to_parquet(os.path.join(tmp, "part-00000.parquet"), index=False)',
          '    for folder, _, files in os.walk(tmp):', '        for file_name in files:', '            path = os.path.join(folder, file_name)',
          '            key = prefix + "/" + os.path.relpath(path, tmp).replace(os.sep, "/")',
          '            hook.load_file(filename=path, key=key, bucket_name=' + py(c.bucket) + ', replace=True)', '            uploaded += 1',
          'return {"rows": len(df), "files": uploaded}'];
        taskDef(n, 'export_' + n.name, /\{ds\}/.test(c.prefix) ? 'ds=None' : '', 'Write ' + rel + ' as Parquet to s3://' + c.bucket + '/' + c.prefix + (pc.length ? ' partitioned by ' + pc.join(', ') : '') + '.', L.join('\n'));
      } else if (n.type === 'lake_table') {
        needRead = true; usesPd = true;
        taskDef(n, 'write_' + n.name, '', 'Write ' + rel + ' to the ' + (c.format || 'delta') + ' table ' + c.location + ' (' + (c.mode || 'append') + ').',
          'df = _read_df(' + py('select * from ' + rel) + ')\n' + G.lakeBody(n, 0) + '\nreturn {"rows": len(df)}');
      } else if (n.type === 'kafka_sink') {
        needRead = true; usesPd = true;
        L = ['import json', '', 'from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook', '',
          'df = _read_df(' + py('select * from ' + rel) + ')', 'producer = KafkaProducerHook(kafka_config_id=' + py(G.conn(p, n, 'kafka')) + ').get_producer()',
          'for row in df.to_dict(orient="records"):', '    producer.produce(' + py(c.topic) + ', ' + (c.key_column ? 'key=str(row[' + py(c.key_column) + ']), ' : '') + 'value=json.dumps(row, default=str))',
          '    producer.poll(0)', 'producer.flush()', 'return {"messages": len(df)}'];
        taskDef(n, 'publish_' + n.name, '', 'Publish ' + rel + ' rows to Kafka topic ' + c.topic + ' as JSON.', L.join('\n'));
      } else if (n.type === 'alert') {
        if (c.channel === 'email') {
          imports['from airflow.operators.email import EmailOperator'] = 1;
          opCall(n, 'EmailOperator', ['to=' + C.pyList(C.asList(c.to)), 'subject=' + py('[Airflow] {{ dag.dag_id }} ' + ({ fails: 'failed', succeeds: 'succeeded', finishes: 'finished' }[c.when || 'succeeds']) + ' for {{ ds }}'), 'html_content=' + py(c.message)]);
        } else {
          imports['from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator'] = 1;
          opCall(n, 'SlackWebhookOperator', ['slack_webhook_conn_id=' + py(p.settings.conns.slack || 'slack_default'), 'message=' + py(c.message)]);
        }
      } else if (n.type === 'sensor') {
        var common = ['mode="reschedule"', 'poke_interval=' + Math.round((+c.poke_minutes || 5) * 60), 'timeout=' + Math.round((+c.timeout_hours || 6) * 3600)];
        var path = jinjaDs(c.path);
        if (c.storage === 'gcs') {
          if (/\/$|\*$/.test(path)) { imports['from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor'] = 1; opCall(n, 'GCSObjectsWithPrefixExistenceSensor', ['bucket=' + py(c.bucket), 'prefix=' + py(path.replace(/\*$/, '')), 'google_cloud_conn_id=' + py(G.conn(p, n, 'gcp'))].concat(common)); }
          else { imports['from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor'] = 1; opCall(n, 'GCSObjectExistenceSensor', ['bucket=' + py(c.bucket), 'object=' + py(path), 'google_cloud_conn_id=' + py(G.conn(p, n, 'gcp'))].concat(common)); }
        } else if (c.storage === 'local') {
          imports['from airflow.sensors.filesystem import FileSensor'] = 1;
          opCall(n, 'FileSensor', ['filepath=' + py(path), 'fs_conn_id="fs_default"'].concat(common));
        } else {
          imports['from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor'] = 1;
          opCall(n, 'S3KeySensor', ['bucket_name=' + py(c.bucket), 'bucket_key=' + py(path), 'wildcard_match=' + (/[*?]/.test(path) ? 'True' : 'False'), 'aws_conn_id=' + py(G.conn(p, n, 'aws'))].concat(common));
        }
      } else if (n.type === 'branch') {
        var ids = function (port) { var out = []; C.downstreamEdges(p, n.id, port).forEach(function (e) { var t = C.nodeById(p, e.to); if (t && C.isTask(t)) out.push(t.name); }); return out; };
        defs.push('@task.branch(task_id=' + JSON.stringify(n.name) + kwStr(n, ', ') + ')\ndef branch_' + n.name + '(logical_date=None):\n    ' + py('Follow "yes" when ' + c.condition + ', otherwise "no".') +
          '\n    run_date = logical_date.date()\n    if ' + c.condition + ':\n        return ' + C.pyList(ids('yes')) + '\n    return ' + C.pyList(ids('no')) + '\n\n');
        calls.push('    t_' + n.name + ' = branch_' + n.name + '()');
      }
    });
    // dependencies
    var deps = [];
    tasks.forEach(function (n) {
      var ups = C.taskUpstreams(p, n.id).map(function (id) { return 't_' + C.nodeById(p, id).name; });
      if (ups.length === 1) deps.push('    ' + ups[0] + ' >> t_' + n.name);
      else if (ups.length > 1) deps.push('    [' + ups.join(', ') + '] >> t_' + n.name);
    });
    var sch = G.scheduleCron(p);
    var sqlImp = Object.keys(sqlImports).sort();
    var imp = ['from __future__ import annotations', '', 'from datetime import timedelta', '', 'import pendulum'];
    if (usesPd) imp.push('import pandas as pd');
    imp.push('');
    Object.keys(imports).sort().forEach(function (k) { imp.push(k); });
    if (sqlImp.length) imp.push(sqlImp.length === 1 ? 'from airflow.providers.common.sql.operators.sql import ' + sqlImp[0] : 'from airflow.providers.common.sql.operators.sql import (\n' + sqlImp.map(function (x) { return '    ' + x + ','; }).join('\n') + '\n)');
    var conns = {}; conns[C.whConn(s)] = 'warehouse (' + C.WAREHOUSES[wh].label + ')';
    p.nodes.forEach(function (n) { var t = n.type; var k = { pg_table: 'postgres', mysql_table: 'mysql', s3_files: 'aws', s3_parquet: 'aws', gcs_files: 'gcp', kafka_topic: 'kafka', kafka_sink: 'kafka' }[t];
      if (t === 'sensor') k = n.config.storage === 'gcs' ? 'gcp' : n.config.storage === 'local' ? null : 'aws';
      if (k) conns[G.conn(p, n, k)] = k; if (t === 'alert' && n.config.channel !== 'email') conns[s.conns.slack || 'slack_default'] = 'slack webhook'; });
    var doc = G.header(p, 'Apache Airflow 2.x DAG (TaskFlow API).', ['',
      'Install: pip install ' + G.packages(p, 'airflow').join(' '),
      'Connections: ' + Object.keys(conns).map(function (k) { return k + ' (' + conns[k] + ')'; }).join(', ')].concat(cycles.length ? ['', 'WARNING: this pipeline has a cycle; Airflow will refuse to load it until it is fixed.'] : []));
    var defaultArgs = ['    "owner": ' + py(s.owner || 'airflow') + ',', '    "depends_on_past": False,', '    "retries": ' + (Math.max(0, parseInt(s.retries, 10) || 0)) + ',', '    "retry_delay": timedelta(minutes=' + (+s.retryDelayMin || 5) + '),'];
    var mails = C.asList(s.failureEmail);
    if (mails.length) defaultArgs.push('    "email": ' + C.pyList(mails) + ',', '    "email_on_failure": True,');
    var sd = String(s.startDate || '2026-01-01').split('-').map(function (x) { return parseInt(x, 10); });
    var schedExpr = sch.info.ok && sch.info.kind === 'none' ? 'None' : py(sch.raw || '@daily');
    var dagFn = s.name + '_dag';
    var out = [G.docstring(doc), imp.join('\n'), '',
      'WAREHOUSE_CONN_ID = ' + py(C.whConn(s)), 'RAW_SCHEMA = ' + py(s.rawSchema), 'STAGING_SCHEMA = ' + py(s.stagingSchema)];
    if (p.nodes.some(function (n) { return n.type === 'dbt_model'; })) out.push('DBT_PROJECT_DIR = ' + py(s.dbtProjectDir || '/opt/airflow/dbt'));
    out.push('', 'default_args = {', defaultArgs.join('\n'), '}', '');
    if (consts.length) out.push('# ---------------------------------------------------------------- SQL', '', consts.join('\n'));
    if (needLoad || needRead || userFns.length) out.push('# ---------------------------------------------------------------- helpers', '', helpers(wh, needLoad, needRead) + userFns.join(''));
    if (defs.length) out.push('# ---------------------------------------------------------------- tasks', '', defs.join(''));
    out.push('@dag(', '    dag_id=' + py(s.name) + ',', s.description ? '    description=' + py(s.description) + ',' : null, '    schedule=' + schedExpr + ',',
      '    start_date=pendulum.datetime(' + (sd[0] || 2026) + ', ' + (sd[1] || 1) + ', ' + (sd[2] || 1) + ', tz=' + py(s.timezone || 'UTC') + '),',
      '    catchup=' + (s.catchup ? 'True' : 'False') + ',', '    max_active_runs=1,', '    default_args=default_args,', '    tags=' + C.pyList(C.asList(s.tags)) + ',', '    doc_md=__doc__,', ')',
      'def ' + dagFn + '():');
    if (!calls.length) out.push('    pass');
    else { out.push(calls.join('\n')); if (deps.length) out.push('', deps.join('\n')); }
    out.push('', '', 'dag_object = ' + dagFn + '()', '', 'if __name__ == "__main__":', '    dag_object.test()', '');
    return out.filter(function (l) { return l !== null; }).join('\n');
  }
  return { generate: generate };
});
