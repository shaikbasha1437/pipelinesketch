/* Shared pieces for the Python exporters (Airflow, Dagster, Prefect). MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./core.js'), require('./nodes.js'));
  else (root.PS = root.PS || {}).gen = factory(root.PS.core, root.PS.nodes);
})(typeof self !== 'undefined' ? self : this, function (C, NODES) {
  'use strict';
  var py = C.pyStr;
  function upper(s) { return C.toIdent(s).toUpperCase(); }
  function connEnv(connId) { return upper(connId || 'conn') + '_URL'; }
  function conn(p, n, key) { return (n.config && n.config.conn_id) || p.settings.conns[key] || (key + '_default'); }
  function relOf(p) { return function (x) { return C.relation(p, x); }; }
  function inputRel(p, n) { var d = C.dataInputs(p, n.id)[0]; return d ? C.relation(p, d) : null; }
  function selectSql(p, n) { return C.compileSelect(p, n, relOf(p), p.settings.warehouse); }
  function buildStatements(p, n) {
    var d = p.settings.warehouse;
    if (C.kindOf(n) === 'sql') return C.materialize(C.relation(p, n), selectSql(p, n), d);
    if (n.type === 'warehouse_table') {
      var src = inputRel(p, n) || '/* missing input */', ins = C.dataInputs(p, n.id)[0];
      return C.sinkStatements(n.config.table, src, n.config.mode, n.config.keys, ins ? C.columnsOf(p, ins) : null, d);
    }
    return null;
  }
  // Check queries: each returns a single row whose values must all be true.
  function checkQueries(p, n) {
    var c = n.config || {}, rel = inputRel(p, n) || '/* missing input */', d = p.settings.warehouse, out = [];
    if (n.type === 'not_null') C.asList(c.columns).forEach(function (col) { out.push({ name: col + '_not_null', sql: 'select count(*) = 0 from ' + rel + ' where ' + col + ' is null' }); });
    if (n.type === 'unique') C.asList(c.columns).forEach(function (col) { out.push({ name: col + '_unique', sql: 'select count(*) = 0 from (\n    select ' + col + ' from ' + rel + '\n    where ' + col + ' is not null\n    group by ' + col + '\n    having count(*) > 1\n) as dupes' }); });
    if (n.type === 'accepted_values') out.push({ name: c.column + '_accepted_values', sql: 'select count(*) = 0 from ' + rel + ' where ' + c.column + ' not in (' + C.asList(c.values).map(C.sqlLit).join(', ') + ')' });
    if (n.type === 'row_count') out.push({ name: 'row_count', sql: 'select ' + rowCountCond(c) + ' from ' + rel });
    if (n.type === 'freshness') out.push({ name: c.column + '_freshness', sql: 'select ' + C.freshnessExpr(c.column, c.max_age_hours, d) + ' from ' + rel });
    if (n.type === 'custom_check') out.push({ name: n.name, sql: 'select count(*) = 0 from (\n' + C.substituteSql(c.sql, C.dataInputs(p, n.id).map(relOf(p)), function (nm) { var m = C.nodeByName(p, nm); return m ? C.relation(p, C.dataProducer(p, m.id) || m) : null; }).trim() + '\n) as failures' });
    return out;
  }
  function num(v) { return v === '' || v == null || isNaN(+v) ? null : +v; }
  function rowCountCond(c) {
    var lo = num(c.min), hi = num(c.max);
    if (lo != null && hi != null) return 'count(*) between ' + lo + ' and ' + hi;
    if (hi != null) return 'count(*) <= ' + hi;
    return 'count(*) >= ' + (lo == null ? 1 : lo);
  }
  function messageExpr(msg, pipelineName) {
    // Plain-Python version of the Airflow-templated alert message.
    return py(String(msg || '')) + '.replace("{{ ds }}", run_date).replace("{{ dag.dag_id }}", ' + JSON.stringify(pipelineName) + ').replace("{{ run_id }}", run_id)';
  }
  function readerExpr(fmt, bytesExpr) {
    if (fmt === 'csv') return 'pd.read_csv(io.BytesIO(' + bytesExpr + '))';
    if (fmt === 'json') return 'pd.read_json(io.BytesIO(' + bytesExpr + '), lines=True)';
    return 'pd.read_parquet(io.BytesIO(' + bytesExpr + '))';
  }
  function extOf(fmt) { return fmt === 'csv' ? '.csv' : fmt === 'json' ? '.json' : '.parquet'; }
  function withDs(s, dsVar) { return /\{ds\}/.test(s) ? py(s) + '.replace("{ds}", ' + dsVar + ')' : py(s); }
  function renameTransform(code, name) { return String(code || '').replace(/^def\s+transform\s*\(/m, 'def transform_' + name + '('); }
  function sortedTasks(p) {
    var t = C.topoSort(p), order = t.order.concat(t.cyclic);
    return order.map(function (id) { return C.nodeById(p, id); });
  }
  function scheduleCron(p) {
    var e = C.effectiveSchedule(p), info = C.scheduleInfo(e);
    return { raw: e, info: info, cron: info.ok && info.kind === 'cron' ? info.cron : null };
  }
  function lakeBody(n, ind) {
    var c = n.config, keys = C.asList(c.keys), parts = C.asList(c.partition_by), L = [];
    if ((c.format || 'delta') === 'iceberg') {
      L.push('import pyarrow as pa', 'from pyiceberg.catalog import load_catalog', '',
        '# Catalog settings come from ~/.pyiceberg.yaml or PYICEBERG_CATALOG__* env vars.',
        'table = load_catalog(' + py(c.catalog || 'default') + ').load_table(' + py(c.location) + ')',
        'data = pa.Table.from_pandas(df, preserve_index=False)');
      if (c.mode === 'merge') L.push('table.upsert(data, join_cols=' + C.pyList(keys) + ')');
      else if (c.mode === 'overwrite') L.push('table.overwrite(data)');
      else L.push('table.append(data)');
    } else {
      L.push('import pyarrow as pa', 'from deltalake import DeltaTable, write_deltalake', 'from deltalake.exceptions import TableNotFoundError', '',
        '# Object-store credentials come from the usual AWS_*/GOOGLE_*/AZURE_* env vars.',
        'uri = ' + py(c.location), 'data = pa.Table.from_pandas(df, preserve_index=False)');
      if (c.mode === 'merge') {
        L.push('try:', '    target = DeltaTable(uri)', 'except TableNotFoundError:',
          '    write_deltalake(uri, data' + (parts.length ? ', partition_by=' + C.pyList(parts) : '') + ')', 'else:',
          '    (', '        target.merge(source=data, predicate=' + py(keys.map(function (k) { return 't.' + k + ' = s.' + k; }).join(' AND ')) + ', source_alias="s", target_alias="t")',
          '        .when_matched_update_all()', '        .when_not_matched_insert_all()', '        .execute()', '    )');
      } else L.push('write_deltalake(uri, data, mode=' + py(c.mode === 'overwrite' ? 'overwrite' : 'append') + (parts.length ? ', partition_by=' + C.pyList(parts) : '') + ')');
    }
    return C.indent(L.join('\n'), ind);
  }
  function packages(p, flavor) {
    var s = new Set(), wh = p.settings.warehouse;
    p.nodes.forEach(function (n) {
      var t = n.type;
      if (flavor === 'airflow') {
        if (t === 'pg_table') s.add('apache-airflow-providers-postgres');
        if (t === 'mysql_table') s.add('apache-airflow-providers-mysql');
        if (t === 's3_files' || t === 's3_parquet' || (t === 'sensor' && n.config.storage === 's3')) s.add('apache-airflow-providers-amazon');
        if (t === 'gcs_files' || (t === 'sensor' && n.config.storage === 'gcs')) s.add('apache-airflow-providers-google');
        if (t === 'kafka_topic' || t === 'kafka_sink') s.add('apache-airflow-providers-apache-kafka');
        if (t === 'alert' && n.config.channel !== 'email') s.add('apache-airflow-providers-slack');
        if (t === 'dbt_model') s.add('dbt-core + dbt-' + wh);
      } else {
        if (t === 'pg_table') s.add('psycopg2-binary');
        if (t === 'mysql_table') s.add('pymysql');
        if (t === 's3_files' || t === 's3_parquet' || (t === 'sensor' && n.config.storage === 's3')) s.add('boto3');
        if (t === 'gcs_files' || (t === 'sensor' && n.config.storage === 'gcs')) s.add('google-cloud-storage');
        if (t === 'kafka_topic' || t === 'kafka_sink') s.add('confluent-kafka');
        if (t === 'dbt_model') s.add('dbt-core dbt-' + wh);
        if (t === 'rest_api' || t === 'alert') s.add('requests');
      }
      if (t === 'lake_table') s.add(n.config.format === 'iceberg' ? 'pyiceberg[pyarrow]' : 'deltalake');
      if (t === 's3_parquet' || t === 'lake_table' || t === 's3_files' || t === 'gcs_files') s.add('pyarrow');
      if (t === 'python') C.asList(n.config.requirements).forEach(function (r) { s.add(r); });
    });
    if (flavor === 'airflow') {
      s.add('apache-airflow-providers-common-sql');
      s.add({ snowflake: 'apache-airflow-providers-snowflake', bigquery: 'apache-airflow-providers-google', postgres: 'apache-airflow-providers-postgres', redshift: 'apache-airflow-providers-amazon', duckdb: 'airflow-provider-duckdb' }[wh]);
    } else {
      s.add('pandas'); s.add('sqlalchemy');
      s.add({ snowflake: 'snowflake-sqlalchemy', bigquery: 'sqlalchemy-bigquery', postgres: 'psycopg2-binary', redshift: 'sqlalchemy-redshift redshift_connector', duckdb: 'duckdb-engine' }[wh]);
    }
    return Array.from(s).sort();
  }
  function header(p, title, extra) {
    var s = p.settings, sch = scheduleCron(p);
    var L = [s.name + (s.description ? ': ' + s.description : ''), '', title,
      'Generated by PipelineSketch (https://shaikbasha1437.github.io/pipelinesketch/).',
      '', 'Owner:     ' + (s.owner || '-'),
      'Schedule:  ' + (sch.raw || 'None') + (sch.info.ok ? ' (' + sch.info.text + ')' : ''),
      'Warehouse: ' + C.WAREHOUSES[s.warehouse].label];
    return L.concat(extra || []).join('\n');
  }
  function docstring(text) { return '"""\n' + String(text).replace(/\\/g, '\\\\').replace(/"""/g, '\\"\\"\\"') + '\n"""'; }

  /* ---- plain Python step bodies for Dagster and Prefect ----
     Each becomes `def step_<name>(wh, run_date, run_id)` returning a dict of metadata. */
  function plainStep(p, n) {
    var c = n.config || {}, s = p.settings, name = n.name, L = [], rel = inputRel(p, n);
    var ret = function (expr) { L.push('return ' + expr); };
    switch (n.type) {
      case 'pg_table': case 'mysql_table': {
        var env = connEnv(conn(p, n, n.type === 'pg_table' ? 'postgres' : 'mysql'));
        var q = 'SELECT * FROM ' + c.schema + '.' + c.table;
        L.push('# Source database URL, e.g. ' + (n.type === 'pg_table' ? 'postgresql+psycopg2://user:pass@host:5432/db' : 'mysql+pymysql://user:pass@host:3306/db'),
          'source = sa.create_engine(os.environ[' + py(env) + '])');
        if (c.incremental_column) {
          L.push('start = dt.date.fromisoformat(run_date)', 'end = start + dt.timedelta(days=1)',
            'query = sa.text(' + py(q + ' WHERE ' + c.incremental_column + ' >= :start AND ' + c.incremental_column + ' < :end') + ')',
            'with source.connect() as conn:', '    df = pd.read_sql_query(query, conn, params={"start": start, "end": end})');
        } else L.push('with source.connect() as conn:', '    df = pd.read_sql_query(sa.text(' + py(q) + '), conn)');
        L.push('wh.load_df(df, RAW_SCHEMA, ' + py(name) + ')'); ret('{"rows": len(df)}'); break;
      }
      case 's3_files':
        L.push('import boto3', '', 'client = boto3.client("s3")', 'prefix = ' + withDs(c.prefix, 'run_date'),
          'keys = [', '    obj["Key"]', '    for page in client.get_paginator("list_objects_v2").paginate(Bucket=' + py(c.bucket) + ', Prefix=prefix)',
          '    for obj in page.get("Contents", [])', '    if obj["Key"].endswith(' + py(extOf(c.format)) + ')', ']',
          'frames = [' + readerExpr(c.format, 'client.get_object(Bucket=' + py(c.bucket) + ', Key=key)["Body"].read()') + ' for key in keys]',
          'df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()', 'wh.load_df(df, RAW_SCHEMA, ' + py(name) + ')');
        ret('{"files": len(keys), "rows": len(df)}'); break;
      case 'gcs_files':
        L.push('from google.cloud import storage', '', 'bucket = storage.Client().bucket(' + py(c.bucket) + ')', 'prefix = ' + withDs(c.prefix, 'run_date'),
          'blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(' + py(extOf(c.format)) + ')]',
          'frames = [' + readerExpr(c.format, 'b.download_as_bytes()') + ' for b in blobs]',
          'df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()', 'wh.load_df(df, RAW_SCHEMA, ' + py(name) + ')');
        ret('{"files": len(blobs), "rows": len(df)}'); break;
      case 'kafka_topic':
        L.push('from confluent_kafka import Consumer', '',
          'consumer = Consumer({', '    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],', '    "group.id": ' + py(s.name + '.' + name) + ',',
          '    "enable.auto.commit": False,', '    "auto.offset.reset": "earliest",', '})', 'consumer.subscribe([' + py(c.topic) + '])', 'records = []', 'try:',
          '    while len(records) < ' + (+c.max_messages || 100000) + ':', '        msg = consumer.poll(timeout=5.0)', '        if msg is None:', '            break',
          '        if msg.error():', '            raise RuntimeError(msg.error())', '        records.append(json.loads(msg.value()))',
          '    df = pd.json_normalize(records)', '    wh.load_df(df, RAW_SCHEMA, ' + py(name) + ')', '    if records:',
          '        consumer.commit(asynchronous=False)  # commit only after the batch is stored (at-least-once)', 'finally:', '    consumer.close()');
        ret('{"messages": len(records)}'); break;
      case 'rest_api': L.push(restBody(c, name, 'wh.load_df(df, RAW_SCHEMA, ' + py(name) + ')')); ret('{"rows": len(df), "pages": pages}'); break;
      case 'python':
        L.push(pythonBody(p, n, 'wh.read_df', 'wh.load_df')); ret('{"rows": len(result)}'); break;
      case 'dbt_model':
        L.push('import subprocess', '', 'subprocess.run(', '    ["dbt", "build", "--select", ' + py(name) + ', "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],', '    check=True,', ')');
        ret('{"model": ' + py(name) + '}'); break;
      case 'sensor': L.push(sensorBody(c)); ret('{"found": found}'); break;
      case 'branch': L.push('run_date = dt.date.fromisoformat(run_date)', 'return bool(' + c.condition + ')'); break;
      case 'warehouse_table': L.push('wh.run(' + stmtList(buildStatements(p, n), 4) + ')'); ret('{"table": ' + py(c.table) + ', "mode": ' + py(c.mode || 'overwrite') + '}'); break;
      case 's3_parquet':
        L.push('import tempfile', '', 'import boto3', '', 'df = wh.read_df(' + py('select * from ' + rel) + ')', 'prefix = ' + withDs(c.prefix, 'run_date').replace(/$/, '.rstrip("/")'),
          'client = boto3.client("s3")', 'uploaded = 0', 'with tempfile.TemporaryDirectory() as tmp:');
        L.push(C.asList(c.partition_by).length ? '    df.to_parquet(tmp, partition_cols=' + C.pyList(C.asList(c.partition_by)) + ', index=False)' : '    df.to_parquet(os.path.join(tmp, "part-00000.parquet"), index=False)');
        L.push('    for folder, _, files in os.walk(tmp):', '        for file_name in files:', '            path = os.path.join(folder, file_name)',
          '            key = prefix + "/" + os.path.relpath(path, tmp).replace(os.sep, "/")', '            client.upload_file(path, ' + py(c.bucket) + ', key)', '            uploaded += 1');
        ret('{"rows": len(df), "files": uploaded}'); break;
      case 'lake_table': L.push('df = wh.read_df(' + py('select * from ' + rel) + ')', lakeBody(n, 0)); ret('{"rows": len(df)}'); break;
      case 'kafka_sink':
        L.push('from confluent_kafka import Producer', '', 'df = wh.read_df(' + py('select * from ' + rel) + ')',
          'producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})', 'for row in df.to_dict(orient="records"):',
          '    producer.produce(' + py(c.topic) + ', ' + (c.key_column ? 'key=str(row[' + py(c.key_column) + ']), ' : '') + 'value=json.dumps(row, default=str))',
          '    producer.poll(0)', 'producer.flush()');
        ret('{"messages": len(df)}'); break;
      case 'alert':
        L.push('text = ' + messageExpr(c.message, s.name));
        if (c.channel === 'email') {
          L.push('import smtplib', 'from email.message import EmailMessage', '', 'mail = EmailMessage()', 'mail["Subject"] = ' + py('[' + s.name + '] ') + ' + run_date',
            'mail["From"] = os.environ.get("SMTP_FROM", "pipelines@localhost")', 'mail["To"] = ' + py(C.asList(c.to).join(', ')), 'mail.set_content(text)',
            'with smtplib.SMTP(os.environ.get("SMTP_HOST", "localhost"), int(os.environ.get("SMTP_PORT", "25"))) as smtp:', '    smtp.send_message(mail)');
        } else L.push('import requests', '', 'requests.post(os.environ["SLACK_WEBHOOK_URL"], json={"text": text}, timeout=30).raise_for_status()');
        ret('{"sent": True}'); break;
      default:
        if (C.kindOf(n) === 'sql') { L.push('wh.run(' + stmtList(buildStatements(p, n), 4) + ')'); ret('{"table": ' + py(C.relation(p, n)) + '}'); }
        else if (C.kindOf(n) === 'check') {
          var qs = checkQueries(p, n);
          L.push('results = {', qs.map(function (q) { return '    ' + py(q.name) + ': wh.check(' + py(q.sql) + '),'; }).join('\n'), '}');
          ret('results');
        } else ret('{}');
    }
    return 'def step_' + name + '(wh, run_date: str, run_id: str) -> dict:\n' + C.indent(L.join('\n'), 4) + '\n';
  }
  function stmtList(stmts, ind) {
    if (!stmts) return '[]';
    var pad = new Array(ind + 5).join(' ');
    return '[\n' + stmts.map(function (st) { return pad + py(st).replace(/\n/g, '\n') + ','; }).join('\n') + '\n' + new Array(ind + 1).join(' ') + ']';
  }
  function restBody(c, name, loadLine) {
    var path = String(c.records_path || '').trim(), next = String(c.next_field || '').trim();
    var L = ['import requests', '', 'headers = {"Accept": "application/json"}'];
    if (c.token_env) L.push('token = os.environ.get(' + py(c.token_env) + ')', 'if token:', '    headers["Authorization"] = f"Bearer {token}"');
    L.push('url, records, pages = ' + py(c.url) + ', [], 0', 'while url and pages < ' + (+c.max_pages || 50) + ':',
      '    resp = requests.get(url, headers=headers, timeout=60)', '    resp.raise_for_status()', '    payload = resp.json()', '    pages += 1', '    items = payload');
    if (path) L.push('    for part in ' + py(path) + '.split("."):', '        items = items[part]');
    L.push('    records.extend(items if isinstance(items, list) else [items])');
    if (next) L.push('    url = payload', '    for part in ' + py(next) + '.split("."):', '        url = url.get(part) if isinstance(url, dict) else None');
    else L.push('    url = None');
    L.push('df = pd.json_normalize(records)', loadLine);
    return L.join('\n');
  }
  function pythonBody(p, n, readFn, loadFn) {
    var ins = C.dataInputs(p, n.id), L = [];
    L.push('df = ' + readFn + '(' + py('select * from ' + (ins[0] ? C.relation(p, ins[0]) : '/* missing input */')) + ')');
    var extra = ins.slice(1);
    L.push('inputs = {' + extra.map(function (x) { return py(x.name) + ': ' + readFn + '(' + py('select * from ' + C.relation(p, x)) + ')'; }).join(', ') + '}');
    L.push('result = transform_' + n.name + '(df, **inputs)', loadFn + '(result, STAGING_SCHEMA, ' + py(n.name) + ')');
    return L.join('\n');
  }
  function sensorBody(c) {
    var L = ['import time', '', 'path = ' + withDs(c.path, 'run_date'), 'deadline = time.monotonic() + ' + Math.round((+c.timeout_hours || 6) * 3600)];
    if (c.storage === 'gcs') L.push('from google.cloud import storage', '', 'bucket = storage.Client().bucket(' + py(c.bucket) + ')', 'exists = lambda: any(True for _ in bucket.list_blobs(prefix=path.rstrip("*"), max_results=1))');
    else if (c.storage === 'local') L.push('import glob', '', 'exists = lambda: bool(glob.glob(path))');
    else L.push('import fnmatch', '', 'import boto3', '', 'client = boto3.client("s3")',
      'def exists():', '    prefix = path.split("*")[0]', '    for page in client.get_paginator("list_objects_v2").paginate(Bucket=' + py(c.bucket) + ', Prefix=prefix):',
      '        if any(fnmatch.fnmatch(o["Key"], path) for o in page.get("Contents", [])):', '            return True', '    return False');
    L.push('found = exists()', 'while not found:', '    if time.monotonic() > deadline:', '        raise TimeoutError(f"Timed out waiting for {path}")',
      '    time.sleep(' + Math.round((+c.poke_minutes || 5) * 60) + ')', '    found = exists()');
    return L.join('\n');
  }
  function warehouseMethods() {
    return [
      'def engine(self) -> sa.Engine:', '    """One shared engine (and pool) per URL for the whole process."""', '    if self.url not in _ENGINES:', '        _ENGINES[self.url] = sa.create_engine(self.url)', '    return _ENGINES[self.url]', '',
      'def run(self, statements: list[str]) -> None:', '    """Run statements in one transaction."""', '    with self.engine().begin() as conn:', '        for statement in statements:', '            conn.exec_driver_sql(statement)', '',
      'def read_df(self, sql: str) -> pd.DataFrame:', '    with self.engine().connect() as conn:', '        return pd.read_sql_query(sql, conn)', '',
      'def load_df(self, df: pd.DataFrame, schema: str, table: str) -> None:', '    """Replace schema.table with the DataFrame."""',
      '    with self.engine().begin() as conn:', '        conn.exec_driver_sql(f"create schema if not exists {schema}")',
      '        df.to_sql(table, conn, schema=schema, if_exists="replace", index=False, chunksize=10_000)', '',
      'def check(self, sql: str) -> bool:', '    """True when every value in the first row is truthy."""', '    with self.engine().connect() as conn:', '        row = conn.exec_driver_sql(sql).first()', '    return row is not None and all(bool(v) for v in row)'
    ].join('\n');
  }
  function urlExample(wh) {
    return { snowflake: 'snowflake://user:pass@account/database/schema?warehouse=wh&role=role', bigquery: 'bigquery://my-project', postgres: 'postgresql+psycopg2://user:pass@host:5432/db',
      redshift: 'redshift+redshift_connector://user:pass@host:5439/db', duckdb: 'duckdb:////data/warehouse.duckdb' }[wh];
  }
  return {
    upper: upper, connEnv: connEnv, conn: conn, relOf: relOf, inputRel: inputRel, selectSql: selectSql, buildStatements: buildStatements,
    checkQueries: checkQueries, rowCountCond: rowCountCond, readerExpr: readerExpr, extOf: extOf, withDs: withDs, renameTransform: renameTransform,
    sortedTasks: sortedTasks, scheduleCron: scheduleCron, lakeBody: lakeBody, packages: packages, header: header, docstring: docstring,
    plainStep: plainStep, stmtList: stmtList, restBody: restBody, pythonBody: pythonBody, warehouseMethods: warehouseMethods, urlExample: urlExample
  };
});
