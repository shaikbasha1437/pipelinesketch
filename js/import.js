/* Importers: PipelineSketch JSON / backups, Mermaid flowcharts, Airflow DAG dependencies. MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./core.js'), require('./nodes.js'));
  else (root.PS = root.PS || {}).importer = factory(root.PS.core, root.PS.nodes);
})(typeof self !== 'undefined' ? self : this, function (C, N) {
  'use strict';
  function finish(p, keepPositions) {
    if (!keepPositions) { var pos = C.layout(p); p.nodes.forEach(function (n) { if (pos[n.id]) { n.x = pos[n.id].x; n.y = pos[n.id].y; } }); }
    return C.normalize(p);
  }
  function connect(p, a, b, label) {
    var ta = N.get(a.type), tb = N.get(b.type), lbl = String(label || '').trim().toLowerCase();
    var fromPort = ta.outPorts.indexOf(lbl) !== -1 ? lbl : null, toPort = tb.inPorts.indexOf(lbl) !== -1 ? lbl : null;
    if (!fromPort) fromPort = ta.outPorts.filter(function (pt) { return !p.edges.some(function (e) { return e.from === a.id && e.fromPort === pt; }); })[0] || ta.outPorts[0];
    if (!toPort) toPort = tb.inPorts.filter(function (pt) { return !p.edges.some(function (e) { return e.to === b.id && e.toPort === pt; }); })[0] || tb.inPorts[0];
    if (!fromPort || !toPort) return null;
    return C.addEdge(p, a.id, fromPort, b.id, toPort);
  }
  /* ---------------- JSON ---------------- */
  // Returns {pipelines: [...]} for a backup or a single pipeline.
  function fromJSON(text) {
    var raw = typeof text === 'string' ? JSON.parse(text) : text;
    if (raw && Array.isArray(raw.pipelines)) return { pipelines: raw.pipelines.map(C.normalize) };
    return { pipelines: [C.normalize(raw)] };
  }
  /* ---------------- type guessing from labels / operators ---------------- */
  var GUESS = [
    [/\bdelta\b|iceberg|lakehouse/i, 'lake_table'],
    [/\b(load|copy|read)\w*\b[^.]*\bfrom\s+s3:\/\//i, 's3_files'], [/\b(load|copy|read)\w*\b[^.]*\bfrom\s+gs:\/\//i, 'gcs_files'],
    [/kafka.*(publish|sink|produce)|(produce|publish).*(kafka|topic)/i, 'kafka_sink'], [/kafka|consume.*(topic|messages)|\btopic\b/i, 'kafka_topic'],
    [/postgres|\bpg\b|psql/i, 'pg_table'], [/mysql|mariadb/i, 'mysql_table'],
    [/(\bs3\b|s3:\/\/).*(parquet|export|write|unload)|(write|export|unload).*s3:\/\//i, 's3_parquet'], [/\bs3\b|s3:\/\//i, 's3_files'], [/\bgcs\b|gs:\/\/|google cloud storage/i, 'gcs_files'],
    [/\bapi\b|http|rest/i, 'rest_api'], [/snowflake/i, 'snowflake_table'], [/bigquery|\bbq\b/i, 'bigquery_table'],
    [/not.?null/i, 'not_null'], [/unique/i, 'unique'], [/accepted|allowed values/i, 'accepted_values'], [/row.?count/i, 'row_count'],
    [/fresh/i, 'freshness'], [/\btest|\bcheck|assert|quality/i, 'custom_check'],
    [/dedup/i, 'dedupe'], [/\bjoin/i, 'join'], [/union|combine/i, 'union'], [/pivot/i, 'pivot'], [/aggregat|group by|rollup|summar/i, 'aggregate'],
    [/filter|where/i, 'filter'], [/\bdbt\b/i, 'dbt_model'], [/python|pandas/i, 'python'],
    [/slack|email|notify|alert/i, 'alert'], [/\bsla\b/i, 'sla'],
    [/wait|sensor|arriv/i, 'sensor'], [/branch|if |decide/i, 'branch'], [/schedule|cron/i, 'schedule'],
    [/load|warehouse|publish|mart|fct_|dim_/i, 'warehouse_table']
  ];
  function guessType(text, fallback) {
    for (var i = 0; i < GUESS.length; i++) if (GUESS[i][0].test(text)) return GUESS[i][1];
    return fallback || 'sql';
  }
  /* ---------------- Mermaid ---------------- */
  var SHAPE_RE = /^([A-Za-z_][\w-]*)\s*(\(\[|\[\(|\[\[|\(\(\(|\(\(|\{\{|\[\/|\[\\|\[|\(|\{|>)([\s\S]*?)(\]\)|\)\]|\]\]|\)\)\)|\)\)|\}\}|\/\]|\\\]|\]|\)|\})$/;
  function cleanLabel(s) {
    s = String(s || '').trim();
    if (/^".*"$/.test(s)) s = s.slice(1, -1);
    return s.replace(/<br\s*\/?>/gi, ' \u00b7 ').replace(/<[^>]+>/g, '').replace(/#quot;/g, '"').replace(/#lt;/g, '<').replace(/#gt;/g, '>').trim();
  }
  function fromMermaid(text) {
    var src = String(text || '');
    if (!/^\s*(%%[^\n]*\n\s*)*(flowchart|graph)\b/i.test(src)) throw new Error('Not a Mermaid flowchart (expected "flowchart LR" or "graph TD").');
    var p = C.createPipeline('imported_pipeline'), byId = {}, hinted = false, settings = null, hints = {}, groupHints = [], sub = [], subgraphs = [], inSub = {};
    src.split('\n').forEach(function (line) {
      var m = /^\s*%%\s*ps:settings\s+(\{.*\})\s*$/.exec(line);
      if (m) { try { settings = JSON.parse(m[1]); } catch (e) { /* ignore */ } return; }
      m = /^\s*%%\s*ps:node\s+(\S+)\s+(\S+)\s+(\{.*\})\s*$/.exec(line);
      if (m) { try { hints[m[1]] = { type: m[2], data: JSON.parse(m[3]) }; } catch (e) { /* ignore */ } }
      m = /^\s*%%\s*ps:group\s+(\{.*\})\s*$/.exec(line);
      if (m) { try { groupHints.push(JSON.parse(m[1])); } catch (e) { /* ignore */ } }
    });
    if (settings) { Object.assign(p.settings, settings); }
    function ensure(id, label, shape) {
      if (byId[id] && sub.length && !inSub[id]) { inSub[id] = true; sub[sub.length - 1].members.push(byId[id].id); }
      if (byId[id]) { if (label && !byId[id]._labelled) { byId[id]._label = label; byId[id]._labelled = true; } return byId[id]; }
      var h = hints[id], type, name;
      if (h && N.get(h.type)) { type = h.type; name = h.data.name || id; hinted = true; }
      else {
        var lbl = cleanLabel(label || id);
        type = shape === '{' ? 'branch' : shape === '[(' ? guessType(lbl, 's3_files') : guessType(lbl + ' ' + id, 'sql');
        name = C.toIdent(id, 'step');
      }
      var n = C.addNode(p, type, 0, 0, name);
      if (sub.length) { sub[sub.length - 1].members.push(n.id); inSub[id] = true; }
      if (h && N.get(h.type)) {
        Object.assign(n.config, h.data.config || {}); n.columns = C.asList(h.data.columns || []); n.notes = h.data.notes || '';
        if (h.data.x != null) { n.x = +h.data.x; n.y = +h.data.y; n._pos = true; }
      } else if (label) n.notes = cleanLabel(label);
      byId[id] = n; return n;
    }
    // Split a statement into node refs and links: a --> b -->|x| c, a & b --> c
    var LINK = /\s*(<?(?:-->|---|==>|-\.->|-\.-|===|--[ox]|~~~)(?:\|([^|]*)\|)?|--\s+([^-][^>]*?)\s+-->)\s*/;
    function parseRef(s) {
      s = s.trim(); if (!s) return null;
      var m = SHAPE_RE.exec(s);
      if (m) return { id: m[1], label: m[3], shape: m[2] };
      var id = /^([A-Za-z_][\w-]*)(?::::\w+)?$/.exec(s);
      return id ? { id: id[1] } : null;
    }
    src.split('\n').forEach(function (raw) {
      var line = raw.replace(/%%.*$/, '').trim().replace(/;$/, '');
      var sg = /^subgraph\s+([^\s\[]+)\s*(?:\[\s*"?([^\]"]*)"?\s*\])?\s*$/i.exec(line);
      if (sg) { sub.push({ title: cleanLabel(sg[2] || sg[1]), members: [] }); subgraphs.push(sub[sub.length - 1]); return; }
      if (/^end$/i.test(line)) { sub.pop(); return; }
      if (!line || /^(flowchart|graph|subgraph|end\b|classDef|class |style |linkStyle|click |direction)/i.test(line)) return;
      var parts = [], labels = [], rest = line, m;
      while ((m = LINK.exec(rest))) {
        parts.push(rest.slice(0, m.index)); labels.push(m[2] || m[3] || '');
        rest = rest.slice(m.index + m[0].length);
      }
      parts.push(rest);
      var groups = parts.map(function (part) { return part.split(/\s+&\s+/).map(parseRef).filter(Boolean).map(function (r) { return ensure(r.id, r.label, r.shape); }); });
      for (var i = 0; i < groups.length - 1; i++) groups[i].forEach(function (a) { groups[i + 1].forEach(function (b) { connect(p, a, b, labels[i]); }); });
    });
    if (!p.nodes.length) throw new Error('No nodes found in the Mermaid diagram.');
    var keep = p.nodes.every(function (n) { return n._pos; });
    p.nodes.forEach(function (n) { delete n._pos; delete n._label; delete n._labelled; });
    var out = finish(p, keep), colors = ['#3b82f6', '#eab308', '#22c55e', '#a855f7', '#14b8a6', '#ef4444'];
    if (groupHints.length) out.groups = groupHints.map(function (g) { return { id: C.newId('g'), title: String(g.title || 'Group'), text: g.text || '', x: +g.x || 0, y: +g.y || 0, w: +g.w || 300, h: +g.h || 200, color: g.color || '#64748b' }; });
    else subgraphs.forEach(function (sgp, i) {
      if (!sgp.members.length) return;
      var b = C.bounds(out, sgp.members);
      out.groups.push({ id: C.newId('g'), title: sgp.title || 'Group', text: '', x: b.x1 - 20, y: b.y1 - 44, w: b.x2 - b.x1 + 40, h: b.y2 - b.y1 + 64, color: colors[i % colors.length] });
    });
    return { pipeline: out, hinted: hinted };
  }
  /* ---------------- Airflow ---------------- */
  var OPS = [
    [/SQLExecuteQueryOperator|SnowflakeOperator|PostgresOperator|BigQueryInsertJobOperator|MySqlOperator|RedshiftSQLOperator|SQLOperator/, 'sql'],
    [/SQLColumnCheckOperator/, 'not_null'], [/SQLTableCheckOperator|SQLThresholdCheckOperator|SQLIntervalCheckOperator/, 'row_count'], [/SQLCheckOperator|SQLValueCheckOperator/, 'custom_check'],
    [/S3KeySensor|GCSObject\w*Sensor|FileSensor|ExternalTaskSensor|\w+Sensor/, 'sensor'], [/EmailOperator|Slack\w*Operator/, 'alert'],
    [/BranchPythonOperator|BranchSQLOperator|ShortCircuitOperator/, 'branch'], [/S3ToSnowflake|GCSToBigQuery|S3ToRedshift|CopyFromExternalStage/, 'warehouse_table'],
    [/ToS3Operator|ToGCSOperator/, 's3_parquet'], [/KafkaProduce|ProduceToTopic/, 'kafka_sink'], [/KafkaConsume|ConsumeFromTopic/, 'kafka_topic'],
    [/PythonOperator|PythonVirtualenvOperator|DockerOperator|KubernetesPodOperator/, 'python']
  ];
  function opType(op, args, id) {
    if (/BashOperator/.test(op)) return /\bdbt\b/.test(args) ? 'dbt_model' : guessType(id, 'python');
    for (var i = 0; i < OPS.length; i++) if (OPS[i][0].test(op)) return OPS[i][1];
    return guessType(id + ' ' + op, 'sql');
  }
  function stripComments(src) {
    // drop # comments outside strings (good enough for dependency lines)
    return src.split('\n').map(function (l) { var q = null; for (var i = 0; i < l.length; i++) { var c = l[i]; if (q) { if (c === '\\') i++; else if (c === q) q = null; } else if (c === '"' || c === "'") q = c; else if (c === '#') return l.slice(0, i); } return l; }).join('\n');
  }
  function fromAirflow(text) {
    var src = stripComments(String(text || ''));
    var p = C.createPipeline('imported_dag'), vars = {}, byTask = {}, decorated = {};
    var dagId = /dag_id\s*=\s*["']([^"']+)["']/.exec(src) || /DAG\(\s*["']([^"']+)["']/.exec(src) || /@dag\([\s\S]*?\)\s*def\s+(\w+)/.exec(src);
    if (dagId) p.settings.name = C.toIdent(dagId[1].replace(/_dag$/, ''), 'imported_dag');
    var sched = /schedule(?:_interval)?\s*=\s*["']([^"']+)["']/.exec(src);
    if (sched) p.settings.schedule = sched[1];
    function node(taskId, type, notes) {
      if (byTask[taskId]) return byTask[taskId];
      var n = C.addNode(p, type, 0, 0, C.toIdent(taskId, 'task'));
      if (notes) n.notes = notes;
      byTask[taskId] = n; return n;
    }
    // operators assigned to variables: t1 = SomeOperator(task_id="x", ...)
    var re = /(\w+)\s*=\s*([A-Z]\w*)\(\s*([\s\S]*?)\)\s*(?=\n\s*\w|\n\s*$|$)/g, m;
    var opRe = /(\w+)\s*=\s*([A-Z]\w*)\s*\(/g;
    while ((m = opRe.exec(src))) {
      var start = m.index + m[0].length, depth = 1, i = start;
      while (i < src.length && depth) { var ch = src[i]; if (ch === '(') depth++; else if (ch === ')') depth--; else if (ch === '"' || ch === "'") { var q = ch; i++; while (i < src.length && src[i] !== q) { if (src[i] === '\\') i++; i++; } } i++; }
      var args = src.slice(start, i - 1), tid = /task_id\s*=\s*["']([^"']+)["']/.exec(args);
      if (!tid) continue;
      var type = opType(m[2], args, tid[1]), n = node(tid[1], type, 'Imported from ' + m[2]);
      var sql = /sql\s*=\s*(?:"""([\s\S]*?)"""|'''([\s\S]*?)'''|"([^"\n]*)"|'([^'\n]*)')/.exec(args);
      if (sql && (type === 'sql' || type === 'custom_check')) n.config.sql = (sql[1] || sql[2] || sql[3] || sql[4]).trim();
      vars[m[1]] = n;
    }
    void re;
    // TaskFlow: @task(...) def fn(...)  and  var = fn()
    var dec = /@task(?:\.(\w+))?(?:\(((?:[^()]|\([^()]*\))*)\))?\s*\n\s*def\s+(\w+)/g;
    while ((m = dec.exec(src))) {
      var tid2 = m[2] && /task_id\s*=\s*["']([^"']+)["']/.exec(m[2]);
      var doc = /^[^\n]*\n\s*(?:"""([\s\S]*?)"""|"([^"\n]*)"|'([^'\n]*)')/.exec(src.slice(m.index + m[0].length));
      decorated[m[3]] = { taskId: tid2 ? tid2[1] : m[3], kind: m[1] || '', hint: m[3] + ' ' + (doc ? (doc[1] || doc[2] || doc[3] || '') : '') };
    }
    var call = /(\w+)\s*=\s*(\w+)\s*\(/g;
    while ((m = call.exec(src))) {
      var d = decorated[m[2]]; if (!d || vars[m[1]]) continue;
      var t = d.kind === 'branch' ? 'branch' : d.kind === 'bash' ? 'dbt_model' : d.kind === 'sensor' ? 'sensor' : guessType(d.hint.replace(/^(extract|run|export)_/, '') + ' ' + d.taskId, 'python');
      vars[m[1]] = node(d.taskId, t, 'Imported from @task' + (d.kind ? '.' + d.kind : '') + ' ' + m[2] + '()');
    }
    // Calls used inline in dependency chains: fn() >> other
    function resolve(tok) {
      tok = tok.trim();
      var lm = /^\[([\s\S]*)\]$/.exec(tok);
      if (lm) return lm[1].split(',').map(resolve).reduce(function (a, b) { return a.concat(b); }, []);
      var cm = /^(\w+)\s*\(.*\)$/.exec(tok);
      if (cm && decorated[cm[1]]) { var d2 = decorated[cm[1]]; return [node(d2.taskId, guessType(d2.taskId, 'python'))]; }
      if (vars[tok]) return [vars[tok]];
      if (/^\w+$/.test(tok) && !/^(dag|None|True|False)$/.test(tok)) { vars[tok] = node(tok, guessType(tok, 'sql'), 'Imported dependency (operator not found)'); return [vars[tok]]; }
      return [];
    }
    var edges = 0;
    function link(a, b) { a.forEach(function (x) { b.forEach(function (y) { if (connect(p, x, y)) edges++; }); }); }
    // join continuation lines inside brackets so lists can span lines
    var flat = src.replace(/\[[^\[\]]*\]/g, function (s) { return s.replace(/\s*\n\s*/g, ' '); });
    flat.split('\n').forEach(function (line) {
      var l = line.trim();
      if (/>>|<</.test(l) && !/^(def|class|if|for|while|return|print)\b/.test(l) && !/["']/.test(l.replace(/\(.*?\)/g, ''))) {
        var toks = l.split(/(>>|<<)/);
        for (var i = 1; i < toks.length; i += 2) {
          var a = resolve(toks[i - 1]), b = resolve(toks[i + 1]);
          if (toks[i] === '>>') link(a, b); else link(b, a);
        }
      }
      var sd;
      if ((sd = /^(\w+)\.set_downstream\((.+)\)\s*$/.exec(l))) link(resolve(sd[1]), resolve(sd[2]));
      if ((sd = /^(\w+)\.set_upstream\((.+)\)\s*$/.exec(l))) link(resolve(sd[2]), resolve(sd[1]));
      if ((sd = /^chain\((.+)\)\s*$/.exec(l))) {
        var items = [], depth = 0, cur = '';
        for (var k = 0; k < sd[1].length; k++) { var c = sd[1][k]; if (c === '[' || c === '(') depth++; if (c === ']' || c === ')') depth--; if (c === ',' && !depth) { items.push(cur); cur = ''; } else cur += c; }
        items.push(cur);
        var res = items.filter(function (s) { return s.trim(); }).map(resolve);
        for (var j = 1; j < res.length; j++) link(res[j - 1], res[j]);
      }
    });
    if (!p.nodes.length) throw new Error('No Airflow tasks found. Paste a DAG file with task_id=... operators or @task functions.');
    return { pipeline: finish(p, false), edges: edges };
  }
  function detect(text) {
    var t = String(text || '').trim();
    if (/^[\[{]/.test(t)) return 'json';
    if (/^(%%[^\n]*\n\s*)*(flowchart|graph)\b/i.test(t)) return 'mermaid';
    if (/airflow|>>|<<|task_id/.test(t)) return 'airflow';
    return null;
  }
  return { fromJSON: fromJSON, fromMermaid: fromMermaid, fromAirflow: fromAirflow, detect: detect, guessType: guessType };
});
