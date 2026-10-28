/* Dagster exporter (assets + blocking asset checks + schedule). MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./core.js'), require('./gen-common.js'), require('./gen-py.js'));
  else (root.PS = root.PS || {}).dagster = factory(root.PS.core, root.PS.gen, root.PS.genpy);
})(typeof self !== 'undefined' ? self : this, function (C, G, P) {
  'use strict';
  var py = C.pyStr;
  function generate(p) {
    var s = p.settings, sch = G.scheduleCron(p);
    var tasks = G.sortedTasks(p).filter(function (n) { return n && C.isTask(n); });
    // Map a task-level input to the asset keys it stands for (checks resolve to the asset they guard).
    function depKeys(n, seen) {
      seen = seen || {}; var out = [];
      P.taskInputs(p, n.id).forEach(function (inp) {
        var u = inp.node;
        if (u.type === 'branch') out.push(u.name + '__' + inp.port);
        else if (C.kindOf(u) === 'check' && checkTarget(u)) { if (!seen[u.id]) { seen[u.id] = 1; out = out.concat(depKeys(u, seen)); } }
        else if (u.type === 'alert' && u.config.when !== 'succeeds') { /* run-status sensors, not assets */ }
        else out.push(u.name);
      });
      // Warehouse tables (not tasks) referenced directly.
      C.upstreamEdges(p, n.id).forEach(function (e) { var u = C.nodeById(p, e.from); if (u && C.kindOf(u) === 'wh_source') out.push(u.name); });
      return out.filter(function (k, i) { return out.indexOf(k) === i; });
    }
    function checkTarget(n) {
      var prod = C.dataProducer(p, n.id);
      return prod && prod.id !== n.id ? prod : null;
    }
    var L = [P.prelude(p, 'Dagster definitions (software-defined assets).', {
      pip: ['dagster', 'dagster-webserver'], lines: ['import dagster as dg', '']
    }, ['Run locally: dagster dev -f ' + s.name + '_dagster.py'])];
    L.push('', 'class Warehouse(dg.ConfigurableResource):', '    """SQLAlchemy connection to the warehouse (configured from WAREHOUSE_URL)."""', '', '    url: str', '',
      C.indent(G.warehouseMethods(), 4), '', '',
      'def _run_date(tags) -> str:', '    """Logical date of a run: the scheduled day, or today for manual runs."""',
      '    scheduled = tags.get("dagster/scheduled_execution_time")', '    return scheduled[:10] if scheduled else dt.date.today().isoformat()', '', '');
    var tr = P.transforms(p); if (tr) L.push(tr);
    L.push('# ---------------------------------------------------------------- steps', '', P.steps(p), '',
      '# ---------------------------------------------------------------- assets', '');
    var SL = [], assetNames = [], keys = [], checks = [], sensors = [], specs = [];
    p.nodes.forEach(function (n) {
      if (C.kindOf(n) === 'wh_source') {
        specs.push('spec_' + n.name);
        L.push('spec_' + n.name + ' = dg.AssetSpec(' + py(n.name) + ', description=' + py('Existing table ' + C.relation(p, n)) + ', group_name="sources")');
      }
    });
    if (specs.length) L.push('', '');
    tasks.forEach(function (n) {
      var deps = depKeys(n), desc = (n.notes || NODESUMMARY(n));
      var depArg = deps.length ? ', deps=' + C.pyList(deps) : '';
      var sla = C.slaFor(p, n.id);
      if (sla) L.push('# SLA: should finish within ' + sla + ' minutes of the scheduled time (alert on it with a Dagster+ freshness policy or a run-duration sensor).');
      if (n.type === 'branch') {
        var k1 = n.name + '__yes', k2 = n.name + '__no'; keys.push(k1, k2); assetNames.push('asset_' + n.name);
        L.push('@dg.multi_asset(', '    name=' + py(n.name) + ',', '    group_name=PIPELINE,', '    specs=[',
          '        dg.AssetSpec(' + py(k1) + depArg + ', skippable=True, group_name=PIPELINE, description="Marker: branch took the yes path"),',
          '        dg.AssetSpec(' + py(k2) + depArg + ', skippable=True, group_name=PIPELINE, description="Marker: branch took the no path"),', '    ],', ')',
          'def asset_' + n.name + '(context: dg.AssetExecutionContext, warehouse: Warehouse):',
          '    ' + py('Branch on: ' + (n.config.condition || '')),
          '    taken = "yes" if step_' + n.name + '(warehouse, _run_date(context.run.tags), context.run_id) else "no"',
          '    context.log.info(f"' + n.name + ' -> {taken}")',
          '    yield dg.MaterializeResult(asset_key=f"' + n.name + '__{taken}", metadata={"taken": taken})', '', '');
        return;
      }
      if (C.kindOf(n) === 'check' && checkTarget(n)) {
        var tgt = checkTarget(n); checks.push('check_' + n.name);
        L.push('@dg.asset_check(asset=' + py(tgt.name) + ', name=' + py(n.name) + ', blocking=True, description=' + py(desc) + ')',
          'def check_' + n.name + '(warehouse: Warehouse) -> dg.AssetCheckResult:',
          '    results = step_' + n.name + '(warehouse, dt.date.today().isoformat(), "")',
          '    return dg.AssetCheckResult(passed=all(results.values()), metadata=results)', '', '');
        return;
      }
      if (n.type === 'alert' && n.config.when !== 'succeeds') {
        var when = n.config.when;
        var mk = function (deco, suffix) {
          sensors.push('sensor_' + n.name + suffix);
          SL.push(deco, 'def sensor_' + n.name + suffix + '(context):',
            '    run = context.dagster_run', '    step_' + n.name + '(None, _run_date(run.tags), run.run_id)', '', '');
        };
        if (when === 'fails' || when === 'finishes') mk('@dg.run_failure_sensor(name=' + py(n.name + '_on_failure') + ', monitored_jobs=[' + s.name + '_job], default_status=dg.DefaultSensorStatus.RUNNING)', '_on_failure');
        if (when === 'finishes') mk('@dg.run_status_sensor(run_status=dg.DagsterRunStatus.SUCCESS, name=' + py(n.name + '_on_success') + ', monitored_jobs=[' + s.name + '_job], default_status=dg.DefaultSensorStatus.RUNNING)', '_on_success');
        return;
      }
      keys.push(n.name); assetNames.push('asset_' + n.name);
      L.push('@dg.asset(name=' + py(n.name) + ', group_name=PIPELINE' + depArg + ', description=' + py(desc) + ')',
        'def asset_' + n.name + '(context: dg.AssetExecutionContext, warehouse: Warehouse) -> dg.MaterializeResult:',
        '    return dg.MaterializeResult(metadata=step_' + n.name + '(warehouse, _run_date(context.run.tags), context.run_id))', '', '');
    });
    // The job must exist before sensors reference it, so sensors are emitted after it.
    var out = L.join('\n').replace(/\s+$/, '') + '\n\n\n# ---------------------------------------------------------------- job, schedule, sensors\n\n';
    out += s.name + '_job = dg.define_asset_job(\n    ' + py(s.name + '_job') + ',\n    selection=dg.AssetSelection.assets(' + keys.map(py).join(', ') + '),\n' +
      ((+s.retries || 0) > 0 ? '    op_retry_policy=dg.RetryPolicy(max_retries=' + (+s.retries) + ', delay=' + Math.round((+s.retryDelayMin || 5) * 60) + '),\n' : '') + ')\n';
    var schedules = [];
    if (sch.cron) {
      schedules.push(s.name + '_schedule');
      out += '\n' + s.name + '_schedule = dg.ScheduleDefinition(\n    name=' + py(s.name + '_schedule') + ',\n    job=' + s.name + '_job,\n    cron_schedule=' + py(sch.cron) +
        ',\n    execution_timezone=' + py(s.timezone || 'UTC') + ',\n    default_status=dg.DefaultScheduleStatus.RUNNING,\n)\n';
    } else out += '\n# Schedule "' + (sch.raw || 'None') + '": no cron schedule; launch ' + s.name + '_job manually or from a sensor.\n';
    if (SL.length) out += '\n\n' + SL.join('\n').replace(/\s+$/, '') + '\n';
    out += '\n\ndefs = dg.Definitions(\n    assets=[' + specs.concat(assetNames).join(', ') + '],\n' +
      (checks.length ? '    asset_checks=[' + checks.join(', ') + '],\n' : '') +
      '    jobs=[' + s.name + '_job],\n' + (schedules.length ? '    schedules=[' + schedules.join(', ') + '],\n' : '') +
      (sensors.length ? '    sensors=[' + sensors.join(', ') + '],\n' : '') +
      '    resources={"warehouse": Warehouse(url=dg.EnvVar("WAREHOUSE_URL"))},\n)\n';
    return out;
  }
  function NODESUMMARY(n) {
    var T = (typeof require === 'function' && typeof module === 'object') ? require('./nodes.js') : (self.PS && self.PS.nodes);
    try { return T.summary(n) || T.get(n.type).label; } catch (e) { return n.type; }
  }
  return { generate: generate };
});
