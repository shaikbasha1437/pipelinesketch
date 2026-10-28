/* Prefect 3 exporter (tasks + flow with futures, hooks and a cron deployment). MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./core.js'), require('./gen-common.js'), require('./gen-py.js'));
  else (root.PS = root.PS || {}).prefect = factory(root.PS.core, root.PS.gen, root.PS.genpy);
})(typeof self !== 'undefined' ? self : this, function (C, G, P) {
  'use strict';
  var py = C.pyStr;
  function generate(p) {
    var s = p.settings, sch = G.scheduleCron(p), flowFn = s.name + '_flow';
    var tasks = G.sortedTasks(p).filter(function (n) { return n && C.isTask(n); });
    var hookAlerts = tasks.filter(function (n) { return n.type === 'alert' && n.config.when !== 'succeeds'; });
    var inFlow = tasks.filter(function (n) { return hookAlerts.indexOf(n) === -1; });
    var retries = +s.retries || 0, delay = Math.round((+s.retryDelayMin || 5) * 60);
    var L = [P.prelude(p, 'Prefect 3 flow.', {
      pip: ['prefect>=3'], lines: ['from prefect import flow, task', 'from prefect.cache_policies import NO_CACHE', 'from prefect.futures import wait', 'from prefect.runtime import flow_run', 'from prefect.schedules import Cron', '']
    }, ['Run once: python ' + s.name + '_prefect.py --once [YYYY-MM-DD]', 'Serve on schedule: python ' + s.name + '_prefect.py'])];
    L.push('', 'class Warehouse:', '    """SQLAlchemy connection to the warehouse."""', '', '    def __init__(self, url: str):', '        self.url = url', '',
      C.indent(G.warehouseMethods(), 4), '', '', 'def warehouse() -> Warehouse:', '    return Warehouse(os.environ["WAREHOUSE_URL"])', '', '');
    var tr = P.transforms(p); if (tr) L.push(tr);
    L.push('# ---------------------------------------------------------------- steps', '', P.steps(p), '', '# ---------------------------------------------------------------- tasks', '');
    inFlow.forEach(function (n) {
      var k = C.kindOf(n), isCheck = k === 'check', r = (isCheck || n.type === 'branch') ? 0 : retries;
      var sla = C.slaFor(p, n.id);
      L.push('@task(name=' + py(n.name) + (r ? ', retries=' + r + ', retry_delay_seconds=' + delay : '') + ', cache_policy=NO_CACHE)',
        'def task_' + n.name + '(run_date: str, run_id: str)' + (n.type === 'branch' ? ' -> bool' : ' -> dict') + ':');
      if (sla) L.splice(L.length - 2, 0, '# SLA: expected to finish within ' + sla + ' minutes of the scheduled start.');
      if (isCheck) L.push('    results = step_' + n.name + '(warehouse(), run_date, run_id)', '    failed = [name for name, ok in results.items() if not ok]',
        '    if failed:', '        raise ValueError(f"Data checks failed: {failed}")', '    return results', '', '');
      else L.push('    return step_' + n.name + '(' + (n.type === 'alert' ? 'None' : 'warehouse()') + ', run_date, run_id)', '', '');
    });
    if (hookAlerts.length) {
      L.push('# ---------------------------------------------------------------- flow hooks', '',
        'def _hook_args(flow_run_obj) -> tuple[str, str]:', '    run_date = (flow_run_obj.parameters or {}).get("run_date") or dt.date.today().isoformat()', '    return run_date, str(flow_run_obj.id)', '', '');
      hookAlerts.forEach(function (n) {
        L.push('def hook_' + n.name + '(flow_obj, flow_run_obj, state) -> None:', '    ' + py('Alert "' + n.name + '" when the flow ' + (n.config.when === 'fails' ? 'fails' : 'finishes') + '.'),
          '    step_' + n.name + '(None, *_hook_args(flow_run_obj))', '', '');
      });
    }
    // Gate expression: a node runs if any of its task inputs ran; branch inputs only on the taken port.
    var gate = {};
    function gateOf(n) {
      if (gate[n.id] !== undefined) return gate[n.id];
      var ins = P.taskInputs(p, n.id).filter(function (i) { return hookAlerts.indexOf(i.node) === -1; });
      var conds = [], free = !ins.length;
      ins.forEach(function (i) {
        var g = i.node.type === 'branch' ? andC(gateOf(i.node), 'branch_' + i.node.name + ' == ' + py(i.port)) : gateOf(i.node);
        if (!g) free = true; else conds.push(g);
      });
      gate[n.id] = free ? '' : conds.length === 1 ? conds[0] : '(' + conds.join(') or (') + ')';
      return gate[n.id];
    }
    function andC(a, b) { return a ? '(' + a + ') and ' + b : b; }
    var hookFails = hookAlerts.map(function (n) { return 'hook_' + n.name; });
    var hookDone = hookAlerts.filter(function (n) { return n.config.when === 'finishes'; }).map(function (n) { return 'hook_' + n.name; });
    L.push('# ---------------------------------------------------------------- flow', '',
      '@flow(name=' + py(s.name) + (s.description ? ', description=' + py(s.description) : '') + ', log_prints=True' +
      (hookFails.length ? ', on_failure=[' + hookFails.join(', ') + ']' : '') + (hookDone.length ? ', on_completion=[' + hookDone.join(', ') + ']' : '') + ')',
      'def ' + flowFn + '(run_date: str | None = None) -> dict:', '    run_date = run_date or dt.date.today().isoformat()', '    run_id = str(flow_run.id or "local")');
    var gated = inFlow.filter(function (n) { return gateOf(n); });
    var branches = inFlow.filter(function (n) { return n.type === 'branch'; });
    branches.forEach(function (b) { L.push('    branch_' + b.name + ' = None'); });
    gated.forEach(function (n) { if (n.type !== 'branch') L.push('    f_' + n.name + ' = None'); });
    var futs = [];
    inFlow.forEach(function (n) {
      var g = gateOf(n), ind = g ? '        ' : '    ';
      var ups = P.taskInputs(p, n.id).filter(function (i) { return i.node.type !== 'branch' && hookAlerts.indexOf(i.node) === -1; }).map(function (i) { return 'f_' + i.node.name; });
      ups = ups.filter(function (u, i) { return ups.indexOf(u) === i; });
      var anyGated = ups.some(function (u) { return gated.some(function (x) { return 'f_' + x.name === u; }); });
      var wf = !ups.length ? '' : ', wait_for=' + (anyGated ? '[f for f in (' + ups.join(', ') + (ups.length === 1 ? ',' : '') + ') if f is not None]' : '[' + ups.join(', ') + ']');
      if (g) L.push('    if ' + g + ':');
      if (n.type === 'branch') L.push(ind + 'branch_' + n.name + ' = "yes" if task_' + n.name + '(run_date, run_id' + wf + ') else "no"', ind + 'print(f"' + n.name + ' -> {branch_' + n.name + '}")');
      else { L.push(ind + 'f_' + n.name + ' = task_' + n.name + '.submit(run_date, run_id' + wf + ')'); futs.push(n.name); }
    });
    L.push('', '    futures = {name: f for name, f in {' + futs.map(function (nm) { return py(nm) + ': f_' + nm; }).join(', ') + '}.items() if f is not None}',
      '    wait(list(futures.values()))', '    # .result() re-raises task failures so the flow run fails (and failure hooks fire).',
      '    return {name: f.result() for name, f in futures.items()}', '', '');
    L.push('if __name__ == "__main__":', '    import sys', '', '    if "--once" in sys.argv:', '        args = [a for a in sys.argv[1:] if a != "--once"]', '        ' + flowFn + '(run_date=args[0] if args else None)', '    else:');
    if (sch.cron) L.push('        ' + flowFn + '.serve(name=' + py(s.name) + ', schedule=Cron(' + py(sch.cron) + ', timezone=' + py(s.timezone || 'UTC') + '))');
    else L.push('        # Schedule "' + (sch.raw || 'None') + '" has no cron equivalent: serve without a schedule and trigger runs on demand.', '        ' + flowFn + '.serve(name=' + py(s.name) + ')');
    return L.join('\n') + '\n';
  }
  return { generate: generate };
});
