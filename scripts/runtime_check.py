"""End-to-end runtime check of generated Dagster + Prefect code on DuckDB.

Seeds the raw landing tables of a DuckDB-warehouse pipeline, replaces the steps
that talk to external systems (files, Postgres, Kafka, lakes) with no-ops, then
runs the real generated SQL, checks and orchestration.
usage: python runtime_check.py <dagster|prefect> <generated.py> <seed.sql>
"""
import importlib.util, os, sys, tempfile

kind, path, seed = sys.argv[1:4]
db = os.path.join(tempfile.mkdtemp(), "wh.duckdb")
os.environ["WAREHOUSE_URL"] = "duckdb:///" + db
import duckdb
con = duckdb.connect(db); con.execute(open(seed).read()); con.close()

spec = importlib.util.spec_from_file_location("gen", path)
mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
EXTERNAL = [n.strip() for n in os.environ.get("STUB_STEPS", "").split(",") if n.strip()]
for name in EXTERNAL:
    setattr(mod, "step_" + name, lambda *a, **k: {"stubbed": True})

if kind == "prefect":
    flow = [v for k, v in vars(mod).items() if k.endswith("_flow")][0]
    result = flow(run_date="2026-10-27")
    print("prefect result keys:", sorted(result))
else:
    import dagster as dg
    job = mod.defs.get_job_def([j for j in dir(mod) if j.endswith("_job")][0])
    res = job.execute_in_process(resources={"warehouse": mod.Warehouse(url=os.environ["WAREHOUSE_URL"])}, instance=dg.DagsterInstance.ephemeral())
    print("dagster success:", res.success)
    checks = [e for e in res.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"]
    print("checks:", [(e.event_specific_data.check_name, e.event_specific_data.passed) for e in checks])
    if not res.success: sys.exit(1)
wh = mod.Warehouse(url=os.environ["WAREHOUSE_URL"]) if kind == "dagster" else mod.Warehouse(os.environ["WAREHOUSE_URL"])
tables = wh.read_df("select table_schema || '.' || table_name as t from information_schema.tables order by 1")["t"]
for t in tables:
    print("  ", t, int(wh.read_df(f"select count(*) as n from {t}")["n"][0]), "rows")
if os.environ.get("SHOW"):
    print(wh.read_df(os.environ["SHOW"]).to_string())
