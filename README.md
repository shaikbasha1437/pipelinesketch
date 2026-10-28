# PipelineSketch

**Sketch a data pipeline on a canvas, get real code out.** Drag sources, transforms, data-quality checks and sinks onto a grid, wire them up, and export a working **Airflow 2.x DAG**, **dbt project**, **Dagster** or **Prefect** flow, **Mermaid** diagram, PNG/SVG image or Markdown doc.

**▶ Live app: https://shaikbasha1437.github.io/pipelinesketch/**

Everything runs in your browser. There is no backend, no account and no tracking. Pipelines are stored in your browser's `localStorage`, and the app works offline once it has loaded.

## Features

**Canvas**
- Pan (drag the background or hold Space), zoom (wheel, pinch, `+` / `-` / `0`), fit to view (`F`) and a clickable minimap
- Drag nodes from the palette (or click one to add it after the selected node, already connected). Connect ports by dragging out → in; drag an input end to re-route or detach an edge.
- Multi-select by Shift-click or box select. Move, nudge with the arrow keys, snap to a 20px grid.
- One-click auto-layout (`Shift+L`): layered, left to right, with no overlaps
- Undo/redo for everything (`Ctrl+Z` / `Ctrl+Y`), copy/cut/paste/duplicate, and delete with an Undo toast
- Groups (`Ctrl+G`) and free-standing comment boxes: frames you can move and resize, with a title and notes
- Connections that would form a cycle are flagged right away

**Configuration and validation**
- A config form for each node type: tables, paths, SQL, keys, column lists, checks, retries and more. Known columns flow downstream. Transforms show a live compiled-SQL preview.
- Pipeline settings: name, owner, warehouse (Snowflake / BigQuery / Postgres / Redshift / DuckDB), schemas, retries, tags, start date and time zone
- A cron schedule editor with presets, a plain-English description ("At 06:00, Monday through Friday") and the next 5 run times in your time zone, DST included
- A live validation panel with cycles, missing config, unconnected inputs, invalid identifiers, duplicate names, missing columns for checks / joins / merge keys, union schema mismatches and broken `ref()`s. Click an issue to jump to the node and field.

**Node types**
| Category | Nodes |
|---|---|
| Sources | Postgres, MySQL, Snowflake and BigQuery tables; S3 and GCS files; Kafka topic; REST API |
| Transforms | SQL, Filter, Select / rename, Join, Aggregate, Dedupe, Union, Pivot, Python function, dbt model |
| Quality | Not null, Unique, Accepted values, Row count range, Freshness, Custom SQL check |
| Sinks | Warehouse table, S3 Parquet, Delta / Iceberg table, Kafka topic, Slack / email alert |
| Control | Schedule, Wait for file, Branch, SLA |

**Export.** Every format has a highlighted preview, Copy, Download, and a "Download everything (.zip)" option.
- **Airflow 2.x**: a TaskFlow DAG that uses `common.sql` / provider operators, sensors, branching, SLAs and callbacks. The header lists the provider packages and connections it needs.
- **dbt**: a full project (`dbt_project.yml`, `sources.yml`, staging/marts models, `schema.yml` tests, profile example), downloadable as a .zip
- **Dagster**: assets, asset checks, a job and a schedule
- **Prefect 3**: tasks and a flow with retries, served on its cron schedule with `.serve()`
- **Mermaid** `flowchart` (optionally with round-trip hints), **PNG / SVG** (dark or light), **Markdown** documentation and **JSON**

**Import**: PipelineSketch JSON (single pipeline or backup), Mermaid flowcharts (subgraphs become groups), and Airflow DAG files (`>>` / `<<` / `set_upstream` / `chain`, operators and `@task` functions).

**Templates**: six ready-made pipelines to start from:
1. S3 → Snowflake daily ELT with tests
2. Kafka → Delta medallion (bronze / silver / gold)
3. Postgres CDC → BigQuery with dedupe
4. API ingestion → dbt marts
5. Ad spend attribution with branch
6. Postgres orders → DuckDB revenue + Iceberg (fully local)

**Saving and sharing**
- Autosave to `localStorage`, with as many named pipelines as you like (open, rename, duplicate, delete)
- A share link: the whole pipeline is compressed into the URL hash (`#p=…`), so nothing is uploaded anywhere
- Backup / restore all pipelines as one JSON file

**UI**: dark and light IDE-style themes, keyboard shortcuts (`?`), and a layout that works on phones (palette drawer, bottom-sheet inspector). Every panel and dialog closes with X, Cancel, Esc, a backdrop click or the browser Back button. It is an installable PWA and works offline.

## Is the generated code real?

Yes. It is meant to be a starting point you can commit. CI-style checks are run against the output of every template:

| Check | Tooling | Result |
|---|---|---|
| Python syntax | `python -m py_compile` | all Airflow / Dagster / Prefect files compile |
| Airflow import | `airflow.models.DagBag` (Airflow 2.10) | 0 import errors, 6/6 DAGs |
| dbt | `dbt parse` (dbt-core 1.12 + dbt-duckdb) | 6/6 projects parse |
| Dagster | load `Definitions` (Dagster 1.13) | 6/6 load |
| Prefect | import flow module (Prefect 3) | 6/6 import |

You still have to fill in real connection IDs, credentials and table names. The generated header comments list what each file expects.

## Run locally

It's a static site with no build step:

```bash
git clone https://github.com/shaikbasha1437/pipelinesketch.git
cd pipelinesketch
python3 -m http.server 8000   # then open http://localhost:8000
```

### Tests

```bash
npm test                         # node --test: graph/topo/cycles, validation, cron, identifiers,
                                 # share-link round trip, importers, zip, generator snapshots
UPDATE_SNAPSHOTS=1 npm test      # accept intentional generator changes
node scripts/export-all.js out   # write every template's Airflow/dbt/Dagster/Prefect/Mermaid output to ./out
```

To check the exports with real tools:

```bash
python -m py_compile out/dags/*.py out/dagster/*.py out/prefect/*.py
AIRFLOW__CORE__LOAD_EXAMPLES=False python -c "from airflow.models import DagBag; b=DagBag('out/dags', include_examples=False); print(b.dag_ids, b.import_errors)"
cd out/dbt/daily_revenue_local && dbt parse --profiles-dir .   # needs dbt-duckdb
```

## Project layout

```
index.html, styles.css         UI shell and theme
js/cron.js                     cron parsing, English descriptions, next runs (time-zone aware)
js/nodes.js                    node type catalog: ports, config fields, defaults
js/core.js                     pipeline model, graph ops, validation, layout, share encoding
js/gen-*.js                    Airflow / dbt / Dagster / Prefect / Mermaid / SVG / Markdown generators
js/import.js                   JSON, Mermaid and Airflow importers
js/templates.js                starter pipelines
js/zip.js, js/highlight.js     tiny zip writer and syntax highlighter
js/modal.js, js/app.js         dialog stack (Esc / Back) and the editor
sw.js, manifest.webmanifest    offline support (network-first, versioned cache)
test/                          node:test suites and generator snapshots
```

## Privacy

There are no servers, cookies or analytics. Your pipelines stay in your browser unless you export them or share a link yourself. Share links keep the data in the URL fragment, which browsers do not send to servers.

## License

[MIT](LICENSE) © 2026 shaikbasha1437
