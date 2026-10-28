/* PipelineSketch templates gallery. MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./core.js'));
  else (root.PS = root.PS || {}).templates = factory(root.PS.core);
})(typeof self !== 'undefined' ? self : this, function (C) {
  'use strict';
  function builder(settings) {
    var p = C.createPipeline(settings.name);
    Object.assign(p.settings, settings);
    var byName = {}, groupsSpec = [];
    var api = {
      add: function (type, name, config, columns) {
        var n = C.addNode(p, type, 0, 0, name);
        Object.assign(n.config, config || {});
        if (columns) n.columns = C.asList(columns);
        byName[name] = n; return api;
      },
      link: function (a, b, fromPort, toPort) { if (!C.addEdge(p, byName[a].id, fromPort || 'out', byName[b].id, toPort || 'in')) throw new Error('bad edge ' + a + '>' + b); return api; },
      chain: function () { for (var i = 1; i < arguments.length; i++) api.link(arguments[i - 1], arguments[i]); return api; },
      group: function (title, names, color, text) { groupsSpec.push({ title: title, names: names, color: color, text: text }); return api; },
      done: function () {
        var pos = C.layout(p);
        p.nodes.forEach(function (n) { if (pos[n.id]) { n.x = pos[n.id].x; n.y = pos[n.id].y; } });
        groupsSpec.forEach(function (g) {
          var b = C.bounds(p, g.names.map(function (nm) { return byName[nm].id; }));
          p.groups.push({ id: C.newId('g'), title: g.title, text: g.text || '', x: b.x1 - 20, y: b.y1 - 44, w: b.x2 - b.x1 + 40, h: b.y2 - b.y1 + 64, color: g.color || '#64748b' });
        });
        return p;
      }
    };
    return api;
  }
  var LIST = [
    { id: 's3-snowflake-elt', title: 'S3 \u2192 Snowflake daily ELT with tests', description: 'Wait for the day\u2019s files, load Parquet from S3, clean and dedupe, test, then merge into a fact table. Slack on success, email on failure, 2-hour SLA.', tags: ['S3', 'Snowflake', 'merge', 'tests'],
      build: function () {
        return builder({ name: 's3_to_snowflake_orders', description: 'Daily orders ELT from S3 into Snowflake', owner: 'data-eng', schedule: '0 6 * * *', warehouse: 'snowflake', tags: ['orders', 'elt', 'snowflake'], failureEmail: 'data-oncall@example.com' })
          .add('sensor', 'wait_for_orders', { storage: 's3', bucket: 'acme-landing', path: 'orders/{ds}/_SUCCESS', poke_minutes: 5, timeout_hours: 6 })
          .add('s3_files', 'orders_raw', { bucket: 'acme-landing', prefix: 'orders/{ds}/', format: 'parquet' }, 'order_id, customer_id, status, amount, updated_at')
          .add('select', 'stg_orders', { columns: 'order_id\ncustomer_id\nlower(status) as status\ncast(amount as number(12, 2)) as amount\nupdated_at' })
          .add('dedupe', 'orders_dedup', { keys: ['order_id'], order_by: 'updated_at desc' })
          .add('not_null', 'orders_not_null', { columns: ['order_id', 'customer_id'] })
          .add('unique', 'orders_unique', { columns: ['order_id'] })
          .add('accepted_values', 'status_values', { column: 'status', values: ['placed', 'shipped', 'delivered', 'returned'] })
          .add('warehouse_table', 'fct_orders', { table: 'analytics.fct_orders', mode: 'merge', keys: ['order_id'] })
          .add('alert', 'notify_success', { channel: 'slack', when: 'succeeds', message: ':white_check_mark: fct_orders refreshed for {{ ds }}' })
          .add('alert', 'notify_failure', { channel: 'email', when: 'fails', to: ['data-oncall@example.com'], message: 'Loading fct_orders failed for {{ ds }} (run {{ run_id }}).' })
          .add('sla', 'sla_2h', { minutes: 120 })
          .chain('wait_for_orders', 'orders_raw', 'stg_orders', 'orders_dedup', 'orders_not_null', 'orders_unique', 'status_values', 'fct_orders', 'notify_success')
          .link('fct_orders', 'notify_failure').link('fct_orders', 'sla_2h')
          .group('Staging', ['stg_orders', 'orders_dedup'], '#3b82f6').group('Data tests', ['orders_not_null', 'orders_unique', 'status_values'], '#eab308')
          .done();
      } },
    { id: 'kafka-delta-medallion', title: 'Kafka \u2192 Delta medallion (bronze / silver / gold)', description: 'Every 15 minutes: land raw events in bronze, clean + dedupe into silver with a merge, aggregate a gold table and publish updates back to Kafka.', tags: ['Kafka', 'Delta Lake', 'medallion', 'micro-batch'],
      build: function () {
        return builder({ name: 'clickstream_medallion', description: 'Micro-batch clickstream into a bronze/silver/gold Delta lake', owner: 'platform', schedule: '*/15 * * * *', warehouse: 'duckdb', tags: ['streaming', 'delta', 'medallion'] })
          .add('kafka_topic', 'clickstream', { topic: 'web.clickstream', max_messages: 50000 }, 'event_id, user_id, event_type, page, event_ts')
          .add('lake_table', 'bronze_events', { format: 'delta', location: 's3://acme-lake/bronze/clickstream', mode: 'append' })
          .add('filter', 'silver_valid', { condition: 'user_id is not null and event_type is not null' })
          .add('dedupe', 'silver_events', { keys: ['event_id'], order_by: 'event_ts desc' })
          .add('not_null', 'silver_checks', { columns: ['event_id', 'user_id'] })
          .add('lake_table', 'silver_delta', { format: 'delta', location: 's3://acme-lake/silver/clickstream', mode: 'merge', keys: ['event_id'] })
          .add('sql', 'gold_activity', { sql: 'select\n    cast(event_ts as date) as event_date,\n    event_type,\n    count(*) as events,\n    count(distinct user_id) as users\nfrom {{ input }}\ngroup by 1, 2' }, 'event_date, event_type, events, users')
          .add('row_count', 'gold_not_empty', { min: 1 })
          .add('lake_table', 'gold_delta', { format: 'delta', location: 's3://acme-lake/gold/daily_activity', mode: 'overwrite', partition_by: ['event_date'] })
          .add('kafka_sink', 'publish_activity', { topic: 'analytics.daily_activity', key_column: 'event_type' })
          .chain('clickstream', 'bronze_events')
          .chain('clickstream', 'silver_valid', 'silver_events', 'silver_checks', 'silver_delta')
          .chain('silver_checks', 'gold_activity', 'gold_not_empty', 'gold_delta')
          .link('gold_not_empty', 'publish_activity')
          .group('Bronze', ['bronze_events'], '#b45309').group('Silver', ['silver_valid', 'silver_events', 'silver_checks', 'silver_delta'], '#94a3b8').group('Gold', ['gold_activity', 'gold_not_empty', 'gold_delta', 'publish_activity'], '#ca8a04')
          .done();
      } },
    { id: 'postgres-cdc-bigquery', title: 'Postgres CDC \u2192 BigQuery with dedupe', description: 'Hourly incremental pull of changed customers, keep the latest version per key, enrich with a BigQuery reference table and merge into a dimension.', tags: ['Postgres', 'BigQuery', 'CDC', 'merge'],
      build: function () {
        return builder({ name: 'customers_cdc_to_bigquery', description: 'Incremental customer changes from Postgres into a BigQuery dimension', owner: 'data-eng', schedule: '@hourly', warehouse: 'bigquery', rawSchema: 'raw', stagingSchema: 'staging', tags: ['cdc', 'bigquery'] })
          .add('pg_table', 'customers_cdc', { schema: 'public', table: 'customers', incremental_column: 'updated_at' }, 'customer_id, email, full_name, country_code, updated_at')
          .add('dedupe', 'customers_latest', { keys: ['customer_id'], order_by: 'updated_at desc' })
          .add('bigquery_table', 'countries', { project: 'acme-analytics', dataset: 'reference', table: 'countries' }, 'country_code, country_name, region')
          .add('join', 'customers_enriched', { how: 'left', on: 'country_code', select: '*' })
          .add('not_null', 'customer_id_not_null', { columns: ['customer_id'] })
          .add('unique', 'customer_id_unique', { columns: ['customer_id'] })
          .add('freshness', 'customers_fresh', { column: 'updated_at', max_age_hours: 3 })
          .add('warehouse_table', 'dim_customers', { table: 'analytics.dim_customers', mode: 'merge', keys: ['customer_id'] })
          .add('alert', 'page_on_failure', { channel: 'email', when: 'fails', to: ['data-oncall@example.com'], message: 'customers CDC failed for {{ ds }}' })
          .chain('customers_cdc', 'customers_latest').link('customers_latest', 'customers_enriched', 'out', 'left').link('countries', 'customers_enriched', 'out', 'right')
          .chain('customers_enriched', 'customer_id_not_null', 'customer_id_unique', 'customers_fresh', 'dim_customers', 'page_on_failure')
          .done();
      } },
    { id: 'api-dbt-marts', title: 'API ingestion \u2192 dbt marts', description: 'Page through a REST API into the raw schema, then build dbt staging models and a customer revenue mart on Snowflake, with tests and a Slack ping.', tags: ['REST API', 'dbt', 'Snowflake'],
      build: function () {
        return builder({ name: 'shop_api_to_marts', description: 'Orders from the shop API modelled with dbt', owner: 'analytics-eng', schedule: '0 7 * * *', warehouse: 'snowflake', tags: ['api', 'dbt'] })
          .add('rest_api', 'api_orders', { url: 'https://api.example.com/v1/orders?updated_since={ds}', records_path: 'data', next_field: 'links.next', token_env: 'SHOP_API_TOKEN', max_pages: 100 }, 'id, customer_id, total, currency, created_at')
          .add('snowflake_table', 'crm_customers', { database: 'ANALYTICS', schema: 'CRM', table: 'CUSTOMERS' }, 'customer_id, name, segment')
          .add('dbt_model', 'stg_api_orders', { sql: 'select\n    id as order_id,\n    customer_id,\n    total as amount,\n    currency,\n    created_at\nfrom {{ input }}', materialized: 'view' }, 'order_id, customer_id, amount, currency, created_at')
          .add('dbt_model', 'stg_customers', { sql: 'select\n    customer_id,\n    name as customer_name,\n    segment\nfrom {{ input }}', materialized: 'view' }, 'customer_id, customer_name, segment')
          .add('dbt_model', 'fct_customer_revenue', { sql: 'select\n    c.customer_id,\n    c.segment,\n    count(o.order_id) as orders,\n    coalesce(sum(o.amount), 0) as revenue\nfrom {{ input }} as c\nleft join {{ input_2 }} as o\n    on o.customer_id = c.customer_id\ngroup by 1, 2', materialized: 'table' }, 'customer_id, segment, orders, revenue')
          .add('unique', 'revenue_unique', { columns: ['customer_id'] })
          .add('custom_check', 'no_negative_revenue', { sql: 'select *\nfrom {{ input }}\nwhere revenue < 0' })
          .add('alert', 'notify_marts', { channel: 'slack', when: 'finishes', message: 'Customer revenue mart run finished for {{ ds }}' })
          .chain('crm_customers', 'stg_customers', 'fct_customer_revenue').chain('api_orders', 'stg_api_orders', 'fct_customer_revenue')
          .chain('fct_customer_revenue', 'revenue_unique', 'no_negative_revenue', 'notify_marts')
          .group('dbt models', ['stg_api_orders', 'stg_customers', 'fct_customer_revenue'], '#f97316')
          .done();
      } },
    { id: 'ads-attribution-branch', title: 'Ad spend attribution with branch', description: 'Union ad spend from GCS and S3, join MySQL customers, pivot by channel, then on weekdays export Parquet and on weekends run a pandas scoring step.', tags: ['MySQL', 'GCS', 'union', 'pivot', 'branch', 'Python'],
      build: function () {
        return builder({ name: 'ad_spend_attribution', description: 'Channel spend per customer from two ad platforms', owner: 'marketing-data', schedule: '0 5 * * *', warehouse: 'postgres', tags: ['marketing'] })
          .add('schedule', 'every_morning', { cron: '30 5 * * *' })
          .add('sensor', 'wait_for_google_ads', { storage: 'gcs', bucket: 'acme-ads', path: 'google/{ds}/' })
          .add('gcs_files', 'ads_google', { bucket: 'acme-ads', prefix: 'google/{ds}/', format: 'csv' }, 'customer_id, channel, spend')
          .add('s3_files', 'ads_meta', { bucket: 'acme-ads-meta', prefix: 'exports/{ds}/', format: 'json' }, 'customer_id, channel, spend')
          .add('mysql_table', 'customers', { schema: 'shop', table: 'customers' }, 'customer_id, email, country')
          .add('union', 'ad_spend', { distinct: false })
          .add('join', 'spend_with_customers', { how: 'inner', on: 'customer_id', select: 'l.customer_id\nr.country\nl.channel\nl.spend' })
          .add('pivot', 'spend_by_channel', { index: ['customer_id', 'country'], pivot_column: 'channel', values: ['search', 'social', 'video'], value_column: 'spend', agg: 'sum' })
          .add('row_count', 'has_rows', { min: 1, max: 5000000 })
          .add('custom_check', 'no_negative_spend', { sql: 'select *\nfrom {{ input }}\nwhere search < 0 or social < 0 or video < 0' })
          .add('branch', 'is_weekday', { condition: 'run_date.weekday() < 5' })
          .add('s3_parquet', 'export_parquet', { bucket: 'acme-exports', prefix: 'attribution/dt={ds}/', partition_by: ['country'] })
          .add('python', 'weekend_scoring', { code: 'def transform(df, **inputs):\n    """Score customers by share of social spend."""\n    total = df[["search", "social", "video"]].fillna(0).sum(axis=1)\n    df["social_share"] = (df["social"].fillna(0) / total.where(total > 0)).fillna(0)\n    return df', requirements: [] })
          .add('warehouse_table', 'weekend_scores', { table: 'marketing.weekend_scores', mode: 'overwrite' })
          .add('sla', 'sla_90m', { minutes: 90 })
          .link('every_morning', 'wait_for_google_ads').chain('wait_for_google_ads', 'ads_google', 'ad_spend').link('ads_meta', 'ad_spend')
          .link('ad_spend', 'spend_with_customers', 'out', 'left').link('customers', 'spend_with_customers', 'out', 'right')
          .chain('spend_with_customers', 'spend_by_channel', 'has_rows', 'no_negative_spend', 'is_weekday')
          .link('is_weekday', 'export_parquet', 'yes').link('is_weekday', 'weekend_scoring', 'no').chain('weekend_scoring', 'weekend_scores')
          .link('export_parquet', 'sla_90m').link('weekend_scores', 'sla_90m')
          .done();
      } },
    { id: 'postgres-duckdb-iceberg', title: 'Postgres orders \u2192 DuckDB revenue + Iceberg', description: 'A small local-first pipeline: wait for an export marker file, copy orders into DuckDB, keep paid ones, roll up daily revenue, append to a table and an Iceberg table.', tags: ['Postgres', 'DuckDB', 'Iceberg', 'aggregate'],
      build: function () {
        return builder({ name: 'daily_revenue_local', description: 'Daily revenue rollup in DuckDB', owner: 'analytics', schedule: '@daily', warehouse: 'duckdb', tags: ['duckdb', 'iceberg'] })
          .add('sensor', 'wait_for_export', { storage: 'local', path: '/data/exports/{ds}/orders.done', poke_minutes: 2, timeout_hours: 3 })
          .add('pg_table', 'orders', { schema: 'shop', table: 'orders' }, 'order_id, customer_id, amount, status, created_at')
          .add('filter', 'paid_orders', { condition: "status = 'paid'" })
          .add('select', 'orders_by_day', { columns: 'order_id\namount\ncast(created_at as date) as order_date' })
          .add('aggregate', 'daily_revenue', { group_by: ['order_date'], aggregations: 'count(*) as orders\nsum(amount) as revenue' })
          .add('row_count', 'revenue_has_rows', { min: 1 })
          .add('warehouse_table', 'revenue_table', { table: 'analytics.daily_revenue', mode: 'append' })
          .add('lake_table', 'revenue_iceberg', { format: 'iceberg', location: 'analytics.daily_revenue', catalog: 'default', mode: 'append' })
          .chain('wait_for_export', 'orders', 'paid_orders', 'orders_by_day', 'daily_revenue', 'revenue_has_rows', 'revenue_table')
          .link('revenue_has_rows', 'revenue_iceberg')
          .done();
      } }
  ];
  function get(id) { return LIST.filter(function (t) { return t.id === id; })[0] || null; }
  return { LIST: LIST, get: get, builder: builder };
});
