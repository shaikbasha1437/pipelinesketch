'use strict';
const test = require('node:test');
const { PS, assert } = require('./helpers');
const C = PS.core, Cron = PS.cron;
const from = Date.UTC(2026, 9, 28, 12, 0); // Wed 2026-10-28 12:00 UTC

test('cron descriptions in plain English', () => {
  assert.equal(C.scheduleInfo('0 6 * * *').text, 'At 06:00');
  assert.match(C.scheduleInfo('*/15 * * * *').text, /every 15 minutes/i);
  assert.match(C.scheduleInfo('0 6 * * 1-5').text, /Monday through Friday/);
  assert.equal(C.scheduleInfo('@daily').kind, 'cron');
  assert.equal(C.scheduleInfo('None').kind, 'none');
  assert.equal(C.scheduleInfo('@once').kind, 'once');
});

test('invalid schedules explain themselves', () => {
  assert.equal(C.scheduleInfo('61 * * * *').ok, false);
  assert.match(C.scheduleInfo('0 0 6 * * *').error, /5 fields/);
  assert.match(C.scheduleInfo('@sometimes').error, /Unknown preset/);
});

test('next runs respect time zones and DST', () => {
  const utc = Cron.nextRuns(Cron.parse('0 6 * * *'), { count: 2, timeZone: 'UTC', from });
  assert.deepEqual(utc.map((t) => new Date(t).toISOString()), ['2026-10-29T06:00:00.000Z', '2026-10-30T06:00:00.000Z']);
  // America/Chicago leaves DST on 2026-11-01: 06:00 local moves from 11:00Z to 12:00Z.
  const chi = Cron.nextRuns(Cron.parse('0 6 * * *'), { count: 5, timeZone: 'America/Chicago', from });
  assert.deepEqual(chi.map((t) => new Date(t).toISOString().slice(0, 16)),
    ['2026-10-29T11:00', '2026-10-30T11:00', '2026-10-31T11:00', '2026-11-01T12:00', '2026-11-02T12:00']);
  const wk = Cron.nextRuns(Cron.parse('0 6 * * 1-5'), { count: 3, timeZone: 'UTC', from: Date.UTC(2026, 9, 30, 7) });
  assert.deepEqual(wk.map((t) => new Date(t).getUTCDay()), [1, 2, 3]);
});

test('presets all parse', () => {
  for (const pr of C.PRESETS) assert.equal(C.scheduleInfo(pr.value).ok, true, pr.value);
});
