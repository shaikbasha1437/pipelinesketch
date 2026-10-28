/* Cron parser, describer and scheduler (from CronSpeak, MIT). No dependencies. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CronCore = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  var MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
  var DAYS = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];
  var MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
  var DAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

  var FIELDS = {
    second: { key: 'second', label: 'Second', min: 0, max: 59 },
    minute: { key: 'minute', label: 'Minute', min: 0, max: 59 },
    hour: { key: 'hour', label: 'Hour', min: 0, max: 23 },
    dom: { key: 'dom', label: 'Day of month', min: 1, max: 31 },
    month: { key: 'month', label: 'Month', min: 1, max: 12, names: MONTHS, nameBase: 1 },
    dow: { key: 'dow', label: 'Day of week', min: 0, max: 7, names: DAYS, nameBase: 0 }
  };

  var MACROS = {
    '@yearly': '0 0 1 1 *', '@annually': '0 0 1 1 *', '@monthly': '0 0 1 * *',
    '@weekly': '0 0 * * 0', '@daily': '0 0 * * *', '@midnight': '0 0 * * *', '@hourly': '0 * * * *'
  };

  function allowedText(spec) {
    var t = spec.min + '–' + spec.max;
    if (spec.key === 'month') t += ' or JAN–DEC';
    if (spec.key === 'dow') t += ' (0 and 7 = Sunday) or SUN–SAT';
    return t;
  }

  function parseNumber(tok, spec) {
    if (/^\d+$/.test(tok)) {
      var n = parseInt(tok, 10);
      if (n < spec.min || n > spec.max) throw new Error('"' + tok + '" is out of range — allowed ' + allowedText(spec));
      return n;
    }
    if (spec.names) {
      var idx = spec.names.indexOf(tok.toUpperCase());
      if (idx !== -1) return idx + spec.nameBase;
      throw new Error('"' + tok + '" isn\'t a valid ' + spec.label.toLowerCase() + ' — use ' + allowedText(spec));
    }
    if (/^[LW#]|[LW#]$|#/.test(tok)) throw new Error('"' + tok + '" uses Quartz-only syntax (L, W, #), which standard cron doesn\'t support');
    throw new Error('"' + tok + '" isn\'t a number — allowed ' + allowedText(spec));
  }

  // Parse one field into { raw, star, parts, values }
  function parseField(raw, spec) {
    if (raw === '') throw new Error('Field is empty');
    var star = raw.charAt(0) === '*' || raw.charAt(0) === '?';
    if (raw.charAt(0) === '?' && spec.key !== 'dom' && spec.key !== 'dow') throw new Error('"?" is only allowed in day-of-month or day-of-week');
    var set = {};
    var parts = raw.split(',').map(function (piece) {
      if (piece === '') throw new Error('Empty list item — check for stray commas');
      var slash = piece.split('/');
      if (slash.length > 2) throw new Error('"' + piece + '" has more than one "/"');
      var base = slash[0], step = null;
      if (slash.length === 2) {
        if (!/^\d+$/.test(slash[1])) throw new Error('Step "' + slash[1] + '" must be a positive whole number');
        step = parseInt(slash[1], 10);
        if (step === 0) throw new Error('Step can\'t be 0');
        if (step > spec.max - spec.min + 1 && spec.key !== 'dow') throw new Error('Step ' + step + ' is larger than the field\'s range (' + allowedText(spec) + ')');
      }
      var part;
      if (base === '*' || base === '?') {
        part = { type: step ? 'step' : 'all', start: spec.min, end: spec.key === 'dow' ? 6 : spec.max, step: step || 1, fromStar: true };
      } else if (base.indexOf('-') !== -1) {
        var ab = base.split('-');
        if (ab.length !== 2 || ab[0] === '' || ab[1] === '') throw new Error('"' + base + '" isn\'t a valid range — use start-end, e.g. 1-5');
        var a = parseNumber(ab[0], spec), b = parseNumber(ab[1], spec);
        if (a > b) throw new Error('Range "' + base + '" goes backwards — start must be ≤ end');
        part = { type: step ? 'step' : 'range', start: a, end: b, step: step || 1 };
      } else {
        var v = parseNumber(base, spec);
        if (step) part = { type: 'step', start: v, end: spec.key === 'dow' ? 6 : spec.max, step: step, openEnd: true };
        else part = { type: 'value', start: v, end: v, step: 1 };
      }
      for (var i = part.start; i <= part.end; i += part.step) set[spec.key === 'dow' && i === 7 ? 0 : i] = true;
      return part;
    });
    var values = Object.keys(set).map(Number).sort(function (x, y) { return x - y; });
    return { raw: raw, star: star, parts: parts, values: values, spec: spec };
  }

  function parse(input) {
    var expr = String(input == null ? '' : input).trim().replace(/\s+/g, ' ');
    var result = { input: expr, ok: false, errors: [], fields: {}, order: [], hasSeconds: false, macro: null, expanded: expr };
    if (!expr) { result.errors.push({ field: null, message: 'Enter a cron expression, e.g. */15 9-17 * * 1-5' }); return result; }
    if (expr.charAt(0) === '@') {
      var m = expr.toLowerCase();
      if (m === '@reboot') { result.errors.push({ field: null, message: '@reboot runs once at startup — it has no repeating schedule to explain' }); return result; }
      if (!MACROS[m]) { result.errors.push({ field: null, message: 'Unknown macro "' + expr + '" — use @yearly, @annually, @monthly, @weekly, @daily, @midnight or @hourly' }); return result; }
      result.macro = m; expr = MACROS[m]; result.expanded = expr;
    }
    var toks = expr.split(' ');
    var keys;
    if (toks.length === 5) keys = ['minute', 'hour', 'dom', 'month', 'dow'];
    else if (toks.length === 6) { keys = ['second', 'minute', 'hour', 'dom', 'month', 'dow']; result.hasSeconds = true; }
    else {
      result.errors.push({ field: null, message: 'Expected 5 fields (minute hour day-of-month month day-of-week) or 6 with seconds first — got ' + toks.length });
      return result;
    }
    result.order = keys;
    keys.forEach(function (k, i) {
      try { result.fields[k] = parseField(toks[i], FIELDS[k]); }
      catch (e) { result.fields[k] = { raw: toks[i], error: e.message, spec: FIELDS[k] }; result.errors.push({ field: k, message: FIELDS[k].label + ': ' + e.message }); }
    });
    if (!result.hasSeconds) result.fields.second = { raw: '0', star: false, parts: [{ type: 'value', start: 0, end: 0, step: 1 }], values: [0], spec: FIELDS.second, implicit: true };
    result.ok = result.errors.length === 0;
    return result;
  }

  /* ---------------- description ---------------- */

  function joinList(arr, conj) {
    conj = conj || 'and';
    if (arr.length <= 1) return arr.join('');
    return arr.slice(0, -1).join(', ') + ' ' + conj + ' ' + arr[arr.length - 1];
  }
  function pad(n) { return n < 10 ? '0' + n : '' + n; }
  function ordinal(n) {
    var s = ['th', 'st', 'nd', 'rd'], v = n % 100;
    return n + (s[(v - 20) % 10] || s[v] || s[0]);
  }
  function clock(h, m, s, opts) {
    var sec = s != null && s !== 0 ? ':' + pad(s) : '';
    if (opts && opts.h24) return pad(h) + ':' + pad(m) + sec;
    var ap = h < 12 ? 'AM' : 'PM', h12 = h % 12 === 0 ? 12 : h % 12;
    return h12 + ':' + pad(m) + sec + ' ' + ap;
  }
  function hourLabel(h, opts) { return clock(h, 0, 0, opts); }
  function capital(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1) : s; }
  function lower(s) { return s ? s.charAt(0).toLowerCase() + s.slice(1) : s; }
  function plural(n, w) { return n === 1 ? w : w + 's'; }
  function isAll(f) { return f.parts.length === 1 && f.parts[0].type === 'all'; }
  function isSingle(f) { return f.parts.length === 1 && f.parts[0].type === 'value'; }
  function onlyValues(f) { return f.parts.every(function (p) { return p.type === 'value'; }); }

  // seconds / minutes share the same shape
  function subUnitPhrase(f, unit) {
    if (isAll(f)) return 'every ' + unit;
    if (onlyValues(f)) {
      var vals = f.values;
      if (vals.length === 1) return 'at ' + vals[0] + ' ' + plural(vals[0], unit) + ' past the ' + (unit === 'second' ? 'minute' : 'hour');
      return 'at ' + unit + 's ' + joinList(vals.map(String)) + ' past the ' + (unit === 'second' ? 'minute' : 'hour');
    }
    var out = [];
    var vals2 = [];
    f.parts.forEach(function (p) {
      if (p.type === 'value') vals2.push(String(p.start));
      else if (p.type === 'range') out.push('every ' + unit + ', ' + unit + 's ' + p.start + ' through ' + p.end + ' past the ' + (unit === 'second' ? 'minute' : 'hour'));
      else if (p.type === 'step') {
        var t = 'every ' + p.step + ' ' + plural(p.step, unit);
        if (p.fromStar) out.push(t);
        else if (p.openEnd) out.push(t + ', starting at ' + unit + ' ' + p.start);
        else out.push(t + ', ' + unit + 's ' + p.start + ' through ' + p.end + ' past the ' + (unit === 'second' ? 'minute' : 'hour'));
      } else if (p.type === 'all') out.push('every ' + unit);
    });
    if (vals2.length) out.push('at ' + plural(vals2.length, unit) + ' ' + joinList(vals2));
    return joinList(out);
  }

  function hourPhrase(f, opts) {
    if (isAll(f)) return '';
    var out = [], vals = [];
    f.parts.forEach(function (p) {
      if (p.type === 'value') vals.push(p.start);
      else if (p.type === 'range') out.push('between ' + clock(p.start, 0, 0, opts) + ' and ' + clock(p.end, 59, 0, opts));
      else if (p.type === 'step') {
        var t = 'every ' + p.step + ' ' + plural(p.step, 'hour');
        if (p.fromStar) out.push(t);
        else if (p.openEnd) out.push(t + ', starting at ' + clock(p.start, 0, 0, opts));
        else out.push(t + ', between ' + clock(p.start, 0, 0, opts) + ' and ' + clock(p.end, 59, 0, opts));
      }
    });
    if (vals.length === 1 && !out.length) return 'between ' + clock(vals[0], 0, 0, opts) + ' and ' + clock(vals[0], 59, 0, opts);
    if (vals.length) out.push('during the ' + joinList(vals.map(function (h) { return hourLabel(h, opts); })) + ' ' + plural(vals.length, 'hour'));
    return joinList(out);
  }

  function timePhrase(p, opts) {
    var S = p.fields.second, M = p.fields.minute, H = p.fields.hour;
    var secTrivial = S.implicit || (isSingle(S) && S.values[0] === 0);
    var secSingle = isSingle(S);
    // Concrete clock times: single minute (and single second) with hour as a short list of values
    if (secSingle && isSingle(M) && onlyValues(H) && H.values.length <= 6) {
      var times = H.values.map(function (h) { return clock(h, M.values[0], S.values[0], opts); });
      return 'At ' + joinList(times);
    }
    var bits = [];
    if (!secTrivial) {
      bits.push(subUnitPhrase(S, 'second'));
      if (!isAll(M)) bits.push(subUnitPhrase(M, 'minute'));
    } else {
      if (isSingle(M) && M.values[0] === 0 && !isAll(H) && H.parts.length === 1 && H.parts[0].type === 'step' && H.parts[0].fromStar) {
        return capital('every ' + H.parts[0].step + ' hours') + ', on the hour';
      }
      if (isSingle(M) && M.values[0] === 0 && isAll(H)) return 'Every hour, on the hour';
      bits.push(subUnitPhrase(M, 'minute'));
    }
    var hp = hourPhrase(H, opts);
    if (hp) bits.push(hp);
    return capital(bits.join(', '));
  }

  function domPhrase(f) {
    var out = [], vals = [];
    f.parts.forEach(function (p) {
      if (p.type === 'value') vals.push(p.start);
      else if (p.type === 'range') out.push('on days ' + p.start + ' through ' + p.end + ' of the month');
      else if (p.type === 'step') {
        var t = 'every ' + (p.step === 1 ? 'day' : p.step + ' days');
        if (p.fromStar) out.push(t + ' of the month (starting on the 1st)');
        else if (p.openEnd) out.push(t + ' of the month, starting on the ' + ordinal(p.start));
        else out.push(t + ', from the ' + ordinal(p.start) + ' through the ' + ordinal(p.end) + ' of the month');
      } else if (p.type === 'all') out.push('every day');
    });
    if (vals.length) out.unshift('on the ' + joinList(vals.map(ordinal)) + ' of the month');
    return joinList(out);
  }

  function nameList(values, names, conj) { return joinList(values.map(function (v) { return names[v]; }), conj); }

  function dowPhrase(f, mode) {
    // mode: 'only' (stand-alone) or 'or' (after a day-of-month phrase)
    if (onlyValues(f) || f.parts.some(function (p) { return p.type === 'step'; })) {
      return (mode === 'or' ? 'on ' : 'only on ') + nameList(f.values, DAY_NAMES, mode === 'or' ? 'or' : 'and');
    }
    var out = [], vals = [];
    f.parts.forEach(function (p) {
      if (p.type === 'value') vals.push(DAY_NAMES[p.start % 7]);
      else if (p.type === 'range') out.push(DAY_NAMES[p.start % 7] + ' through ' + DAY_NAMES[p.end % 7]);
    });
    var txt = joinList(out.concat(vals), mode === 'or' ? 'or' : 'and');
    return mode === 'or' ? 'on ' + txt : txt;
  }

  function monthPhrase(f) {
    if (isAll(f)) return '';
    if (onlyValues(f) || f.parts.some(function (p) { return p.type === 'step'; })) return 'only in ' + nameList(f.values.map(function (v) { return v - 1; }), MONTH_NAMES);
    var out = [], vals = [];
    f.parts.forEach(function (p) {
      if (p.type === 'value') vals.push(MONTH_NAMES[p.start - 1]);
      else if (p.type === 'range') out.push(MONTH_NAMES[p.start - 1] + ' through ' + MONTH_NAMES[p.end - 1]);
    });
    return joinList(out.concat(vals));
  }

  function dayLogic(p) {
    var D = p.fields.dom, W = p.fields.dow;
    var domR = !isAll(D), dowR = !isAll(W);
    if (!domR && !dowR) return 'none';
    if (domR && dowR) return D.star || W.star ? 'and' : 'or';
    return domR ? 'dom' : 'dow';
  }

  function describe(p, opts) {
    if (!p || !p.ok) return '';
    var segs = [timePhrase(p, opts)];
    var logic = dayLogic(p);
    if (logic === 'dom') segs.push(domPhrase(p.fields.dom));
    else if (logic === 'dow') segs.push(dowPhrase(p.fields.dow, 'only'));
    else if (logic === 'or') segs.push(domPhrase(p.fields.dom) + ' or ' + dowPhrase(p.fields.dow, 'or'));
    else if (logic === 'and') segs.push(domPhrase(p.fields.dom) + ', but only if it falls on ' + nameList(p.fields.dow.values, DAY_NAMES, 'or'));
    var mp = monthPhrase(p.fields.month);
    if (mp) segs.push(mp);
    return segs.join(', ');
  }

  // Compact summary of the concrete values a field matches
  function summarizeValues(f) {
    var spec = f.spec, vals = f.values;
    var full = spec.key === 'dow' ? 7 : spec.max - spec.min + 1;
    if (vals.length === full) return 'every ' + spec.label.toLowerCase();
    var label = function (v) {
      if (spec.key === 'month') return MONTH_NAMES[v - 1].slice(0, 3);
      if (spec.key === 'dow') return DAY_NAMES[v].slice(0, 3);
      return String(v);
    };
    var runs = [], i = 0;
    while (i < vals.length) {
      var j = i;
      while (j + 1 < vals.length && vals[j + 1] === vals[j] + 1) j++;
      if (j - i >= 2) runs.push(label(vals[i]) + '–' + label(vals[j]));
      else for (var k = i; k <= j; k++) runs.push(label(vals[k]));
      i = j + 1;
    }
    var txt = runs.length > 16 ? runs.slice(0, 16).join(', ') + ', …' : runs.join(', ');
    return txt + ' (' + vals.length + ' ' + plural(vals.length, 'value') + ')';
  }

  /* ---------------- scheduling ---------------- */

  var fmtCache = {};
  function getFmt(tz) {
    if (!fmtCache[tz]) {
      fmtCache[tz] = new Intl.DateTimeFormat('en-US', {
        timeZone: tz, hourCycle: 'h23', year: 'numeric', month: 'numeric', day: 'numeric',
        hour: 'numeric', minute: 'numeric', second: 'numeric'
      });
    }
    return fmtCache[tz];
  }
  function wallParts(t, tz) {
    var o = {};
    getFmt(tz).formatToParts(new Date(t)).forEach(function (x) { if (x.type !== 'literal') o[x.type] = parseInt(x.value, 10); });
    if (o.hour === 24) o.hour = 0;
    return { y: o.year, mo: o.month, d: o.day, h: o.hour, mi: o.minute, s: o.second };
  }
  function tzOffset(t, tz) {
    var w = wallParts(t, tz);
    return Date.UTC(w.y, w.mo - 1, w.d, w.h, w.mi, w.s) - Math.floor(t / 1000) * 1000;
  }
  // Wall-clock time in tz -> epoch ms. Returns earliest instant for ambiguous times, null for non-existent (DST gap).
  function wallToInstant(y, mo, d, h, mi, s, tz) {
    var g = Date.UTC(y, mo - 1, d, h, mi, s);
    var offs = [tzOffset(g - 14 * 3600e3, tz), tzOffset(g, tz), tzOffset(g + 14 * 3600e3, tz)];
    var best = null;
    offs.forEach(function (off) {
      var t = g - off;
      var w = wallParts(t, tz);
      if (w.y === y && w.mo === mo && w.d === d && w.h === h && w.mi === mi && w.s === s) {
        if (best === null || t < best) best = t;
      }
    });
    return best;
  }

  function daysInMonth(y, m) { return new Date(Date.UTC(y, m, 0)).getUTCDate(); }

  function canEverRun(p) {
    var logic = dayLogic(p);
    if (logic === 'dom' || logic === 'and') {
      var minDom = p.fields.dom.values[0];
      var ok = p.fields.month.values.some(function (m) { return minDom <= (m === 2 ? 29 : daysInMonth(2001, m)); });
      if (!ok) return false;
    }
    return true;
  }

  function nextRuns(p, opts) {
    opts = opts || {};
    var count = opts.count || 10, tz = opts.timeZone || 'UTC';
    var from = opts.from != null ? +opts.from : Date.now();
    from = Math.floor(from / 1000) * 1000;
    var res = [];
    if (!p || !p.ok || !canEverRun(p)) return res;
    var F = p.fields, logic = dayLogic(p);
    var monthSet = toSet(F.month.values), domSet = toSet(F.dom.values), dowSet = toSet(F.dow.values);
    var now = wallParts(from, tz);
    var nowKey = now.h * 3600 + now.mi * 60 + now.s;
    var maxDays = opts.maxDays || 366 * 60;
    for (var i = 0; i <= maxDays && res.length < count; i++) {
      var dt = new Date(Date.UTC(now.y, now.mo - 1, now.d + i));
      var y = dt.getUTCFullYear(), mo = dt.getUTCMonth() + 1, d = dt.getUTCDate(), wd = dt.getUTCDay();
      if (!monthSet[mo]) continue;
      var dm = !!domSet[d], wm = !!dowSet[wd];
      var hit = logic === 'or' ? (dm || wm) : (dm && wm);
      if (!hit) continue;
      for (var hi = 0; hi < F.hour.values.length && res.length < count; hi++) {
        var h = F.hour.values[hi];
        if (i === 0 && h < now.h) continue;
        for (var mi = 0; mi < F.minute.values.length && res.length < count; mi++) {
          var m = F.minute.values[mi];
          if (i === 0 && h * 3600 + m * 60 + 59 < nowKey) continue;
          for (var si = 0; si < F.second.values.length && res.length < count; si++) {
            var s = F.second.values[si];
            if (i === 0 && h * 3600 + m * 60 + s <= nowKey - 7200) continue;
            var t = wallToInstant(y, mo, d, h, m, s, tz);
            if (t !== null && t > from && res.indexOf(t) === -1) res.push(t);
          }
        }
      }
    }
    res.sort(function (a, b) { return a - b; });
    return res.slice(0, count);
  }
  function toSet(arr) { var o = {}; arr.forEach(function (v) { o[v] = true; }); return o; }

  return {
    parse: parse, describe: describe, nextRuns: nextRuns, summarizeValues: summarizeValues,
    dayLogic: dayLogic, canEverRun: canEverRun, wallParts: wallParts, wallToInstant: wallToInstant,
    allowedText: allowedText, FIELDS: FIELDS, MACROS: MACROS, clock: clock
  };
});
