/* Tiny syntax highlighter for the export previews (Python, SQL, YAML, Mermaid, Markdown, JSON). MIT. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else (root.PS = root.PS || {}).highlight = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  function esc(s) { return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
  function words(list) { return new RegExp('^(?:' + list.split(' ').join('|') + ')$', 'i'); }
  var PY_KW = words('False None True and as assert async await break class continue def del elif else except finally for from global if import in is lambda nonlocal not or pass raise return try while with yield');
  var SQL_KW = words('select from where and or not in is null as on using join inner left right full outer cross group by order having limit union all distinct case when then else end create replace table view schema if exists drop insert into values update set delete merge matched with qualify over partition row_number count sum avg min max cast interval true false desc asc between like');
  var RULES = {
    python: [['com', /#[^\n]*/y], ['str', /[rbfRBF]{0,2}("""[\s\S]*?"""|'''[\s\S]*?'''|"(?:\\.|[^"\\\n])*"|'(?:\\.|[^'\\\n])*')/y], ['dec', /@[\w.]+/y], ['num', /\b\d[\d_]*(?:\.\d+)?\b/y], ['word', /[A-Za-z_]\w*/y]],
    sql: [['tpl', /\{\{[\s\S]*?\}\}|\{%[\s\S]*?%\}/y], ['com', /--[^\n]*|\/\*[\s\S]*?\*\//y], ['str', /'(?:''|[^'])*'/y], ['num', /\b\d+(?:\.\d+)?\b/y], ['word', /[A-Za-z_]\w*/y]],
    yaml: [['com', /#[^\n]*/y], ['tpl', /\{\{[\s\S]*?\}\}/y], ['key', /^[ \t-]*[\w.\-+]+(?=:)/my], ['str', /"(?:\\.|[^"\\])*"|'(?:''|[^'])*'/y], ['num', /\b\d+(?:\.\d+)?\b/y]],
    mermaid: [['com', /%%[^\n]*/y], ['kw', /\b(?:flowchart|graph|subgraph|end|classDef|class|style|linkStyle|LR|TD|TB|RL|BT)\b/y], ['str', /"[^"\n]*"/y], ['op', /-->|==>|-\.->|---|\|[^|\n]*\|/y]],
    markdown: [['kw', /^#{1,6} [^\n]*/my], ['str', /`[^`\n]+`/y], ['com', /^```[^\n]*/my], ['op', /^\s*[-*] |\|/my]],
    json: [['key', /"(?:\\.|[^"\\])*"(?=\s*:)/y], ['str', /"(?:\\.|[^"\\])*"/y], ['num', /-?\b\d+(?:\.\d+)?(?:e[+-]?\d+)?\b/iy], ['kw', /\b(?:true|false|null)\b/y]]
  };
  function highlight(code, lang) {
    var rules = RULES[lang]; code = String(code || '');
    if (!rules) return esc(code);
    var out = '', i = 0, plain = '';
    var flush = function () { if (plain) { out += esc(plain); plain = ''; } };
    while (i < code.length) {
      var hit = null;
      for (var r = 0; r < rules.length; r++) {
        var re = rules[r][1]; re.lastIndex = i;
        // ^ anchors (multiline) only match at line starts
        var m = re.exec(code);
        if (m && m.index === i && m[0].length) { hit = [rules[r][0], m[0]]; break; }
      }
      if (!hit) { plain += code[i++]; continue; }
      var cls = hit[0], text = hit[1];
      if (cls === 'word') {
        // identifiers: highlight keywords only, and keep words whole
        var isKw = (lang === 'python' ? PY_KW : SQL_KW).test(text);
        if (isKw) { flush(); out += '<span class="hl-kw">' + esc(text) + '</span>'; } else plain += text;
      } else { flush(); out += '<span class="hl-' + cls + '">' + esc(text) + '</span>'; }
      i += text.length;
    }
    flush();
    return out;
  }
  return { highlight: highlight, esc: esc };
});
