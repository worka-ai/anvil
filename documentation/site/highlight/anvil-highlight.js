(function () {
  const keywordSets = {
    bash: ["case", "do", "done", "elif", "else", "esac", "export", "fi", "for", "function", "if", "in", "local", "then", "while"],
    java: ["boolean", "break", "catch", "class", "const", "else", "final", "for", "if", "import", "new", "private", "public", "return", "static", "throw", "throws", "try", "var", "void"],
    js: ["await", "break", "case", "catch", "class", "const", "else", "export", "for", "from", "function", "if", "import", "let", "new", "return", "throw", "try", "var", "while", "yield"],
    json: [],
    python: ["as", "async", "await", "break", "class", "def", "elif", "else", "except", "finally", "for", "from", "if", "import", "in", "lambda", "pass", "raise", "return", "try", "while", "with", "yield"],
    rust: ["async", "await", "break", "const", "continue", "crate", "enum", "fn", "for", "if", "impl", "in", "let", "match", "mod", "move", "mut", "pub", "ref", "return", "self", "static", "struct", "trait", "type", "use", "where", "while"],
    text: []
  };
  const literalSets = {
    java: ["false", "null", "true"],
    js: ["false", "null", "true", "undefined"],
    json: ["false", "null", "true"],
    python: ["False", "None", "True"],
    rust: ["Err", "None", "Ok", "Some", "false", "true"]
  };

  function languageFor(element) {
    const classes = String(element.className || "").split(/\s+/);
    for (const cls of classes) {
      const match = /^(?:lang|language)-(.+)$/.exec(cls);
      if (match) {
        return normalize(match[1]);
      }
    }
    return "text";
  }

  function normalize(language) {
    const value = String(language || "text").toLowerCase();
    if (["javascript", "node", "nodejs"].includes(value)) return "js";
    if (["shell", "sh", "zsh"].includes(value)) return "bash";
    if (["plain", "plaintext", "txt"].includes(value)) return "text";
    return keywordSets[value] ? value : "text";
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function scan(code, language) {
    const segments = [];
    let plain = "";
    let i = 0;

    function flushPlain() {
      if (plain) {
        segments.push(["plain", plain]);
        plain = "";
      }
    }

    function readLineComment() {
      const end = code.indexOf("\n", i);
      const stop = end === -1 ? code.length : end;
      flushPlain();
      segments.push(["comment", code.slice(i, stop)]);
      i = stop;
    }

    function readBlockComment() {
      const end = code.indexOf("*/", i + 2);
      const stop = end === -1 ? code.length : end + 2;
      flushPlain();
      segments.push(["comment", code.slice(i, stop)]);
      i = stop;
    }

    function readString(quote) {
      const triple = language === "python" && code.slice(i, i + 3) === quote.repeat(3);
      let j = i + (triple ? 3 : 1);
      while (j < code.length) {
        if (!triple && code[j] === "\\") {
          j += 2;
          continue;
        }
        if (triple && code.slice(j, j + 3) === quote.repeat(3)) {
          j += 3;
          break;
        }
        if (!triple && code[j] === quote) {
          j += 1;
          break;
        }
        j += 1;
      }
      flushPlain();
      segments.push(["string", code.slice(i, j)]);
      i = j;
    }

    while (i < code.length) {
      const ch = code[i];
      const next = code.slice(i, i + 2);
      const hashComment = (language === "bash" || language === "python") && ch === "#";
      if ((language === "js" || language === "java" || language === "rust") && next === "//") {
        readLineComment();
      } else if ((language === "js" || language === "java" || language === "rust") && next === "/*") {
        readBlockComment();
      } else if (hashComment) {
        readLineComment();
      } else if (ch === '"' || ch === "'" || (language === "js" && ch === "`")) {
        readString(ch);
      } else {
        plain += ch;
        i += 1;
      }
    }
    flushPlain();
    return segments;
  }

  function wordsRegex(words) {
    if (!words || words.length === 0) return null;
    return new RegExp("\\b(" + words.map((word) => word.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|") + ")\\b", "g");
  }

  function renderPlain(value, language) {
    let html = escapeHtml(value);
    const keywords = wordsRegex(keywordSets[language]);
    const literals = wordsRegex(literalSets[language]);
    if (keywords) {
      html = html.replace(keywords, '<span class="hljs-keyword">$1</span>');
    }
    if (literals) {
      html = html.replace(literals, '<span class="hljs-literal">$1</span>');
    }
    html = html.replace(/\b(-?(?:0x[0-9a-fA-F]+|\d+(?:\.\d+)?))\b/g, '<span class="hljs-number">$1</span>');
    return html;
  }

  function render(code, language) {
    return scan(code, language).map(([kind, value]) => {
      if (kind === "plain") return renderPlain(value, language);
      if (kind === "comment") return '<span class="hljs-comment">' + escapeHtml(value) + '</span>';
      return '<span class="hljs-string">' + escapeHtml(value) + '</span>';
    }).join("");
  }

  function highlightElement(element) {
    const language = languageFor(element);
    element.innerHTML = render(element.textContent || "", language);
    element.classList.add("hljs");
    if (!element.classList.contains("language-" + language)) {
      element.classList.add("language-" + language);
    }
  }

  function highlightAll() {
    document.querySelectorAll("pre code").forEach(highlightElement);
  }

  window.hljs = window.hljs || {};
  window.hljs.highlightElement = highlightElement;
  window.hljs.highlightAll = highlightAll;
  window.hljs.configure = window.hljs.configure || function () {};
})();
