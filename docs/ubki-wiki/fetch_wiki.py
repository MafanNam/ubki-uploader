"""Refresh the local snapshot of the UBKI wiki pages that matter for data transmission.

Usage: python docs/ubki-wiki/fetch_wiki.py

The Confluence space is public; its REST API returns the full page body, whereas
WebFetch/HTML scraping truncates long tables. Pages are converted to Markdown
(tables kept as tables, code samples as fenced blocks) and written next to this
script together with the upload/response XSDs. curl is used instead of urllib
because the local Python has no CA bundle configured.
"""
import json
import re
import subprocess
from html.parser import HTMLParser
from pathlib import Path

BASE = "https://wiki-ubki.atlassian.net/wiki/rest/api"
OUT = Path(__file__).resolve().parent

# (file name, page id) — the transmission subtree plus the shared pages it relies on
PAGES = [
    ("00_general_principles", 109707565),
    ("01_auth", 110395469),
    ("02_test_environment", 112885967),
    ("10_transmission_overview", 112984161),
    ("11_person_request_response", 114983026),
    ("12_params_tech_block", 115376129),
    ("13_params_ident_person", 115441682),
    ("14_params_deals", 115376165),
    ("15_params_contacts", 115376194),
    ("16_errors_and_notices", 114753770),
    ("17_sentreestr_api", 114917533),
    ("18_api_versions", 114753794),
    ("19_changelog", 112591067),
    ("20_dictionaries_index", 110526795),
]
# dictionaries referenced by the upload spec (dir.0 = errtype of auth/system errors)
DICTIONARIES = {
    0: 110526802, 1: 110723466, 2: 110886963, 3: 110886976, 4: 110886989,
    5: 110854166, 6: 110887002, 7: 111509547, 8: 111509560, 9: 111476772,
    10: 111509573, 12: 111476785, 13: 111509586, 14: 111476811, 15: 111476824,
    16: 111476837, 17: 111509612, 18: 111509669, 22: 111509723, 23: 111509736,
    24: 111509749, 50: 112722013, 51: 112689231, 56: 113115185, 62: 113180752,
}
XSDS = {
    "upload.xsd": "https://secure.ubki.ua/upload.xsd",
    "response.xsd": "https://secure.ubki.ua/response.xsd",
}
# the navigation footer every page repeats
FOOTER = re.compile(r"\n(Див\. у цьому розділі також|#+ Вступ\n).*", re.S)


def curl(url: str) -> bytes:
    return subprocess.run(["curl", "-sf", "--max-time", "60", url],
                          capture_output=True, check=True).stdout


class ToMarkdown(HTMLParser):
    def __init__(self):
        super().__init__()
        self.out = []          # finished blocks
        self.buf = []          # current inline text
        self.pre = 0
        self.expand = 0        # <div> depth inside an "expand" macro (one <p> per code line)
        self.skip = 0
        self.row = None        # list of cells while inside <tr>
        self.table = None      # list of rows while inside <table>

    def _flush(self, prefix=""):
        text = re.sub(r"\s+", " ", "".join(self.buf)).strip()
        self.buf = []
        if not text:
            return
        if self.row is not None:
            # headings/bullets inside a table cell would break the Markdown row
            self.row[-1] += (" " if self.row[-1] else "") + text
        else:
            self.out.append(prefix + text)

    def handle_starttag(self, tag, attrs):
        if self.expand and tag in ("table", "ul", "ol"):
            # an expand macro that wraps structured content is not a code sample
            self.pre -= 1
            self.expand = 0
        if tag in ("script", "style"):
            self.skip += 1
        elif tag == "pre" or (tag == "div" and "expand-content" in (dict(attrs).get("class") or "")):
            self._flush()
            self.pre += 1
            self.expand = 1 if tag == "div" else 0
        elif self.pre:
            if self.expand and tag == "div":
                self.expand += 1
        elif tag == "table":
            self._flush()
            self.table = []
        elif tag == "tr" and self.table is not None:
            self.row = []
        elif tag in ("td", "th") and self.row is not None:
            self.row.append("")
        elif tag == "br":
            self.buf.append(" / " if self.row is not None else "\n")
        elif tag in ("p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._flush()

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.skip -= 1
        elif tag == "pre" and not self.expand:
            self._end_code()
        elif self.pre:
            if self.expand and tag == "p":
                self.buf.append("\n")
            elif self.expand and tag == "div":
                self.expand -= 1
                if not self.expand:
                    self._end_code()
        elif tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self._flush("#" * min(int(tag[1]) + 1, 6) + " ")
        elif tag == "li":
            self._flush("- ")
        elif tag in ("p", "div", "td", "th"):
            self._flush()
        elif tag == "tr" and self.row is not None:
            self._flush()
            if any(c.strip() for c in self.row):
                self.table.append(self.row)
            self.row = None
        elif tag == "table" and self.table is not None:
            self._flush()
            self._emit_table()
            self.table = None

    def _end_code(self):
        code = "".join(self.buf).replace("\xa0", " ").strip("\n")
        self.buf = []
        self.pre -= 1
        if self.row is None and not re.search(r"[{<]", code):
            # a collapsed prose paragraph, not a JSON/XML sample
            self.out.append(re.sub(r"\s+", " ", code).strip())
            return
        block = f"```\n{code}\n```"
        if self.row is not None:
            # a code sample inside a table cell cannot live in a Markdown row:
            # the row keeps a pointer, the code follows the table
            self.row[-1] += " (див. нижче)"
            self.out.append(("CODE", block))
        else:
            self.out.append(block)

    def _emit_table(self):
        rows = [[c.replace("|", "\\|") for c in r] for r in self.table]
        codes = [b for b in self.out if isinstance(b, tuple)]
        self.out = [b for b in self.out if not isinstance(b, tuple)]
        if rows:
            width = max(len(r) for r in rows)
            rows = [r + [""] * (width - len(r)) for r in rows]
            lines = ["| " + " | ".join(rows[0]) + " |", "|" + " --- |" * width]
            lines += ["| " + " | ".join(r) + " |" for r in rows[1:]]
            self.out.append("\n".join(lines))
        self.out.extend(block for _, block in codes)

    def handle_data(self, data):
        if not self.skip:
            self.buf.append(data)

    def markdown(self):
        self._flush()
        return "\n\n".join(b for b in self.out if isinstance(b, str))


def fetch_page(name: str, page_id: int) -> None:
    page = json.loads(curl(f"{BASE}/content/{page_id}?expand=body.view,version"))
    parser = ToMarkdown()
    parser.feed(page["body"]["view"]["value"])
    body = FOOTER.sub("", parser.markdown()).strip()
    url = f"https://wiki-ubki.atlassian.net/wiki/spaces/Spec/pages/{page_id}"
    header = (f"# {page['title']}\n\n> Source: {url} (version {page['version']['number']},"
              f" edited {page['version']['when'][:10]})\n")
    (OUT / f"{name}.md").write_text(header + "\n" + body + "\n", encoding="utf-8")
    print(f"{name}.md  {len(body)} chars")


def main() -> None:
    for name, page_id in PAGES:
        fetch_page(name, page_id)
    for num, page_id in DICTIONARIES.items():
        fetch_page(f"dict_{num:02d}", page_id)
    for name, url in XSDS.items():
        (OUT / name).write_bytes(curl(url))
        print(name)


if __name__ == "__main__":
    main()
