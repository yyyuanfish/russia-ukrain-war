#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Harvest entities starting from the English Wikipedia page "Russo-Ukrainian war":
- Scrape all internal wiki links from the page (content + External links section) and
  from bottom navboxes whose title contains "Russo-Ukrainian" (case-insensitive).
- Map Wikipedia titles -> Wikidata QIDs using the MediaWiki API.
- For all QIDs, query Wikidata via SPARQL to retrieve multilingual labels/descriptions,
  sitelinks, instance-of types, and basic geo/citizenship properties for attribution.
- Infer an attribution label in {"Russia","Ukraine","American","other"} using:
  a) structured props (P27, P17, P495, P131, P276, P159) with country mapping
  b) fallback on descriptions (en/uk/ru) using keyword heuristics
- Save results as JSONL and print a summary report (also as JSON).

Usage:
  python ru_ua_wiki_link_harvest.py \
    --start-url https://en.wikipedia.org/wiki/Russo-Ukrainian_war \
    --out-entities data/entities_from_links.jsonl \
    --out-report data/report.json \
    --prior-entities data/ru_ua_keywords.jsonl

Dependencies:
  pip install -r requirements_extended.txt
"""

import argparse
import json
import os
import re
import sys
import time
from urllib.parse import urlparse, urljoin, unquote

import requests
from bs4 import BeautifulSoup
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON

# -------------------------- Config & constants --------------------------

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "ru-ua-wiki-link-harvester/1.0 (academic research; contact unavailable)"

# Country QIDs
Q_RUSSIA = "Q159"
Q_UKRAINE = "Q212"
Q_USA = "Q30"

# Type anchors for categorization (instance of / subclass of)
Q_HUMAN = "Q5"
Q_BATTLE = "Q178561"
Q_MILITARY_CONFLICT = "Q180684"
Q_ATTACK = "Q645883"
Q_MASS_KILLING = "Q167442"
Q_MILITARY_UNIT = "Q176799"
Q_ORGANIZATION = "Q43229"
Q_GOVERNMENT = "Q7188"
Q_LAW = "Q820655"
Q_PUBLIC_POLICY = "Q7163"
Q_ECON_SANCTION = "Q618779"
Q_RESOLUTION = "Q182994"
Q_CONSPIRACY_THEORY = "Q17379835"
Q_PROPAGANDA = "Q215080"
# 人、战斗、军事冲突、军事单位、组织、法律、制裁、宣传等

ALLOWED_NAMESPACES = {""}  # Keep only mainspace (no colon in title)

HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json"}

# Simple language keyword fallback for attribution via descriptions
# 描述文本里找归属的关键词字典：
# 每个 attribution label（Russia / Ukraine / American）
# 下有 en/ru/uk 三种语言的若干 regex pattern，比如 \brussian\b、российск 等

ATTRIB_KEYWORDS = {
    "Russia": {
        "en": [r"\brussian\b"],
        "ru": [r"\bроссийск", r"\bрусск"],
        "uk": [r"\bросійськ", r"\bросіян"],
    },
    "Ukraine": {
        "en": [r"\bukrainian\b"],
        "ru": [r"\bукраинск", r"\bукраинец", r"\bукраинка"],
        "uk": [r"\bукраїнськ", r"\bукраїнець", r"\bукраїнка"],
    },
    "American": {
        "en": [r"\bamerican\b", r"\bU\.?S\.?\b", r"\bUnited States\b"],
        "ru": [r"\bамерикан"],
        "uk": [r"\bамерикан"],
    },
}

# Output dirs default
DEFAULT_ENTITIES_OUT = "entities_from_links.jsonl"
DEFAULT_REPORT_OUT = "report.json"
SPARQL_LOG = "SPARQL_QUERIES.log"

# -------------------------- Utilities --------------------------

def mkparents(path: str):
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    # 取出文件路径的目录 d，如果为空就用 "."

def clean_title_from_href(href: str) -> str | None:
    """
    Normalize an internal wiki link ("/wiki/Title") into a page title.
    Exclude non-main namespaces (those with colon), fragments, and files.
    """
    try:
        href = href.split("#", 1)[0]
        if not href.startswith("/wiki/"):
            return None
        title = href[len("/wiki/"):]
        title = unquote(title)
        # Exclude File:, Template:, Category:, Help:, etc.
        if ":" in title:
            ns = title.split(":", 1)[0]
            if ns not in ALLOWED_NAMESPACES:
                return None
        # Exclude obvious non-article pages
        if title.startswith(("Main_Page",)):
            return None
        # Keep
        return title
    except Exception:
        return None

def is_internal_wiki_link(url: str) -> bool:
    try:
        u = urlparse(url)
        return u.netloc.endswith("wikipedia.org") and u.path.startswith("/wiki/")
    except Exception:
        return False
    # 判断是不是某个语言的 wikipedia（*.wikipedia.org），路径以 /wiki/ 开

def fetch_html(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")
# 用 requests 把网页抓下来，检查状态码

def extract_links_from_external_section(soup: BeautifulSoup, base_url: str) -> set[str]:
    # external links 
    """
    Find the 'External links' section and collect all <a> hrefs under it until the next h2.
    """
    links = set()
    ext_span = soup.select_one('span#External_links') # 找 id 为 External_links 的 span
    if ext_span:
        curr = ext_span.find_parent(["h2","h3"])
        node = curr.find_next_sibling()
        while node and node.name not in ("h2",):
            for a in node.find_all("a", href=True):
                href = a["href"]
                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("/"):
                    href = urljoin(base_url, href)
                links.add(href)
            node = node.find_next_sibling()
    return links
# 如果找到了：
# 向上找到最近的 h2 或 h3（标题节点）。
# 从这个标题的下一个 sibling 开始，一直往下走 next_sibling：
# 如果遇到下一个 h2，停止（说明下一大节开始了）。
# 在当前 node 里 find_all("a")。
# href 以 // 开头：补上 https:
# 以 / 开头：用 urljoin 拼上 base URL。
# 其余保持原样。
# 都加入 links 集合。
# 返回 links。
# 这里不筛选 wikipedia.org，外部链接也会被收集，但后面只会保留内部 wiki 链接用于后续。


def extract_links_from_navboxes(soup: BeautifulSoup, base_url: str, contains_text="Russo-Ukrainian") -> set[str]:
    # boxes
    """
    Collect all links within bottom navboxes whose title contains a given substring.
    在页面底部 navbox（那种导航盒）里，找标题包含 “Russo-Ukrainian” 的 navbox，收集里面所有 <a> 的链接
    """
    links = set()
    for navbox in soup.select("table.navbox"):
        title_el = navbox.select_one(".navbox-title")
        title_txt = title_el.get_text(" ", strip=True) if title_el else ""
        if contains_text.lower() in title_txt.lower():
            for a in navbox.find_all("a", href=True):
                href = a["href"]
                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("/"):
                    href = urljoin(base_url, href)
                links.add(href)
    return links

def extract_all_internal_wiki_links(soup: BeautifulSoup, base_url: str) -> set[str]:
    # 在正文区域 #mw-content-text .mw-parser-output 里，把所有内部 wiki 链接提出来
# content = soup.select_one("#mw-content-text .mw-parser-output")。
# 如果找不到，就退而求其次用整个 soup。
# 在 content 中找到所有 <a href>。
# 对 href 处理 // / / 和 urljoin。
# 用 is_internal_wiki_link 检查是不是维基内部链接。
# 是的话放到 links。

    """
    Collect all internal wikipedia links in the page content (mw-parser-output).
    """
    links = set()
    content = soup.select_one("#mw-content-text .mw-parser-output")
    if not content:
        content = soup
    for a in content.find_all("a", href=True):
        href = a["href"]
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = urljoin(base_url, href)
        if is_internal_wiki_link(href):
            links.add(href)
    return links

def titles_from_urls(urls: set[str]) -> list[str]:
# 输入：一堆 url（里面既有 enwiki，也可能有其它语言）。
# 对每个 url：
# 如果 is_internal_wiki_link(u) 为真：
# 用 urlparse(u).path 拿到路径，比如 /wiki/Some_Title。
# 用 clean_title_from_href 变成标题。
# 最后 sorted(set(titles)) 去重 + 排序

    titles = []
    for u in urls:
        if is_internal_wiki_link(u):
            title = clean_title_from_href(urlparse(u).path)
            if title:
                titles.append(title)
    return sorted(set(titles))

def wikipedia_titles_to_qids(titles: list[str], batch=40) -> dict[str, str]:
    """
    Resolve enwiki page titles -> Wikidata QIDs using MediaWiki API.
    Returns dict {title: qid}. Entries without QID are omitted.
    用 enwiki 的 MediaWiki API，把标题转换成 Wikidata QID
    """
# qmap = {}。
# 设置 api = "https://en.wikipedia.org/w/api.php"。
# 把 titles 按 batch 大小（默认 40）切成多段：
# titles 用 | 拼接，作为 titles 参数。
# 请求参数：action=query, prop=pageprops, ppprop=wikibase_item。
# 请求。
# data["query"]["pages"] 里，每个 page 有 title 和 pageprops.wikibase_item。
# 有 QID 就 qmap[title] = qid。
# time.sleep(0.1)，对 API 礼貌。
# 返回 {title: qid} 字典

    qmap = {}
    api = "https://en.wikipedia.org/w/api.php"
    for i in range(0, len(titles), batch):
        chunk = titles[i:i+batch]
        params = {
            "action": "query",
            "format": "json",
            "prop": "pageprops",
            "ppprop": "wikibase_item",
            "titles": "|".join(chunk),
        }
        r = requests.get(api, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        pages = data.get("query", {}).get("pages", {})
        for pageid, page in pages.items():
            title = page.get("title")
            pp = page.get("pageprops", {})
            qid = pp.get("wikibase_item")
            if title and qid:
                qmap[title] = qid
        time.sleep(0.1)  # be polite
    return qmap

# -------------------------- SPARQL --------------------------

# 根据一批 QID 拼出一个大的 SPARQL 查询字符串
# values = " ".join(f"wd:{q}" for q in qids)，变成 wd:Q123 wd:Q456 ...。
# 拼出一个 SELECT ... WHERE { VALUES ?item { ... } ... } 查询：
# 拿 labels/descriptions：
# rdfs:label en/uk/ru
# schema:description en/uk/ru
# 拿 sitelinks：
# ?enwiki / ?ruwiki / ?ukwiki 是 schema:about ?item 和 schema:isPartOf 某个 wiki 的 URL。
# 拿 P31 instance-of。
# 拿 attribution 相关的属性：P27/P17/P495/P131/P276/P159

def build_sparql_for_qids(qids: list[str]) -> str:
    """
    Build a SPARQL query to fetch labels/descriptions/sitelinks and key props
    for classification from a VALUES ?item { wd:Q... } list.
    """
    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?item ?label_en ?label_uk ?label_ru ?desc_en ?desc_uk ?desc_ru
       ?enwiki ?ruwiki ?ukwiki ?inst ?citizenship ?country ?origin ?located ?location ?hq
WHERE {{
  VALUES ?item {{ {values} }}

  # Multilingual labels & descriptions
  OPTIONAL {{ ?item rdfs:label ?label_en . FILTER(LANG(?label_en) = "en") }}
  OPTIONAL {{ ?item rdfs:label ?label_uk . FILTER(LANG(?label_uk) = "uk") }}
  OPTIONAL {{ ?item rdfs:label ?label_ru . FILTER(LANG(?label_ru) = "ru") }}
  OPTIONAL {{ ?item schema:description ?desc_en . FILTER(LANG(?desc_en) = "en") }}
  OPTIONAL {{ ?item schema:description ?desc_uk . FILTER(LANG(?desc_uk) = "uk") }}
  OPTIONAL {{ ?item schema:description ?desc_ru . FILTER(LANG(?desc_ru) = "ru") }}

  # Sitelinks
  OPTIONAL {{ ?enwiki schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> . }}
  OPTIONAL {{ ?ruwiki schema:about ?item ; schema:isPartOf <https://ru.wikipedia.org/> . }}
  OPTIONAL {{ ?ukwiki schema:about ?item ; schema:isPartOf <https://uk.wikipedia.org/> . }}

  # Instance-of (for category inference)
  OPTIONAL {{ ?item wdt:P31 ?inst . }}

  # Properties for attribution:
  OPTIONAL {{ ?item wdt:P27 ?citizenship . }}     # country of citizenship (people)
  OPTIONAL {{ ?item wdt:P17 ?country . }}         # country (org/place)
  OPTIONAL {{ ?item wdt:P495 ?origin . }}         # country of origin (work/org/equipment)
  OPTIONAL {{ ?item wdt:P131 ?located . }}        # located in the administrative territorial entity
  OPTIONAL {{ ?item wdt:P276 ?location . }}       # location
  OPTIONAL {{ ?item wdt:P159 ?hq . }}             # headquarters location
}}
"""
    return query

def run_sparql(query: str) -> dict:
# 用 SPARQLWrapper 指定 endpoint + user-agent。
# 设置查询 + 返回格式为 JSON。
# 打印一份查询到 stdout。
# 追加写入 SPARQL_QUERIES.log（方便你之后 copy-paste 到 query.wikidata.org 检查）。
# 调用 .query().convert() 得到 JSON 结果返回

    sparql = SPARQLWrapper(WIKIDATA_SPARQL, agent=USER_AGENT)
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQL_JSON)
    print("\n--- SPARQL QUERY ---\n", query)
    with open(SPARQL_LOG, "a", encoding="utf-8") as f:
        f.write(query + "\n\n")
    return sparql.query().convert()

# -------------------------- Inference helpers --------------------------
# 分类和归属的 helper
# 每个 category 一堆 anchor QID；如果 instance-of 里包含这些 QID，就认为是对应类型。
def qid_from_uri(uri: str | None) -> str | None:
    
    if not uri:
        return None
    return uri.rsplit("/", 1)[-1]

CATEGORY_MAP = {
    "person": {Q_HUMAN},
    "event": {Q_BATTLE, Q_MILITARY_CONFLICT, Q_ATTACK, Q_MASS_KILLING},
    "organization": {Q_MILITARY_UNIT, Q_ORGANIZATION, Q_GOVERNMENT},
    "policy": {Q_LAW, Q_PUBLIC_POLICY, Q_ECON_SANCTION, Q_RESOLUTION},
    "media_narrative": {Q_CONSPIRACY_THEORY, Q_PROPAGANDA},
}

def infer_category(instance_qids: set[str]) -> str | None:
    for cat, anchors in CATEGORY_MAP.items():
        if instance_qids & anchors:
            return cat
    return None

def infer_attribution(structured_qids: list[str], descriptions: dict) -> tuple[str, list[str], str]:
# 先看 structured 信息（属性 QID）：
# 如果 structured_qids 里有 "Q159" → "Russia", hits=["Q159"], method="structured"；
# 有 "Q212" → "Ukraine"；
# 有 "Q30" → "American"。
# 如果上一步都没命中：
# 遍历 ATTRIB_KEYWORDS：
# label = "Russia"/"Ukraine"/"American"
# 每个 lang（en/ru/uk）取 description 文本（小写），用正则匹配 pattern：
# 一旦匹配成功，就返回对应 label，hits 记录是哪种语言+pattern，method="description_fallback"。
# 再都没命中：
# 返回 "other", hits=[], method="none"。

    """
    Decide attribution in {"Russia","Ukraine","American","other"}.
    1) If structured properties include Q159/Q212/Q30, return accordingly (first-hit wins in order RU, UA, US).
    2) Fallback: check keywords in descriptions (en/ru/uk).
    Returns (label, hits, method).
    """
    hits = []
    if "Q159" in structured_qids:
        return "Russia", ["Q159"], "structured"
    if "Q212" in structured_qids:
        return "Ukraine", ["Q212"], "structured"
    if "Q30" in structured_qids:
        return "American", ["Q30"], "structured"

    for label, langs in ATTRIB_KEYWORDS.items():
        for lang, patterns in langs.items():
            text = (descriptions.get(lang) or "").lower()
            for pat in patterns:
                if re.search(pat, text):
                    return label, [f"desc:{lang}:{pat}"], "description_fallback"

    return "other", [], "none"

# -------------------------- Main pipeline --------------------------

def main():
# 解析参数
# 定义命令行参数：
# --start-url：起始 enwiki 页面，默认是 Russo-Ukrainian war。
# --out-entities：JSONL 输出（entities）。
# --out-report：总结统计 JSON。
# --prior-entities：之前的 harvest 文件（分析 overlap 用）。
# --limit-titles：debug 时限制处理多少个 title。
# args = ap.parse_args()：得到参数对象

    ap = argparse.ArgumentParser()
    ap.add_argument("--start-url", default="https://en.wikipedia.org/wiki/Russo-Ukrainian_war", help="EN Wikipedia page URL to start from")
    ap.add_argument("--out-entities", default="entities_from_links.jsonl", help="Output JSONL of harvested entities")
    ap.add_argument("--out-report", default="report.json", help="Output JSON summary report")
    ap.add_argument("--prior-entities", default=None, help="Optional JSON or JSONL file from prior Wikidata harvest for overlap stats")
    ap.add_argument("--limit-titles", type=int, default=None, help="Limit number of titles to process (debug)")
    args = ap.parse_args()

    for p in (args.out_entities, args.out_report, "SPARQL_QUERIES.log"):
        d = os.path.dirname(p) or "."
        os.makedirs(d, exist_ok=True)
    # Reset SPARQL log
    with open("SPARQL_QUERIES.log", "w", encoding="utf-8") as f:
        f.write("# SPARQL queries issued by ru_ua_wiki_link_harvest.py\n\n")

    # 1) Fetch HTML
    print(f"Fetching: {args.start_url}")
    soup = fetch_html(args.start_url)

    # 2) Collect links
    base = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(args.start_url))
    links_all = extract_all_internal_wiki_links(soup, base)
    links_ext = extract_links_from_external_section(soup, base)
    links_nav = extract_links_from_navboxes(soup, base, contains_text="Russo-Ukrainian")
    links = set()
    links |= links_all
    links |= {u for u in links_ext if is_internal_wiki_link(u)}
    links |= links_nav

    print(f"Total links collected (internal only for mapping): {len(links)}")

    # 3) Normalize to page titles
    titles = titles_from_urls(links)
    if args.limit_titles:
        titles = titles[:args.limit_titles]
    print(f"Unique enwiki titles: {len(titles)}")

    # 4) Resolve titles -> QIDs
    title2qid = wikipedia_titles_to_qids(titles)
    qids = sorted(set(title2qid.values()))
    print(f"Resolved QIDs: {len(qids)}")

    # 5) Batch SPARQL for QIDs
    entities = []
    BATCH = 100
    for i in range(0, len(qids), BATCH):
        batch_qids = qids[i:i+BATCH]
        query = build_sparql_for_qids(batch_qids)
        data = run_sparql(query)
        for b in data["results"]["bindings"]:
            get = lambda k: b.get(k, {}).get("value")
            item_uri = get("item")
            qid = qid_from_uri(item_uri)
            if not qid:
                continue

            inst = qid_from_uri(get("inst"))
            citizenship = qid_from_uri(get("citizenship"))
            country = qid_from_uri(get("country"))
            origin = qid_from_uri(get("origin"))
            located = qid_from_uri(get("located"))
            location = qid_from_uri(get("location"))
            hq = qid_from_uri(get("hq"))

            rec = next((r for r in entities if r["qid"] == qid), None)
            if rec is None:
                rec = {
                    "qid": qid,
                    "labels": {
                        "en": get("label_en"),
                        "uk": get("label_uk"),
                        "ru": get("label_ru"),
                    },
                    "descriptions": {
                        "en": get("desc_en"),
                        "uk": get("desc_uk"),
                        "ru": get("desc_ru"),
                    },
                    "sitelinks": {
                        "enwiki": get("enwiki"),
                        "ruwiki": get("ruwiki"),
                        "ukwiki": get("ukwiki"),
                    },
                    "instance_of": set(),
                    "raw_attrib_qids": set(),
                }
                entities.append(rec)

            if inst:
                rec["instance_of"].add(inst)
            for v in (citizenship, country, origin, located, location, hq):
                if v:
                    rec["raw_attrib_qids"].add(v)

    # 6) Post-process: category & attribution
    for rec in entities:
        inst_set = set(rec.pop("instance_of"))
        rec["instance_of"] = sorted(inst_set)
        category = infer_category(inst_set)
        rec["category"] = category

        structured = sorted(set(rec.pop("raw_attrib_qids")))
        attribution, hits, method = infer_attribution(structured, rec.get("descriptions", {}))
        rec["attribution"] = attribution
        rec["attribution_detail"] = {"method": method, "hits": hits, "structured_qids": structured}

    # 7) Save JSONL
    with open(args.out_entities, "w", encoding="utf-8") as f:
        for r in entities:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"Wrote entities: {args.out_entities}  ({len(entities)} records)")

    # 8) Summary & overlap with prior file
    def load_prior_qids(prior_path: str) -> set[str]:
        if not prior_path or not os.path.exists(prior_path):
            return set()
        qids = set()
        if prior_path.endswith(".jsonl"):
            with open(prior_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    q = obj.get("qid")
                    if q:
                        qids.add(q)
        else:
            obj = json.load(open(prior_path, "r", encoding="utf-8"))
            if isinstance(obj, list):
                for o in obj:
                    q = o.get("qid")
                    if q:
                        qids.add(q)
        return qids

    prior_qids = load_prior_qids(args.prior_entities)
    new_qids = {e["qid"] for e in entities}
    inter = new_qids & prior_qids if prior_qids else set()

    def lang_cov(lang):
        return sum(1 for e in entities if (e["labels"].get(lang) or e["descriptions"].get(lang)))
    cov = {lang: lang_cov(lang) for lang in ("en","uk","ru")}

    attrib_counts = {}
    for e in entities:
        attrib_counts[e["attribution"]] = attrib_counts.get(e["attribution"], 0) + 1

    cat_counts = {}
    for e in entities:
        cat = e.get("category") or "unknown"
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    summary = {
        "start_url": args.start_url,
        "total_internal_links": len(links),
        "unique_titles": len(titles),
        "resolved_qids": len(new_qids),
        "overlap_with_prior": len(inter),
        "overlap_rate": (len(inter) / len(new_qids)) if new_qids else 0.0,
        "language_coverage_nonempty_label_or_desc": cov,
        "attribution_counts": attrib_counts,
        "category_counts": cat_counts,
        "note": "SPARQL queries saved in SPARQL_QUERIES.log; copy-paste to query.wikidata.org for sanity check."
    }
    with open(args.out_report, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Summary:", json.dumps(summary, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
