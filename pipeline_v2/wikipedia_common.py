#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: wikipedia_common.py

Main purpose:
- Provide one shared Wikipedia utility layer for config-driven harvest scripts.
- Keep navbox/category crawling logic conflict-agnostic and reusable.
- Avoid duplicating Wikipedia API/parsing code across multiple harvesters.

What this module provides:
- MediaWiki API wrappers with retry/backoff.
- Optional shared logger hook (`set_logger`) so API requests/progress can be written to one pipeline log file.
- Seed URL parsing (language + title extraction).
- Rendered page HTML retrieval (`action=parse`) and soup parsing.
- Navbox utilities (select navbox, extract wiki links, title extraction).
- Category utilities (fetch categories, fetch category members, BFS/DFS category walk).
- Language-link resolution across Wikipedias.
- Wikipedia title -> Wikidata QID resolution (with 413/414-safe chunk splitting).
- Config-driven instance-of hint mapping (`source.hint`) via `harvest_hints.instance_of_map`.

When it is called:
- During harvesting only.
- Used by `harvest_navboxes.py` and `harvest_categories.py` as their core Wikipedia crawler/parser.
- Used by `harvest_wikidata.py` only for optional seed-page resolution from `navbox_seed_url`.

This module is not intended to be run directly.
It is imported by:
- harvest_wikidata.py
- harvest_navboxes.py
- harvest_categories.py

What it does NOT do:
- It does not classify entities.
- It does not merge records.
- It does not write final attribution labels.
- Those steps are handled by `attribution.py` + `pipeline_common.py`.
"""

import re
import sys
import time
from collections import deque
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup

DEFAULT_WIKI_USER_AGENT = "conflict-pipeline-wikipedia/1.0 (config-driven research)"
HEADERS_JSON = {"User-Agent": DEFAULT_WIKI_USER_AGENT, "Accept": "application/json"}

ALLOWED_NAMESPACES = {""}
DEFAULT_INSTANCE_OF_HINT_MAP = {
    "person": {"Q5"},
    "event": {"Q178561", "Q180684", "Q645883", "Q167442"},
    "organization": {"Q176799", "Q43229", "Q7188"},
    "policy": {"Q820655", "Q7163", "Q618779", "Q182994"},
    "media_narrative": {"Q17379835", "Q215080"},
}
_LOGGER = None


def set_logger(logger) -> None:
    global _LOGGER
    _LOGGER = logger


def _log_info(msg: str) -> None:
    if _LOGGER:
        _LOGGER.info(msg)
    else:
        print(msg)


def _log_warning(msg: str) -> None:
    if _LOGGER:
        _LOGGER.warning(msg)
    else:
        print(f"WARN: {msg}")


def _summarize_params(params: Dict) -> str:
    keys = [
        "action",
        "prop",
        "list",
        "page",
        "titles",
        "cmtitle",
        "ppprop",
        "cmtype",
    ]
    parts: List[str] = []
    for k in keys:
        if k not in params:
            continue
        v = params.get(k)
        if k == "titles" and isinstance(v, str):
            titles_count = v.count("|") + 1 if v else 0
            shown = v[:120] + ("..." if len(v) > 120 else "")
            parts.append(f"{k}=<{titles_count} titles> {shown}")
            continue
        parts.append(f"{k}={v}")
    return ", ".join(parts)


def normalize_qids(vals, default: Optional[Set[str]] = None) -> Set[str]:
    out: Set[str] = set()
    if isinstance(vals, (list, tuple, set)):
        for v in vals:
            if isinstance(v, str) and re.fullmatch(r"Q\d+", v.strip()):
                out.add(v.strip())
    if out:
        return out
    if isinstance(default, set):
        return set(default)
    return set()


def instance_hint_map_from_config(config: dict) -> Dict[str, Set[str]]:
    hcfg = config.get("harvest_hints") if isinstance(config.get("harvest_hints"), dict) else {}
    raw = hcfg.get("instance_of_map") if isinstance(hcfg.get("instance_of_map"), dict) else {}
    if not raw:
        return {k: set(v) for k, v in DEFAULT_INSTANCE_OF_HINT_MAP.items()}

    out: Dict[str, Set[str]] = {}
    for k, vals in raw.items():
        if not isinstance(k, str):
            continue
        kk = k.strip()
        if not kk:
            continue
        qset = normalize_qids(vals)
        if qset:
            out[kk] = qset
    if out:
        return out
    return {k: set(v) for k, v in DEFAULT_INSTANCE_OF_HINT_MAP.items()}


def infer_category_hint(instance_qids: Set[str], hint_map: Optional[Dict[str, Set[str]]] = None) -> str:
    mapping = hint_map if isinstance(hint_map, dict) and hint_map else DEFAULT_INSTANCE_OF_HINT_MAP
    for label, anchors in mapping.items():
        if instance_qids & anchors:
            return label
    return "unknown"


def wiki_api_for_lang(lang: str) -> str:
    return f"https://{lang}.wikipedia.org/w/api.php"


def infer_source_lang_and_title_from_url(start_url: str, default_lang: str = "en") -> Tuple[str, str]:
    lang = default_lang
    title = start_url.strip()
    if start_url.startswith("http"):
        u = urlparse(start_url)
        host = u.netloc.lower()
        if host.endswith(".wikipedia.org"):
            sub = host.split(".wikipedia.org", 1)[0]
            if sub:
                lang = sub
        if u.path.startswith("/wiki/"):
            title = unquote(u.path[len("/wiki/"):])
    return lang, title


def page_title_from_start_url(start_url: str) -> str:
    return infer_source_lang_and_title_from_url(start_url)[1]


def _mw_api_get(
    params: Dict,
    sleep: float = 0.0,
    retries: int = 8,
    api_url: Optional[str] = None,
    use_post: bool = False,
) -> Dict:
    if not api_url:
        api_url = wiki_api_for_lang("en")

    req_params = dict(params)
    req_params.setdefault("format", "json")
    req_params.setdefault("formatversion", 2)

    last_status = None
    last_head = ""
    last_exc = None

    for attempt in range(retries):
        try:
            method = "POST" if use_post else "GET"
            _log_info(
                f"[wiki_api] {method} {api_url} | "
                f"attempt={attempt + 1}/{retries} | {_summarize_params(req_params)}"
            )
            if use_post:
                r = requests.post(api_url, data=req_params, headers=HEADERS_JSON, timeout=60)
            else:
                r = requests.get(api_url, params=req_params, headers=HEADERS_JSON, timeout=60)
            last_status = r.status_code
            last_head = r.text[:200]

            if r.status_code in (403, 429, 500, 502, 503, 504):
                _log_warning(
                    f"[wiki_api] retryable status={r.status_code}, sleeping before retry"
                )
                time.sleep(min(60.0, (2 ** attempt) + 0.8))
                continue

            r.raise_for_status()
            if sleep > 0:
                time.sleep(sleep)
            return r.json()

        except Exception as exc:
            last_exc = exc
            _log_warning(f"[wiki_api] request failed: {exc}")
            time.sleep(min(60.0, (2 ** attempt) + 0.8))

    raise RuntimeError(
        f"MediaWiki API failed. last_status={last_status}, last_head={last_head!r}, last_error={repr(last_exc)}"
    )


def _mw_query_all_pages(
    params: Dict,
    sleep: float = 0.0,
    continue_key: str = "continue",
    api_url: Optional[str] = None,
    max_items: Optional[int] = None,
) -> List[Dict]:
    out_items: List[Dict] = []
    cont = {}
    page_idx = 0
    while True:
        page_idx += 1
        req = dict(params)
        req.update(cont)
        data = _mw_api_get(req, sleep=sleep, api_url=api_url)

        if "query" in data:
            q = data["query"]
            if isinstance(q.get("pages"), list):
                out_items.extend(q["pages"])
            elif isinstance(q.get("categorymembers"), list):
                out_items.extend(q["categorymembers"])

        _log_info(f"[wiki_api] page_chunk={page_idx} cumulative_items={len(out_items)}")

        if max_items and len(out_items) >= max_items:
            return out_items[:max_items]

        nxt = data.get(continue_key)
        if not nxt:
            break
        cont = nxt
    return out_items


def fetch_rendered_html_via_parse(page_title: str, sleep: float, api_url: Optional[str] = None) -> str:
    data = _mw_api_get(
        {
            "action": "parse",
            "page": page_title,
            "prop": "text",
            "redirects": 1,
        },
        sleep=sleep,
        api_url=api_url,
    )
    if "error" in data:
        raise RuntimeError(f"MediaWiki parse error: {data['error']}")

    text_field = (data.get("parse") or {}).get("text")
    if text_field is None:
        raise RuntimeError("MediaWiki parse returned no 'text' field.")

    if isinstance(text_field, dict) and "*" in text_field:
        html = text_field["*"]
    elif isinstance(text_field, str):
        html = text_field
    else:
        html = str(text_field)

    if not html or len(html) < 100:
        raise RuntimeError("Parsed HTML looks too small; likely a fetch issue.")
    return html


def fetch_page_categories(page_title: str, sleep: float = 0.2, api_url: Optional[str] = None) -> Set[str]:
    pages = _mw_query_all_pages(
        {
            "action": "query",
            "prop": "categories",
            "titles": page_title,
            "cllimit": "max",
            "redirects": 1,
            "clshow": "!hidden",
        },
        sleep=sleep,
        api_url=api_url,
    )
    out: Set[str] = set()
    for p in pages:
        for c in (p.get("categories") or []):
            t = c.get("title")
            ns = c.get("ns")
            if isinstance(t, str) and (ns == 14 or t.startswith("Category:")):
                out.add(t)
    return out


def fetch_category_members(
    category_title: str,
    sleep: float = 0.2,
    api_url: Optional[str] = None,
    max_members: Optional[int] = None,
) -> List[Dict]:
    return _mw_query_all_pages(
        {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": category_title,
            "cmtype": "page|subcat",
            "cmlimit": "max",
        },
        sleep=sleep,
        api_url=api_url,
        max_items=max_members,
    )


def keep_subcategory_by_keywords(category_title: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    t = category_title.lower()
    return any(k.lower() in t for k in keywords)


def walk_categories_collect_titles(
    root_categories: Set[str],
    depth: int,
    strategy: str = "bfs",
    keywords: Optional[List[str]] = None,
    sleep: float = 0.2,
    api_url: Optional[str] = None,
    max_categories: int = 0,
    max_titles: int = 0,
    max_members_per_category: int = 0,
    progress_every: int = 25,
    progress_prefix: str = "",
) -> Dict[str, object]:
    kw = keywords or []
    visited_categories: Set[str] = set()
    article_titles: Set[str] = set()
    queued_categories: Set[str] = set()

    agenda = deque()
    for c in sorted(root_categories):
        agenda.append((c, 0))
        queued_categories.add(c)

    stop_reason = ""
    while agenda:
        if max_categories > 0 and len(visited_categories) >= max_categories:
            stop_reason = "max_categories_reached"
            break

        if strategy == "dfs":
            cat, d = agenda.pop()
        else:
            cat, d = agenda.popleft()

        if cat in visited_categories:
            continue
        visited_categories.add(cat)

        if d > depth:
            continue

        try:
            members = fetch_category_members(
                cat,
                sleep=sleep,
                api_url=api_url,
                max_members=(max_members_per_category if max_members_per_category > 0 else None),
            )
        except Exception as exc:
            _log_warning(f"failed to fetch category '{cat}': {exc}")
            continue

        if progress_every > 0 and (len(visited_categories) % progress_every == 0):
            pfx = f"{progress_prefix} " if progress_prefix else ""
            _log_info(
                f"{pfx}progress: visited_categories={len(visited_categories)}, "
                f"queued={len(agenda)}, titles={len(article_titles)}"
            )

        for m in members:
            ns = m.get("ns")
            title = m.get("title")
            if not isinstance(title, str):
                continue

            if ns == 14:
                if d < depth and keep_subcategory_by_keywords(title, kw):
                    if title not in visited_categories and title not in queued_categories:
                        agenda.append((title, d + 1))
                        queued_categories.add(title)
            elif ns == 0:
                article_titles.add(title)
                if max_titles > 0 and len(article_titles) >= max_titles:
                    stop_reason = "max_titles_reached"
                    break

        if stop_reason:
            break

    return {
        "titles": article_titles,
        "visited_categories": visited_categories,
        "stop_reason": stop_reason or "completed",
    }


def fetch_langlinks(page_title: str, source_lang: str = "en", sleep: float = 0.2) -> Dict[str, str]:
    api_url = wiki_api_for_lang(source_lang)
    data = _mw_api_get(
        {
            "action": "query",
            "prop": "langlinks",
            "titles": page_title,
            "lllimit": "max",
            "redirects": 1,
        },
        sleep=sleep,
        api_url=api_url,
    )
    out: Dict[str, str] = {}
    for p in data.get("query", {}).get("pages", []):
        for ll in (p.get("langlinks") or []):
            lang = ll.get("lang")
            title = ll.get("title")
            if isinstance(lang, str) and isinstance(title, str):
                out[lang] = title
    return out


def soup_from_html(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        _log_warning("lxml parser not available. Please run: pip install lxml")
        sys.exit(1)


def is_internal_wiki_link(url: str) -> bool:
    try:
        u = urlparse(url)
        return u.netloc.endswith("wikipedia.org") and u.path.startswith("/wiki/")
    except Exception:
        return False


def clean_title_from_href_path(href_path: str) -> Optional[str]:
    try:
        href_path = href_path.split("#", 1)[0]
        if not href_path.startswith("/wiki/"):
            return None
        title = unquote(href_path[len("/wiki/"):])
        if ":" in title:
            ns = title.split(":", 1)[0]
            if ns not in ALLOWED_NAMESPACES:
                return None
        if title.startswith("Main_Page"):
            return None
        return title
    except Exception:
        return None


def navbox_title_text(navbox) -> str:
    t = navbox.select_one(".navbox-title")
    if t:
        return " ".join(t.get_text(" ", strip=True).split())
    cap = navbox.select_one("caption")
    if cap:
        return " ".join(cap.get_text(" ", strip=True).split())
    return " ".join(navbox.get_text(" ", strip=True).split())[:120]


def select_top_level_navboxes(soup: BeautifulSoup) -> List[object]:
    all_navboxes = soup.select("table.navbox, table.vertical-navbox, div.navbox, nav.navbox")

    top = []
    for nb in all_navboxes:
        parent_navbox = nb.find_parent(class_="navbox")
        if parent_navbox is None:
            top.append(nb)

    top2 = []
    for nb in top:
        cls = " ".join(nb.get("class", []))
        if "navbox-subgroup" in cls:
            continue
        top2.append(nb)
    return top2


def select_one_navbox(soup: BeautifulSoup, title_query: Optional[str], index: int) -> Optional[object]:
    navboxes = select_top_level_navboxes(soup)
    if not navboxes:
        return None

    if title_query and title_query.strip():
        tq = title_query.strip().lower()
        for nb in navboxes:
            t = navbox_title_text(nb).lower()
            if tq in t:
                return nb

    idx = max(0, min(index, len(navboxes) - 1))
    return navboxes[idx]


def extract_links_from_one_navbox(
    soup: BeautifulSoup,
    base_url: str,
    navbox_title: Optional[str],
    navbox_index: int,
) -> Set[str]:
    links: Set[str] = set()
    navbox = select_one_navbox(soup, navbox_title, navbox_index)
    if navbox is None:
        return links

    for a in navbox.find_all("a", href=True):
        href = a["href"]
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = urljoin(base_url, href)

        if is_internal_wiki_link(href):
            links.add(href)
    return links


def titles_from_urls(urls: Set[str]) -> List[str]:
    titles: List[str] = []
    for u in urls:
        if is_internal_wiki_link(u):
            t = clean_title_from_href_path(urlparse(u).path)
            if t:
                titles.append(t)
    return sorted(set(titles))


def wikipedia_titles_to_qids(titles: List[str], lang: str = "en", batch: int = 40, sleep: float = 0.2) -> Dict[str, str]:
    qmap: Dict[str, str] = {}
    api_url = wiki_api_for_lang(lang)
    _log_info(f"[titles_to_qids] lang={lang} titles={len(titles)} batch={batch}")

    def resolve_chunk(chunk: List[str]) -> Dict[str, str]:
        if not chunk:
            return {}
        try:
            data = _mw_api_get(
                {
                    "action": "query",
                    "prop": "pageprops",
                    "ppprop": "wikibase_item",
                    "titles": "|".join(chunk),
                    "redirects": 1,
                },
                sleep=sleep,
                api_url=api_url,
                use_post=True,
            )
        except RuntimeError as exc:
            msg = str(exc)
            if len(chunk) > 1 and (" 414 " in f" {msg} " or " 413 " in f" {msg} " or "Too Long" in msg):
                _log_warning(f"[titles_to_qids] split_chunk_due_to_uri_limit size={len(chunk)}")
                mid = len(chunk) // 2
                out = {}
                out.update(resolve_chunk(chunk[:mid]))
                out.update(resolve_chunk(chunk[mid:]))
                return out
            raise

        out: Dict[str, str] = {}
        pages = data.get("query", {}).get("pages", [])
        for page in pages:
            title = page.get("title")
            qid = (page.get("pageprops") or {}).get("wikibase_item")
            if title and qid:
                out[title] = qid
        return out

    for i in range(0, len(titles), batch):
        chunk = titles[i : i + batch]
        _log_info(
            f"[titles_to_qids] lang={lang} chunk={(i // batch) + 1}/"
            f"{((len(titles) - 1) // batch) + 1 if titles else 0} size={len(chunk)}"
        )
        qmap.update(resolve_chunk(chunk))
    _log_info(f"[titles_to_qids] lang={lang} resolved={len(qmap)}/{len(titles)}")
    return qmap
