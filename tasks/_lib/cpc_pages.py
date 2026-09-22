"""Conservative application scope for CPC report pages; retain unresolved blocks."""
import re

_FULL_APP = re.compile(r"\b(?:[CN]\s*[-:]?\s*)?(\d{6})\s*([A-Z]{2,4})\b", re.I)
_IDENTITY_LINE = re.compile(
    r"^\s*(?:(?:ULURP|APPLICATIONS?)(?:\s+(?:NO|NUMBER))?[\s:#.()-]*|"
    r"\d{1,3}\s+)?[ (]*(?:[CN]\s*[-:]?\s*)?\d{6}\s*[A-Z]{2,4}\b", re.I
)
_FORM_FIELD = re.compile(r"^\s*(?:ULURP|APPLICATIONS?)\b", re.I)
_BOROUGHS = ("bronx", "brooklyn", "manhattan", "queens", "staten island")


def _normalize_application(value):
    match = _FULL_APP.search(value or "")
    if not match:
        return None
    # Attachment forms often omit C/N; retain the number and action code.
    return f"{match.group(1)} {match.group(2).upper()}"


def _application_signals(page_text):
    identity_text = page_text[:1200] + "\n" + page_text[-700:]
    identity_lines = [line for line in identity_text.splitlines() if _IDENTITY_LINE.match(line)]
    identity_lines += [line for line in page_text.splitlines()
                       if _FORM_FIELD.match(line) and _IDENTITY_LINE.match(line)]
    full = {_normalize_application(m.group(0)) for line in identity_lines for m in _FULL_APP.finditer(line)}
    all_full = {_normalize_application(m.group(0)) for m in _FULL_APP.finditer(page_text)}
    return {x for x in full if x}, {x for x in all_full if x}


def _is_document_start(page_text):
    top = " ".join(page_text[:900].lower().split())
    return (
        "city planning commission" in top[:300]
        or "community/borough board" in top[:450]
        or "community board recommendation" in top[:450]
        or ("borough president" in top[:350]
            and ("recommendation" in top[:500] or "testimony" in top[:500]))
        or "recommendation report" in top[:350]
        or "testimony of" in top[:350]
        or "report of the" in top[:350]
        or "concurring statement" in top[:200]
        or "dissenting statement" in top[:200]
    )


def _header_borough(page_text):
    top = " ".join(page_text[:1800].lower().split())
    for borough in _BOROUGHS:
        if (f"borough of {borough}" in top or f"{borough} borough president" in top
                or f"borough president of {borough}" in top):
            return borough
    return None


def scope_pages(text, source_application, allowed_applications, core_end_page,
                source_borough):
    """Return conservative page scope rows while preserving each page's text.

    ``text`` may be a form-feed-delimited string or a sequence of page strings.
    ``core_end_page`` is the last parsed focal CPC-report page; use zero for
    a separately supplied attachment or related-report PDF.
    """
    source = _normalize_application(source_application)
    allowed = {_normalize_application(x) for x in allowed_applications}
    allowed.discard(None)
    if source:
        allowed.add(source)
    source_borough = (source_borough or "").strip().lower()

    rows = []
    current_scope = None
    current_start = None
    page_texts = text.split('\f') if isinstance(text, str) else text
    for page_number, page_text in enumerate(page_texts, 1):
        if not page_text.strip() or not re.search(r"[A-Za-z0-9]", page_text):
            if page_number > core_end_page:
                current_scope = "unresolved"
                current_start = page_number
            rows.append({"page": page_number, "scope": "blank",
                         "reason": "No usable OCR text; block identity not inferred.",
                         "block_start_page": current_start, "text": page_text})
            continue

        full_apps, all_full_apps = _application_signals(page_text)
        allowed_hit = bool(full_apps & allowed)
        foreign_apps = full_apps - allowed
        incidental_foreign = all_full_apps - allowed
        new_block = _is_document_start(page_text)
        page_borough = _header_borough(page_text) if new_block else None
        borough_conflict = bool(page_borough and source_borough
                                and page_borough != source_borough)

        if page_number <= core_end_page:
            scope, reason = "in_scope", "Inside the parsed main CPC report."
            if page_number == 1:
                current_start = 1
        elif new_block:
            current_start = page_number
            if allowed_hit and borough_conflict:
                scope = "unresolved"
                reason = "New block has an allowed docket but a conflicting borough header."
            elif allowed_hit:
                scope, reason = "in_scope", "New block identifies an allowed application."
            elif borough_conflict:
                scope = "unresolved"
                reason = f"New block identifies conflicting borough {page_borough}; verify its application."
            elif foreign_apps:
                scope = "unresolved"
                reason = "New block identifies an unallowed application; distinguish another case from an OCR error."
            else:
                scope = "unresolved"
                reason = "New document header has no clear allowed or contrary identity."
        elif allowed_hit:
            scope, reason = "in_scope", "Page identifies an allowed application."
            if current_start is None:
                current_start = page_number
        elif page_number == core_end_page + 1:
            scope = "unresolved"
            reason = "First post-core page lacks an explicit allowed identity."
            current_start = page_number
        elif incidental_foreign and page_number > core_end_page:
            scope = "unresolved"
            reason = "Post-core page cites a foreign docket without an allowed block identity."
            current_start = page_number
        elif current_scope is not None:
            scope, reason = current_scope, "Continuation inherits the preceding document block."
        else:
            scope, reason = "unresolved", "No document identity or preceding block is available."
            current_start = page_number

        current_scope = scope
        rows.append({"page": page_number, "scope": scope, "reason": reason,
                     "block_start_page": current_start, "text": page_text})
    return rows
