"""Literal CPC review facts, shared by extraction and its validation audit."""

import re

NUMBER_WORDS = {
    "a": 1, "an": 1, "no": 0, "none": 0, "zero": 0,
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
    "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
    "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40,
    "fifty": 50, "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
}
ONES = r"one|two|three|four|five|six|seven|eight|nine"
TENS = r"twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety"
SMALL_NUMBER = rf"(?:(?:{TENS})(?:[- ](?:{ONES}))?|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|ten|{ONES})"
WORD_NUMBER = rf"(?:(?:{ONES}|a|an) hundred(?: and)?(?: {SMALL_NUMBER})?|{SMALL_NUMBER}|none|zero|no|an|a)"
NUMBER = rf"(?:[0-9]+(?:,[0-9]{{3}})*|{WORD_NUMBER}(?:\s*\([0-9]+\))?)"
REPAIR_WORDS = [
    *[word for word in NUMBER_WORDS if len(word) > 2], "hundred",
    "approval", "disapproval", "approve", "disapprove", "abstentions",
    "abstaining", "abstained", "favor", "opposed", "opposition",
    "speakers", "speaker", "appearances", "resolution", "community",
    "board", "borough", "president", "unanimously", "recommendation",
]
OCR_WORDS = [
    (re.compile(r"\b" + r"[\s-]*".join(word) + r"\b", re.I), word)
    for word in sorted(REPAIR_WORDS, key=len, reverse=True)
]
SUPPORT = r"(?:in favor|in support|supporting (?:the )?(?:application|proposal|project))"
OPPOSITION = r"(?:in opposition|opposed|against(?: the (?:application|proposal|project))?)"
SPEAKER_DESCRIPTION = (
    r"(?: (?:speakers?|appearances?|individuals?|persons?|people|witnesses?|members?)\b"
    rf"(?:(?!\b(?:in favor|in support|supporting|in opposition|opposed|against|{SMALL_NUMBER}|[0-9]+|none|zero|no)\b)[^.;:]){{0,90}}?)?"
    r"\s+(?:were |spoke |testified |appeared )?"
)
NO_OTHERS = re.compile(r"\b(?:there (?:was|were) )?no other (?:speakers?|appearances?)\b", re.I)


def repair_ocr(text):
    text = re.sub(r"-\s*\n\s*", "", text)
    text = re.sub(r"(?<=\d)(?=[A-Za-z])|(?<=[A-Za-z])(?=\d)", " ", text)
    for pattern, word in OCR_WORDS:
        text = pattern.sub(word, text)
    text = re.sub(rf"\b({TENS})\s*-\s*({ONES})\b", r"\1-\2", text, flags=re.I)
    text = re.sub(r"\bnonein\b", "none in", text, flags=re.I)
    return re.sub(r"\s+", " ", text).strip()


def number(value):
    value = value.lower().strip()
    parenthetical = re.search(r"\s*\(([0-9]+)\)$", value)
    if parenthetical:
        written = number(value[:parenthetical.start()])
        return written if written == int(parenthetical[1]) else None
    if re.fullmatch(r"[0-9]+(?:,[0-9]{3})*", value):
        return int(value.replace(",", ""))
    total = 0
    for word in value.replace("-", " ").split():
        if word == "and":
            continue
        if word == "hundred":
            total *= 100
        elif word in NUMBER_WORDS:
            total += NUMBER_WORDS[word]
        else:
            return None
    return total


def evidence(text, start, end):
    return text[max(0, start - 180):min(len(text), end + 250)]


def prose_review_section(text, section):
    """Recover a bounded actor review when prose transitions cross printed lines."""
    text = repair_ocr(text)
    if section == "community_board":
        start = re.search(
            r"\bcommunity (?:planning )?board\s+(?:no\.?\s*|#\s*)?\d+\b.{0,200}?"
            r"\b(?:held a public hearing|recommended|adopted a resolution|voted)\b", text, re.I,
        )
        stop = r"\bborough (?:president|board)\b|\b(?:city planning )?commission\b|\bconsiderations?\b"
    else:
        start = re.search(
            r"\b(?:city planning )?commission\b.{0,200}?\b(?:scheduled|held)\b"
            r".{0,100}?\bpublic hearing\b", text, re.I,
        )
        stop = r"\bconsiderations?\b|\bresolution\b|\bcommission (?:therefore )?(?:believes|finds|considers)\b"
    if start is None:
        return ""
    end = re.search(stop, text[start.end():], re.I)
    if end is None:
        return ""
    return text[start.start():start.end() + end.start()]


def board_review(text):
    text = repair_ocr(text)
    result = dict(
        position="not_reported", position_rule="no_position_match",
        reported_for=None, reported_against=None, votes_for=None, votes_against=None,
        abstentions=None, abstention_rule="not_stated", effective_against=None,
        status="no_tally_match", vote_rule="none", evidence="", candidate_count=0,
    )
    if not text:
        result.update(status="no_section", position_rule="no_section")
        return result

    position_patterns = [
        ("no_recommendation", "no_recommendation", r"waived (?:its|the) right|failed to act|did not (?:submit|issue|make) a recommendation|neither approval nor disapproval|no recommendation"),
        ("oppose", "approval_motion_denied", r"denied (?:the )?recommendation to approve"),
        ("oppose", "disapproval", r"recommend\w* (?:to )?(?:disapprov\w*|denial|deny)|(?:resolution |motion )(?:of |to |recommending )?(?:disapprov\w*|oppos\w*)|\bdisapprov\w*|voted to (?:deny|reject)|rejected (?:the )?application"),
        ("support", "approval", r"recommend\w* (?:to )?approv\w*|\bapprov(?:ed|es) (?:the |this |these )?(?:application|proposal|project)|voted to approve|granted (?:conditional )?approval|(?:resolution|motion) (?:to approve|in favor of)|voted in favor of (?:the |this )?(?:application|proposal)|\bsupports? the (?:application|proposal|disposition)"),
    ]
    positions = []
    for position, rule, pattern in position_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            positions.append((match.start(), position, rule, match))
    # A denied approval motion contains the word "approve"; its full phrase wins.
    if positions:
        _, position, rule, position_match = min(positions, key=lambda item: item[0])
        conditions = re.search(
            r"\bwith (?:the (?:following )?)?(?:conditions?|provisions?)|\bsubject to\b|\bprovided that\b|\bunless\b|\bconditional\w* approval",
            text[max(0, position_match.start() - 180):position_match.end() + 300], re.I,
        )
        if conditions and position in {"support", "oppose"}:
            position = "support_with_conditions" if position == "support" else "oppose_unless_conditions"
        result.update(position=position, position_rule=rule,
                      evidence=evidence(text, position_match.start(), position_match.end()))
    if re.search(r"\bnon[- ]complying\b|\brecommendation did not comply\b|"
                 r"\bdid not constitute a majority\b|\bfailed to adopt a positive recommendation\b", text, re.I):
        result.update(position="no_recommendation", position_rule="noncomplying_recommendation")

    patterns = [
        ("for_against", rf"\b(?P<first>{NUMBER})(?: (?:board )?members?)?(?: voting)? (?:in favor|for|supporting)\b[^.;:]{{0,90}}?\b(?P<second>{NUMBER})(?: members?)?(?: voting)? (?:against|opposed|in opposition)\b"),
        ("against_for", rf"\b(?P<second>{NUMBER})(?: members?)?(?: voting)? (?:against|opposed|in opposition)\b[^.;:]{{0,90}}?\b(?P<first>{NUMBER})(?: members?)?(?: voting)? (?:in favor|for|supporting)\b"),
        ("table", rf"#?\s*in favor\s*:?\s*(?P<first>{NUMBER})[^.;:]{{0,90}}?#?\s*(?:against|opposed)\s*:?\s*(?P<second>{NUMBER})\b"),
        ("vote_to", rf"\b(?:by (?:a )?vote of|vote(?:d| was)?|voting)\s+(?P<first>{NUMBER})(?:\s+to\s+|\s+[-/–—]\s+|(?<=\d)[-/–—](?=\d))(?P<second>{NUMBER})\b"),
    ]
    candidates = sorted(
        [(match.start(), -(match.end() - match.start()), rule, match)
         for rule, pattern in patterns for match in re.finditer(pattern, text, re.I)],
        key=lambda item: item[:2],
    )
    accepted = []
    for _, _, rule, match in candidates:
        if not any(match.start() < prior.end() and prior.start() < match.end()
                   for _, prior in accepted):
            accepted.append((rule, match))
    result["candidate_count"] = len(accepted)
    if len(accepted) > 1:
        result.update(status="multiple_tallies", vote_rule="review_required",
                      evidence=" || ".join(evidence(text, match.start(), match.end()) for _, match in accepted))
        return result
    # A second board or a second one-sided vote is not the first board's tally.
    board_ids = set(re.findall(r"\bcommunity (?:planning )?board\s+(?:no\.?\s*|#\s*)?(\d+)\b", text, re.I))
    if len(board_ids) > 1:
        result.update(status="multiple_boards", vote_rule="review_required", evidence=text)
        return result
    if not accepted:
        unanimous = re.search(r"\bunanimous(?:ly)?\b", text, re.I)
        one_side = re.search(rf"\b(?P<count>{NUMBER})(?: (?:board )?members?)? voting in favor\b", text, re.I)
        if unanimous and one_side and result["position"] != "not_reported":
            raw_for = number(one_side["count"])
            raw_against = 0
            match = one_side
            rule = "unanimous_count"
        elif unanimous and result["position"].startswith(("support", "oppose")):
            result.update(status="partial", vote_rule="unanimous_without_total")
            if result["position"].startswith("support"):
                result["votes_against"] = 0
            else:
                result["votes_for"] = 0
            return result
        else:
            return result
    else:
        rule, match = accepted[0]
        raw_for, raw_against = number(match["first"]), number(match["second"])
    result.update(reported_for=raw_for, reported_against=raw_against,
                  evidence=evidence(text, match.start(), match.end()))
    abstention = re.search(rf"\b(?P<count>{NUMBER})\s+(?:abstain(?:ed|ing)|abstentions?)\b",
                           text[match.end():match.end() + 180], re.I)
    if abstention:
        result["abstentions"] = number(abstention["count"])
    if re.search(r"\babstentions (?:are |were )?(?:counted|treated|recorded) as (?:disapproval|opposition|negative) votes", text, re.I):
        result["abstention_rule"] = "opposition_explicit"
    if raw_for is None or raw_against is None:
        result.update(status="inconsistent_number", vote_rule="review_required")
        return result
    if result["position"] == "no_recommendation":
        result.update(status="procedural_vote", vote_rule="not_proposal_aligned")
        return result
    if result["position"] == "not_reported":
        result.update(status="unknown_motion", vote_rule="review_required")
        return result
    # Preserve literal votes and abstentions even when abstentions affect the motion.
    invert = result["position"].startswith("oppose") and result["position_rule"] != "approval_motion_denied"
    if invert and raw_for < raw_against:
        result.update(status="ambiguous_alignment", vote_rule="review_required")
        return result
    if result["position"] == "support_with_conditions" and re.search(
        r"\b(?:disapprov\w*|oppos\w*|different site|alternate site|another site)\b",
        text[match.end():match.end() + 600], re.I,
    ):
        result.update(status="substantive_conditions", vote_rule="review_required")
        return result
    result.update(
        votes_for=raw_against if invert else raw_for,
        votes_against=raw_for if invert else raw_against,
        status="resolved", vote_rule="inverted_disapproval" if invert else "direct_approval",
    )
    if result["abstention_rule"] == "opposition_explicit" and result["abstentions"] is not None:
        result["effective_against"] = result["votes_against"] + result["abstentions"]
    return result


def hearing_speakers(text):
    text = repair_ocr(text)
    result = dict(votes_for=None, votes_against=None, status="no_count_match",
                  rule="none", evidence="", hearing_count=0)
    if not text:
        result["status"] = "no_section"
        return result
    hearings = list(re.finditer(r"\b(?:the )?(?:continued )?hearing was duly held\b", text, re.I))
    blocks = [text[match.start():hearings[i + 1].start() if i + 1 < len(hearings) else len(text)]
              for i, match in enumerate(hearings)] if hearings else [text]
    result["hearing_count"] = len(blocks)
    if len(blocks) == 1 and re.search(r"\bhearing was continued\b", text, re.I):
        result.update(status="continued_hearing", rule="review_required", evidence=text)
        return result
    counts = []
    excerpts = []
    rules = []
    for block in blocks:
        no_speakers = re.search(
            r"\b(?:there (?:was|were) )?no (?:speakers|appearances)\b"
            r"(?!\s+(?:(?:appeared|spoke|testified)\s+)?(?:in favor|in support|against|opposed|in opposition|except|other than))", block, re.I,
        )
        if no_speakers:
            counts.append((0, 0))
            excerpts.append(evidence(block, no_speakers.start(), no_speakers.end()))
            rules.append("explicit_none")
            continue
        sides = []
        for side, stance in [("for", SUPPORT), ("against", OPPOSITION)]:
            matches = list(re.finditer(
                rf"\b(?P<count>{NUMBER}){SPEAKER_DESCRIPTION}{stance}", block, re.I,
            ))
            if matches:
                values = {number(match["count"]) for match in matches}
                value = next(iter(values)) if len(values) == 1 else None
                if len(matches) > 1:
                    result.update(status="conflicting_counts" if len(values) > 1 else "multiple_counts",
                                  rule="review_required", evidence=block)
                    return result
                match = matches[0]
            else:
                # A named singular actor counts only when the text says they appeared/spoke.
                match = re.search(
                    rf"\b(?:the |a |an )?(?:applicant|owner|attorney|representative|speaker)\b"
                    rf"[^.;:]{{0,60}}?\b(?:appeared|spoke|testified)\b[^.;:]{{0,30}}?{stance}", block, re.I,
                )
                if match and re.search(r"\b(?:team|members|representatives|attorneys|and)\b", match.group(), re.I):
                    match = None
                value = 1 if match else None
            sides.append(value)
            if match:
                excerpts.append(evidence(block, match.start(), match.end()))
        if NO_OTHERS.search(block):
            if sides[0] is not None and sides[0] > 0 and sides[1] is None and not re.search(OPPOSITION, block, re.I):
                sides[1] = 0
            elif sides[1] is not None and sides[1] > 0 and sides[0] is None and not re.search(SUPPORT, block, re.I):
                sides[0] = 0
        counts.append(tuple(sides))
        rules.append("independent_sides" if all(value is not None for value in sides) else "one_side")
    result.update(
        votes_for=sum(pair[0] for pair in counts) if all(pair[0] is not None for pair in counts) else None,
        votes_against=sum(pair[1] for pair in counts) if all(pair[1] is not None for pair in counts) else None,
        evidence=" || ".join(dict.fromkeys(excerpts)), rule="; ".join(dict.fromkeys(rules)),
    )
    if result["votes_for"] is not None and result["votes_against"] is not None:
        result["status"] = "resolved" if len(blocks) == 1 else "multiple_hearings"
    elif result["votes_for"] is not None or result["votes_against"] is not None:
        result["status"] = "partial"
    elif re.search(rf"\b{NUMBER} speakers?\b", text, re.I):
        result["status"] = "total_without_polarity"
    return result
