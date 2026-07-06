#!/usr/bin/env python3

import csv
import re
from pathlib import Path


# setwd("tasks/audits/audit_ulurp_cpc_text_signals/code")


NO_OPPOSITION = re.compile(
    r"\b(no opposition|there was no opposition|there were no speakers?.{0,40}opposition|"
    r"no one.{0,40}opposition|0 opposed|0 in opposition|0 opposition|none opposed|"
    r"none in opposition|without opposition|not (?:be )?opposed|would not be opposed|"
    r"no objection|as opposed to)\b",
    re.IGNORECASE,
)

PROCEDURAL_COUNCIL = re.compile(
    r"\b(transmitted to the city council|filed with.{0,80}city council|"
    r"referred to.{0,80}city council|approved by.{0,80}city council|"
    r"city council.{0,80}approved|pursuant to section 197-d|"
    r"office of the speaker.{0,80}city council)\b",
    re.IGNORECASE,
)

NON_GOVERNMENT_COUNCIL = re.compile(
    r"\b(parks council|planning council|joint planning council|"
    r"community council|tenant council|council on|district council of|"
    r"central labor council)\b",
    re.IGNORECASE,
)

SPECIAL_PERMIT_OR_DOCUMENT_REVISION = re.compile(
    r"\b(grant of a special permit.{0,120}modify|to modify.{0,80}requirements|"
    r"to modify .{0,80}regulations|"
    r"requires? modifications? of .{0,80}regulations|"
    r"requires? the modification of .{0,80}regulations|"
    r"requested .{0,40}bulk modifications|"
    r"bulk modifications shall have minimal adverse effects|"
    r"such bulk modifications|"
    r"modifications specifically granted|modifications herein granted|"
    r"except for the modifications|special permit modifications|"
    r"modifications? of use or bulk regulations|last date revised|latest revision|"
    r"application requests? a modification|"
    r"authorizations? for modification|"
    r"modification of existing topography|"
    r"modification of grades necessitated|"
    r"revised certificate of occupancy|"
    r"modified landmarks transfer|"
    r"waterfront revitalization program .{0,80}as amended|"
    r"application.{0,80}amendment of the zoning map|"
    r"application.{0,80}zoning map amendment|"
    r"amended .{0,40}urban renewal plan|"
    r"amended .{0,40}urban renewal project|"
    r"amended .{0,40}urban renewal area|"
    r"amended cpurp|"
    r"amended .{0,30}cpurp|"
    r"pursuant to .{0,60}amended cpurp|"
    r"prior action to map|"
    r"modified land use approvals|"
    r"modified bulk regulations|"
    r"special provisions for bulk modification|"
    r"this modification will allow|"
    r"revised .{0,30}alternative|"
    r"rezoning changed most|"
    r"would be changed from|"
    r"have become .{0,80}as .{0,40}changed|"
    r"needs .{0,80}changed|"
    r"revised scope of work|"
    r"no significant effect.{0,80}upon modification)\b",
    re.IGNORECASE,
)

CONCRETE_CONDITIONAL_APPROVAL = re.compile(
    r"\b(approve|approved|approval|recommend(?:ed)? approval).{0,80}"
    r"(with modifications?\s*/\s*conditions?|with conditions?|with the condition that|subject to|provided that|unless)\b|"
    r"\b(with modifications?\s*/\s*conditions?|with conditions?|with the condition that|subject to|provided that|unless).{0,80}"
    r"(approve|approved|approval|recommend(?:ed)? approval)\b",
    re.IGNORECASE,
)

CONDITION_DETAIL = re.compile(
    r"\b(condition|conditions|roof|repair|fencing|screening|landscap|parking|"
    r"hours|loading|access|traffic|mitigation|restrictive declaration|agreement|"
    r"memorandum|shall|must|require)\b",
    re.IGNORECASE,
)

FORM_ONLY_CONDITIONS = re.compile(
    r"\b(attach additional sheets?|recommendation attached|recommendation to follow|"
    r"explanation of recommendation[- ]modifications?/conditions?)\b",
    re.IGNORECASE,
)

CONCRETE_REVIEW_CHANGE = re.compile(
    r"\b(proposal was modified|application was modified|modified application|"
    r"modified special permit application|a-application|changes? to the application include|"
    r"\b[cn]\s*\d{6}\s*\(a\)|"
    r"elimination of .{0,80}from the proposed|remove the .{0,80}from the proposed|"
    r"in response to concerns? raised by|in response to a comment received at the public hearing|"
    r"in response to these concerns|applicants? revised|applicant revised|"
    r"revised the applications?|modified the applications?|submitted .{0,60}\\(a\\)|"
    r"modified to seek)\b",
    re.IGNORECASE,
)

REVISION_TRUE = re.compile(
    r"\b(after the public hearing|subsequent to|in response to|at the request of|"
    r"as requested by|applicant.{0,100}(agreed|committed|revised|modified|changed|reduced)|"
    r"agreed to|committed to|scaled back|reduced|reducing|shrink|shrinking|"
    r"modified proposal|modified application|modification of the scope|"
    r"revised proposal|revised application|revised plans?)\b",
    re.IGNORECASE,
)

OPPOSITION_DIRECT = re.compile(
    r"\b(oppos(?:e|es|ed|ing|ition)|object(?:ed|ion|ions)?|against the application|"
    r"spoke against|testified against|speakers? in opposition)\b",
    re.IGNORECASE,
)

CONCERN_CONTEXT = re.compile(
    r"\b(community board|borough president|community|resident|residents|speaker|"
    r"civic|association|tenant|neighbors?|elected official|council member|"
    r"councilmember|city council)\b.{0,160}\b(concern|concerns|concerned)\b|"
    r"\b(concern|concerns|concerned)\b.{0,160}\b(community board|borough president|"
    r"community|resident|residents|speaker|civic|association|tenant|neighbors?|"
    r"elected official|council member|councilmember|city council)\b",
    re.IGNORECASE,
)

PUBLIC_CONCERN_CONTEXT = re.compile(
    r"\b(speakers?|participants?|testimony|testified|public hearing|community board|"
    r"\bcb\s?\d+|borough president|board|senator|assembly member|council member|"
    r"councilmember|resident|residents|community|neighbors?|public|civic|association)\b"
    r".{0,180}\b(concern|concerns|concerned)\b|"
    r"\b(concern|concerns|concerned)\b.{0,180}\b(speakers?|participants?|testimony|"
    r"testified|public hearing|community board|\bcb\s?\d+|borough president|board|"
    r"senator|assembly member|council member|councilmember|resident|residents|"
    r"community|neighbors?|public|civic|association)\b|"
    r"\bi have .{0,40}concerns?\b",
    re.IGNORECASE,
)

INTERNAL_CONCERN_CONTEXT = re.compile(
    r"\b(commissions?|commission|department|dcp staff|planning staff|agency|mta)\b.{0,120}"
    r"\b(concern|concerns|concerned)\b|"
    r"\b(concern|concerns|concerned)\b.{0,120}"
    r"\b(commissions?|commission|department|dcp staff|planning staff|agency|mta)\b|"
    r"\b(other environmental concerns|primary issues of concern analyzed for consistency)\b",
    re.IGNORECASE,
)

SECTION_OR_ACTOR_ATTRIBUTION = {
    "attribution_community_board": re.compile(
        r"\bcommunity board\b.{0,180}\b(request\w*|concern\w*|condition\w*|recommend\w*|"
        r"approv\w*|disapprov\w*|oppos\w*|support\w*|vot\w*|resolution)\b|"
        r"\b(request\w*|concern\w*|condition\w*|recommend\w*|approv\w*|"
        r"disapprov\w*|oppos\w*|support\w*|vot\w*|resolution)\b.{0,180}\bcommunity board\b",
        re.IGNORECASE,
    ),
    "attribution_borough_president": re.compile(
        r"\bborough president\b.{0,180}\b(request\w*|concern\w*|condition\w*|recommend\w*|"
        r"approv\w*|disapprov\w*|oppos\w*|support\w*|modif\w*|urg\w*|consult\w*)\b|"
        r"\b(request\w*|concern\w*|condition\w*|recommend\w*|approv\w*|"
        r"disapprov\w*|oppos\w*|support\w*|modif\w*|urg\w*|consult\w*)\b.{0,180}\bborough president\b",
        re.IGNORECASE,
    ),
    "attribution_council_member": re.compile(
        r"\b(council member|councilmember|local city council member|city council|"
        r"the council)\b.{0,180}\b(request\w*|concern\w*|condition\w*|recommend\w*|"
        r"support\w*|oppos\w*|modif\w*|consult\w*|meeting|met|urg\w*)\b|"
        r"\b(request\w*|concern\w*|condition\w*|recommend\w*|support\w*|"
        r"oppos\w*|modif\w*|consult\w*|meeting|met|urg\w*)\b.{0,180}\b(council member|councilmember|"
        r"local city council member|city council|the council)\b",
        re.IGNORECASE,
    ),
    "attribution_civic_group": re.compile(
        r"\b(civic|association|community organization|community group|tenant association|"
        r"neighborhood association)\b.{0,180}\b(request\w*|concern\w*|condition\w*|"
        r"recommend\w*|support\w*|oppos\w*|object\w*|consult\w*|determin\w*)\b|"
        r"\b(request\w*|concern\w*|condition\w*|recommend\w*|support\w*|"
        r"oppos\w*|object\w*|consult\w*|determin\w*)\b.{0,180}"
        r"\b(civic|association|community organization|community group|tenant association|"
        r"neighborhood association)\b",
        re.IGNORECASE,
    ),
    "attribution_applicant": re.compile(
        r"\bapplicant\b.{0,180}\b(agreed|committed|revised|modified|changed|"
        r"reduced|met with|meeting with|responded)\b|"
        r"\b(agreed|committed|revised|modified|changed|reduced|met with|"
        r"meeting with|responded)\b.{0,180}\bapplicant\b",
        re.IGNORECASE,
    ),
}

CONDITIONS_TRUE = re.compile(
    r"\b(applicant shall|restrictive declaration|terms and conditions|"
    r"points of agreement|memorandum of understanding|committed to|commitment|agreed to|"
    r"shall provide|shall be required|letter of intent)\b",
    re.IGNORECASE,
)

CONDITIONS_FALSE = re.compile(
    r"\b(shall conform to all applicable provisions of the zoning resolution|"
    r"except for the modifications)\b",
    re.IGNORECASE,
)


def clean_text(row):
    text = " ".join((row["sentence"] + " " + row["context"]).split())
    return re.sub(r"\bcommuni\s+ty\b", "community", text, flags=re.IGNORECASE)


def label_revision(text):
    if SPECIAL_PERMIT_OR_DOCUMENT_REVISION.search(text):
        return "0", "high", "Requested zoning relief, plan/date revision, or legal modification boilerplate."
    if CONCRETE_REVIEW_CHANGE.search(text):
        return "1", "high", "Review-stage project/application change or A-application language appears."
    if CONCRETE_CONDITIONAL_APPROVAL.search(text) and CONDITION_DETAIL.search(text):
        if FORM_ONLY_CONDITIONS.search(text) and not re.search(
            r"\b(roof|repair|fencing|screening|landscap|parking|hours|loading|access|"
            r"traffic|mitigation|restrictive declaration|agreement|memorandum|shall|must|require)\b",
            text,
            re.IGNORECASE,
        ):
            return "0", "high", "Condition form is present, but no substantive condition is visible."
        return "1", "medium", "Review body approved or recommended approval with concrete conditions/modifications."
    if REVISION_TRUE.search(text):
        return "1", "high", "Sentence describes a proposal change, reduction, or applicant concession."
    if re.search(r"\b(revised|modified|modification|amended|changed)\b", text, re.IGNORECASE):
        return "unclear", "medium", "Revision word appears, but the sentence does not clearly show a negotiated project change."
    return "0", "medium", "No clear project revision or concession in the sentence/context."


def label_council(text, signal_family):
    if PROCEDURAL_COUNCIL.search(text):
        return "0", "high", "Council appears in procedural approval, referral, filing, or transmission language."
    if re.search(
        r"\bbefore final city council approval\b|\bcity council phase\b|"
        r"\bcommunity board phase, borough president, city planning commission and city council\b",
        text,
        re.IGNORECASE,
    ):
        return "0", "high", "Council appears only as procedural timing or process-stage language."
    if re.search(
        r"\bcity council\b.{0,120}\b(obtain written commitments|should obtain|"
        r"must receive|modify the application|should modify|conditionally approve)\b|"
        r"\b(obtain written commitments|should obtain|must receive|modify the application|"
        r"should modify|conditionally approve)\b.{0,120}\bcity council\b|"
        r"\bcity council members\b.{0,120}\b(recognized|regular meetings|commitments)\b",
        text,
        re.IGNORECASE,
    ):
        return "1", "high", "Council/member is tied to commitments, modifications, or conditional approval."
    if NON_GOVERNMENT_COUNCIL.search(text) and not re.search(
        r"\b(city council|council member|councilmember|the council)\b",
        text,
        re.IGNORECASE,
    ):
        return "0", "high", "Council word refers to a non-City-Council organization."
    if SECTION_OR_ACTOR_ATTRIBUTION["attribution_council_member"].search(text):
        if signal_family == "attribution_council_member":
            return "1", "high", "Sentence attributes a position, request, concern, condition, or consultation to Council/member."
        return "1", "high", "Council/member appears as a substantive actor."
    if re.search(r"\b(council member|councilmember|city council|the council)\b", text, re.IGNORECASE):
        return "unclear", "medium", "Council/member is mentioned, but substantive role is ambiguous."
    return "0", "medium", "No substantive Council/member role in the sentence/context."


def label_opposition(text, signal_family, section):
    if NO_OPPOSITION.search(text):
        return "0", "high", "Sentence says there was no opposition or zero opposed."
    if OPPOSITION_DIRECT.search(text):
        return "1", "high", "Direct opposition/objecting/against language appears."
    if PUBLIC_CONCERN_CONTEXT.search(text):
        return "1", "medium", "Concern language is tied to public testimony, a review body, or elected official."
    if INTERNAL_CONCERN_CONTEXT.search(text):
        return "0", "high", "Concern language is internal agency/CPC review language, not public opposition."
    if section in {"community_board", "borough_president"} and re.search(
        r"\b(concern|concerns|concerned)\b",
        text,
        re.IGNORECASE,
    ):
        return "1", "medium", "Concern language appears in a Community Board or Borough President section."
    if CONCERN_CONTEXT.search(text):
        return "1", "medium", "Concern language is tied to a public actor or review body."
    if "concern" in text.lower():
        return "unclear", "medium", "Concern language appears, but public opposition is ambiguous."
    return "0", "medium", "No clear opposition language."


def label_attribution(text, signal_family):
    if signal_family == "attribution_council_member":
        return label_council(text, signal_family)
    if signal_family == "attribution_unspecified":
        if re.search(
            r"\b(in response to concerns|in response to comments|at the request of|"
            r"as requested by|after meeting with|in consultation with)\b",
            text,
            re.IGNORECASE,
        ):
            return "1", "medium", "Attribution phrase appears, but the actor may be unnamed or broad."
        return "0", "medium", "No clear attribution phrase."

    if signal_family == "attribution_applicant" and re.search(
        r"\bapplicant\b.{0,160}\b(filed|is filed|submitted|is being submitted|"
        r"request|seeks|is seeking|requested|requests|requesting|proposes|is proposing|"
        r"requested an? (amendment|approval|special permit|authorization))\b",
        text,
        re.IGNORECASE,
    ) and not re.search(
        r"\bapplicant\b.{0,180}\b(agreed|committed|revised|modified|changed|"
        r"responded|addressed|willing|restated their commitment)\b",
        text,
        re.IGNORECASE,
    ):
        return "0", "high", "Applicant is only described as filing/seeking approval, not making a concession."
    if signal_family == "attribution_applicant" and re.search(
        r"\bapplicant'?s request\b|\bapplicant\b.{0,120}\bhas not met\b",
        text,
        re.IGNORECASE,
    ):
        return "0", "high", "Applicant is only the requester or object of criticism, not making a concession."
    if signal_family == "attribution_applicant" and re.search(
        r"\bapplicant\b.{0,180}\b(willing to comply|willing to return|"
        r"reiterated concerns|addressed these concerns|restated their commitment|continue .{0,60}working with|"
        r"shall|must|be required to|extend use|address community concerns)\b",
        text,
        re.IGNORECASE,
    ):
        return "1", "medium", "Applicant is tied to a commitment, required action, or response to review concerns."

    pattern = SECTION_OR_ACTOR_ATTRIBUTION.get(signal_family)
    if pattern and pattern.search(text):
        return "1", "medium", "Sentence attributes a request, concern, recommendation, condition, or position to the named actor type."

    return "unclear", "medium", "Actor appears, but attribution of a concrete request/concern/condition is ambiguous."


def label_conditions(text, signal_family):
    if signal_family in {"restrictive_declaration", "points_of_agreement", "dollar_terms"}:
        return "1", "high", "Sentence contains the named legal/monetary term."
    if CONDITIONS_FALSE.search(text):
        return "0", "high", "Generic zoning-resolution conformance or modification boilerplate."
    if CONDITIONS_TRUE.search(text):
        return "1", "high", "Sentence contains a condition, commitment, agreement, or restrictive declaration."
    if re.search(r"\b(shall|condition|commitment|agreed)\b", text, re.IGNORECASE):
        return "unclear", "medium", "Condition-like language appears, but substantive commitment is ambiguous."
    return "0", "medium", "No clear condition or commitment."


def label_community_board(text, signal_family):
    if signal_family == "community_board_disapproval":
        if re.search(
            r"\b(city planning commission resolution|hereby disapproved)\b",
            text,
            re.IGNORECASE,
        ) and not re.search(
            r"\bcommunity board\b.{0,200}\b(disapprov\w*|recommend(?:ed)? disapproval|"
            r"voted against|oppos\w*)\b",
            text,
            re.IGNORECASE,
        ):
            return "0", "high", "Disapproval is CPC parcel-resolution language, not a Community Board disapproval."
        if re.search(
            r"\bcommunity board\b.{0,220}\b(disapprov\w*|voted against|"
            r"recommend(?:ed)? disapproval|oppos\w*)\b|"
            r"\b(disapprov\w*|recommend(?:ed)? disapproval|oppos\w*)\b.{0,220}"
            r"\bcommunity board\b|"
            r"\bapplication was disapproved by both the community board\b|"
            r"\brecommendation .{0,120}\bdisapprove\b",
            text,
            re.IGNORECASE,
        ):
            return "1", "high", "Community Board disapproval/opposition is stated."
        return "unclear", "medium", "Community Board appears, but disapproval is ambiguous."

    if signal_family == "community_board_conditioned_approval":
        if re.search(
            r"\bcommunity board\b.{0,260}\b(approv\w*|recommend\w*|voted in favor)"
            r".{0,140}\b(condition\w*|provided that|subject to|unless|with the following conditions)\b|"
            r"\bcommunity board\b.{0,220}\brequested .{0,80}\bconditions of its approval\b|"
            r"\bcommunity board\b.{0,260}\b(condition(?:al|ally)? approv\w*|approval with conditions)\b|"
            r"\bcondition(?:ed)? (?:our |its )?approval\b|"
            r"\bconditions? (?:were|was) approved by the community board\b|"
            r"\bapprove the application with the same conditions set forth by community board\b",
            text,
            re.IGNORECASE,
        ):
            return "1", "high", "Community Board conditional approval is stated."
        return "unclear", "medium", "Community Board approval/conditions appear, but conditional approval is ambiguous."

    return "unclear", "medium", "Community Board rule not recognized."


def label_row(row):
    signal_family = row["signal_family"]
    text = clean_text(row)

    if signal_family == "revision_concession":
        return label_revision(text)
    if signal_family == "substantive_council_member":
        return label_council(text, signal_family)
    if signal_family.startswith("opposition_"):
        return label_opposition(text, signal_family, row["section"])
    if signal_family.startswith("attribution_"):
        return label_attribution(text, signal_family)
    if signal_family in {
        "conditions_commitments",
        "restrictive_declaration",
        "points_of_agreement",
        "dollar_terms",
    }:
        return label_conditions(text, signal_family)
    if signal_family in {
        "community_board_disapproval",
        "community_board_conditioned_approval",
    }:
        return label_community_board(text, signal_family)

    return "unclear", "low", "No assistant labeling rule for this signal family."


def read_human_labels():
    labels = {}
    with Path("manual_ulurp_cpc_text_signal_kwic_labels.csv").open(newline="", encoding="utf-8") as input_file:
        for row in csv.DictReader(input_file):
            labels[(row["signal_family"], row["application_number"])] = row
    return labels


def main():
    human_labels = read_human_labels()

    with Path("../output/ulurp_cpc_text_signal_kwic_sample.csv").open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        input_fieldnames = reader.fieldnames or []
        rows = list(reader)

    output_fieldnames = input_fieldnames + [
        "assistant_true_positive",
        "assistant_confidence",
        "assistant_reason",
        "human_confidence",
        "human_review_id",
    ]

    with Path("../output/ulurp_cpc_text_signal_kwic_assistant_labels.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output_file:
        writer = csv.DictWriter(output_file, fieldnames=output_fieldnames)
        writer.writeheader()
        for row in rows:
            assistant_true_positive, assistant_confidence, assistant_reason = label_row(row)
            human_label = human_labels.get((row["signal_family"], row["application_number"]))
            if human_label:
                row["manual_true_positive"] = human_label["human_true_positive"]
                row["manual_reason"] = human_label["human_reason"]
                row["human_confidence"] = human_label["human_confidence"]
                row["human_review_id"] = human_label["review_id"]
            else:
                row["human_confidence"] = ""
                row["human_review_id"] = ""
            row["assistant_true_positive"] = assistant_true_positive
            row["assistant_confidence"] = assistant_confidence
            row["assistant_reason"] = assistant_reason
            writer.writerow(row)


if __name__ == "__main__":
    main()
