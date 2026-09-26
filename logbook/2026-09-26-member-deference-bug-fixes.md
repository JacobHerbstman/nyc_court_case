---
title: "Member-deference pipeline corrections"
author: "Research note prepared by Claude for Jacob Herbstman and Tyler Jacobson"
date: "September 26, 2026"
---

A review of the Council member-deference pipeline found eleven correctness
problems, from the choice of each matter's final action to how abstentions and
companion matters were counted. After fixing them at their producers, the count
of land-use events adopted over a local member's roll-call opposition falls from
37 to 31 over 1998-2025, and the number of events with an observed local
position rises from 4,688 to 4,799. Much of the old count stands: every old
override that remains is still an override. Six of the 37 go away. Three were
abstention-only votes, and three were companion matters of a project already
counted, which the new event grouping merges. The new, easier-to-read series
(among events the local member opposed, the share adopted anyway) is 31 of 89
over the whole period. Its trailing five-year value peaks near 73% around 2011
and falls to about 8-20% after 2019. Annual denominators are small (0-11
opposed events a year), so the annual shares are noisy.

## Which action is final

Legistar lists a matter's history newest date first, but actions on the same
day are not listed in the order they happened. The old code sorted by date and
sequence and kept the last row, which picked the earliest-listed action on the
final date: often "Approved by Committee" instead of "Approved, by Council".
Simply taking history sequence 1 is not right either. On a Council meeting day
the introduction and referral rows for a companion resolution, or the day's
committee vote, can be listed above the Council vote (LU 0262-2002 lists
"Referred to Comm by Council" above its "Disapproved by Council" vote). The vote
panel now takes, on the latest date, the first-listed substantive City Council
action. If there is none, it takes the first substantive action, and failing
that the first action. Introductions, referrals, hearings, and layovers do not
count as substantive. The final action changes for 3,432 of 15,006 matters. The
nonapproval vote fetch now targets 585 matters instead of 491. All 167
disapproved matters are now included (previously 142). The rest are 231 matters
filed by the Council (previously 187) and 187 withdrawal filings (previously
162). That required 97 more public Legistar action pages on the nonapproval side, plus 29 matter pages for geography verification. The approval-side
roll-call fetch keeps the latest Council approval event (lowest sequence). This
changes the page for two 2003 call-ups (M 0745-2003, M 0746-2003). Their new
target is a consent entry with no roll call, so these two matters lose a 48-0
vote. Five cached pages saved for the superseded events were moved to
`superseded` folders, and fetchers now check that each saved page carries the
HistoryDetail ID being requested.

## Roster and vote matching

The old roster let Wikipedia rows govern 26 districts, read year-only Wikipedia
terms a year late, and kept only "Council Member" and "Speaker" titles. That
left out Farías (D18, 2024-25), Paladino (D19, 2025), Borelli, Powers, Cumbo,
Matteo, Ignizio and others while they held leadership titles. The roster now
uses official Legistar office records for every Council title except Public
Advocate: 547 rows, plus 5 whose district comes from a Wikipedia surname match
(Duane, Weiner, Lasher, Fusco, Como). Three rows are corrected in a small
committed ledger. Brannan appears in District 47 in Legistar while Treyger and
then Kagan held it, but he represented District 43 through 2023. Grodenchik's
listed start overlaps Mark Weprin; it is moved to his first roll-call vote.
Wikipedia fills only three gaps: Sidamon-Eristoff (D4, 1998-99), Fiala (D51,
1998-2001), and Narcisse (D46, 2024-25). All other gaps are vacancies. Pedro
Espada, Jr. (2003) has no recoverable district and stays out. District 1 now
reads Freed to 2001, Gerson 2002-2009, Chin 2010-2021, and Marte from 2022.

Local members are now matched to roll calls by Legistar `person_id`, with one
shared rule on both the approval and nonapproval sides. The three rows without a
person id, the Wikipedia gap fills, match on a unique surname. This removes the
name-key collisions (for example "Rafael L. Espinal, Jr." and "Rafael Salamanca,
Jr." both reduced to "rafael jr"). On the approval side, roll calls where no
local member could be found fall from 349 to 55, and partial matches fall from
42 to 7.

## What counts as opposition and adoption

Adoption now comes from the matter's disposition, not from which vote page was
fetched. Direction now depends on the motion. A yes vote on a disapproval
resolution, on a Council approval of a disapproved matter, or on a filing is a
vote against the project. This was previously handled only by a hard-coded
exclusion for the Hamilton Avenue transfer station, which is now removed. LU
0468-2005, the East 91st Street transfer station, now shows Speaker Miller
opposing and the project disapproved. The old code counted it as adopted with
local support. Hamilton Avenue (LU 0470-2005) shows the local member supporting
and the project disapproved, so it is not an override. An adopted "Resolution
disapproving ..." is treated as a disapproved project.

Abstentions are now treated like Excused or Recused: no position is observed.
This follows the decision set in the brief for this pass, because Council
abstentions usually look like recusals. Only four matters in the panel have a
local abstention, forming three events, and all three were overrides:
Moskowitz (LU 0283-2004), A. King (LU 0747-2012), and Yeger (LU 0201/0202-2018).
All three drop out, and the abstainers stay countable in `local_member_abstain`.

Following the same brief, one local no vote still counts as opposition on items that
touch several districts. The panel now records `n_affected_districts`. Three of
the 31 overrides touch more than one district. LU 0810-2008, an HPD item across
nine districts, is an override because of Mealy's no vote. The other two are the
1998 LU 0170/0171 pair (two districts) and the 2016 LU 0341-0344 group, where
Barron voted no and Espinal voted yes. No threshold exclusion was applied.

## Events and geography

Events are now connected components of matters in the same query year that share
any ZAP project id or application key. On the same final sample, the old
string-based grouping gives 4,934 events and the new grouping gives 4,813. The
2021 Blood Center, the 2009 Dock Street matters, and the 2000 Staten Island
zoning matters on which Fiala voted no are now each one event. The same "any no
vote" rule applies within an event: 18 events have both supporting and opposing
matters, and they count as opposed. Fourteen events have matters with
conflicting outcomes and are left out. Most pair an adopted call-up motion with
a disapproved application.

The shared Council-district text parser now expands ranges ("CD 33-37"). It
reads "CD #32" and "22CD", and it stops reading "Queens CD 3" or "within CD 7" as
Council districts. A bare "CD" can mean Community District. It is therefore
accepted only when the text names a Community Board separately or when the
number is above 18, since no Community District has a higher number. "Queens,
22CD, CD1" now gives District 22 alone. With the same parser used for the
Legistar matter index, district assignments change for 162 approval roll calls.
Of those, 110 previously missing rows gain a district (mostly "CB#3, CD#36"
agenda notes), and 13 become missing. Borough parsing for nonapproval BBL
recovery now requires a unique borough name; this changes one title and no
recovered districts. The application-number pattern is now defined once, for six-
to eight-digit numbers. Vote dates are ISO throughout. Verification now covers
all 159 unresolved nonapproval matters, not only the 130 on the committed review
list, and stops on any failed download. The conservative nonapproval list has
467 matters with accepted geography (previously 398) and 118 still pending
review.

## Remaining concerns

These were outside the scope of this pass. Geography recovery is still
asymmetric: nonapproval matters get BBL, address, and official-page recovery,
but approval matters do not. Current MapPLUTO Council districts, drawn on 2023
lines, are used as a fallback for historical votes. The matter universe still
includes call-up motions and other procedural matters, whose adoption is not a
project decision. Final votes do not show earlier bargaining or withdrawals.

Reproduction: `make` in
`tasks/summarize_council_land_use_decision_trends/code` rebuilds the chain from
the saved Legistar pages; a second `make` rebuilds nothing. The baseline
snapshot was taken before the edits on September 26, 2026.
