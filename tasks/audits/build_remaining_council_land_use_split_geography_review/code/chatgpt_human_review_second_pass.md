# Second Pass On Human-Review Rows

This records a focused ChatGPT second pass on the 10 rows still marked `needs_human_review`
after the first completed split-vote geography review ledger.

The prompt instructed ChatGPT to use only the provided Legistar/CPC evidence unless it
explicitly identified an additional official source to check, and to avoid assigning Council
districts from community districts alone.

| review_id | verdict | districts | confidence | reason | official_check_needed |
| --- | --- | --- | --- | --- | --- |
| rem_split_geo_0015 | INCLUDE_AS_PROJECT_GEOGRAPHY | 3 | medium | The text amendment is tied by official companion matters to the 360 West 43rd Street project, so "CD#4" should be read as Manhattan Community District 4, not Council District 4. | No |
| rem_split_geo_0016 | INCLUDE_AS_PROJECT_GEOGRAPHY | 3 | medium | Official companion records give the 360 West 43rd Street site and BBLs; CD 3 is acceptable as externally coded from site geography. | No |
| rem_split_geo_0027 | INCLUDE_AS_PROJECT_GEOGRAPHY | 33;34 | medium | The Greenpoint-Williamsburg rezoning is an official 183-block project-area action, and the provided evidence identifies the affected local members as Yassky and Reyna. | No |
| rem_split_geo_0030 | INCLUDE_AS_PROJECT_GEOGRAPHY | 23;24;27;28 | medium | The Jamaica Plan is an official large-area rezoning, and the provided companion/local-member evidence supports the four-district assignment. | No |
| rem_split_geo_0057 | INCLUDE_AS_PROJECT_GEOGRAPHY | 44 | medium | Maple Lane Views has an official site boundary; CD 44 is acceptable as externally coded from that site rather than from Community District 12 alone. | No |
| rem_split_geo_0067 | INCLUDE_AS_PROJECT_GEOGRAPHY | 5 | medium | The official record gives the site-specific 203-205 East 92nd Street address, so CD 5 is acceptable as externally coded from project geography. | No |
| rem_split_geo_0134 | NEEDS_MORE_OFFICIAL_CHECK |  | high | The JFK AirTrain matter is a real corridor/property action, but the provided evidence gives only Queens Community Districts and no historical Council District bridge. | Check full CPC property schedules/maps or historical 1999 Council district overlay for the affected parcels/corridor. |
| rem_split_geo_0135 | NEEDS_MORE_OFFICIAL_CHECK |  | high | Same as the companion AirTrain matter: the provided evidence supports corridor geography but not a defensible Council District assignment. | Check full CPC property schedules/maps or historical 1999 Council district overlay for the affected parcels/corridor. |
| rem_split_geo_0296 | NEEDS_MORE_OFFICIAL_CHECK | 24;27;28? | high | This appears related to the Jamaica Plan, but the provided evidence does not prove whether LU 0500/Res 1053 shares the full four-district geography or a narrower subset. | Check LU 0500/Res 1053 full official text and any CPC exhibits for the specific Jamaica Gateway Urban Renewal Plan component. |
| rem_split_geo_0301 | INCLUDE_AS_MIXED_PROJECT_GEOGRAPHY | 33;34 | medium | The Broadway Triangle records identify related project components in both CD 33 and CD 34, so the bundle should remain multi-district rather than collapsed. | No |

Codex follow-up: the ledger promotes rows 0015, 0016, 0027, 0030, 0057, 0067, and 0301
out of human review. Rows 0134, 0135, and 0296 remain flagged.
