# Tyler CPC coding discrepancies for review

Scope: the 160 rows in `cpc_llm_training_labels_tyler.xlsx` with `coding_complete == 1`. Rows Tyler had not completed were excluded. I reviewed the linked CPC report narratives against the workbook codebook and the one-page coding example. The original workbook has not been changed.

The checklist below contains high-confidence disagreements only. Page references are PDF page numbers in the linked report, which can differ from a page number printed inside an attached Community Board or Borough President report.

## Substantive coding discrepancies

- [ ] **T009 — PAL Arnold & Marie Schwartz Early Learn Center (C 160331 PQK).** Change `traffic_parking` **0 -> 1**, `procedural_response` **0 -> 1**, and `explicit_local_response` **0 -> 1**. The report says the Borough President raised a traffic-and-safety concern and ACS was looking into it with the appropriate agencies (PDF p. 3). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/160331.pdf)

Tyler Response: pretty minor concern that did not appear to affect approval

- [ ] **T011 — John Walter Edwards Apartments (C 910360 ZMK).** Change `local_request_condition` **0 -> 1** and `cb_request_or_opposition` **0 -> 1**. Community Board 16 conditionally approved subject to ending the construction lease and removing debris (PDF pp. 14-15). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/910360.pdf)

Tyler Response: not really opposition to the project. Removing debree is pretty basic

- [ ] **T012 — Commerce Bank 65th Street (C 060272 ZMK).** Change `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, `scale_character_preservation` **0 -> 1**, and `environment_open_space` **0 -> 1**. The CB requested R5B zoning to prevent out-of-scale development and a contaminated-soil/tank remediation plan; the report says the zoning change was included in a related rezoning and the remediation plan was required (PDF p. 5). `cb_request_or_opposition` is already correctly 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/060272.pdf)

Tyler Response: Both of these conditions are actually baked into the approval already. Made rec'd changes. 

- [ ] **T016 — Lynch Street Rezoning (C 950526 ZMK).** Change `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, and `bp_request_or_opposition` **0 -> 1**. At the Borough President's request, the applicant representative agreed to notify affected storage-use owners and then provided the notification letters (PDF p. 14). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/950526.pdf)

Notifying nearby owners seems minor. 

- [ ] **T024 — 213-227 West 28th Street Parking Special Permits (C 200013 ZSM).** Change `affordability_displacement` **0 -> 1**, `traffic_parking` **0 -> 1**, and `environment_open_space` **0 -> 1**. The CB and BP objections concerned parking in a transit-rich area, congestion/greenhouse-gas effects, and pricing/availability for affordable-housing residents (PDF pp. 3, 7-8). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/200013.pdf)

I added environmental concern.

- [ ] **T027 — Nelson Avenue Playground (C 970367 MMX).** Change `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, and `environment_open_space` **0 -> 1**. In response to the BP's noise/glare concern and requested design changes, Parks agreed to place a passive green area next to residences (PDF p. 3). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/970367.pdf)

Seems very minor

- [ ] **T028 — Special College Point District (N 090318 ZRQ).** Change `dev_direction` **none -> mixed**, `cb_request_or_opposition` **0 -> 1**, `civic_group_position` **none_or_procedural -> support_or_request**, `explicit_local_response` **0 -> 1**, `approved_unresolved_objection` **0 -> 1**, and `environment_open_space` **0 -> 1**. This is a substantive non-housing zoning change; the study began in response to CB and named civic/business-group concerns, and CPC expressly says the CB's request to reserve the former airport for open space/soft recreation was outside scope (PDF pp. 1, 18). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/090318.pdf)

Added changes in response.

- [ ] **T030 — C-O-P/Red Hook Stores (C 020048 PPK).** Change `local_request_condition` **0 -> 1** and `bp_request_or_opposition` **0 -> 1**. The report records the Borough President's approval with conditions and points to the related special-permit report for the details (PDF p. 4). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/020048.pdf)

- [ ] **T033 — C-O-P (C 960282 PPK).** Change `local_request_condition` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, `explicit_local_response` **0 -> 1**, and `approved_unresolved_objection` **0 -> 1**. The BP imposed use/clean-and-fence conditions; CPC expressly responded to and declined some of them while approving (PDF pp. 2-3). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/960282.pdf)

Made changes

- [ ] **T037 — Landing Road Rezoning (C 970578 ZMX).** Change `local_request_condition` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, and `traffic_parking` **0 -> 1**. The BP approved with a condition calling for a pedestrian-safety review and corrective sidewalk work (PDF p. 2). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/970578.pdf)

Made changes


- [ ] **T039 — South Avenue Retail Development (C 160174 ZSR).** Change `revision_or_concession` **0 -> 1**, `procedural_response` **0 -> 1**, and `traffic_parking` **0 -> 1**. Local residents raised traffic/foot-traffic concerns, and the applicant committed to routing measures plus post-opening traffic monitoring (PDF pp. 11, 14). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/160174.pdf)

Agree


- [ ] **T043 — 259 10th Avenue (C 110334 ZSM).** Change `local_request_condition` **0 -> 1** and `revision_or_concession` **0 -> 1**. The CB and BP conditionally approved; the applicant committed in writing to address both conditions, including a traffic study/mitigation (PDF pp. 3-4). `procedural_response` is already correctly 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/110334.pdf)

Agree

- [ ] **T044 — 551 West 21st Street Parking Garage (C 150110 ZSM).** Change `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, `cb_request_or_opposition` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, `traffic_parking` **0 -> 1**, and `scale_character_preservation` **0 -> 1**. CB/BP requested changes to signage, street presence, and bicycle parking; in response, the applicant revised the drawings to relocate bicycle parking (PDF pp. 4, 7). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/150110.pdf)

These seem minor

- [ ] **T048 — Ninth Street Rezoning (C 210348 ZMK).** Change `procedural_response` **0 -> 1** and `traffic_parking` **0 -> 1**. The record includes construction coordination and a community oversight task force, plus substantive parking/loading/road-user issues (PDF pp. 7-16). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/210348.pdf)

Agree

- [ ] **T051 — Powell Boulevard Apartments (C 770472 HOM).** Change `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, and `cb_request_or_opposition` **0 -> 1**. The CB's approval was expressly conditioned on a written memorandum governing minority contracting and information provision, and the developer agreed (PDF p. 4). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/770472.pdf)

Agree with request, but don't see evidence that the developer agreed


- [ ] **T052 — New York Wheel (C 150447 ZSR).** Change `bp_request_or_opposition` **0 -> 1** and `procedural_response` **0 -> 1**. The BP approved with detailed conditions, and the response included a traffic study/consultation and continuing dialogue (PDF pp. 24-30). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/150447.pdf)

Agree

- [ ] **T054 — 343 West 47th Street Demolition Special Permit (C 240244 ZSM).** Change `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, `cb_request_or_opposition` **0 -> 1**, `scale_character_preservation` **0 -> 1**, and `environment_open_space` **0 -> 1**. The CB requested neighbor/park protections; the applicant revised the design after CB feedback and signed a detailed commitment covering repairs, pest abatement, construction management, and park protection (PDF pp. 6-8, 26). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/240244.pdf)

Agree

- [ ] **T057 — C-O-P (C 920315 PPK).** Change `local_opposition` **0 -> 1**, `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, `approved_unresolved_objection` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, and `environment_open_space` **0 -> 1**. The BP disapproved with property-specific conditions; HPD agreed to reevaluate one parcel at the BP's insistence, while CPC expressly rejected or deferred other open-space/disposition requests (PDF pp. 3-5, 15). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/920315.pdf)

Agree


- [ ] **T059 — NBC of Negro Women Day Care Center (C 900685 PQX).** Change `local_request_condition` **0 -> 1** and `bp_request_or_opposition` **0 -> 1**. The BP recommended a shorter approval period and required specified repairs before/after lease execution (PDF pp. 3-4). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/900685.pdf)

Too minor

- [ ] **T064 — 137-61 Northern Boulevard (C 120403 ZMQ).** Change `local_request_condition` **0 -> 1**, `cb_request_or_opposition` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, `traffic_parking` **0 -> 1**, `scale_character_preservation` **0 -> 1**, and `approved_unresolved_objection` **0 -> 1**. CB and BP conditioned approval on site-plan/design changes addressing the adjacent landmark and traffic/parking; CPC said those conditions were beyond its purview (PDF p. 8). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/120403.pdf)

Agree

- [ ] **T065 — 1233 57th Street Rezoning (C 230117 ZMK).** Change `traffic_parking` **0 -> 1**. The report says neighborhood opposition led to revised plans that reduced parking, among other changes (PDF pp. 1-2). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/230117.pdf)

Agree

- [ ] **T066 — C-O-P/W. 184th Street Garage (C 970324 DMM).** Change `councilmember_position` **none_or_procedural -> support_or_request** and `traffic_parking` **0 -> 1**. Council Member Linares requested development of the garage, and the report discusses local concern about traffic congestion and parking (PDF pp. 7-10). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/970324.pdf)

Agree

- [ ] **T068 — Harlem NCP CB 11 (C 200277 HAM).** Change `local_request_condition` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, and `affordability_displacement` **0 -> 1**. The BP approved while requesting applicant conditions concerning qualification for affordable units and related housing access (PDF pp. 4-5). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/200277.pdf)

agree

- [ ] **T071 — C-O-P (10 Parcels) (C 980703 PPX).** Change `local_request_condition` **0 -> 1** and `bp_request_or_opposition` **0 -> 1**. The BP recommended approval with a modification that specified parcels be transferred to HPD to facilitate housing (PDF pp. 2-3). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/980703.pdf)

Agree

- [ ] **T073 — Eastpoint Development (C 960270 MEQ).** Change `traffic_parking` **0 -> 1** and `infrastructure_services` **0 -> 1**. Hearing participants raised traffic concerns and the need for community services (PDF pp. 13-15). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/960270.pdf)

Agree

- [ ] **T078 — 116-122 West 21st Street (C 030031 ZSM).** Change `local_request_condition` **0 -> 1**, `cb_request_or_opposition` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, and `traffic_parking` **0 -> 1**. CB/BP made parking-layout and capacity conditions for the garage (PDF pp. 3-4, 8-10). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/030031.pdf)

Changed

- [ ] **T080 — Lincoln Center New York Public Library (C 860383 PSM).** Change `speakers_against` **0 -> 1**, `local_opposition` **0 -> 1**, and `approved_unresolved_objection` **0 -> 1**. The CPC hearing had one opposing speaker who proposed relocating the branch instead; CPC approved without resolving that alternative (PDF pp. 3-4). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/860383.pdf)

Agree

- [ ] **T087 — 22-60 46th Street Rezoning (C 190267 ZMQ).** Change `scale_character_preservation` **0 -> 1**. Six neighboring homeowners opposed the project as out of scale with the lower-density context; the CB also imposed a height condition (PDF p. 11). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/190267.pdf)

Agree


- [ ] **T094 — NYPD Evidence Storage and Central Records (C 150188 PCK).** Change `procedural_response` **0 -> 1**. The BP's requested lease terms included annual reporting, monitoring, and consultation with the CB and elected officials (PDF pp. 3-4). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/150188.pdf)

Too minor

- [ ] **T096 — DUMBO Rezoning (C 090310 ZMK).** Change `infrastructure_services` **0 -> 1**. A named neighborhood organization opposed the proposal partly because of possible effects on schools (PDF p. 18). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/090310.pdf)

Agree

- [ ] **T097 — 1010 Pacific Street Zoning (C 180042 ZMK).** Change `infrastructure_services` **0 -> 1** and `environment_open_space` **0 -> 1**. An opposing resident asked that effects on schools, parks, and infrastructure be examined (PDF p. 10). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/180042.pdf)

Agree 

- [ ] **T100 — Alafia Street Mapping (C 240082 MMK).** Change `local_request_condition` **0 -> 1**, `cb_request_or_opposition` **0 -> 1**, and `bp_request_or_opposition` **0 -> 1**. The CB approved with detailed communication, pedestrian-safety, and quality-of-life conditions; the BP added a condition about street names (PDF pp. 5-6). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/240082.pdf)

Agree

- [ ] **T116 — 330 Jay Street (C 990677 ZSK).** Change `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, `councilmember_position` **none_or_procedural -> support_or_request**, and `civic_group_position` **none_or_procedural -> opposition**. The applicant modified traffic mitigation expressly to address CB/BP concerns; the report also identifies a councilmember's permit-zone effort and a named association's opposition (PDF pp. 19-26). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/990677.pdf)

Done


- [ ] **T123 — CPC report C 851020 PPQ.** Change `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, and `cb_request_or_opposition` **0 -> 1**. The CB approved disposition with restrictions, and part of the application was withdrawn after certification (PDF pp. 1-2). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/851020.pdf)


Done 

- [ ] **T125 — Special 4th Avenue Enhanced Commercial District (C 110386 ZMK).** Change `affordability_displacement` **0 -> 1** and `traffic_parking` **0 -> 1**. CB/BP recommendations substantively addressed affordable-housing incentives and parking requirements/waivers (PDF pp. 4-7). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/110386.pdf)

Done

- [ ] **T126 — First Amendment East New York I URP (C 910119 HUK).** Change `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, `councilmember_position` **none_or_procedural -> support_or_request**, `civic_group_position` **none_or_procedural -> support_or_request**, `affordability_displacement` **0 -> 1**, and `environment_open_space` **0 -> 1**. The BP requested park/site maintenance and tenant protections; EDC/Parks made related commitments, and the report credits the councilmember and civic groups with supporting the plan (PDF pp. 3-6, 19). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/910119.pdf)

Agree


- [ ] **T129 — Lexington Avenue Rezoning (C 960576 ZMM).** Change `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, and `councilmember_position` **none_or_procedural -> support_or_request**. The report says CB support was contingent on applicant commitments, which the BP says the applicant accepted; the district councilmember also supported the project (PDF pp. 5, 13). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/960576.pdf)

Agree

- [ ] **T135 — Bartow Avenue Animal Shelter (C 180346 PSX).** Change `traffic_parking` **0 -> 1** and `infrastructure_services` **0 -> 1**. Opposing residents raised traffic, mass-transit crowding, and inadequate community-center capacity (PDF p. 6). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/180346.pdf)

Agree

- [ ] **T140 — Ruelles (C 820751 TCM).** Change `traffic_parking` **0 -> 1**. The documented community objections included triple parking (PDF p. 2). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/820751.pdf)

Agree

- [ ] **T143 — Variety Boys and Girls Club Rezoning (C 180085 ZMQ).** Change `local_opposition` **0 -> 1**, `local_request_condition` **0 -> 1**, `cb_request_or_opposition` **0 -> 1**, `affordability_displacement` **0 -> 1**, `scale_character_preservation` **0 -> 1**, and `environment_open_space` **0 -> 1**. The CB vote included eight opposed and approval was conditioned on affordability, equitable unit distribution, reduced visual height impacts, landscaping, and environmentally neutral design (PDF p. 8). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/180085.pdf)

Agree

- [ ] **T147 — CPC report C 780413 HUK.** Change `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, `approved_unresolved_objection` **0 -> 1**, and `cb_request_or_opposition` **0 -> 1**. The proposal cut planned open space by 42%; in response to the CB concern, Parks said it would seek a replacement park, but delivery remained contingent on funding and approvals (PDF p. 3). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/780413.pdf)


Park concern is sortof unrelated

- [ ] **T148 — Fulton Park URP, 2nd Amendment (C 030300 ZMK).** Change `bp_request_or_opposition` **0 -> 1**. This report states that the Borough President approved with conditions, even though the detailed recommendation appears in the related URP report (PDF pp. 1-2). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/030300.pdf)

AGree


- [ ] **T153 — SPARC Kips Bay (C 240369 ZMM).** Change `procedural_response` **0 -> 1**, `councilmember_position` **none_or_procedural -> support_or_request**, `scale_character_preservation` **0 -> 1**, and `infrastructure_services` **0 -> 1**. A councilmember co-chaired the task force, and the CB conditions call for transit-service commitments, public-facility improvements, and design/sunlight protections (PDF pp. 10-11, 21-23). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/240369.pdf)

Agree

- [ ] **T154 — 2118 Avenue U (C 230351 ZMK).** Change `local_request_condition` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, `traffic_parking` **0 -> 1**, `scale_character_preservation` **0 -> 1**, and `approved_unresolved_objection` **0 -> 1**. The BP conditioned approval on increasing floor area/unit count and reducing or waiving parking; CPC approved without adopting those project-specific requests (PDF pp. 6-10). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/230351.pdf)


Agree

- [ ] **T155 — C-O-P (C 880076 PPK).** Change `local_request_condition` **0 -> 1** and `cb_request_or_opposition` **0 -> 1**. Community Board 5 adopted special, property-specific disposition recommendations (PDF pp. 2-4). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/880076.pdf)

Agree

- [ ] **T163 — 159 West 48th Street (C 090367 ZSM).** Change `revision_or_concession` **0 -> 1**. At the hearing, the applicant agreed to install rooftop screening to protect adjacent buildings from vehicle headlights (PDF p. 6). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/090367.pdf)

Too Minor

- [ ] **T166 — Crown Heights Brooklyn Public Library (C 940206 PCK).** Change `local_request_condition` **0 -> 1**, `procedural_response` **0 -> 1**, and `bp_request_or_opposition` **0 -> 1**. The BP's approval imposed conditions for regular consultation with the CB and monitoring/reporting on renovation performance (PDF pp. 3-5). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/940206.pdf)

Minor

- [ ] **T167 — 14-20 West 40th Street (C 080042 ZSM).** Change `environment_open_space` **0 -> 1**. Residents and a named neighborhood coalition opposed the project over shadows/daylight impacts on Bryant Park and the need for an EIS (PDF pp. 8-9). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/080042.pdf)

sure

- [ ] **T168 — CPC report C 770448 HDQ.** Change `revision_or_concession` **0 -> 1**. The report records the developer's agreement to use contractors from the identified minority association (PDF p. 3). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/770448.pdf)

Sure

- [ ] **T172 — 9th Avenue Bridge (C 950447 MMK).** Change `local_request_condition` **0 -> 1**, `revision_or_concession` **0 -> 1**, `explicit_local_response` **0 -> 1**, `cb_request_or_opposition` **0 -> 1**, and `traffic_parking` **0 -> 1**. DOT agreed to restore pavement expressly to address the CB's safety concern (PDF p. 3). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/950447.pdf)

Sure

- [ ] **T173 — C-O-P (C 960573 PPX).** Change `local_request_condition` **0 -> 1** and `cb_request_or_opposition` **0 -> 1**. The CB approved with property-use conditions, including preserving current zoning and accepting parking, residential, or child-care uses (PDF pp. 2-3). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/960573.pdf)

Minor

- [ ] **T174 — C-O-P/7 Parcels (C 980323 PPQ).** Change `cb_request_or_opposition` **0 -> 1**, `bp_request_or_opposition` **0 -> 1**, and `explicit_local_response` **0 -> 1**. Both CB and BP imposed property-specific disposition restrictions, and CPC expressly rejected the BP's accessory-community-facility restriction (PDF pp. 5-8). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/980323.pdf)

Agree

- [ ] **T180 — 703 Myrtle Avenue Rezoning (C 220453 ZMK).** Change `local_opposition` **0 -> 1**, `local_request_condition` **0 -> 1**, `cb_request_or_opposition` **0 -> 1**, and `scale_character_preservation` **0 -> 1**. The CB vote included nine opposed and conditioned support on a 95-foot height cap (PDF p. 6). [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/220453.pdf)



## Completion/evidence discrepancies

These rows are marked `coding_complete == 1` but do not satisfy the codebook's evidence requirements:

- [ ] Missing `evidence_pages`: **T074, T075, T082, T099, T159**.
- [ ] Missing `evidence_summary`: **T062, T063, T065-T069, T075, T077-T082, T087-T088, T096, T100-T101, T103-T105, T122, T133, T137, T143, T152-T155, T168, T175**.

## Coding convention to settle before editing

Two judgment calls recur across the sheet:

1. A conditioned approval is a positive `local_request_condition` and a positive CB/BP actor label even when the condition is minor and the actor ultimately supports the application. This follows the codebook note that conditioned approval counts.
2. `revision_or_concession` includes commitments and mitigation, not only a formally amended ULURP application. `explicit_local_response` is narrower and requires the report to link the response to a local request or concern.

## Final batch: T182-T200

Scope: T182-T200 in the current workbook. T181 was already included in the prior audit, so this batch contains 19 newly completed rows rather than 20. The checklist below is independent of the earlier recommendations and does not repeat T181.

- [ ] **T182 — Lower Broadway/LMM Study Tribeca (C 940309 ZMM).** Change `procedural_response` **0 -> 1** and `councilmember_position` **none_or_procedural -> support_or_request**. The BP requested a Chambers Street traffic study, and CPC reported that DCP had obtained funding to initiate one. A City Councilmember supported the rezoning while requesting a 10,000-square-foot retail cap (PDF pp. 13, 15, 17). Narrow `evidence_pages` from **1-20** to the operative material at **12-18**, and expand the summary to mention the local conditions, CPC modifications, and rejected or deferred demands. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/940309.pdf)


Agree

- [ ] **T183 — Fania Gersham House (C 851036 HDM).** Change `dev_direction` **upzone -> none** and `revision_or_concession` **0 -> 1**. The action disposes of an existing building for rehabilitation rather than adding zoning density. CPC requested white-painted facades to improve daylight, and the sponsor agreed and initiated the school-wall approval process (PDF p. 2). Fill the blank `evidence_summary` and `coding_confidence` (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/851036.pdf)

Agree

- [ ] **T184 — HRA Group Foster Home (C 900399 PQX).** Change `local_opposition` **1 -> 0** and `infrastructure_services` **0 -> 1**. The BP approved with lease, repair, landlord-performance, and complaint-response conditions; that is a local request about a public facility, not opposition. CPC approved without resolving those conditions (PDF pp. 3-4). Use **3-4** as the evidence pages and fill `coding_confidence` (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/900399.pdf)

Agree

- [ ] **T185 — Newtown Creek Nature Walk (C 160243 PSK).** Change `local_request_condition`, `revision_or_concession`, `procedural_response`, `explicit_local_response`, `approved_unresolved_objection`, `cb_request_or_opposition`, `bp_request_or_opposition`, `traffic_parking`, and `infrastructure_services` **0 -> 1**. CB/BP conditions covered lighting, trash, cyclist/pedestrian safety, a call box, and a bike lane; DEP agreed to the call box and undertook coordination on the bike lane, which CPC called beyond scope (PDF pp. 4-8). Use **4-8** as the evidence pages and fill the blank summary and confidence (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/160243.pdf)

Added for bike infrastructure

- [ ] **T186 — Kew Gardens Hills Estates (C 880040 MMQ).** Change `speakers_for` **0 -> 11**, `revision_or_concession` **0 -> 1**, and `traffic_parking` **1 -> 0**; `speakers_against = 0` is correct. The linked map-amendment report sends the joint-hearing testimony to the related special-permit report, which records 11 speakers in favor and none opposed. The applicant signed substantive traffic, noise, and plumbing mitigations, but the traffic issue is attributed only to CEQR, not to a local actor, so it does not satisfy the local-issue rule (map-amendment PDF pp. 3-8; related special-permit PDF p. 13). Use **3-8; related report p. 13** as the evidence reference, revise the summary, and fill confidence (**high**); until then, `coding_complete` cannot be 1. [Map-amendment report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/880040.pdf) · [Related special-permit report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/880041.pdf)

I think applying things to the EIR isnt too relevant

- [ ] **T187 — C-O-P (C 940135 PPX).** I agree with the substantive labels. The row is nevertheless incomplete: `evidence_summary` and `coding_confidence` are blank. A suitable summary is that CB and BP approved, the CPC hearing had no appearances, and no substantive local request or opposition was documented (PDF pp. 4-6). Set confidence to **high** or set `coding_complete` to 0 until the evidence fields are filled. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/940135.pdf)

Agree 

- [ ] **T188 — Domino Sugar (C 140132 ZSK).** Change `procedural_response` **0 -> 1**, `approved_unresolved_objection` **0 -> 1**, `civic_group_position` **both -> opposition**, and `traffic_parking` **0 -> 1**. The BP requested traffic monitoring, CB involvement, consultation, and outreach; the restrictive declaration created a community review process. CPC modified the affordable-housing text but left several specific affordability, school, transit, parking, environmental, and open-space demands unresolved. Multiple named organizations opposed part or all of the proposal, so the codebook requires **opposition**, not the non-codebook value **both** (PDF pp. 25-33, 37-47). Use **25-47** as the evidence pages, expand the summary, and fill confidence (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/140132.pdf)


Agree

- [ ] **T189 — Rose Hill (C 820449 ZMX).** Change `revision_or_concession` **0 -> 1**. The sponsor-signed conditional negative declaration incorporated noise, recreation-area, sewer, shuttle, and crosswalk mitigations into the project (PDF pp. 1-2). Fill the blank summary and confidence (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/820449.pdf)

Seems minor

- [ ] **T190 — Arthur Kill Road/Richmond Avenue Rezoning (C 060063 ZMR).** Change `local_request_condition`, `procedural_response`, `explicit_local_response`, and `scale_character_preservation` **0 -> 1**. The locally constituted Growth Management Task Force recommended lower-density rules after DCP initiated a study in response to concerns about out-of-character development; the map and text changes implement those recommendations (PDF pp. 2-4, 6-7). Use **2-7** as the evidence pages, revise the summary, and fill confidence (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/060063.pdf)

No opposition seemed to initiate via the ULURP process

- [ ] **T191 — Resilient Neighborhoods: Old Howard Beach (C 210133 ZMQ).** Change `local_request_condition`, `explicit_local_response`, `approved_unresolved_objection`, `cb_request_or_opposition`, `bp_request_or_opposition`, `scale_character_preservation`, and `infrastructure_services` **0 -> 1**. CB and BP sought tighter limits on community facilities in the floodplain. CPC expressly answered those requests but approved without adopting all of them; the report also treats low-density neighborhood character and health/community-facility risk as substantive local issues (PDF pp. 7-9). Use **7-9** as the evidence pages, revise the summary, and fill confidence (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/210133.pdf)

Agree

- [ ] **T192 — Convention Center Special District (C 900052 ZMM).** Change `specific_project` **1 -> 0** and `infrastructure_services` **0 -> 1**. This application establishes an area-wide special district that the report says operates with or without the related project. CB4 expressly raised infrastructure impacts alongside displacement, traffic, open space, and urban-design concerns (PDF pp. 13-18). Use **13-24** as the evidence pages, make the summary more specific, and fill confidence (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/900052.pdf)

Still think its a big specific projecct, agree on infrastructure

- [ ] **T193 — 123-12 Sutphin Boulevard Rezoning (C 240186 ZMQ).** Change `local_opposition`, `local_request_condition`, `approved_unresolved_objection`, `cb_request_or_opposition`, `bp_request_or_opposition`, `scale_character_preservation`, `infrastructure_services`, and `environment_open_space` **0 -> 1**. The attached CB form records an unfavorable action and the BP report identifies CB objections about height, outreach, and sewer capacity. The BP imposed hiring/reporting, community-space, permeable-pavement, and rain-garden conditions, which CPC did not adopt when approving (PDF pp. 7-9, 11-14). Use **7-9** as the CPC evidence pages, fill the summary, and set confidence to **medium** because the CPC narrative says the CB declined to vote while the attached form says unfavorable; until documented, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/240186.pdf)

Agree

- [ ] **T194 — Clearview Expressway/26th Avenue/Kennedy Street (C 780287 ZMQ).** Change `local_request_condition` **1 -> 0** and `explicit_local_response` **1 -> 0**. The report documents five opposing appearances and local questions about sewer and water capacity, but it does not identify a requested change or expressly link the restrictive declaration to those speakers; the codebook says not to infer causation (PDF pp. 1-3). Use **1-3** as the evidence pages, fill the blank summary, and set confidence to **medium** because detailed testimony is in a related report; until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/780287.pdf)

Agree 

- [ ] **T195 — Red Hook Park Ballfield (C 140227 MCK).** Change `local_request_condition`, `revision_or_concession`, `approved_unresolved_objection`, `bp_request_or_opposition`, and `environment_open_space` **0 -> 1**. The BP conditioned approval on construction timing and local hiring. The applicants committed to off-season construction, but CPC approved without incorporating the BP's hiring demand or making the timing commitment binding; the matter concerns substantive local use and improvement of a public park (PDF pp. 5-9). Use **5-9** as the evidence pages and fill the blank summary and confidence (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/140227.pdf)

Seems minor


- [ ] **T196 — R2X Rezoning (C 910255 ZMK).** Change `local_request_condition`, `procedural_response`, `explicit_local_response`, `cb_request_or_opposition`, and `bp_request_or_opposition` **0 -> 1**. The application originated with residents and CB12 seeking larger-home rules; the BP asked why East 10th Street was excluded and pressed DCP on broader commitments, and DCP described future proposals and sharing as a response to those commitments (PDF pp. 7-8, 13-14). Keep `dev_direction = downzone`, which the report states expressly. Revise the summary, fill confidence (**high**), and use **7-8; attached BP report pp. 1-2** as the evidence reference; until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/910255.pdf)

Vague opposition not worth coding

- [ ] **T197 — SoHo Tower (C 170382 ZSM).** Change `speakers_for` **1 -> 2**. The CPC report identifies two supporting speakers—the applicant's land-use attorney and project architect—and none opposed (PDF pp. 3-4). Fill `coding_confidence` (**high**) and revise the summary to note the absence of local opposition or conditions; until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/170382.pdf)

- [ ] **T198 — Children's World Day Care Center (C 920381 PQK).** Change `revision_or_concession`, `approved_unresolved_objection`, and `environment_open_space` **0 -> 1**. HRA assured CPC that repairs would be made and submitted a scope/repair process; CPC nevertheless approved for up to 20 years without adopting the BP's shorter-term, inspection, enforcement, reporting, and system-reform conditions. The local record also identifies an unusable rooftop play area as a substantive issue (PDF pp. 3-6). Use **3-6** as the evidence pages, expand the summary, and fill confidence (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/920381.pdf)

Not too

- [ ] **T199 — 27-24 College Point Boulevard Commercial Overlay (C 220185 ZMQ).** Change `dev_direction` **upzone -> mixed**, and change `local_request_condition`, `procedural_response`, `explicit_local_response`, `approved_unresolved_objection`, `bp_request_or_opposition`, `traffic_parking`, and `environment_open_space` **0 -> 1**. This is a non-housing commercial zoning change. The BP imposed hiring/reporting and sustainability conditions; pedestrian safety at the drive-through was discussed locally, and the applicant said it would look into striping and a speed bump. CPC approved without resolving the BP conditions (PDF pp. 6-7, 14-15). Replace the copied/mismatched summary, use **6-7; attached BP report p. 2** as the evidence reference, and fill confidence (**high**); until then, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/220185.pdf)

- [ ] **T200 — 148-150 Greene Street Special Permit (C 010691 ZSM).** I agree with the substantive labels. Fill the blank `coding_confidence` (**high**) and use **3-5** rather than **1-4** for the hearing, consideration, and preservation rationale. The summary should note unanimous CB/BP approval and no opposition (PDF pp. 3-5). Until confidence is filled, `coding_complete` cannot be 1. [Report](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/010691.pdf)
