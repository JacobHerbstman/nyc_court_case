# Spot-check codebook

Definitions are copied from the human codebook
(`tasks/audits/sample_ulurp_cpc_llm_training_reports/code/build_cpc_llm_training_workbooks.mjs`).
Code the application(s) in the report bundle, not other projects the report mentions.

## councilmember_position
- `support_or_request`: an individual councilmember supports the proposal or makes a
  substantive request, condition, or recommendation without opposing it.
- `opposition`: an individual councilmember opposes all or part of the proposal.
- `none_or_procedural`: no substantive individual councilmember role is documented.
- Exclude Council procedure. If members differ, use opposition when any member
  opposes and note the mixed positions.

## civic_group_position
- `support_or_request`: a named civic, neighborhood, tenant, business, or community
  organization supports the proposal or makes a substantive request without opposing it.
- `opposition`: a named organization opposes all or part of the proposal.
- `none_or_procedural`: no named organization takes a substantive position.
- Exclude residents speaking only as individuals. If groups differ, use opposition
  when any group opposes and note the mixed positions.

## bp_request_or_opposition
- `1`: a borough president opposes or requests a substantive change, condition,
  commitment, or alternative.
- `0`: the BP is absent, appears only procedurally, or supports without a
  substantive request.
- Conditioned approval counts even when the BP does not recommend disapproval.

## cb_request_or_opposition
- `1`: a community board opposes or requests a substantive change, condition,
  commitment, or alternative.
- `0`: the CB is absent, appears only procedurally, or supports without a
  substantive request.
- Conditioned approval counts even when the CB does not recommend disapproval.

## substantial_local_opposition
- `1`: the report documents meaningful local opposition to the application or
  project, such as a recommendation of disapproval, organized opposition, or
  material objections to its core scope, use, density, or design.
- `0`: no substantial local opposition is documented.
- Do not count approval with minor conditions, routine mitigation requests, isolated
  technical comments, or dissenting votes when the institution recommends approval.

## cpc_support_speakers
- Number of reported speaker appearances in support across all CPC public-hearing dates.
- Use 0 only when the report establishes that nobody spoke in support. Leave blank
  when no exact count is reported. The same person may be counted again at a
  continued hearing.

## cpc_opposition_speakers
- Number of reported speaker appearances in opposition across all CPC public-hearing dates.
- Exclude letters, written testimony, petitions, and organizations that did not appear
  as speakers. The same person may be counted again at a continued hearing.
- Leave blank when no exact count is reported (added here to match the support rule).
