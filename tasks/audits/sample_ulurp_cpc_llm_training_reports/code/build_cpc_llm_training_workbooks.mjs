#!/usr/bin/env node

import crypto from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs/promises";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const cliArgs = process.argv.slice(2);
if (cliArgs.length !== 8) {
  throw new Error(
    "Usage: node build_cpc_llm_training_workbooks.mjs " +
    "<start_year> <end_year> <common_reports> " +
    "<unique_candidates_per_coder> <unique_reports_per_coder> " +
    "<preserved_jacob_reports> <sample_seed> <jacob|tyler|guide>",
  );
}

const startYear = Number(cliArgs[0]);
const endYear = Number(cliArgs[1]);
const commonReports = Number(cliArgs[2]);
const uniqueCandidatesPerCoder = Number(cliArgs[3]);
const uniqueReportsPerCoder = Number(cliArgs[4]);
const preservedJacobReports = Number(cliArgs[5]);
const sampleSeed = cliArgs[6];
const outputKind = cliArgs[7];

if (
  !Number.isInteger(startYear) ||
  !Number.isInteger(endYear) ||
  !Number.isInteger(commonReports) ||
  !Number.isInteger(uniqueCandidatesPerCoder) ||
  !Number.isInteger(uniqueReportsPerCoder) ||
  !Number.isInteger(preservedJacobReports) ||
  startYear > endYear ||
  commonReports < 1 ||
  uniqueCandidatesPerCoder < uniqueReportsPerCoder ||
  uniqueReportsPerCoder < 1 ||
  preservedJacobReports < 0 ||
  preservedJacobReports > commonReports + uniqueReportsPerCoder ||
  !["jacob", "tyler", "guide"].includes(outputKind)
) {
  throw new Error("Invalid CPC training-sample arguments.");
}

const projectLabels = [
  {
    name: "specific_project",
    one: "The application concerns a concrete, identified development, building, facility, or site-specific proposal.",
    zero: "The application is an area-wide or neighborhood-wide change without an identified imminent project.",
    note: "Code the proposal under review, not possible future projects.",
  },
  {
    name: "zone_change",
    upzone: "The literal zoning map or text change increases permitted density or development capacity.",
    downzone: "The literal zoning map or text change reduces permitted density or development capacity.",
    mixed: "The literal zoning action contains both increases and reductions.",
    none: "The application does not make a zoning map or text change.",
    note: "Code the formal zoning action; special permits and other development-enabling approvals alone are none.",
  },
  {
    name: "dev_direction",
    more: "The dominant overall effect materially increases units, floor area, building bulk, developable land, or enables a substantial redevelopment.",
    lower: "The dominant overall effect materially reduces development capacity or prevents or scales back a substantial redevelopment.",
    mixed: "Material components point both ways and neither direction clearly dominates.",
    none: "The application has no meaningful overall development direction.",
    note: "Code practical development effects, including non-zoning approvals, but require materiality. Legal approval alone is insufficient. Routine renewals, sidewalk cafes, parking adjustments, operating permissions, and public infrastructure are none unless integral to a substantial development. A minor offsetting change does not make a proposal mixed.",
  },
];

const processLabels = [
  {
    name: "substantial_local_opposition",
    one: "The report documents meaningful local opposition to the application or project, such as a recommendation of disapproval, organized opposition, or material objections to its core scope, use, density, or design.",
    zero: "No substantial local opposition is documented.",
    note: "Do not count approval with minor conditions, routine mitigation requests, isolated technical comments, or dissenting votes when the institution recommends approval.",
  },
  {
    name: "local_request_condition",
    one: "A local actor requests a change, condition, commitment, mitigation, or alternative.",
    zero: "No substantive local request or condition is documented.",
    note: "Do not count procedural referral or generic consultation language.",
  },
  {
    name: "revision_or_concession",
    one: "The project or application changed, or the applicant or agency made a substantive concession, commitment, or mitigation.",
    zero: "No substantive revision, concession, commitment, or mitigation is documented.",
    note: "Exclude boilerplate modifications, document dates, studies, outreach, and monitoring alone.",
  },
  {
    name: "procedural_response",
    one: "The response is a study, task force, monitoring, reporting, outreach, or future consultation.",
    zero: "No such procedural response is documented.",
    note: "This may coexist with a revision or concession; exclude a study that merely predates review.",
  },
  {
    name: "explicit_local_response",
    one: "The report explicitly links local opposition or a local request to a change or response.",
    zero: "No explicit local-to-response link is stated.",
    note: "Do not infer causation only because a change followed a hearing.",
  },
  {
    name: "approved_unresolved_objection",
    one: "CPC approved while rejecting, deferring, or leaving a specific local objection unresolved.",
    zero: "The report does not identify a specific unresolved local objection at approval.",
    note: "Code 0 when there is no objection, the objection was resolved, or CPC did not approve. General opposition alone is insufficient.",
  },
];

const actorBinaryLabels = [
  {
    name: "cb_request_or_opposition",
    one: "A community board opposes or requests a substantive change, condition, commitment, or alternative.",
    zero: "The CB is absent, appears only procedurally, or supports without a substantive request.",
    note: "Conditioned approval counts even when the CB does not recommend disapproval.",
  },
  {
    name: "bp_request_or_opposition",
    one: "A borough president opposes or requests a substantive change, condition, commitment, or alternative.",
    zero: "The BP is absent, appears only procedurally, or supports without a substantive request.",
    note: "Conditioned approval counts even when the BP does not recommend disapproval.",
  },
];

const actorPositionLabels = [
  {
    name: "councilmember_position",
    support: "An individual councilmember supports the proposal or makes a substantive request, condition, or recommendation without opposing it.",
    opposition: "An individual councilmember opposes all or part of the proposal.",
    none: "No substantive individual councilmember role is documented.",
    note: "Exclude Council procedure. If members differ, use opposition when any member opposes and note the mixed positions.",
  },
  {
    name: "civic_group_position",
    support: "A named civic, neighborhood, tenant, business, or community organization supports the proposal or makes a substantive request without opposing it.",
    opposition: "A named organization opposes all or part of the proposal.",
    none: "No named organization takes a substantive position.",
    note: "Exclude residents speaking only as individuals. If groups differ, use opposition when any group opposes and note the mixed positions.",
  },
];

const countLabels = [
  {
    name: "cpc_support_speakers",
    rule: "Number of reported speaker appearances in support across all CPC public-hearing dates.",
    note: "Use 0 only when the report establishes that nobody spoke in support. Leave blank when no exact count is reported. The same person may be counted again at a continued hearing.",
  },
  {
    name: "cpc_opposition_speakers",
    rule: "Number of reported speaker appearances in opposition across all CPC public-hearing dates.",
    note: "Exclude letters, written testimony, petitions, and organizations that did not appear as speakers. The same person may be counted again at a continued hearing.",
  },
  {
    name: "cb_support_votes",
    rule: "Number of community board votes reported in support of approving the application.",
    note: "Leave blank when the report gives the recommendation but not the vote count.",
  },
  {
    name: "cb_opposition_votes",
    rule: "Number of community board votes reported in support of disapproving the application.",
    note: "Leave blank when the report gives the recommendation but not the vote count.",
  },
];

const issueLabels = [
  {
    name: "affordability_displacement",
    one: "Affordability, displacement, tenant protection, harassment, or housing access is substantively discussed in review, testimony, requested conditions, or CPC consideration.",
    zero: "Those topics are absent or appear only in the project description.",
    note: "Affordable units alone do not make this positive.",
  },
  {
    name: "traffic_parking",
    one: "Traffic, parking, loading, trucks, congestion, or curb use is substantively discussed in review, testimony, requested conditions, or CPC consideration.",
    zero: "No such substantive review issue is documented.",
    note: "Exclude neutral transportation descriptions.",
  },
  {
    name: "scale_character_preservation",
    one: "Scale, height, density, bulk, design, shadows, neighborhood character, landmarks, or preservation is substantively discussed in review, testimony, requested conditions, or CPC consideration.",
    zero: "No such substantive review issue is documented.",
    note: "Exclude neutral dimensional descriptions and landmark names alone.",
  },
  {
    name: "infrastructure_services",
    one: "Schools, sewer, transit, sanitation, utilities, public facilities, or service capacity is substantively discussed in review, testimony, requested conditions, or CPC consideration.",
    zero: "No such substantive review issue is documented.",
    note: "Exclude background service descriptions.",
  },
  {
    name: "environment_open_space",
    one: "Environmental effects, remediation, water quality, parks, waterfront access, or open space is substantively discussed in review, testimony, requested conditions, or CPC consideration.",
    zero: "No such substantive review issue is documented.",
    note: "Routine CEQR language alone is insufficient.",
  },
];

const codingLabels = [
  { ...projectLabels[0], section: "Project", type: "binary" },
  { ...projectLabels[1], section: "Project", type: "zone_change" },
  { ...projectLabels[2], section: "Project", type: "development_direction" },
  ...processLabels.map((label) => ({ ...label, section: "Process", type: "binary" })),
  ...actorBinaryLabels.map((label) => ({ ...label, section: "Actors", type: "binary" })),
  ...actorPositionLabels.map((label) => ({ ...label, section: "Actors", type: "position" })),
  ...countLabels.map((label) => ({ ...label, section: "Actors", type: "count" })),
  ...issueLabels.map((label) => ({ ...label, section: "Issues", type: "binary" })),
];

function stableHash(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function applicationKey(value) {
  return String(value ?? "").toUpperCase().replaceAll(/\s+/g, "");
}

function applicationGroup(actionCode) {
  if (["ZM", "ZR", "ZS"].includes(actionCode)) return "zoning";
  if (actionCode === "PP") return "property_disposition";
  return "other";
}

function columnLetter(columnNumber) {
  let number = columnNumber;
  let letters = "";
  while (number > 0) {
    number -= 1;
    letters = String.fromCharCode(65 + (number % 26)) + letters;
    number = Math.floor(number / 26);
  }
  return letters;
}

function xmlAttribute(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

async function readCsvRecords(path, sheetName) {
  const csvText = await fs.readFile(path, "utf8");
  const csvWorkbook = await Workbook.fromCSV(csvText, { sheetName });
  const values = csvWorkbook.worksheets.getItem(sheetName).getUsedRange(true).values;
  const headers = values[0].map(String);
  return values.slice(1).map((row) =>
    Object.fromEntries(headers.map((header, index) => [header, row[index] ?? ""])),
  );
}

const narrativeRows = await readCsvRecords(
  "../input/official_ulurp_cpc_narrative_manifest.csv",
  "Narratives",
);
const initialLabelRows = await readCsvRecords(
  "../input/ulurp_cpc_initial_event_labels.csv",
  "InitialLabels",
);
const validationLabelRows = await readCsvRecords(
  "../input/ulurp_cpc_event_validation_labels.csv",
  "ValidationLabels",
);

const heldOutDocumentIds = new Set(
  validationLabelRows.map((row) => String(row.document_id)).filter(Boolean),
);
const previouslyCodedApplications = new Set(
  initialLabelRows
    .filter((row) => row.official_cpc_report_inclusion === "included")
    .map((row) => applicationKey(row.official_application_number))
    .filter(Boolean),
);

const sampleFrame = narrativeRows.filter((row) => {
  const year = Number(row.official_vote_year);
  return (
    row.analysis_narrative_unit_flag === "TRUE" &&
    year >= startYear &&
    year <= endYear &&
    !heldOutDocumentIds.has(String(row.document_id)) &&
    !previouslyCodedApplications.has(applicationKey(row.application_number))
  );
});

if (new Set(sampleFrame.map((row) => row.document_id)).size !== sampleFrame.length) {
  throw new Error("The CPC sample frame contains duplicate document_id values.");
}

const totalReports = commonReports + 2 * uniqueCandidatesPerCoder;
const yearCount = endYear - startYear + 1;
if (totalReports % yearCount !== 0) {
  throw new Error("The requested sample cannot be distributed evenly across years.");
}

const assignments = { common: [], jacob: [], tyler: [] };
let carriedShortfall = 0;

for (let year = startYear; year <= endYear; year += 1) {
  const yearRows = sampleFrame
    .filter((row) => Number(row.official_vote_year) === year)
    .sort((left, right) =>
      stableHash(`${left.document_id}|${sampleSeed}|selection`).localeCompare(
        stableHash(`${right.document_id}|${sampleSeed}|selection`),
      ),
    );

  const requestedYearReports = totalReports / yearCount + carriedShortfall;
  const selectedYearReports = Math.min(requestedYearReports, yearRows.length);
  carriedShortfall = requestedYearReports - selectedYearReports;

  const commonYearReports = Math.round(
    selectedYearReports * commonReports / totalReports,
  );
  const jacobYearReports = Math.round(
    selectedYearReports * uniqueCandidatesPerCoder / totalReports,
  );
  const tylerYearReports = selectedYearReports - commonYearReports - jacobYearReports;

  assignments.common.push(...yearRows.slice(0, commonYearReports));
  const uniquePool = yearRows.slice(commonYearReports, selectedYearReports);
  const yearAssignments = { jacob: [], tyler: [] };
  const groupCounts = { jacob: new Map(), tyler: new Map() };
  const tieOrder = year % 2 === 0 ? ["jacob", "tyler"] : ["tyler", "jacob"];

  for (const row of uniquePool) {
    const group = applicationGroup(row.action_code);
    const availableCoders = tieOrder.filter((coder) =>
      coder === "jacob"
        ? yearAssignments[coder].length < jacobYearReports
        : yearAssignments[coder].length < tylerYearReports,
    );
    const coder = availableCoders.sort((left, right) => {
      const leftGroupCount = groupCounts[left].get(group) ?? 0;
      const rightGroupCount = groupCounts[right].get(group) ?? 0;
      if (leftGroupCount !== rightGroupCount) return leftGroupCount - rightGroupCount;
      if (yearAssignments[left].length !== yearAssignments[right].length) {
        return yearAssignments[left].length - yearAssignments[right].length;
      }
      return tieOrder.indexOf(left) - tieOrder.indexOf(right);
    })[0];
    yearAssignments[coder].push(row);
    groupCounts[coder].set(group, (groupCounts[coder].get(group) ?? 0) + 1);
  }

  assignments.jacob.push(...yearAssignments.jacob);
  assignments.tyler.push(...yearAssignments.tyler);
}

if (carriedShortfall !== 0) {
  throw new Error(`The CPC sample frame is short by ${carriedShortfall} reports.`);
}
if (
  assignments.common.length !== commonReports ||
  assignments.jacob.length !== uniqueCandidatesPerCoder ||
  assignments.tyler.length !== uniqueCandidatesPerCoder
) {
  throw new Error("The annual sample allocation did not produce the requested coder totals.");
}

const commonIds = new Map(
  [...assignments.common]
    .sort((left, right) =>
      stableHash(`${left.document_id}|${sampleSeed}|shared-id`).localeCompare(
        stableHash(`${right.document_id}|${sampleSeed}|shared-id`),
      ),
    )
    .map((row, index) => [row.document_id, `C${String(index + 1).padStart(3, "0")}`]),
);
const calibrationOrder = new Map(
  ["C093", "C094", "C041", "C009", "C014", "C089", "C006", "C020", "C007", "C056"]
    .map((sharedId, index) => [sharedId, index]),
);
const commonIdValues = new Set(commonIds.values());
if ([...calibrationOrder.keys()].some((sharedId) => !commonIdValues.has(sharedId))) {
  throw new Error("A calibration report is missing from the common sample.");
}

async function writeCoderWorkbook(coder) {
  const reviewPrefix = coder === "jacob" ? "J" : "T";
  const originalRows = [
    ...assignments.common.map((row) => ({ ...row, sample_group: "common" })),
    ...assignments[coder].map((row) => ({ ...row, sample_group: `${coder}_only` })),
  ]
    .map((row) => ({
      ...row,
      shared_id: commonIds.get(row.document_id) ?? "",
      project_name: row.official_project_name || `CPC report ${row.application_number}`,
    }))
    .sort((left, right) => {
      const leftCalibrationOrder = calibrationOrder.get(left.shared_id);
      const rightCalibrationOrder = calibrationOrder.get(right.shared_id);
      if (leftCalibrationOrder !== undefined || rightCalibrationOrder !== undefined) {
        return (leftCalibrationOrder ?? calibrationOrder.size) -
          (rightCalibrationOrder ?? calibrationOrder.size);
      }
      return stableHash(`${left.document_id}|${sampleSeed}|${coder}|workbook-order`).localeCompare(
        stableHash(`${right.document_id}|${sampleSeed}|${coder}|workbook-order`),
      );
    });

  const preservedReports = coder === "jacob" ? preservedJacobReports : calibrationOrder.size;
  const preservedRows = originalRows.slice(0, preservedReports);
  const preservedIds = new Set(preservedRows.map((row) => row.document_id));
  const remainingCommonRows = originalRows.filter(
    (row) => row.sample_group === "common" && !preservedIds.has(row.document_id),
  );
  const selectedUniqueRows = preservedRows.filter((row) => row.sample_group !== "common");
  const selectedUniqueIds = new Set(selectedUniqueRows.map((row) => row.document_id));
  const uniqueYearCounts = new Map();
  for (const row of selectedUniqueRows) {
    const year = Number(row.official_vote_year);
    uniqueYearCounts.set(year, (uniqueYearCounts.get(year) ?? 0) + 1);
  }

  const remainingUniqueRows = originalRows.filter(
    (row) => row.sample_group !== "common" && !selectedUniqueIds.has(row.document_id),
  );
  while (selectedUniqueRows.length < uniqueReportsPerCoder) {
    remainingUniqueRows.sort((left, right) => {
      const leftYear = Number(left.official_vote_year);
      const rightYear = Number(right.official_vote_year);
      const yearCountDifference =
        (uniqueYearCounts.get(leftYear) ?? 0) - (uniqueYearCounts.get(rightYear) ?? 0);
      if (yearCountDifference !== 0) return yearCountDifference;
      const groupOrder = { zoning: 0, other: 1, property_disposition: 2 };
      const groupDifference =
        groupOrder[applicationGroup(left.action_code)] -
        groupOrder[applicationGroup(right.action_code)];
      if (groupDifference !== 0) return groupDifference;
      const wordDifference = Number(right.narrative_word_count) - Number(left.narrative_word_count);
      if (wordDifference !== 0) return wordDifference;
      return stableHash(`${left.document_id}|${sampleSeed}|priority`).localeCompare(
        stableHash(`${right.document_id}|${sampleSeed}|priority`),
      );
    });
    const selectedRow = remainingUniqueRows.shift();
    if (!selectedRow) throw new Error(`The ${coder} unique candidate pool is too small.`);
    selectedUniqueRows.push(selectedRow);
    const year = Number(selectedRow.official_vote_year);
    uniqueYearCounts.set(year, (uniqueYearCounts.get(year) ?? 0) + 1);
  }

  const coderRows = [
    ...preservedRows,
    ...remainingCommonRows,
    ...selectedUniqueRows.filter((row) => !preservedIds.has(row.document_id)),
  ].map((row, index) => ({
    ...row,
    review_id: `${reviewPrefix}${String(index + 1).padStart(3, "0")}`,
  }));

  if (
    coderRows.length !== commonReports + uniqueReportsPerCoder ||
    new Set(coderRows.map((row) => row.document_id)).size !== coderRows.length
  ) {
    throw new Error(`The ${coder} workbook does not contain the requested unique reports.`);
  }

  const savedRows = coder === "jacob"
    ? await readCsvRecords("../input/ulurp_cpc_training_labels_jacob.csv", "JacobLabels")
    : [];
  const savedByDocumentId = new Map(savedRows.map((row) => [row.document_id, row]));

  const workbook = Workbook.create();
  const codingSheet = workbook.worksheets.add("Coding");
  const codebookSheet = workbook.worksheets.add("Codebook");

  const columns = [
    "review_id",
    "project_name",
    "vote_year",
    "application_number",
    "open_report",
    ...codingLabels.map((label) => label.name),
    "evidence_pages",
    "evidence_summary",
    "coding_confidence",
    "coding_complete",
    "coder_notes",
    "sample_group",
    "shared_id",
    "document_id",
    "action_code",
    "community_district",
  ];

  const values = [
    columns,
    ...coderRows.map((row) => {
      const saved = savedByDocumentId.get(row.document_id);
      return [
      row.review_id,
      row.project_name,
      Number(row.official_vote_year),
      row.application_number,
      "Open PDF",
      ...codingLabels.map((label) => saved?.[label.name] ?? null),
      saved?.evidence_pages ?? null,
      saved?.evidence_summary ?? null,
      saved?.coding_confidence ?? null,
      saved?.coding_complete ?? null,
      saved?.coder_notes ?? null,
      row.sample_group,
      row.shared_id,
      row.document_id,
      row.action_code,
      row.official_community_district,
      ];
    }),
  ];

  const finalColumn = columnLetter(columns.length);
  codingSheet.getRange(`A1:${finalColumn}${values.length}`).values = values;

  const codingTable = codingSheet.tables.add(
    `A1:${finalColumn}${values.length}`,
    true,
    coder === "jacob" ? "JacobCodingTable" : "TylerCodingTable",
  );
  codingTable.style = "TableStyleLight9";
  codingSheet.showGridLines = false;
  codingSheet.getRange(`A1:${finalColumn}1`).format = {
    font: { bold: true, color: "#FFFFFF", size: 9 },
    verticalAlignment: "center",
    wrapText: true,
  };
  codingSheet.getRange("A1:E1").format.fill = "#40484F";

  const projectStart = 6;
  const projectEnd = projectStart + projectLabels.length - 1;
  const processStart = projectEnd + 1;
  const processEnd = processStart + processLabels.length - 1;
  const actorStart = processEnd + 1;
  const positionStart = actorStart + actorBinaryLabels.length;
  const positionEnd = positionStart + actorPositionLabels.length - 1;
  const countStart = positionEnd + 1;
  const actorEnd = countStart + countLabels.length - 1;
  const issueStart = actorEnd + 1;
  const issueEnd = issueStart + issueLabels.length - 1;
  const reviewStart = issueEnd + 1;
  codingSheet.getRange(`${columnLetter(projectStart)}1:${columnLetter(projectEnd)}1`).format.fill = "#6B5B73";
  codingSheet.getRange(`${columnLetter(processStart)}1:${columnLetter(processEnd)}1`).format.fill = "#256B57";
  codingSheet.getRange(`${columnLetter(actorStart)}1:${columnLetter(actorEnd)}1`).format.fill = "#386A8A";
  codingSheet.getRange(`${columnLetter(issueStart)}1:${columnLetter(issueEnd)}1`).format.fill = "#8A6A24";
  codingSheet.getRange(`${columnLetter(reviewStart)}1:${finalColumn}1`).format.fill = "#5C6470";
  codingSheet.getRange(`A1:${finalColumn}1`).format.rowHeight = 48;
  codingSheet.getRange(`A2:${finalColumn}${values.length}`).format = {
    font: { size: 10 },
    verticalAlignment: "center",
  };

  codingSheet.getRange(
    `${columnLetter(projectStart)}2:${columnLetter(projectStart)}${values.length}`,
  ).dataValidation = {
    rule: { type: "list", values: ["0", "1", "unclear"] },
  };
  codingSheet.getRange(
    `${columnLetter(projectStart)}2:${columnLetter(projectStart)}${values.length}`,
  ).format = { horizontalAlignment: "center" };

  codingSheet.getRange(
    `${columnLetter(projectStart + 1)}2:${columnLetter(projectStart + 1)}${values.length}`,
  ).dataValidation = {
    rule: { type: "list", values: ["upzone", "downzone", "mixed", "none", "unclear"] },
  };
  codingSheet.getRange(
    `${columnLetter(projectStart + 1)}2:${columnLetter(projectStart + 1)}${values.length}`,
  ).format = { horizontalAlignment: "center" };

  codingSheet.getRange(
    `${columnLetter(projectEnd)}2:${columnLetter(projectEnd)}${values.length}`,
  ).dataValidation = {
    rule: { type: "list", values: ["more", "lower", "mixed", "none", "unclear"] },
  };
  codingSheet.getRange(
    `${columnLetter(projectEnd)}2:${columnLetter(projectEnd)}${values.length}`,
  ).format = { horizontalAlignment: "center" };

  for (const [start, end] of [[processStart, positionStart - 1], [issueStart, issueEnd]]) {
    const binaryRange = codingSheet.getRange(
      `${columnLetter(start)}2:${columnLetter(end)}${values.length}`,
    );
    binaryRange.dataValidation = {
      rule: { type: "list", values: ["0", "1", "unclear"] },
    };
    binaryRange.format = { horizontalAlignment: "center" };
  }

  const positionRange = codingSheet.getRange(
    `${columnLetter(positionStart)}2:${columnLetter(positionEnd)}${values.length}`,
  );
  positionRange.dataValidation = {
    rule: {
      type: "list",
      values: ["none_or_procedural", "support_or_request", "opposition", "unclear"],
    },
  };
  positionRange.format = { horizontalAlignment: "center" };

  const countRange = codingSheet.getRange(
    `${columnLetter(countStart)}2:${columnLetter(actorEnd)}${values.length}`,
  );
  countRange.dataValidation = {
    rule: { type: "whole", operator: "between", formula1: 0, formula2: 999 },
  };
  countRange.format = {
    horizontalAlignment: "center",
    numberFormat: "0",
  };

  const confidenceColumn = columnLetter(columns.indexOf("coding_confidence") + 1);
  codingSheet.getRange(`${confidenceColumn}2:${confidenceColumn}${values.length}`).dataValidation = {
    rule: { type: "list", values: ["high", "medium", "low"] },
  };
  const completeColumn = columnLetter(columns.indexOf("coding_complete") + 1);
  codingSheet.getRange(`${completeColumn}2:${completeColumn}${values.length}`).dataValidation = {
    rule: { type: "list", values: ["0", "1"] },
  };

  const widths = {
    review_id: 10,
    project_name: 31,
    vote_year: 9,
    application_number: 17,
    open_report: 12,
    specific_project: 15,
    zone_change: 15,
    dev_direction: 16,
    sample_group: 14,
    shared_id: 10,
    document_id: 22,
    action_code: 10,
    community_district: 16,
    councilmember_position: 23,
    civic_group_position: 23,
    cpc_support_speakers: 19,
    cpc_opposition_speakers: 21,
    cb_support_votes: 17,
    cb_opposition_votes: 19,
    evidence_pages: 14,
    evidence_summary: 42,
    coding_confidence: 14,
    coding_complete: 13,
    coder_notes: 34,
  };
  columns.forEach((column, index) => {
    codingSheet.getRange(
      `${columnLetter(index + 1)}1:${columnLetter(index + 1)}${values.length}`,
    ).format.columnWidth = widths[column] ?? 17;
  });

  const codebookRows = [
    [
      "General",
      "all binary labels",
      "0, 1, unclear",
      "Use 1 only when the CPC report supports the definition.",
      "Use 0 when the report does not support the definition.",
      "Use unclear sparingly. A blank cell means not yet coded.",
    ],
    ...codingLabels.map((label) => {
      if (label.type === "binary") {
        return [
          label.section,
          label.name,
          "0, 1, unclear",
          label.one,
          label.zero,
          label.note,
        ];
      }
      if (label.type === "zone_change") {
        return [
          label.section,
          label.name,
          "upzone, downzone, mixed, none, unclear",
          `upzone: ${label.upzone}; downzone: ${label.downzone}`,
          `mixed: ${label.mixed}; none: ${label.none}`,
          label.note,
        ];
      }
      if (label.type === "development_direction") {
        return [
          label.section,
          label.name,
          "more, lower, mixed, none, unclear",
          `more: ${label.more}; lower: ${label.lower}`,
          `mixed: ${label.mixed}; none: ${label.none}`,
          label.note,
        ];
      }
      if (label.type === "count") {
        return [
          label.section,
          label.name,
          "nonnegative integer or blank",
          label.rule,
          "Leave blank when the report does not provide an exact count.",
          label.note,
        ];
      }
      return [
          label.section,
          label.name,
          "none_or_procedural, support_or_request, opposition, unclear",
          `support_or_request: ${label.support}; opposition: ${label.opposition}`,
          `none_or_procedural: ${label.none}`,
          label.note,
        ];
    }),
    [
      "Review",
      "evidence_pages",
      "free text",
      "",
      "",
      "Printed CPC-report page numbers supporting the coding, such as 16; 23-24.",
    ],
    [
      "Review",
      "evidence_summary",
      "one sentence",
      "",
      "",
      "Briefly state the opposition/request, response, and unresolved issue if present.",
    ],
    [
      "Review",
      "coding_confidence",
      "high, medium, low",
      "",
      "",
      "Confidence in the full row after reading the report.",
    ],
    [
      "Review",
      "coding_complete",
      "0, 1",
      "",
      "",
      "Enter 1 only after every coding field and the evidence fields are complete.",
    ],
    [
      "Review",
      "coder_notes",
      "free text",
      "",
      "",
      "Optional ambiguity or adjudication note.",
    ],
  ];
  const codebookValues = [
    ["section", "variable", "allowed_values", "coding_rule", "absence_rule", "notes"],
    ...codebookRows,
  ];
  codebookSheet.getRange(`A1:F${codebookValues.length}`).values = codebookValues;
  const codebookTable = codebookSheet.tables.add(
    `A1:F${codebookValues.length}`,
    true,
    coder === "jacob" ? "JacobCodebookTable" : "TylerCodebookTable",
  );
  codebookTable.style = "TableStyleLight9";
  codebookSheet.showGridLines = false;
  codebookSheet.getRange("A1:F1").format = {
    fill: "#40484F",
    font: { bold: true, color: "#FFFFFF", size: 10 },
  };
  codebookSheet.getRange(`A2:F${codebookValues.length}`).format = {
    font: { size: 10 },
    verticalAlignment: "top",
    wrapText: true,
  };
  [14, 30, 18, 48, 48, 48].forEach((width, index) => {
    codebookSheet.getRange(
      `${columnLetter(index + 1)}1:${columnLetter(index + 1)}${codebookValues.length}`,
    ).format.columnWidth = width;
  });
  codebookSheet.getRange(`A1:F${codebookValues.length}`).format.autofitRows();

  const output = await SpreadsheetFile.exportXlsx(workbook);
  const temporaryPath = `../temp/cpc_llm_training_labels_${coder}.xlsx`;
  const extractedPath = `../temp/cpc_llm_training_labels_${coder}`;
  const outputPath = `../output/cpc_llm_training_labels_${coder}.xlsx`;
  await output.save(temporaryPath);
  await fs.rm(`${temporaryPath}.inspect.ndjson`, { force: true });
  await fs.rm(extractedPath, { recursive: true, force: true });
  execFileSync("unzip", ["-o", "-q", temporaryPath, "-d", extractedPath]);

  const worksheetPath = `${extractedPath}/xl/worksheets/sheet1.xml`;
  let worksheetXml = await fs.readFile(worksheetPath, "utf8");
  if (
    worksheetXml.includes("<x:hyperlinks") ||
    !worksheetXml.includes("</x:dataValidations>")
  ) {
    throw new Error("Could not identify the Coding sheet hyperlink position.");
  }
  const hyperlinks = coderRows.map((row, index) =>
    `<x:hyperlink ref="E${index + 2}" r:id="rIdPdf${index + 2}"/>`,
  ).join("");
  worksheetXml = worksheetXml.replace(
    "</x:dataValidations>",
    `</x:dataValidations><x:hyperlinks xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">${hyperlinks}</x:hyperlinks>`,
  );

  const sheetView =
    '<x:sheetViews><x:sheetView showGridLines="0" ' +
    'workbookViewId="0" /></x:sheetViews>';
  const codingSheetView =
    '<x:sheetViews><x:sheetView showGridLines="0" workbookViewId="0">' +
    '<x:pane xSplit="5" ySplit="1" topLeftCell="F2" ' +
    'activePane="bottomRight" state="frozen"/>' +
    '<x:selection pane="topRight" activeCell="F1" sqref="F1"/>' +
    '<x:selection pane="bottomLeft" activeCell="A2" sqref="A2"/>' +
    '<x:selection pane="bottomRight" activeCell="F2" sqref="F2"/>' +
    '</x:sheetView></x:sheetViews>';
  if (!worksheetXml.includes(sheetView)) {
    throw new Error("Could not identify the Coding sheet view.");
  }
  worksheetXml = worksheetXml.replace(sheetView, codingSheetView);
  await fs.writeFile(worksheetPath, worksheetXml, "utf8");

  const worksheetRelationshipsPath =
    `${extractedPath}/xl/worksheets/_rels/sheet1.xml.rels`;
  let worksheetRelationshipsXml = await fs.readFile(worksheetRelationshipsPath, "utf8");
  if (!worksheetRelationshipsXml.includes("</Relationships>")) {
    throw new Error("Could not identify the Coding sheet relationships.");
  }
  const hyperlinkRelationships = coderRows.map((row, index) =>
    '<Relationship Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink" ' +
    `Target="${xmlAttribute(row.official_pdf_url)}" ` +
    `TargetMode="External" Id="rIdPdf${index + 2}" />`,
  ).join("");
  worksheetRelationshipsXml = worksheetRelationshipsXml.replace(
    "</Relationships>",
    `${hyperlinkRelationships}</Relationships>`,
  );
  await fs.writeFile(
    worksheetRelationshipsPath,
    worksheetRelationshipsXml,
    "utf8",
  );

  const codebookPath = `${extractedPath}/xl/worksheets/sheet2.xml`;
  let codebookXml = await fs.readFile(codebookPath, "utf8");
  const codebookSheetView =
    '<x:sheetViews><x:sheetView showGridLines="0" workbookViewId="0">' +
    '<x:pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" ' +
    'state="frozen"/>' +
    '<x:selection pane="bottomLeft" activeCell="A2" sqref="A2"/>' +
    '</x:sheetView></x:sheetViews>';
  if (!codebookXml.includes(sheetView)) {
    throw new Error("Could not identify the Codebook sheet view.");
  }
  codebookXml = codebookXml.replace(sheetView, codebookSheetView);
  await fs.writeFile(codebookPath, codebookXml, "utf8");

  await fs.rm(outputPath, { force: true });
  execFileSync(
    "zip",
    ["-q", "-r", "-X", `../../output/cpc_llm_training_labels_${coder}.xlsx`, "."],
    { cwd: extractedPath },
  );
  await fs.rm(temporaryPath, { force: true });
}

function writeGuide() {
  const guide = String.raw`\documentclass[10pt]{article}
\usepackage[margin=0.65in]{geometry}
\usepackage[T1]{fontenc}
\usepackage{array}
\usepackage{booktabs}
\usepackage{longtable}
\usepackage{ragged2e}
\usepackage[table]{xcolor}
\usepackage[colorlinks=true,urlcolor=blue]{hyperref}

\newcolumntype{L}[1]{>{\RaggedRight\arraybackslash}p{#1}}
\renewcommand{\arraystretch}{1.00}
\setlength{\LTpre}{0pt}
\setlength{\LTpost}{0pt}
\setlength{\parindent}{0pt}
\setlength{\parskip}{3pt}

\begin{document}

\section*{CPC manual-label example: Piers 35 and 36}

Use \href{https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/920019.pdf}{C 920019 PSM (1992)} as the shared reference. The useful review narrative is on printed pages 16--24. This report records CB, BP, councilmember, business, and resident opposition followed by a materially revised application.

Binary fields use \texttt{1}, \texttt{0}, or \texttt{unclear}.

The zoning-change field uses \texttt{upzone}, \texttt{downzone}, \texttt{mixed}, or \texttt{none} and records the literal zoning map or text action. The development-direction field uses \texttt{more}, \texttt{lower}, \texttt{mixed}, or \texttt{none} and records the dominant practical development effect. A non-zoning approval can be \texttt{more} when it materially increases units, floor area, building bulk, developable land, or enables a substantial redevelopment. Legal approval alone is insufficient: routine renewals, sidewalk cafes, parking adjustments, operating permissions, and public infrastructure are \texttt{none} unless integral to a substantial development. A minor offsetting change does not by itself make development direction mixed.

Actor-position fields use \nolinkurl{none_or_procedural}, \nolinkurl{support_or_request}, \texttt{opposition}, or \texttt{unclear}. Code only what the report documents; do not infer a causal response from timing alone.

Count fields record reported speaker appearances across all CPC hearing dates and formal Community Board vote totals. Use zero only when the report establishes zero; leave the cell blank when the count is not reported. Written testimony does not count as a speaker. A person who speaks at two hearing dates may be counted twice because the report does not identify every speaker.

\footnotesize
\begin{longtable}{@{}L{0.08\textwidth}L{0.22\textwidth}L{0.12\textwidth}L{0.50\textwidth}@{}}
\toprule
\rowcolor{gray!15}
\textbf{Section} & \textbf{Label} & \textbf{Code} & \textbf{Evidence} \\
\midrule
\endfirsthead
\toprule
\rowcolor{gray!15}
\textbf{Section} & \textbf{Label} & \textbf{Code} & \textbf{Evidence} \\
\midrule
\endhead
Project & \nolinkurl{specific_project} & \texttt{1} & The application concerns a concrete municipal-facility proposal at Piers 35 and 36. \\
Project & \nolinkurl{zone_change} & \texttt{none} & The application does not amend the zoning map or text. \\
Project & \nolinkurl{dev_direction} & \texttt{mixed} & The proposal substantially changes facility uses but does not clearly increase or reduce permitted development capacity. \\
Process & \nolinkurl{substantial_local_opposition} & \texttt{1} & CB3 unanimously recommended disapproval (p.~16); 15 speakers opposed at CPC, including elected officials, businesses, and residents (pp.~17--19). \\
Process & \nolinkurl{local_request_condition} & \texttt{1} & Local actors sought rejection, alternatives, reduced vehicle concentration, and waterfront access (pp.~17--19). \\
Process & \nolinkurl{revision_or_concession} & \texttt{1} & The amendment removed multi-agency fueling, reduced uses and vehicles, added an esplanade, and imposed a seven-year limit (pp.~1, 5--6, 23--24). \\
Process & \nolinkurl{procedural_response} & \texttt{0} & The response did not include a study, task force, monitoring, reporting, outreach, or future consultation. \\
Process & \nolinkurl{explicit_local_response} & \texttt{1} & The report names BP, legislators, Council members, and the local community, then says DGS changed the proposal "in response to these concerns" (pp.~23--24). \\
Process & \nolinkurl{approved_unresolved_objection} & \texttt{0} & CPC describes a revised proposal; it does not expressly reject or defer a specific remaining local demand in its decision. \\
Actors & \nolinkurl{cb_request_or_opposition} & \texttt{1} & CB3 recommended disapproval 35--0 and sought alternatives and changes (p.~16). \\
Actors & \nolinkurl{bp_request_or_opposition} & \texttt{1} & The BP recommended disapproval and testified in opposition (p.~17). \\
Actors & \nolinkurl{councilmember_position} & \texttt{opposition} & The report explicitly lists council members from the 1st and 2nd Districts among opposition speakers (pp.~17, 19). \\
Actors & \nolinkurl{civic_group_position} & \texttt{opposition} & Named business and community organizations appear in the opposition testimony (pp.~17--19). \\
Actors & \nolinkurl{cpc_support_speakers} & \texttt{14} & The two CPC hearing dates report eight and six speakers in favor (pp.~17--18). \\
Actors & \nolinkurl{cpc_opposition_speakers} & \texttt{30} & The two CPC hearing dates each report fifteen speakers in opposition (pp.~17--18). \\
Actors & \nolinkurl{cb_support_votes} & \texttt{0} & CB3 recommended disapproval by a vote of 35--0 (p.~16). \\
Actors & \nolinkurl{cb_opposition_votes} & \texttt{35} & CB3 recommended disapproval by a vote of 35--0 (p.~16). \\
Issues & \nolinkurl{affordability_displacement} & \texttt{0} & References to a low-income neighborhood do not by themselves make affordability or displacement a review issue. \\
Issues & \nolinkurl{traffic_parking} & \texttt{1} & Opponents focused on vehicle concentration, parking, fueling, and traffic effects (pp.~17--19, 23--24). \\
Issues & \nolinkurl{scale_character_preservation} & \texttt{1} & The facility's size and compatibility with the surrounding residential community were substantive objections (pp.~17--19). \\
Issues & \nolinkurl{infrastructure_services} & \texttt{1} & The dispute concerned the siting and scale of sanitation, health, and municipal vehicle facilities (pp.~16--24). \\
Issues & \nolinkurl{environment_open_space} & \texttt{1} & Waterfront use, environmental effects, and public esplanade access were central issues (pp.~17--24). \\
\bottomrule
\end{longtable}

\normalsize
\textbf{Evidence summary example.} "CB3, the BP, councilmembers, businesses, and residents opposed the facility; after review, DGS removed multi-agency fueling, reduced the program, added an esplanade, and limited new uses to seven years."

\end{document}`;
  return fs.writeFile(
    "../output/cpc_llm_training_label_guide.tex",
    `${guide.trim()}\n`,
    "utf8",
  );
}

if (outputKind === "guide") {
  await writeGuide();
} else {
  await writeCoderWorkbook(outputKind);
}
