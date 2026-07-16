#!/usr/bin/env node

import crypto from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs/promises";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const cliArgs = process.argv.slice(2);
if (cliArgs.length !== 6) {
  throw new Error(
    "Usage: node build_cpc_llm_training_workbooks.mjs " +
    "<start_year> <end_year> <common_reports> " +
    "<unique_reports_per_coder> <sample_seed> <jacob|tyler|guide>",
  );
}

const startYear = Number(cliArgs[0]);
const endYear = Number(cliArgs[1]);
const commonReports = Number(cliArgs[2]);
const uniqueReportsPerCoder = Number(cliArgs[3]);
const sampleSeed = cliArgs[4];
const outputKind = cliArgs[5];

if (
  !Number.isInteger(startYear) ||
  !Number.isInteger(endYear) ||
  !Number.isInteger(commonReports) ||
  !Number.isInteger(uniqueReportsPerCoder) ||
  startYear > endYear ||
  commonReports < 1 ||
  uniqueReportsPerCoder < 1 ||
  !["jacob", "tyler", "guide"].includes(outputKind)
) {
  throw new Error("Invalid CPC training-sample arguments.");
}

const processLabels = [
  {
    name: "local_opposition",
    one: "The report documents substantive local opposition to the application or project.",
    zero: "No substantive local opposition is documented.",
    note: "Count CB, BP, councilmember, civic-group, resident, or tenant opposition.",
  },
  {
    name: "local_request_condition",
    one: "A local actor requests a change, condition, commitment, mitigation, or alternative.",
    zero: "No substantive local request or condition is documented.",
    note: "Do not count procedural referral or generic consultation language.",
  },
  {
    name: "precert_local_change",
    one: "The report says local input initiated or changed the plan before ULURP certification.",
    zero: "No pre-certification local initiation or change is documented.",
    note: "Timing alone is insufficient; the report must connect local input to the plan.",
  },
  {
    name: "project_revised",
    one: "The application, project scope, design, use, density, or mapped area changed during review.",
    zero: "The proposal did not change, or the text only describes the relief originally requested.",
    note: "Do not count boilerplate special-permit modifications or document revision dates.",
  },
  {
    name: "commitment_mitigation",
    one: "The applicant or agency made a substantive commitment, agreement, or mitigation measure.",
    zero: "No substantive commitment or mitigation is documented.",
    note: "This can be positive even when the formal application did not change.",
  },
  {
    name: "process_study_response",
    one: "The response is a study, task force, monitoring, reporting, outreach, or future consultation.",
    zero: "No such procedural response is documented.",
    note: "Code the response offered, not a study that merely predates review.",
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
    note: "General opposition alone is insufficient without an identifiable unresolved demand.",
  },
];

const actorLabels = [
  {
    name: "cb_substantive",
    one: "A community board states a substantive position, request, condition, or recommendation.",
    zero: "The CB is absent or appears only in procedural referral/hearing language.",
    note: "Support counts as substantive; opposition is coded separately.",
  },
  {
    name: "cb_opposition",
    one: "A community board opposes or recommends disapproval of all or part of the proposal.",
    zero: "No CB opposition is documented.",
    note: "Conditioned approval is not opposition unless the report also states opposition.",
  },
  {
    name: "bp_substantive",
    one: "A borough president states a substantive position, request, condition, or recommendation.",
    zero: "The BP is absent or appears only in procedural review language.",
    note: "Support counts as substantive; opposition is coded separately.",
  },
  {
    name: "bp_opposition",
    one: "A borough president opposes or recommends disapproval of all or part of the proposal.",
    zero: "No BP opposition is documented.",
    note: "Do not infer opposition from conditions alone.",
  },
  {
    name: "councilmember_substantive",
    one: "An individual councilmember states a substantive position, request, condition, or recommendation.",
    zero: "No substantive individual councilmember role is documented.",
    note: "Exclude filing with or referral to the City Council.",
  },
  {
    name: "councilmember_opposition",
    one: "An individual councilmember opposes all or part of the proposal.",
    zero: "No individual councilmember opposition is documented.",
    note: "Do not treat Council procedure as member opposition.",
  },
  {
    name: "civic_group_substantive",
    one: "A named civic, neighborhood, tenant, business, or community organization takes a substantive position.",
    zero: "No substantive organization role is documented.",
    note: "Residents speaking only as individuals do not satisfy this label.",
  },
  {
    name: "civic_group_opposition",
    one: "A civic, neighborhood, tenant, business, or community organization opposes the proposal.",
    zero: "No organization opposition is documented.",
    note: "The group need not oppose every component.",
  },
  {
    name: "council_institutional_action",
    one: "The City Council formally acts, modifies, votes, or adopts a substantive position described in the report.",
    zero: "The Council appears only procedurally, or only an individual member acts.",
    note: "Exclude Charter 197-d filing and referral boilerplate.",
  },
];

const issueLabels = [
  {
    name: "affordability_displacement",
    one: "Affordability, displacement, tenant protection, harassment, or housing access is a substantive local issue.",
    zero: "Those topics are absent or appear only in the project description.",
    note: "Affordable units alone do not make this positive.",
  },
  {
    name: "traffic_parking",
    one: "Traffic, parking, loading, trucks, congestion, or curb use is a substantive local issue.",
    zero: "No such local issue is documented.",
    note: "Code the review issue, not a neutral transportation description.",
  },
  {
    name: "scale_height_character",
    one: "Scale, height, density, bulk, design, shadows, or neighborhood character is a substantive local issue.",
    zero: "No such local issue is documented.",
    note: "A neutral zoning or dimensional description is insufficient.",
  },
  {
    name: "infrastructure_services",
    one: "Schools, sewer, transit, sanitation, utilities, public facilities, or service capacity is a substantive local issue.",
    zero: "No such local issue is documented.",
    note: "Code facility siting or capacity disputes, not background service descriptions.",
  },
  {
    name: "environment_open_space",
    one: "Environmental effects, remediation, water quality, parks, waterfront access, or open space is a substantive local issue.",
    zero: "No such local issue is documented.",
    note: "Routine CEQR language alone is insufficient.",
  },
  {
    name: "historic_preservation",
    one: "Historic resources, landmarks, preservation, or historic character is a substantive local issue.",
    zero: "No such local issue is documented.",
    note: "A landmark name alone is insufficient.",
  },
];

const binaryLabels = [
  ...processLabels.map((label) => ({ ...label, section: "Process" })),
  ...actorLabels.map((label) => ({ ...label, section: "Actors" })),
  ...issueLabels.map((label) => ({ ...label, section: "Issues" })),
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

const totalReports = commonReports + 2 * uniqueReportsPerCoder;
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
    selectedYearReports * uniqueReportsPerCoder / totalReports,
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
  assignments.jacob.length !== uniqueReportsPerCoder ||
  assignments.tyler.length !== uniqueReportsPerCoder
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

async function writeCoderWorkbook(coder) {
  const reviewPrefix = coder === "jacob" ? "J" : "T";
  const coderRows = [
    ...assignments.common.map((row) => ({ ...row, sample_group: "common" })),
    ...assignments[coder].map((row) => ({ ...row, sample_group: `${coder}_only` })),
  ]
    .sort((left, right) =>
      stableHash(`${left.document_id}|${sampleSeed}|${coder}|workbook-order`).localeCompare(
        stableHash(`${right.document_id}|${sampleSeed}|${coder}|workbook-order`),
      ),
    )
    .map((row, index) => ({
      ...row,
      review_id: `${reviewPrefix}${String(index + 1).padStart(3, "0")}`,
      shared_id: commonIds.get(row.document_id) ?? "",
      project_name: row.official_project_name || `CPC report ${row.application_number}`,
    }));

  const workbook = Workbook.create();
  const codingSheet = workbook.worksheets.add("Coding");
  const codebookSheet = workbook.worksheets.add("Codebook");

  const columns = [
    "review_id",
    "project_name",
    "vote_year",
    "application_number",
    "open_report",
    "sample_group",
    "shared_id",
    "document_id",
    "action_code",
    "community_district",
    ...binaryLabels.map((label) => label.name),
    "other_issue_codes",
    "evidence_pages",
    "evidence_summary",
    "coding_confidence",
    "coding_complete",
    "coder_notes",
  ];

  const values = [
    columns,
    ...coderRows.map((row) => [
      row.review_id,
      row.project_name,
      Number(row.official_vote_year),
      row.application_number,
      null,
      row.sample_group,
      row.shared_id,
      row.document_id,
      row.action_code,
      row.official_community_district,
      ...binaryLabels.map(() => null),
      null,
      null,
      null,
      null,
      null,
      null,
    ]),
  ];

  const finalColumn = columnLetter(columns.length);
  codingSheet.getRange(`A1:${finalColumn}${values.length}`).values = values;
  const reportLinkColumn = columnLetter(columns.indexOf("open_report") + 1);
  codingSheet.getRange(`${reportLinkColumn}2:${reportLinkColumn}${values.length}`).formulas =
    coderRows.map((row) => [
      `=HYPERLINK("${String(row.official_pdf_url).replaceAll('"', '""')}","Open PDF")`,
    ]);

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
  codingSheet.getRange(`A1:${columnLetter(10)}1`).format.fill = "#40484F";

  const processStart = 11;
  const processEnd = processStart + processLabels.length - 1;
  const actorStart = processEnd + 1;
  const actorEnd = actorStart + actorLabels.length - 1;
  const issueStart = actorEnd + 1;
  const issueEnd = issueStart + issueLabels.length;
  const reviewStart = issueEnd + 1;
  codingSheet.getRange(`${columnLetter(processStart)}1:${columnLetter(processEnd)}1`).format.fill = "#256B57";
  codingSheet.getRange(`${columnLetter(actorStart)}1:${columnLetter(actorEnd)}1`).format.fill = "#386A8A";
  codingSheet.getRange(`${columnLetter(issueStart)}1:${columnLetter(issueEnd)}1`).format.fill = "#8A6A24";
  codingSheet.getRange(`${columnLetter(reviewStart)}1:${finalColumn}1`).format.fill = "#5C6470";
  codingSheet.getRange(`A1:${finalColumn}1`).format.rowHeight = 48;
  codingSheet.getRange(`A2:${finalColumn}${values.length}`).format = {
    font: { size: 10 },
    verticalAlignment: "center",
  };

  const labelRange = codingSheet.getRange(
    `${columnLetter(processStart)}2:${columnLetter(actorEnd + issueLabels.length)}${values.length}`,
  );
  labelRange.dataValidation = {
    rule: { type: "list", values: ["0", "1", "unclear"] },
  };
  labelRange.format = { horizontalAlignment: "center" };

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
    sample_group: 14,
    shared_id: 10,
    document_id: 22,
    action_code: 10,
    community_district: 16,
    other_issue_codes: 28,
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
    ...binaryLabels.map((label) => [
      label.section,
      label.name,
      "0, 1, unclear",
      label.one,
      label.zero,
      label.note,
    ]),
    [
      "Review",
      "other_issue_codes",
      "free text",
      "",
      "",
      "Optional semicolon-separated issues not captured by the six issue labels.",
    ],
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
      "Enter 1 only after every binary label and the evidence fields are complete.",
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
    ["section", "variable", "allowed_values", "enter_1_when", "enter_0_when", "notes"],
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
  execFileSync("unzip", ["-o", "-q", temporaryPath, "-d", extractedPath]);

  const worksheetPath = `${extractedPath}/xl/worksheets/sheet1.xml`;
  const hyperlinkCache = new RegExp(
    '(<x:c r="E\\d+"[^>]*?) t="e">' +
      '(<x:f>HYPERLINK\\([^<]+\\)<\\/x:f>)' +
      '<x:v>HYPERLINK is not implemented\\.[^<]*friendlyName=Open PDF' +
      '<\\/x:v><\\/x:c>',
    "g",
  );
  let worksheetXml = await fs.readFile(worksheetPath, "utf8");
  const hyperlinkCount = [...worksheetXml.matchAll(hyperlinkCache)].length;
  if (hyperlinkCount !== coderRows.length) {
    throw new Error(`Expected ${coderRows.length} hyperlink cells, found ${hyperlinkCount}.`);
  }
  worksheetXml = worksheetXml.replace(
    hyperlinkCache,
    '$1 t="str">$2<x:v>Open PDF</x:v></x:c>',
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
  const guide = `# CPC manual-label example: Piers 35 and 36

Use [C 920019 PSM (1992)](https://www1.nyc.gov/assets/planning/download/pdf/about/cpc/920019.pdf) as the shared reference. The useful review narrative is on printed pages 16-24. This is a strong example because the report records CB, BP, councilmember, business, and resident opposition, followed by a materially revised application.

Enter \`1\` only when the report supports the label, \`0\` when it does not, and \`unclear\` only when the report cannot resolve the question. Do not infer a causal response from timing alone.

| Section | Label | Code | Where the decision comes from |
|---|---|---:|---|
| Process | \`local_opposition\` | 1 | CB3 unanimously recommended disapproval (p. 16); 15 speakers opposed at CPC, including elected officials, businesses, and residents (pp. 17-19). |
| Process | \`local_request_condition\` | 1 | Local actors sought rejection, alternatives, reduced vehicle concentration, and waterfront access (pp. 16-19). |
| Process | \`precert_local_change\` | 0 | The report does not say local input changed the proposal before certification. |
| Process | \`project_revised\` | 1 | The August 7 amendment removed multi-agency fueling, reduced uses and vehicles, added an esplanade, and imposed a seven-year limit (pp. 1, 5-6, 23-24). |
| Process | \`commitment_mitigation\` | 1 | DGS committed to use and vehicle limits and a continuous public esplanade (pp. 23-24). |
| Process | \`process_study_response\` | 0 | The adopted response was a project change, not merely a study, task force, monitoring, or future consultation. |
| Process | \`explicit_local_response\` | 1 | The report names BP, legislators, Council members, and the local community, then says DGS changed the proposal “in response to these concerns” (pp. 23-24). |
| Process | \`approved_unresolved_objection\` | 0 | CPC describes a revised proposal; it does not expressly reject or defer a specific remaining local demand in its decision. |
| Actors | \`cb_substantive\` | 1 | CB3 adopted a detailed resolution (p. 16). |
| Actors | \`cb_opposition\` | 1 | CB3 recommended disapproval 35-0 (p. 16). |
| Actors | \`bp_substantive\` | 1 | The Manhattan BP issued a substantive recommendation (p. 17). |
| Actors | \`bp_opposition\` | 1 | The BP recommended disapproval and testified in opposition (p. 17). |
| Actors | \`councilmember_substantive\` | 1 | Council members from the 1st and 2nd Districts appeared in the opposition record (pp. 17, 19). |
| Actors | \`councilmember_opposition\` | 1 | The report explicitly lists those council members among opposition speakers (pp. 17, 19). |
| Actors | \`civic_group_substantive\` | 1 | Local business and community representatives presented substantive positions (pp. 17-19). |
| Actors | \`civic_group_opposition\` | 1 | Those organizations and businesses appear in the opposition testimony (pp. 17-19). |
| Actors | \`council_institutional_action\` | 0 | Individual members and a Council President representative appear, but the report does not describe a formal Council vote or modification. |
| Issues | \`affordability_displacement\` | 0 | References to a low-income neighborhood do not by themselves make affordability or displacement a review issue. |
| Issues | \`traffic_parking\` | 1 | Opponents focused on vehicle concentration, parking, fueling, and traffic effects (pp. 17-19, 23-24). |
| Issues | \`scale_height_character\` | 1 | The size and compatibility of the facility with the surrounding residential community were substantive objections (pp. 17-19). |
| Issues | \`infrastructure_services\` | 1 | The dispute concerned the siting and scale of sanitation, health, and municipal vehicle facilities (pp. 16-24). |
| Issues | \`environment_open_space\` | 1 | Waterfront use, environmental effects, and public esplanade access were central issues (pp. 17-24). |
| Issues | \`historic_preservation\` | 0 | No substantive historic-preservation issue appears. |

For \`other_issue_codes\`, a concise entry would be \`fueling;waterfront_use;municipal_facility_siting\`. For \`evidence_summary\`, one sentence is enough: “CB3, the BP, councilmembers, businesses, and residents opposed the facility; after review, DGS removed multi-agency fueling, reduced the program, added an esplanade, and limited new uses to seven years.”
`;
  return fs.writeFile("../output/cpc_llm_training_label_guide.md", `${guide.trim()}\n`, "utf8");
}

if (outputKind === "guide") {
  await writeGuide();
} else {
  await writeCoderWorkbook(outputKind);
}
