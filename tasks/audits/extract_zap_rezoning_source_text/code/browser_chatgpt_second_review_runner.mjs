import fs from "node:fs/promises";
import path from "node:path";


const EXPECTED_HEADER = "project_id,second_review_status,first_pass_direction,first_pass_confidence,second_pass_direction,second_pass_class,second_pass_housing_intent,second_pass_scope_type,up_component_present,down_component_present,dominant_capacity_effect,mixed_split_needed,manual_review_priority,second_pass_confidence,review_recommendation,key_source_citation,second_pass_note";
const EXPECTED_COLUMN_COUNT = EXPECTED_HEADER.split(",").length;


function parseManifest(text) {
  const lines = text.trim().split(/\r?\n/);
  const headers = lines[0].split(",");
  return lines.slice(1).filter(Boolean).map((line) => {
    const parts = line.split(",");
    const row = {};
    headers.forEach((header, index) => {
      row[header] = parts[index] || "";
    });
    return row;
  });
}


function cleanCsvResponse(text) {
  let cleaned = text.trim().replace(/^```(?:csv)?\s*/i, "").replace(/```$/i, "").trim();
  const headerIndex = cleaned.indexOf(EXPECTED_HEADER);
  if (headerIndex > 0) {
    cleaned = cleaned.slice(headerIndex).trim();
  }
  return `${cleaned}${cleaned.endsWith("\n") ? "" : "\n"}`;
}


function parseResponseProjectIds(csvText) {
  const lines = csvText.trim().split(/\r?\n/).filter(Boolean);
  if (lines[0] !== EXPECTED_HEADER) {
    return [];
  }
  return lines.slice(1).map((line) => line.split(",", 1)[0].replace(/^"|"$/g, "").trim());
}


function hasExactlyExpectedProjectIds(csvText, expectedIds) {
  const lines = csvText.trim().split(/\r?\n/).filter(Boolean);
  if (lines.slice(1).some((line) => line.split(",").length < EXPECTED_COLUMN_COUNT)) {
    return false;
  }
  const actualIds = parseResponseProjectIds(csvText);
  if (actualIds.length !== expectedIds.length) {
    return false;
  }
  const actualSet = new Set(actualIds);
  return actualSet.size === expectedIds.length && expectedIds.every((projectId) => actualSet.has(projectId));
}


export async function createGptSecondReviewRunner({ chatTab, taskRoot, tempResponseDir }) {
  await fs.mkdir(tempResponseDir, { recursive: true });

  async function clearComposer() {
    const removeLocator = chatTab.playwright.locator("button[aria-label^=\"Remove file\"]");
    const count = await removeLocator.count();
    for (let index = count - 1; index >= 0; index -= 1) {
      await removeLocator.nth(index).click();
      await chatTab.playwright.waitForTimeout(250);
    }

    await chatTab.playwright.locator("#prompt-textarea").click();
    await chatTab.cua.keypress({ keys: ["Meta", "A"] });
    await chatTab.cua.keypress({ keys: ["BACKSPACE"] });
    await chatTab.playwright.waitForTimeout(250);
  }

  async function readBatchRow(batchId) {
    const manifestText = await fs.readFile(
      path.join(taskRoot, "output/zap_rezoning_chatgpt_second_review_batch_manifest.csv"),
      "utf8",
    );
    const row = parseManifest(manifestText).find((candidate) => candidate.batch_id === batchId);
    if (!row) {
      throw new Error(`Missing second-review manifest batch ${batchId}`);
    }
    return row;
  }

  async function getBatchPayload(batchId) {
    const row = await readBatchRow(batchId);
    const expectedIds = row.project_ids.split("|");
    const responsePath = path.join(
      tempResponseDir,
      `zap_rezoning_chatgpt_second_review_response_batch_${batchId}.csv`,
    );

    const batchPath = path.normalize(path.join(taskRoot, "code", row.batch_path));
    const promptText = await fs.readFile(batchPath, "utf8");
    const guardedPrompt = [
      `SECOND_REVIEW_BATCH_ID: ${batchId}`,
      `Return exactly one CSV row for each and only each of these project IDs: ${expectedIds.join(", ")}`,
      "",
      promptText,
      "",
      "Return only the CSV, with no markdown fence and no prose.",
    ].join("\n");
    return { guardedPrompt, expectedIds, responsePath };
  }

  async function findMatchingResponse(expectedIds) {
    const turnTexts = await chatTab.playwright.evaluate((expectedHeader) => (
      Array.from(document.querySelectorAll("[data-testid^=\"conversation-turn-\"]"))
        .map((element) => element.innerText || "")
        .filter((text) => text.includes(expectedHeader))
    ), EXPECTED_HEADER);

    for (let index = turnTexts.length - 1; index >= 0; index -= 1) {
      const candidateCsv = cleanCsvResponse(turnTexts[index]);
      if (candidateCsv.startsWith(EXPECTED_HEADER) && hasExactlyExpectedProjectIds(candidateCsv, expectedIds)) {
        return candidateCsv;
      }
    }
    return "";
  }

  async function sendOneBatch(batchId) {
    const { guardedPrompt, expectedIds } = await getBatchPayload(batchId);
    await clearComposer();
    await chatTab.clipboard.writeText(guardedPrompt);

    const beforeTurnCount = await chatTab.playwright.evaluate(
      () => Array.from(document.querySelectorAll("[data-testid^=\"conversation-turn-\"]")).length,
    );

    await chatTab.playwright.locator("#prompt-textarea").click();
    await chatTab.cua.keypress({ keys: ["Meta", "V"] });

    let uploadState = null;
    for (let attempt = 0; attempt < 90; attempt += 1) {
      await chatTab.playwright.waitForTimeout(1000);
      uploadState = await chatTab.playwright.evaluate(() => ({
        removeCount: Array.from(document.querySelectorAll("button[aria-label^=\"Remove file\"]")).length,
        sendDisabled: document.querySelector("[data-testid=\"send-button\"]")?.disabled ?? null,
        composerLen: (document.querySelector("#prompt-textarea")?.innerText || "").length,
      }));
      if ((uploadState.removeCount === 1 || uploadState.composerLen > 1000) && uploadState.sendDisabled === false) {
        break;
      }
    }
    if (!uploadState || uploadState.sendDisabled !== false) {
      throw new Error(`Second-review batch ${batchId} did not become sendable: ${JSON.stringify(uploadState)}`);
    }

    await chatTab.playwright.locator("[data-testid=\"send-button\"]").click();
    return {
      batch_id: batchId,
      status: "sent",
      expected_ids: expectedIds,
      before_turn_count: beforeTurnCount,
    };
  }

  async function harvestOneBatch(batchId) {
    const { expectedIds, responsePath } = await getBatchPayload(batchId);
    const copiedCsv = await findMatchingResponse(expectedIds);

    if (!copiedCsv.startsWith(EXPECTED_HEADER) || !hasExactlyExpectedProjectIds(copiedCsv, expectedIds)) {
      return {
        batch_id: batchId,
        status: "not_ready",
        expected_ids: expectedIds,
      };
    }

    await fs.writeFile(responsePath, copiedCsv, "utf8");
    return {
      batch_id: batchId,
      status: "saved_tmp_guarded",
      chars: copiedCsv.length,
      ids: expectedIds,
    };
  }

  async function runOneBatch(batchId) {
    await sendOneBatch(batchId);

    for (let attempt = 0; attempt < 32; attempt += 1) {
      await chatTab.playwright.waitForTimeout(2500);
      const response = await harvestOneBatch(batchId);
      if (response.status === "saved_tmp_guarded") {
        return response;
      }
    }

    throw new Error(`Second-review batch ${batchId} was not ready before the short runner timeout`);
  }

  return { sendOneBatch, harvestOneBatch, runOneBatch };
}
