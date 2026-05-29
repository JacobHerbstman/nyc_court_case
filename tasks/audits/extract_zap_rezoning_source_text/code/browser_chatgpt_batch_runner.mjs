import fs from "node:fs/promises";
import path from "node:path";


const EXPECTED_HEADER = "project_id,chatgpt_review_status,suggested_rezoning_direction,suggested_rezoning_class,suggested_housing_intent,suggested_scope_type,suggested_scope_blocks,suggested_scope_acres,suggested_confidence,suggested_evidence_note";


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


export async function createGptBatchRunner({ chatTab, taskRoot, tempResponseDir }) {
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
      path.join(taskRoot, "output/zap_rezoning_chatgpt_review_batch_manifest.csv"),
      "utf8",
    );
    const row = parseManifest(manifestText).find((candidate) => candidate.batch_id === batchId);
    if (!row) {
      throw new Error(`Missing manifest batch ${batchId}`);
    }
    return row;
  }

  async function runOneBatch(batchId) {
    const row = await readBatchRow(batchId);
    const expectedIds = row.project_ids.split("|");
    const responsePath = path.join(
      tempResponseDir,
      `zap_rezoning_chatgpt_review_response_batch_${batchId}.csv`,
    );

    const batchPath = path.normalize(path.join(taskRoot, "code", row.batch_path));
    const promptText = await fs.readFile(batchPath, "utf8");
    const guardedPrompt = [
      `BATCH_ID: ${batchId}`,
      `Return exactly one CSV row for each and only each of these project IDs: ${expectedIds.join(", ")}`,
      "",
      promptText,
      "",
      "Return only the CSV, with no markdown fence and no prose.",
    ].join("\n");

    await clearComposer();
    await chatTab.clipboard.writeText(guardedPrompt);

    const beforeCopyCount = await chatTab.playwright
      .locator("[data-testid=\"copy-turn-action-button\"][aria-label=\"Copy response\"]")
      .count();
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
      throw new Error(`Batch ${batchId} did not become sendable: ${JSON.stringify(uploadState)}`);
    }

    await chatTab.playwright.locator("[data-testid=\"send-button\"]").click();

    let copiedCsv = "";
    let lastState = null;
    for (let attempt = 0; attempt < 180; attempt += 1) {
      await chatTab.playwright.waitForTimeout(2500);
      lastState = await chatTab.playwright.evaluate(() => {
        const stop = !!document.querySelector("[data-testid=\"stop-button\"]");
        const turns = Array.from(document.querySelectorAll("[data-testid^=\"conversation-turn-\"]")).map((element) => ({
          testid: element.getAttribute("data-testid"),
          text: (element.innerText || "").slice(0, 1600),
        }));
        return { stop, turnCount: turns.length, last: turns[turns.length - 1] || null };
      });

      const copyButtons = chatTab.playwright.locator(
        "[data-testid=\"copy-turn-action-button\"][aria-label=\"Copy response\"]",
      );
      const copyCountNow = await copyButtons.count();

      if (
        !lastState.stop
        && lastState.turnCount >= beforeTurnCount + 2
        && copyCountNow > beforeCopyCount
        && lastState.last
        && lastState.last.text.includes(EXPECTED_HEADER)
      ) {
        await copyButtons.nth(copyCountNow - 1).click();
        await chatTab.playwright.waitForTimeout(1000);
        copiedCsv = cleanCsvResponse(await chatTab.clipboard.readText());
        if (copiedCsv.startsWith(EXPECTED_HEADER) && expectedIds.every((projectId) => copiedCsv.includes(projectId))) {
          break;
        }
      }
    }

    if (!copiedCsv.startsWith(EXPECTED_HEADER) || !expectedIds.every((projectId) => copiedCsv.includes(projectId))) {
      if (copiedCsv) {
        await fs.writeFile(responsePath.replace(/\.csv$/, ".raw.txt"), copiedCsv, "utf8");
      }
      throw new Error(`Batch ${batchId} returned a missing or mismatched CSV`);
    }

    await fs.writeFile(responsePath, copiedCsv, "utf8");
    return {
      batch_id: batchId,
      status: "saved_tmp_guarded",
      chars: copiedCsv.length,
      ids: expectedIds,
      last_turn: lastState.last?.testid || "",
    };
  }

  return { runOneBatch };
}
