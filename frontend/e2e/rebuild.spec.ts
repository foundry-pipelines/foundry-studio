import { expect, test } from "@playwright/test";
import { dropCard, dropDataset, handle, wire } from "./helpers";

const IN = "dataset:Input";
const MID = "dataset:ConvertedPackets";
const OUT = "dataset:Output";

test("PacketPipeline rebuilt from scratch deploys successfully", async ({ page }) => {
  await page.goto("/?new=1");
  await page.getByTestId("pipeline-name").fill("EnrichmentPipeline");
  const box = (await page.getByTestId("canvas").boundingBox())!;
  // Cards add operators without a Ref (default branch), like the manifest in git.
  await dropDataset(page, "PacketPipeline", "Output", box.width * 0.9, box.height / 2);
  await dropCard(page, "Isp", box.width * 0.68, box.height / 2 + 140);
  await dropCard(page, "Decode", box.width * 0.68, box.height / 2 - 140);
  await dropDataset(page, "PacketPipeline", "ConvertedPackets", box.width * 0.48, box.height / 2);
  await dropCard(page, "XmlToJson", box.width * 0.28, box.height / 2 + 140);
  await dropDataset(page, "PacketPipeline", "Input", box.width * 0.08, box.height / 2);

  // Wire in an arbitrary order: the written manifest doesn't depend on it.
  await wire(page, handle(page, "Isp", "out"), handle(page, OUT, "in"));
  await wire(page, handle(page, MID, "out"), handle(page, "Decode", "in"));
  await wire(page, handle(page, IN, "out"), handle(page, "XmlToJson", "in"));
  await wire(page, handle(page, "Decode", "out"), handle(page, OUT, "in"));
  await wire(page, handle(page, MID, "out"), handle(page, "Isp", "in"));
  await wire(page, handle(page, "XmlToJson", "out"), handle(page, MID, "in"));
  await expect(page.locator(".react-flow__edge")).toHaveCount(6);
  await expect(page.getByTestId("validation-status")).toContainText("valid");

  await page.getByTestId("deploy").click();
  await expect(page.getByTestId("deployed")).toBeVisible();
});
