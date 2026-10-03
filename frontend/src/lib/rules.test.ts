import { describe, expect, it } from "vitest";
import type { GraphNode, OperatorInfo } from "../types";
import { checkConnection, emits, expects, inputsOf, outputsOf, shortReason, type GraphLike, type Operators } from "./rules";

const info = (name: string, input: string, output: string, tagged?: [string, string]): OperatorInfo => {
  const branch = { ref: "abc", label: "main@abc", kind: "branch" as const, commit: "abc", committed_date: null, input, output, source: "operator.yaml", warnings: [], web_url: "", runtime: "dotnet" as const };
  const tag = tagged && { ...branch, ref: "v1.0.0", label: "v1.0.0", kind: "tag" as const, commit: "def", input: tagged[0], output: tagged[1] };
  return {
    id: `foundry-platform/example-operators/${name}:`, project: `foundry-platform/example-operators/${name}`, path: "", name, description: "",
    repo: `https://gitlab.com/foundry-platform/example-operators/${name}.git`, web_url: "", input, output, warnings: [],
    latest: tag ? "v1.0.0" : "abc", head: "abc", versions: tag ? [tag, branch] : [branch], runtime: "dotnet",
  };
};
const operators: Operators = Object.fromEntries(
  [info("XmlToJson", "XmlPackets", "Packets"), info("Decode", "Packets", "EnrichedPackets", ["XmlPackets", "Packets"])].map((i) => [i.id, i]),
);
const t = (id: string, Ref?: string): GraphNode => ({
  id, kind: "operator", operator: { Repo: `https://gitlab.com/foundry-platform/example-operators/${id}.git`, ...(Ref ? { Ref } : {}) },
});
const d = (name: string, ...AllowedTypes: string[]): GraphNode => ({
  id: `dataset:${name}`, kind: "dataset", dataset: name, datasetSpec: { Config: "kafka/prod", AllowedTypes, Topic: name.toLowerCase() },
});

const base = (): GraphLike => ({
  nodes: [t("XmlToJson"), t("Decode"), d("Input", "XmlPackets"), d("Converted", "Packets"), d("Output", "EnrichedPackets")],
  edges: [{ source: "dataset:Input", target: "XmlToJson" }],
});

describe("types", () => {
  it("come from operator.yaml at the Ref for operators and AllowedTypes for Kafkas", () => {
    const g = base();
    expect(emits(g.nodes[0], operators)).toBe("Packets");
    expect(expects(g.nodes[1], operators)).toBe("Packets");
    expect(expects(t("Decode", "v1.0.0"), operators)).toBe("XmlPackets");
    expect(emits(g.nodes[2], operators)).toBe("XmlPackets");
    expect(expects(t("Unknown"), operators)).toBeUndefined();
  });
});

describe("checkConnection", () => {
  it("accepts matching schemas", () => {
    const g = base();
    expect(checkConnection(g, operators, "XmlToJson", "dataset:Converted")).toEqual({ ok: true });
    expect(checkConnection(g, operators, "dataset:Converted", "Decode")).toEqual({ ok: true });
  });

  it("refuses mismatched types in the validators' words", () => {
    const g = base();
    const out = checkConnection(g, operators, "XmlToJson", "dataset:Output");
    expect(out).toMatchObject({ ok: false, kind: "type" });
    expect(!out.ok && out.reason).toBe("Flow: XmlToJson -> Output: XmlToJson writes Packets but Output allows EnrichedPackets");
    const into = checkConnection(g, operators, "dataset:Input", "Decode");
    expect(!into.ok && shortReason(into.reason)).toBe("Input allows XmlPackets but Decode reads Packets");
  });

  it("takes writers of any type a Kafka allows, but only readers of all of them", () => {
    const g = base();
    g.nodes.push(d("Mixed", "Packets", "XmlPackets"));
    expect(checkConnection(g, operators, "XmlToJson", "dataset:Mixed")).toEqual({ ok: true });
    const into = checkConnection(g, operators, "dataset:Mixed", "Decode");
    expect(!into.ok && into.reason).toBe("Flow: Mixed -> Decode: Mixed allows XmlPackets but Decode reads Packets");
  });

  it("lets flink operators read several Kafkas", () => {
    const g = base();
    const join = info("Join", "Packets | EnrichedPackets", "EnrichedPackets");
    const withJoin: Operators = { ...operators, [join.id]: { ...join, versions: join.versions.map((v) => ({ ...v, runtime: "flink" as const })) } };
    g.nodes.push(t("Join"));
    g.edges.push({ source: "dataset:Converted", target: "Join" });
    expect(checkConnection(g, withJoin, "dataset:Output", "Join")).toEqual({ ok: true });
    expect(checkConnection(g, operators, "dataset:Output", "Join")).toMatchObject({ ok: false, kind: "single" });
  });

  it("allows one input (dotnet) and one output per operator", () => {
    const g = base();
    g.nodes.push(d("Other", "XmlPackets"));
    expect(checkConnection(g, operators, "dataset:Other", "XmlToJson")).toMatchObject({ ok: false, kind: "single" });
    g.edges.push({ source: "XmlToJson", target: "dataset:Converted" });
    g.nodes.push(d("Converted2", "Packets"));
    expect(checkConnection(g, operators, "XmlToJson", "dataset:Converted2")).toMatchObject({ ok: false, kind: "single" });
  });

  it("refuses wrong kinds, duplicates, self loops and cycles", () => {
    const g = base();
    expect(checkConnection(g, operators, "XmlToJson", "Decode")).toMatchObject({ ok: false, kind: "kind" });
    expect(checkConnection(g, operators, "dataset:Input", "dataset:Output")).toMatchObject({ ok: false, kind: "kind" });
    expect(checkConnection(g, operators, "dataset:Input", "XmlToJson")).toMatchObject({ ok: false, kind: "duplicate" });

    const loop: GraphLike = {
      nodes: [t("A"), t("B"), d("X", "Packets"), d("Y", "Packets")],
      edges: [
        { source: "dataset:X", target: "A" },
        { source: "A", target: "dataset:Y" },
        { source: "dataset:Y", target: "B" },
      ],
    };
    expect(checkConnection(loop, operators, "B", "dataset:X")).toMatchObject({ ok: false, kind: "cycle" });
    const self: GraphLike = { nodes: [t("A"), d("X", "Packets")], edges: [{ source: "dataset:X", target: "A" }] };
    expect(checkConnection(self, operators, "A", "dataset:X")).toMatchObject({ ok: false, kind: "self" });
  });
});

describe("wiring", () => {
  it("lists an operator's In and Out", () => {
    const g = base();
    g.edges.push({ source: "XmlToJson", target: "dataset:Converted" });
    expect(inputsOf(g, "XmlToJson")).toEqual(["Input"]);
    expect(outputsOf(g, "XmlToJson")).toEqual(["Converted"]);
    expect(outputsOf(g, "Decode")).toEqual([]);
  });
});
