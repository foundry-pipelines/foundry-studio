// Connection rules for live feedback while dragging a wire. They mirror scripts' validators
// (pipeline/validate/typecheck.py), worded the same way, but those stay authoritative: a refused
// drop asks the backend for their message, and the top bar shows their validation of the whole
// manifest.
//
// A wire is a Flow entry and always joins an operator and a Kafka: Kafka -> operator (it reads it),
// operator -> Kafka (it writes it). An operator writes one Kafka and, unless its Runtime is flink,
// reads one. Its types (in, out) and Runtime are those its operator.yaml declares at its Ref.
// "Schema" in the names below means a type: a type name from schema-registry.

import type { GraphEdge, GraphNode, OperatorInfo } from "../types";
import { findInfo, versionFor } from "./versions";

export interface GraphLike {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export type Check = { ok: true } | { ok: false; reason: string; kind: RefusalKind };
export type RefusalKind = "kind" | "duplicate" | "type" | "cycle" | "self" | "single";

export type Operators = Record<string, OperatorInfo>;

/** A list of types, or "A | B" as operator.yaml lists show: undefined for none. */
export function schemaList(value: string | string[] | null | undefined): string[] | undefined {
  const list = Array.isArray(value) ? value.map(String) : value ? value.split(" | ") : [];
  return list.length ? list : undefined;
}

/** How a schema or list of them is shown: "A | B". */
export function schemaLabel(value: string | string[] | null | undefined): string | undefined {
  return Array.isArray(value) ? value.join(" | ") : value || undefined;
}

/** The types a Kafka allows (its AllowedTypes), as a label; undefined for none. */
export function datasetSchema(node: GraphNode | undefined): string | undefined {
  return schemaList(node?.datasetSpec?.AllowedTypes)?.join(" | ");
}

/** An operator's Runtime, from its operator.yaml at its Ref (dotnet when unknown). */
export function runtimeOf(node: GraphNode | undefined, operators: Operators): "dotnet" | "flink" {
  return versionFor(findInfo(node?.operator, Object.values(operators)), node?.operator?.Ref)?.runtime ?? "dotnet";
}

/** An operator's in and out schemas: its operator.yaml at its Ref (no Ref: the default branch). */
export function operatorschemas(node: GraphNode | undefined, operators: Operators): { input?: string; output?: string } {
  const v = versionFor(findInfo(node?.operator, Object.values(operators)), node?.operator?.Ref);
  return { input: schemaList(v?.input)?.join(" | "), output: schemaList(v?.output)?.join(" | ") };
}

/** The types a node writes: an operator's out, a Kafka's own. */
export function emits(node: GraphNode | undefined, operators: Operators): string | undefined {
  if (!node) return undefined;
  return node.kind === "operator" ? operatorschemas(node, operators).output : datasetSchema(node);
}

/** The types a node reads: an operator's in, a Kafka's own. */
export function expects(node: GraphNode | undefined, operators: Operators): string | undefined {
  if (!node) return undefined;
  return node.kind === "operator" ? operatorschemas(node, operators).input : datasetSchema(node);
}

/** The Kafkas an operator writes, in name order (one, unless miswired). */
export function outputsOf(graph: GraphLike, operator: string): string[] {
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  const ds = graph.edges
    .filter((e) => e.source === operator)
    .map((e) => byId.get(e.target))
    .filter((n) => n?.kind === "dataset")
    .map((n) => n!.dataset!);
  return [...new Set(ds)].sort();
}

/** The Kafkas an operator reads, in name order. */
export function inputsOf(graph: GraphLike, operator: string): string[] {
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  const ds = graph.edges
    .filter((e) => e.target === operator)
    .map((e) => byId.get(e.source))
    .filter((n) => n?.kind === "dataset")
    .map((n) => n!.dataset!);
  return [...new Set(ds)].sort();
}

function reaches(edges: GraphEdge[], from: string, to: string): boolean {
  const seen = new Set<string>();
  const stack = [from];
  while (stack.length) {
    const n = stack.pop()!;
    if (n === to) return true;
    if (seen.has(n)) continue;
    seen.add(n);
    for (const e of edges) if (e.source === n) stack.push(e.target);
  }
  return false;
}

export function checkConnection(graph: GraphLike, operators: Operators, source: string, target: string): Check {
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  const s = byId.get(source);
  const t = byId.get(target);
  if (!s || !t) return { ok: false, kind: "kind", reason: "unknown node" };
  if (s.kind === "operator" && t.kind === "operator") {
    return { ok: false, kind: "kind", reason: `${source} -> ${target}: operators connect through a Kafka; drop a Kafka between them` };
  }
  if (s.kind === "dataset" && t.kind === "dataset") {
    return { ok: false, kind: "kind", reason: "Kafkas connect through an operator" };
  }
  if (graph.edges.some((e) => e.source === source && e.target === target)) {
    return { ok: false, kind: "duplicate", reason: "already connected" };
  }
  if (s.kind === "operator") {
    const ds = t.dataset!;
    const writes = outputsOf(graph, source);
    if (writes.length) {
      return { ok: false, kind: "single", reason: `${source}: already writes ${writes[0]}; an operator writes one Kafka` };
    }
    if (inputsOf(graph, source).includes(ds)) {
      return { ok: false, kind: "self", reason: `Flow has a cycle: ${ds} -> ${source} -> ${ds}` };
    }
    // The Kafka must allow every type the operator writes.
    const outs = schemaList(emits(s, operators));
    const allowed = schemaList(datasetSchema(t)) ?? [];
    const extra = (outs ?? []).filter((o) => !allowed.includes(o));
    if (outs && extra.length) {
      return {
        ok: false,
        kind: "type",
        reason: `Flow: ${source} -> ${ds}: ${source} writes ${[...extra].sort().join(", ")} but ${ds} allows ${[...allowed].sort().join(", ") || "nothing"}`,
      };
    }
  } else {
    const ds = s.dataset!;
    const reads = inputsOf(graph, target);
    if (reads.length && runtimeOf(t, operators) !== "flink") {
      return { ok: false, kind: "single", reason: `${target}: already reads ${reads[0]}; a dotnet operator reads one Kafka` };
    }
    if (outputsOf(graph, target).includes(ds)) {
      return { ok: false, kind: "self", reason: `Flow has a cycle: ${target} -> ${ds} -> ${target}` };
    }
    // A reader must read every type the Kafka allows.
    const allowed = schemaList(datasetSchema(s));
    const want = schemaList(expects(t, operators));
    const extra = (allowed ?? []).filter((a) => !(want ?? []).includes(a));
    if (allowed && want && extra.length) {
      return {
        ok: false,
        kind: "type",
        reason: `Flow: ${ds} -> ${target}: ${ds} allows ${[...extra].sort().join(", ")} but ${target} reads ${[...want].sort().join(", ")}`,
      };
    }
  }
  if (reaches(graph.edges, target, source)) {
    return { ok: false, kind: "cycle", reason: `cycle detected — ${target} already feeds ${source}` };
  }
  return { ok: true };
}

/** The short form shown in the drag tooltip: "Input allows XmlPackets but Decode reads Packets". */
export function shortReason(reason: string): string {
  return reason.replace(/^Flow: \S+ -> \S+: /, "").replace(/^[^\s:]+: already/, "already");
}
