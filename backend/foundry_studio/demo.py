"""Offline demo: local stand-ins for the foundry repos on GitLab, and a workspace.

    python -m foundry_studio.demo init                          # (re)create the demo
    python -m foundry_studio.demo tag decoder v1.1.0 # commit + tag, like a release push
    python -m foundry_studio.demo add Deduplicate Packets Packets "Drops repeated guids"
    python -m foundry_studio.demo remove Deduplicate

The repos mirror the real ones: foundry-platform/registries/config-registry (connection profiles),
foundry-platform/registries/schema-registry (the type names) and one foundry-platform/example-operators
project per operator, each with an operator.yaml at its root. The
pipeline-manifests holds PacketPipeline. Nothing here needs a token, Docker or Kafka; Live data
shows generated records.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
from pathlib import Path

from .config import REPO_ROOT

DEFAULT_ROOT = REPO_ROOT / ".demo-gitlab"
CONFIGS = "foundry-platform/registries/config-registry"
SCHEMA_REGISTRY = "foundry-platform/registries/schema-registry"
OPERATORS_GROUP = "foundry-platform/example-operators"
PIPELINES = "foundry-platform/pipeline-manifests"

# Fixed dates keep commit SHAs identical across `init` runs.
FIXED_DATE = "2026-09-01T12:00:00+00:00"

PROFILES = {
    "kafka/prod.json": ('{\n  "ConnectionSettings": {"Brokers": "kafka-internal:9092", "SecurityProtocol": "SASL_SSL",\n'
                        '    "SaslMechanism": "SCRAM-SHA-512", "SecretRef": "kafka-internal-creds"}\n}\n'),
    "kafka/load.json": ('{\n  "ConnectionSettings": {"Brokers": "kafka-load:9092", "SecurityProtocol": "SASL_SSL",\n'
                        '    "SaslMechanism": "SCRAM-SHA-512", "SecretRef": "kafka-load-creds"}\n}\n'),
}
TYPES = {  # type -> owner; EnrichedPackets has no schema, like the real one
    "XmlPackets": "xml-parser", "Packets": "xml-parser", "DecodeEnrichment": "decoder",
    "IspEnrichment": "isp-enricher", "EnrichedPackets": "packet-joiner",
}
REGISTRY_FILES = {
    **{f"{t}/metadata.yaml": f"owner: {OPERATORS_GROUP}/{o}\n" + ("ignoreSchema: true\n" if t == "EnrichedPackets" else "")
       for t, o in TYPES.items()},
    **{f"{t}/v1.json": ('{\n  "title": "%s",\n  "type": "object",\n  "properties": {"guid": {"type": "string", "format": "guid"}}\n}\n' % t)
       for t in TYPES if t != "EnrichedPackets"},
}
OPERATORS = {  # project -> (name, in, out, description)
    "xml-parser": ("XmlToJson", "XmlPackets", "Packets", "Converts XML packets to JSON; data stays base64."),
    "decoder": ("Decode", "Packets", "EnrichedPackets", "Decodes the base64 payload to text."),
    "isp-enricher": ("Isp", "Packets", "EnrichedPackets", "Adds the ISP of host_ip, from CIDR ranges in appsettings."),
}
MANIFEST = f"""\
Name: EnrichmentPipeline

ConfigRegistry:
  Repo: {CONFIGS}
  Ref: main

Kafkas:
  Input:
    Config: kafka/prod
    AllowedTypes: [XmlPackets]
    Topic: raw.xml

  ConvertedPackets:
    Config: kafka/prod
    AllowedTypes: [Packets]
    Topic: enrichment.packets

  Output:
    Config: kafka/prod
    AllowedTypes: [EnrichedPackets]
    Topic: packets.enriched

Operators:
  XmlToJson:
    Repo: {OPERATORS_GROUP}/xml-parser
    Ref: main

  Decode:
    Repo: {OPERATORS_GROUP}/decoder
    Ref: main

  Isp:
    Repo: {OPERATORS_GROUP}/isp-enricher
    Ref: main

Flow:
    - Input -> XmlToJson
    - XmlToJson -> ConvertedPackets

    - ConvertedPackets -> Decode
    - Decode -> Output

    - ConvertedPackets -> Isp
    - Isp -> Output
"""


def workspace(root: Path) -> Path:
    """Legacy: tests that still reference the workspace path."""
    return root / PIPELINES


def _git(repo: Path, *args: str, date: str | None = None) -> str:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Foundry Demo", "GIT_AUTHOR_EMAIL": "demo@example.invalid",
           "GIT_COMMITTER_NAME": "Foundry Demo", "GIT_COMMITTER_EMAIL": "demo@example.invalid"}
    if date:
        env["GIT_AUTHOR_DATE"] = env["GIT_COMMITTER_DATE"] = date
    res = subprocess.run(["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", *args],
                         cwd=repo, env=env, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {res.stderr.strip()}")
    return res.stdout.strip()


def operator_files(name: str, input: str, output: str, version: str = "1.0.0",
                      description: str | None = None) -> dict[str, str]:
    """An operator repo in the shape of the real ones (minimal: no Kafka loop)."""
    return {
        "operator.yaml": (f"name: {name}\ndescription: {description or name}\nRuntime: dotnet\n\n"
                           f"in: [{input}]\nout: [{output}]\n"),
        f"{name}.csproj": (
            '<Project Sdk="Microsoft.NET.Sdk.Worker">\n  <PropertyGroup>\n    <TargetFramework>net8.0</TargetFramework>\n'
            f"    <Version>{version}</Version>\n  </PropertyGroup>\n  <ItemGroup>\n"
            '    <PackageReference Include="Foundry.MetaSettings" Version="2.*" />\n  </ItemGroup>\n</Project>\n'),
        "Program.cs": (f"// Demo stand-in: {input} -> {output}.\n"
                       f"static {output} Operator({input} p) => new() {{ Guid = p.Guid }};\n"),
    }


def _repo(root: Path, project: str, files: dict[str, str], message: str, tag: str | None = None) -> Path:
    repo = root / project
    repo.mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "main")
    for rel, text in files.items():
        (repo / rel).parent.mkdir(parents=True, exist_ok=True)
        (repo / rel).write_text(text)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message, date=FIXED_DATE)
    if tag:
        _git(repo, "tag", tag, date=FIXED_DATE)
    return repo


def init(root: Path = DEFAULT_ROOT) -> Path:
    if root.exists():
        shutil.rmtree(root)
    _repo(root, CONFIGS, PROFILES, "Kafka connection profiles")
    _repo(root, SCHEMA_REGISTRY, REGISTRY_FILES, "Pipeline types")
    for project, (name, i, o, desc) in OPERATORS.items():
        _repo(root, f"{OPERATORS_GROUP}/{project}", operator_files(name, i, o, description=desc), f"{name} 1.0.0", "v1.0.0")
    _repo(root, PIPELINES, {"PacketPipeline/PipelineManifest.yaml": MANIFEST}, "PacketPipeline manifest")
    return root


def tag(root: Path, project: str, version: str) -> str:
    repo = root / OPERATORS_GROUP / project
    csproj = next(repo.glob("*.csproj"))
    csproj.write_text(re.sub(r"<Version>[^<]*</Version>", f"<Version>{version.lstrip('v')}</Version>",
                             csproj.read_text()))
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", f"{project} {version}")
    _git(repo, "tag", version)
    return f"{project} {version}"


def add(root: Path, name: str, input: str, output: str, description: str | None = None) -> None:
    _repo(root, f"{OPERATORS_GROUP}/{name}", operator_files(name, input, output, "0.1.0", description), f"Add {name}")


def remove(root: Path, name: str) -> None:
    repo = root / OPERATORS_GROUP / name
    _git(repo, "rm", "-q", "operator.yaml")
    _git(repo, "commit", "-q", "-m", f"{name} is no longer an operator")


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(prog="python -m foundry_studio.demo", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path(os.environ.get("STUDIO_DEMO_ROOT", DEFAULT_ROOT)))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    s = sub.add_parser("tag"); s.add_argument("project"); s.add_argument("version")
    s = sub.add_parser("add"); s.add_argument("name"); s.add_argument("input"); s.add_argument("output")
    s.add_argument("description", nargs="?")
    s = sub.add_parser("remove"); s.add_argument("name")
    a = ap.parse_args(argv)
    if a.cmd == "init":
        print(f"demo in {init(a.root)}")
    elif a.cmd == "tag":
        print(f"tagged {tag(a.root, a.project, a.version)}")
    elif a.cmd == "add":
        add(a.root, a.name, a.input, a.output, a.description)
        print(f"added {a.name}")
    elif a.cmd == "remove":
        remove(a.root, a.name)
        print(f"removed {a.name}")


if __name__ == "__main__":
    main()
