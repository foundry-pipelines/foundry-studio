"""Pipeline manifests, read from and deployed to the pipeline-manifests repo via GitLab API.

    <repo>/<folder>/PipelineManifest.yaml           the pipeline

A pipeline is known by its folder; its Name is a field in the manifest. Studio reads
manifests from git and edits them in-browser. Export downloads the YAML; Deploy commits
to a branch and opens an MR.
"""

from __future__ import annotations

import json
import re
import time

import yaml

from .gitlab import GitLab, GitLabError
from .manifest import graph_to_manifest, manifest_to_graph

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
MANIFEST = "PipelineManifest.yaml"
LAYOUT = "PipelineManifest.layout.json"


class PipelineError(Exception):
    def __init__(self, status: int, message: str, errors: list[dict] | None = None, log: str = ""):
        self.status = status
        self.errors = errors or []
        self.log = log
        super().__init__(message)


def check_name(name: str) -> str:
    if not NAME_RE.match(name or ""):
        raise PipelineError(400, f"pipeline name {name!r} must match {NAME_RE.pattern}")
    return name


class PipelineRepo:
    """Reads pipelines from GitLab and deploys changes via the commits API."""

    def __init__(self, gitlab: GitLab, project: str):
        self.gitlab = gitlab
        self.project = project

    async def _default_branch(self) -> str:
        proj = await self.gitlab.get_project(self.project)
        return proj["default_branch"]

    async def list(self) -> list[dict]:
        branch = await self._default_branch()
        try:
            tree = await self.gitlab.list_tree(self.project, branch, recursive=False)
        except GitLabError:
            return []
        folders = sorted(
            (e["name"] for e in tree if e["type"] == "tree" and NAME_RE.match(e["name"])),
            key=str.lower,
        )
        out = []
        for folder in folders:
            raw = await self.gitlab.get_file(self.project, f"{folder}/{MANIFEST}", branch)
            if raw is None:
                continue
            try:
                name = manifest_to_graph(raw.decode())["name"]
            except (ValueError, yaml.YAMLError, UnicodeDecodeError):
                name = ""
            out.append({"folder": folder, "name": name or folder})
        return out

    async def datasets(self) -> list[dict]:
        branch = await self._default_branch()
        pipelines = await self.list()
        out = []
        for p in pipelines:
            raw_bytes = await self.gitlab.get_file(self.project, f"{p['folder']}/{MANIFEST}", branch)
            if raw_bytes is None:
                continue
            try:
                raw = yaml.safe_load(raw_bytes.decode()) or {}
            except (yaml.YAMLError, UnicodeDecodeError):
                continue
            datasets = raw.get("Kafkas") if isinstance(raw, dict) else None
            for name, spec in (datasets if isinstance(datasets, dict) else {}).items():
                if isinstance(spec, dict):
                    out.append({"folder": p["folder"], "pipeline": p["name"], "name": str(name), "spec": spec})
        return out

    async def load(self, folder: str) -> dict:
        check_name(folder)
        branch = await self._default_branch()
        raw = await self.gitlab.get_file(self.project, f"{folder}/{MANIFEST}", branch)
        if raw is None:
            raise PipelineError(404, f"no {MANIFEST} in {self.project}/{folder}")
        text = raw.decode()
        try:
            graph = manifest_to_graph(text)
        except (ValueError, yaml.YAMLError) as e:
            raise PipelineError(422, f"{folder}/{MANIFEST}: {e}")
        layout_raw = await self.gitlab.get_file(self.project, f"{folder}/{LAYOUT}", branch)
        layout = json.loads(layout_raw.decode()) if layout_raw else None
        return {"graph": graph, "layout": layout, "manifest": text, "folder": folder}

    async def deploy(self, folder: str | None, graph: dict, layout: dict | None) -> dict:
        """Commit the manifest to a new branch and open an MR."""
        pipeline_name = str(graph.get("name") or "")
        if folder is None:
            folder = check_name(pipeline_name)
        else:
            check_name(folder)

        branch = await self._default_branch()
        manifest_text = graph_to_manifest(graph)
        timestamp = int(time.time())
        deploy_branch = f"studio/{folder}-{timestamp}"

        actions = [
            {"action": "update" if await self.gitlab.get_file(self.project, f"{folder}/{MANIFEST}", branch) else "create",
             "file_path": f"{folder}/{MANIFEST}",
             "content": manifest_text},
        ]
        if layout is not None:
            layout_text = json.dumps(layout, indent=2, sort_keys=True) + "\n"
            existing_layout = await self.gitlab.get_file(self.project, f"{folder}/{LAYOUT}", branch)
            actions.append({
                "action": "update" if existing_layout else "create",
                "file_path": f"{folder}/{LAYOUT}",
                "content": layout_text,
            })

        try:
            await self.gitlab.create_branch(self.project, deploy_branch, branch)
        except GitLabError as e:
            raise PipelineError(502, f"failed to create branch {deploy_branch}: {e}")

        commit_msg = f"studio: update {pipeline_name or folder}"
        try:
            commit = await self.gitlab.create_commit(self.project, deploy_branch, commit_msg, actions)
        except GitLabError as e:
            raise PipelineError(502, f"failed to commit: {e}")

        mr_title = f"Studio: update {pipeline_name or folder}"
        try:
            mr = await self.gitlab.create_mr(self.project, deploy_branch, branch, mr_title)
        except GitLabError as e:
            raise PipelineError(502, f"committed to {deploy_branch} but MR creation failed: {e}")

        return {
            "folder": folder,
            "branch": deploy_branch,
            "commit": commit.get("id") or commit.get("short_id", ""),
            "mr": {"iid": mr["iid"], "url": mr["web_url"], "title": mr["title"]},
        }
