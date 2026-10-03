"""Pipelines are read from git and deployed via commits API + MR."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from foundry_studio import demo
from foundry_studio.app import create_app
from foundry_studio.pipelines import PipelineError


@pytest.fixture
def client(services):
    with TestClient(create_app(services, start_watcher=False)) as c:
        yield c


def test_lists_pipelines_from_git(client):
    pipelines = client.get("/api/pipelines").json()["pipelines"]
    assert any(p["folder"] == "PacketPipeline" for p in pipelines)


def test_load_pipeline_from_git(client):
    loaded = client.get("/api/pipelines/PacketPipeline").json()
    assert loaded["folder"] == "PacketPipeline"
    assert loaded["graph"]["name"] == "EnrichmentPipeline"
    assert "manifest" in loaded


def test_deploy_creates_branch_and_mr(client):
    loaded = client.get("/api/pipelines/PacketPipeline").json()
    graph = loaded["graph"]
    layout = {"version": 1, "positions": {"Isp": {"x": 1, "y": 2}}}
    r = client.post("/api/deploy", json={"graph": {**graph, "_folder": "PacketPipeline"}, "layout": layout})
    assert r.status_code == 200
    body = r.json()
    assert body["folder"] == "PacketPipeline"
    assert body["branch"].startswith("studio/PacketPipeline-")
    assert body["mr"]["iid"] == 1
    assert "url" in body["mr"]


def test_deploy_new_pipeline(client):
    template = client.get("/api/template").json()
    r = client.post("/api/deploy", json={"graph": {**template, "name": "Fresh"}, "layout": None})
    assert r.status_code == 200
    body = r.json()
    assert body["folder"] == "Fresh"
    assert body["branch"].startswith("studio/Fresh-")


def test_export_returns_manifest_yaml(client):
    loaded = client.get("/api/pipelines/PacketPipeline").json()
    r = client.post("/api/export", json={"graph": loaded["graph"]})
    assert r.status_code == 200
    assert "Name: EnrichmentPipeline" in r.json()["manifest"]


def test_datasets_across_pipelines(client):
    names = {(d["folder"], d["name"], d["spec"]["Topic"]) for d in client.get("/api/datasets").json()["datasets"]}
    assert ("PacketPipeline", "Output", "packets.enriched") in names and len(names) == 3


def test_bad_names_rejected():
    from foundry_studio.pipelines import check_name
    with pytest.raises(PipelineError) as e:
        check_name("../etc")
    assert e.value.status == 400
