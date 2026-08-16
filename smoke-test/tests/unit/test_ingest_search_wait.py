import json
from unittest.mock import MagicMock

import pytest

from tests.utils import (
    entity_urns_from_ingest_file,
    wait_for_browse_path_entity,
    wait_for_ingested_urns_searchable,
)

pytestmark = pytest.mark.no_cypress_suite1

SNAPSHOT_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-3,PROD)"
MCP_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)"


def test_entity_urns_from_snapshot_and_mcp(tmp_path):
    payload = [
        {
            "proposedSnapshot": {
                "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                    "urn": SNAPSHOT_URN,
                    "aspects": [],
                }
            }
        },
        {"entityUrn": MCP_URN, "aspectName": "datasetProperties"},
        {"entityUrn": MCP_URN, "aspectName": "schemaMetadata"},
    ]
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps(payload))

    assert entity_urns_from_ingest_file(str(path)) == [SNAPSHOT_URN, MCP_URN]


def test_wait_for_ingested_urns_searchable_retries_until_found(tmp_path, monkeypatch):
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps([{"entityUrn": MCP_URN}]))

    sleeps: list = []
    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", sleeps.append)

    search_calls: list = []

    def fake_search(auth_session, urns):
        search_calls.append(list(urns))
        if len(search_calls) < 2:
            return set()
        return {MCP_URN}

    monkeypatch.setattr("tests.utils._search_results_contain_urns", fake_search)

    wait_for_ingested_urns_searchable(MagicMock(), str(path))

    assert search_calls == [[MCP_URN], [MCP_URN]]
    assert sleeps == [1]


def test_wait_for_ingested_urns_searchable_times_out(tmp_path, monkeypatch):
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps([{"entityUrn": MCP_URN}]))

    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (0, 2))
    monkeypatch.setattr("tests.utils.time.sleep", lambda seconds: None)
    monkeypatch.setattr(
        "tests.utils._search_results_contain_urns", lambda *args, **kwargs: set()
    )

    with pytest.raises(AssertionError, match="not searchable"):
        wait_for_ingested_urns_searchable(MagicMock(), str(path))


def test_wait_for_browse_path_entity_retries_until_found(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", sleeps.append)

    responses = [
        {"data": {"browse": {"entities": []}}},
        {"data": {"browse": {"entities": [{"urn": SNAPSHOT_URN}]}}},
    ]

    def fake_graphql(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("tests.utils.execute_graphql", fake_graphql)

    wait_for_browse_path_entity(
        MagicMock(),
        path=["prod"],
        expected_urn=SNAPSHOT_URN,
        entity_type="DATASET",
    )
    assert sleeps == [1]
    assert responses == []
