"""
test_ckan_discover.py

Unit tests for src/ingest/ckan_discover.py. No network: a fake session
replaces requests.
"""

import json

import pytest
import yaml

import ckan_discover as cd
import ckan_ingest as ci

BASE = "https://ckan.example/api/action"
SEARCH_URL = BASE + "/package_search"


class FakeResponse:
    def __init__(self, status=200, payload=None, content_type="application/json"):
        self.status_code = status
        self.headers = {"content-type": content_type}
        self._body = json.dumps(payload if payload is not None else {}).encode()

    @property
    def text(self):
        return self._body.decode()

    def json(self):
        return json.loads(self._body)


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def get(self, url, params=None, timeout=None):
        self.requests.append((url, params))
        return self.response


def make_package(**overrides):
    package = {
        "id": "90fed587-1364-4f33-a9ee-208181dc0b97",
        "name": "lmia-employers",
        "title_translated": {"en": "TFWP Positive LMIA Employers", "fr": "EMT"},
        "organization": {"title": "Employment and Social Development Canada"},
        "metadata_modified": "2025-10-23T15:50:27",
        "resources": [
            {"format": "csv"},
            {"format": "XLSX"},
            {"format": "csv"},
            {"format": ""},
        ],
    }
    package.update(overrides)
    return package


def test_search_packages_returns_results_and_sends_query():
    session = FakeSession(
        FakeResponse(payload={"success": True, "result": {"results": [make_package()]}})
    )
    found = cd.search_packages(session, "lmia", base_url=BASE + "/", rows=5)
    assert [p["name"] for p in found] == ["lmia-employers"]
    assert session.requests == [(SEARCH_URL, {"q": "lmia", "rows": 5})]


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(504, content_type="text/html"),
        FakeResponse(200, {"success": True, "result": {}}, "text/html"),
        FakeResponse(200, {"success": False, "result": {"results": []}}),
        FakeResponse(200, {"success": True, "result": {}}),
    ],
)
def test_search_packages_rejects_untrusted_responses(response):
    with pytest.raises(ci.UpstreamError):
        cd.search_packages(FakeSession(response), "lmia", base_url=BASE)


def test_package_title_prefers_english_then_falls_back():
    assert cd.package_title(make_package()) == "TFWP Positive LMIA Employers"
    assert cd.package_title({"title": "Plain", "name": "n"}) == "Plain"
    assert cd.package_title({"name": "only-name"}) == "only-name"


def test_package_formats_are_upper_case_sorted_and_unique():
    assert cd.package_formats(make_package()) == ("CSV", "XLSX")


def test_slugify_key_is_folder_safe_and_bounded():
    assert cd.slugify_key("TFWP Positive LMIA Employers!") == "tfwp_positive_lmia_employers"
    assert cd.slugify_key("***") == "dataset"
    long_key = cd.slugify_key("word " * 30)
    assert len(long_key) <= cd.MAX_KEY_LENGTH and not long_key.endswith("_")


def test_summarise_package_picks_scan_fields():
    summary = cd.summarise_package(make_package())
    assert summary == {
        "title": "TFWP Positive LMIA Employers",
        "ckan_id": "90fed587-1364-4f33-a9ee-208181dc0b97",
        "organization": "Employment and Social Development Canada",
        "formats": ("CSV", "XLSX"),
        "resources": 4,
        "metadata_modified": "2025-10-23T15:50:27",
    }


def test_registry_entry_yaml_is_accepted_by_parse_registry():
    # Quotes in the title must not break the YAML.
    package = make_package(title_translated={"en": 'Employers: "LMIA" list'})
    draft = cd.registry_entry_yaml(package, domain="immigration")
    raw = {"datasets": yaml.safe_load("datasets:\n" + draft)["datasets"]}

    (spec,) = ci.parse_registry(raw)
    assert spec.domain == "immigration"
    assert spec.ckan_id == package["id"]
    assert spec.formats == ("CSV", "XLSX")
    assert spec.enabled is False
    assert spec.description == 'Employers: "LMIA" list'
