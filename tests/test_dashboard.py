"""The AI/BI dashboard: its datasets are the saved queries, and every widget reads one."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = json.loads(
    (ROOT / "resources/dashboards/claims_lakehouse.lvdash.json").read_text(encoding="utf-8")
)
QUERIES = {p.read_text(encoding="utf-8").strip() for p in (ROOT / "queries").glob("*.sql")}


def test_every_dataset_is_a_saved_query_verbatim():
    datasets = ["".join(d["queryLines"]).strip() for d in DASHBOARD["datasets"]]
    assert set(datasets) == QUERIES  # each saved query once, and nothing else


def test_every_widget_reads_a_dataset_and_shows_only_fields_it_selects():
    names = {d["name"] for d in DASHBOARD["datasets"]}
    for item in (w for page in DASHBOARD["pages"] for w in page["layout"]):
        widget = item["widget"]
        [query] = widget["queries"]
        assert query["query"]["datasetName"] in names
        fields = {f["name"] for f in query["query"]["fields"]}
        shown = {e["fieldName"] for e in widget["spec"]["encodings"].values()}
        assert shown <= fields, widget["name"]


def test_widgets_fit_the_six_column_grid():
    for item in (w for page in DASHBOARD["pages"] for w in page["layout"]):
        position = item["position"]
        assert position["x"] >= 0 and position["x"] + position["width"] <= 6
