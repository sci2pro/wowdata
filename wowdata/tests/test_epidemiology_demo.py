from pathlib import Path

import petl as etl

from wowdata import Pipeline


REPO_ROOT = Path(__file__).resolve().parents[2]


def _copy_example(tmp_path, name: str) -> Path:
    src = REPO_ROOT / "examples" / name
    dst = tmp_path / name
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    return dst


def test_epi_line_list_pipeline_from_yaml(tmp_path):
    _copy_example(tmp_path, "epi_line_list_raw.csv")
    _copy_example(tmp_path, "epi_sites.csv")
    pipeline_yaml = _copy_example(tmp_path, "epi_line_list_cleanup.yaml")
    out = tmp_path / "epi_line_list_clean.csv"

    pipe = Pipeline.load_yaml(pipeline_yaml)
    pipe.run()

    rows = list(etl.fromcsv(out))
    header = rows[0]
    data = rows[1:]

    assert "facility_name" in header
    assert "county" in header
    assert "is_admitted" in header

    idx_case_id = header.index("case_id")
    idx_facility_name = header.index("facility_name")
    idx_is_admitted = header.index("is_admitted")

    assert len(data) == 3
    by_case = {row[idx_case_id]: row for row in data}
    assert by_case["CL-001"][idx_facility_name] == "Kijiji Health Centre"
    assert by_case["CL-001"][idx_is_admitted] == "True"


def test_epi_weekly_incidence_pipeline_from_yaml(tmp_path):
    _copy_example(tmp_path, "epi_weekly_incidence_raw.csv")
    pipeline_yaml = _copy_example(tmp_path, "epi_weekly_incidence_cleanup.yaml")
    out = tmp_path / "epi_weekly_incidence_clean.csv"

    pipe = Pipeline.load_yaml(pipeline_yaml)
    pipe.run()

    rows = list(etl.fromcsv(out))
    header = rows[0]
    data = rows[1:]

    assert "district_key" in header
    assert "status_clean" in header
    assert "incidence_flag" in header

    idx_district_code = header.index("district_code")
    idx_status_clean = header.index("status_clean")
    idx_incidence_flag = header.index("incidence_flag")

    assert len(data) == 3
    by_code = {row[idx_district_code]: row for row in data}
    assert by_code["012"][idx_status_clean] == "Final"
    assert by_code["012"][idx_incidence_flag] == "True"
