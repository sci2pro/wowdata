import petl as etl

from wowdata import Pipeline


def _write(path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_climate_heat_events_pipeline_from_yaml(tmp_path):
    observations = tmp_path / "climate_observations_raw.csv"
    stations = tmp_path / "climate_stations.csv"
    out = tmp_path / "climate_heat_events.csv"
    pipeline_yaml = tmp_path / "climate_heat_events.yaml"

    _write(
        observations,
        (
            "station_id,date,tmax_c,prcp_mm,qc_flag\n"
            "ST001,2025-01-12,39.4,0.0,A\n"
            "ST001,2025-01-13,41.2,1.2,A\n"
            "ST002,2025-01-13,not_available,0.0,A\n"
            "ST003,2025-01-14,36.8,,A\n"
            "ST004,2025-01-14,42.7,0.0,B\n"
            "ST005,2025-01-14,40.1,5.3,A\n"
        ),
    )

    _write(
        stations,
        (
            "station_id,station_name,country,elevation_m\n"
            "ST001,Nairobi Central,KE,1661\n"
            "ST002,Garissa,KE,151\n"
            "ST003,Mombasa,KE,50\n"
            "ST005,Kisumu,KE,1131\n"
        ),
    )

    _write(
        pipeline_yaml,
        (
            "wowdata: 0\n"
            "pipeline:\n"
            "  start:\n"
            "    uri: climate_observations_raw.csv\n"
            "    type: csv\n"
            "  steps:\n"
            "    - transform:\n"
            "        op: cast\n"
            "        params:\n"
            "          types:\n"
            "            tmax_c: number\n"
            "            prcp_mm: number\n"
            "          on_error: \"null\"\n"
            "    - transform:\n"
            "        op: filter\n"
            "        params:\n"
            "          where: \"qc_flag == 'A'\"\n"
            "    - transform:\n"
            "        op: filter\n"
            "        params:\n"
            "          where: \"tmax_c >= 40\"\n"
            "    - transform:\n"
            "        op: join\n"
            "        params:\n"
            "          right: climate_stations.csv\n"
            "          on: [station_id]\n"
            "          how: left\n"
            "    - transform:\n"
            "        op: derive\n"
            "        params:\n"
            "          new: is_extreme\n"
            "          expr: \"tmax_c >= 42\"\n"
            "    - sink:\n"
            "        uri: climate_heat_events.csv\n"
            "        type: csv\n"
        ),
    )

    pipe = Pipeline.from_yaml(pipeline_yaml)
    pipe.run()

    rows = list(etl.fromcsv(out))

    header = rows[0]
    assert "station_name" in header
    assert "country" in header
    assert "is_extreme" in header

    idx_station_id = header.index("station_id")
    idx_station_name = header.index("station_name")
    data = rows[1:]
    assert len(data) == 2
    assert {r[idx_station_id] for r in data} == {"ST001", "ST005"}
    assert {r[idx_station_name] for r in data} == {"Nairobi Central", "Kisumu"}
