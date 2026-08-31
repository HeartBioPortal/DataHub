import json
import sqlite3
import zlib
import zipfile
from io import StringIO
from pathlib import Path

from datahub.structural_variant_gene_index import (
    StructuralVariantGeneIndexBuilder,
    iter_top_level_object,
)


def test_iter_top_level_object_streams_nested_values_across_small_chunks() -> None:
    payload = {
        "ANK2": {"variants": [{"id": "nssv1", "note": "a } comma, quote \""}]},
        "PCSK9": {"variants": []},
    }

    assert list(
        iter_top_level_object(
            StringIO(json.dumps(payload)),
            chunk_size=7,
        )
    ) == list(payload.items())


def test_structural_variant_gene_index_is_resumable_and_complete_only_at_eof(
    tmp_path: Path,
) -> None:
    source = tmp_path / "sv.json.zip"
    payload = {
        "ANK2": {"variants": [{"variant_id": "nssv1"}]},
        "PCSK9": {"variants": [{"variant_id": "nssv2"}]},
        "HMGCR": {"variants": []},
    }
    with zipfile.ZipFile(source, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sv.json", json.dumps(payload))

    working = tmp_path / "sv.sqlite3.building"
    checkpoint = tmp_path / "sv.checkpoint.json"
    first = StructuralVariantGeneIndexBuilder(
        source_path=source,
        working_path=working,
        checkpoint_path=checkpoint,
        progress_interval=1,
    )
    first_summary = first.build(limit=1)
    first.close()

    assert first_summary["complete"] is False
    assert json.loads(checkpoint.read_text())["complete"] is False
    with sqlite3.connect(working) as connection:
        assert dict(connection.execute("SELECT key, value FROM metadata"))["status"] == "building"

    resumed = StructuralVariantGeneIndexBuilder(
        source_path=source,
        working_path=working,
        checkpoint_path=checkpoint,
        resume=True,
        progress_interval=1,
    )
    final_summary = resumed.build()
    resumed.close()

    assert final_summary["complete"] is True
    assert final_summary["total_indexed_genes"] == 3
    assert json.loads(checkpoint.read_text())["complete"] is True
    with sqlite3.connect(working) as connection:
        metadata = dict(connection.execute("SELECT key, value FROM metadata"))
        assert metadata["status"] == "complete"
        assert metadata["gene_count"] == "3"
        row = connection.execute(
            "SELECT payload_zlib FROM gene_payloads WHERE gene = 'ANK2'"
        ).fetchone()
    assert json.loads(zlib.decompress(row[0])) == payload["ANK2"]
