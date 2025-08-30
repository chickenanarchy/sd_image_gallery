# Integration Tests (Consolidated)

All integration coverage now lives in `test_combined_integration.py`.

Removed legacy modules (previously separate) to reduce duplication and speed up
collection:
	- test_integration_suite.py
	- test_additional_coverage.py
	- test_extended_coverage.py
	- test_run_webui_launch.py

Key coverage retained:
	- DB init / integrity & repair paths (vacuum, corruption handling)
	- Indexing FAST vs FULL refresh & FTS migration
	- Deduplication logic
	- Core WebUI endpoints (gallery, search, metadata, thumbs, images, favicon, metadata_fields)
	- Sorting variants + time facet filters and count cache
	- File operations (sync & async: ids + query scope, truncation path)
	- Thumbnail fallback path
	- WebUI launch wrapper smoke (patched subprocess call)

Performance metrics still append to `perf_log.txt` (index_time, search_sample_ms,
thumb_time_ms, file_ops_time_ms). Each run begins with a UTC timestamp header.

Running:
```bash
pytest -q test_integration
```

Extending: Prefer adding tests into logical sections inside
`test_combined_integration.py` instead of new modules. Use markers (e.g.
`@pytest.mark.slow`) for heavier scenarios if introduced later.
