# Project review

## Summary of changes made
- Added lazy import wrappers for `down_data.ui`, `down_data.ui.pages`, and `down_data.ui.widgets` to prevent hard Qt dependencies during backend-only workflows and CI test discovery.

## Quick health check
- **Backend utilities**: PFR HTML helpers and player directory logic are cohesive and already covered by focused unit tests.
- **UI layer**: Architecture is well-organised (navigation shell, grid system, shared panels), but module initialisation previously pulled in PySide6 immediately, which caused failures in headless environments. The new lazy imports mitigate that risk while preserving attribute access patterns.

## Recommendations
- **Background search execution**: Player search currently runs synchronously on the UI thread; moving filtering into a worker (e.g., `QThreadPool + QRunnable`) would keep the interface responsive during heavy queries. 【F:README.md†L64-L74】
- **CI-friendly test target**: Prefer invoking the explicit unit suite (`python -m unittest tests.test_pfr_html_utils ...`) or adding a `tests/__init__.py` that scopes discovery to backend tests, avoiding Qt imports in headless pipelines. 【F:README.md†L55-L61】【F:docs/PROJECT_REVIEW.md†L4-L10】
- **Dependency note**: When running the full UI in containers, ensure OpenGL runtime libraries (e.g., `libGL`) are present to satisfy PySide6. Lazy imports reduce accidental failures, but the runtime dependency remains for real UI usage. 【F:down_data/ui/__init__.py†L1-L22】
