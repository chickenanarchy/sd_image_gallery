# Project Architecture

```mermaid
graph TD
    subgraph Backend [Backend: FastAPI, Python]
        A[FastAPI App]
        B[SQLite Database]
        A -->|Reads/Writes| B
    end

    subgraph Frontend [Frontend: HTMX, Jinja2, CSS]
        C[HTMX Interactions]
        D[Jinja2 Templates]
        E[CSS Styles]
        D -->|Uses| E
        C -->|Triggers| D
    end

    A -->|Serves| D
    D -->|Displays| C
```

The indexer populates and updates the primary `files` table; FTS triggers maintain the `files_fts` virtual table when supported. The web app provides search, pagination, semantic select-all, and asynchronous bulk operations (move / copy / delete) with progress polling.
