# Project Architecture

```mermaid
flowchart LR
    subgraph IDX[Indexing CLI]
        I1[sd_index_manager.py]
    end
    subgraph DATA[SQLite]
        TBL[(files table)]
        FTS[(files_fts)]
    end
    subgraph APP[FastAPI]
        R[Routes/Search]
        J[Async Jobs]
    end
    subgraph FRONTEND[Browser]
        HTMX[HTMX]
        TPL[Jinja2]
        CSS[CSS]
    end

    I1 --> TBL
    TBL --> FTS
    R --> TBL
    J --> TBL
    HTMX --> R
    R --> TPL --> HTMX
    TPL --> CSS
    J -. status .-> HTMX
```

The indexer populates and updates the primary `files` table; FTS triggers maintain the `files_fts` virtual table when supported. The web app provides search, pagination, semantic select-all, and asynchronous bulk operations (move / copy / delete) with progress polling.
