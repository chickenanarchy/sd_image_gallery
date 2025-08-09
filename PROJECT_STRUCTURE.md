# Project Architecture

```mermaid
%%{init: {'theme':'neutral','flowchart':{'curve':'basis'}} }%%
flowchart LR
    classDef comp fill:#f5f7fa,stroke:#d5dce3,stroke-width:1px,corner-radius:4px;
    classDef store fill:#eef7ff,stroke:#8fb8e8;
    classDef virt fill:#fff4e5,stroke:#e2c188;
    classDef proc fill:#f0f5ff,stroke:#9bb3e5;
    classDef async fill:#fef6ff,stroke:#d9a8e6,stroke-dasharray:4 2;

    subgraph IDX[CLI Indexer]
        I1[sd_index_manager.py\n(Recursive Scan + Parse)]:::proc
    end

    subgraph DATA[SQLite Store]
        TBL[(files table)\nPaths + Hash + Meta]:::store
        FTS[(files_fts)\nFTS5 Virtual]:::virt
    end

    subgraph APP[FastAPI]
        R[Routes / Search Builder]:::comp
        J[Async Job Manager\n(Bulk Ops Threads)]:::async
    end

    subgraph FRONTEND[Browser UI]
        HTMX[HTMX Requests / Events]:::comp
        TPL[Jinja2 Templates]:::comp
        CSS[Gallery Styles]:::comp
    end

    I1 -->|INSERT / UPDATE| TBL
    TBL --> FTS
    R --> TBL
    J --> TBL
    HTMX --> R
    R --> TPL --> HTMX
    TPL --> CSS
    J -. progress JSON .-> HTMX

    %% Legend
    subgraph LEGEND[Legend]
        L1[comp = component]:::comp
        L2[proc = long-running / processing]:::proc
        L3[store = persisted data]:::store
        L4[virt = virtual index]:::virt
        L5[async = background tasks]:::async
    end
```

The indexer populates and updates the primary `files` table; FTS triggers maintain the `files_fts` virtual table when supported. The web app provides search, pagination, semantic select-all, and asynchronous bulk operations (move / copy / delete) with progress polling.
