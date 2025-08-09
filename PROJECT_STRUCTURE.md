# Project Architecture

```mermaid
graph TD
    subgraph Indexer [CLI Indexer]
        I1[sd_index_manager.py]
        I1 --> DB[(SQLite DB)]
    end

    subgraph WebAPI [FastAPI App]
        R[Routes & Search]
        J[Async Job Manager]
        R --> DB
        J --> DB
    end

    subgraph Frontend [HTMX + Jinja2]
        T[Templates]
        H[HTMX / JS]
        C[CSS]
        H --> R
        T --> H
        T --> C
    end

    I1 --> R
    R --> T
    J --> H
