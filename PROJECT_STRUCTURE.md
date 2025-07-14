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
