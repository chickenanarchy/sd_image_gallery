# Project Structure

```mermaid
graph TD
    A[sd_index_manager.py] -->|Uses| B[sd_index.db-shm]
    A -->|Uses| C[sd_index.db-wal]
    A -->|Imports| D[webui/main.py]
    D -->|Renders| E[webui/templates/gallery.html]
    D -->|Renders| F[webui/templates/gallery_items.html]
    E -->|Uses| G[webui/static/gallery.css]
    F -->|Uses| G
