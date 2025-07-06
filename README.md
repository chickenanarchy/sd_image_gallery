# SD Image Gallery

A web-based gallery and search tool for Stable Diffusion images with embedded metadata.

## Overview

This project indexes a large collection of images potentially containing embedded Stable Diffusion metadata using the [sd-parsers](https://github.com/d3x-at/sd-parsers) library. It stores the extracted metadata in an SQLite database and provides a web UI for browsing, searching, and viewing images along with their metadata.

## Features

- **Indexing:** Recursively scans directories for images, extracts Stable Diffusion metadata, and stores it in a database.
- **Gallery:** Displays thumbnails of indexed images in a responsive, infinite-scroll style grid.
- **Search:** Supports multi-condition text search on metadata JSON with logical operators (AND, OR, NOT).
- **Image Viewer:** Click thumbnails to view full-size images alongside formatted metadata.
- **Lightweight Web UI:** Built with FastAPI backend and HTMX-enhanced frontend for minimal JavaScript and fast interactions.

## Getting Started

### Prerequisites

- Python 3.8+
- [sd-parsers](https://github.com/d3x-at/sd-parsers) Python package
- Git (for cloning and version control)

### Installation

1. Clone the repository:

   ```bash
   git clone git@github.com:chickenanarchy/sd_image_gallery.git
   cd sd_image_gallery
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Run the index manager script:

   ```bash
   python sd_index_manager.py
   ```

4. Choose option 1 to index your image directory.

5. Choose option 3 to launch the Web UI.

6. Open your browser at [http://localhost:8000](http://localhost:8000) to browse and search your images.

## Usage

- Use the search bar to enter text queries that filter images by metadata.
- Click the "+" button to add additional search lines with logical operators.
- Click on thumbnails to view full images and metadata details.

## Project Structure

- `sd_index_manager.py`: Main script for indexing and managing the database.
- `webui/`: FastAPI backend and frontend templates.
- `sd_parsers_test.py`: Utility script to test metadata extraction from images.
- `test/`: Sample images for testing.

## Contributing

Contributions are welcome! Please fork the repository and submit pull requests.

## License

MIT License

## Contact

GitHub: [chickenanarchy](https://github.com/chickenanarchy)

Email: chickenanarchy@gmail.com
