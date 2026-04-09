# Setup Guide

## Prerequisites

- Python 3.12+
- Git
- [uv](https://docs.astral.sh/uv/)

## Installation

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd <repo-name>
   ```

2. Create a virtual environment:
   ```bash
   uv venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   uv pip install -r requirements.txt
   ```

## Running the Project

```bash
python src/main.py
```

## Running Tests

```bash
pytest tests/
```
