# Spotify Download

This repo has the Python scripts to:
- extract (download) the data from Spotify using Spotify API.
- transform the data from JSON to CSV file format.

## Getting Started

Follow these instructions to set up and run the project on your local machine.

### Prerequisites

- Python 3.8+ installed on your machine
- `pip` installed (comes with Python)

### Setup

#### 1. Clone the repository

```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
```

#### 2. Create and activate a virtual environment

On Windows:
```bash
python -m venv venv
.\venv\Scripts\activate
```

On macOS/Linux:
```bash
python3 -m venv venv
source venv/bin/activate
```

#### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### Deactivating the Virtual Environment

To deactivate the virtual environment, simply run:

```bash
deactivate
```

### Additional Notes

If you face issues with permissions, you may need to use python3 and pip3 instead of python and pip.

Ensure that all dependencies are correctly listed in requirements.txt. You can generate this file using:

```bash
pip freeze > requirements.txt
```

Replace "your-username/your-repo-name" with your actual repository information and adjust project-specific commands as needed!