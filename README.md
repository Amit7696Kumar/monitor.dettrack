# Meter OCR - End User -> Server -> Admin

## What it does
- User page: upload meter image
- Server: runs OCR on uploaded meter images
- Admin page: shows extracted text + uploaded image + raw OCR JSON

## Prerequisites
- Python 3.10+ recommended
- VS Code

## Setup (macOS M1)
1) Open terminal and go to project root.
2) Create virtual environment:

   python3 -m venv .venv
   source .venv/bin/activate

3) Install backend dependencies:

   pip install -r server/requirements.txt

4) Run the server:

   uvicorn server.app:app --host 0.0.0.0 --port 8000

## Ubuntu Scripts
- First-time setup + run: `./ubuntu_setup_and_run.sh`
- Run after setup: `./run_project_ubuntu.sh`
- The Ubuntu run script checks OS, virtualenv, required Python packages, and required model files before starting `uvicorn`.

## Recommended Security Env Vars
- `SESSION_SECRET_KEY`: set a long random secret for signed session cookies
- `SESSION_COOKIE_HTTPS_ONLY=1`: enable when the site is served behind HTTPS
- `SESSION_COOKIE_SAMESITE=lax`: default safe browser behavior for session cookies
- `SESSION_MAX_AGE=604800`: example 7-day session lifetime in seconds

## Google Cloud Vision OCR (for uploaded images)
- Install deps: `pip install -r server/requirements.txt`
- Configure Google Vision API key:
  - `export GCV_API_KEY=your_google_vision_api_key`
- OCR backend mode:
  - `export OCR_BACKEND=gcv` (default, GCV only)
  - `export OCR_BACKEND=gcv_then_tesseract` (optional, GCV first then local fallback)
  - `export OCR_BACKEND=tesseract` (optional, local only)

## URLs
- User upload page: http://localhost:8000/
- Admin page: http://localhost:8000/admin

## Notes
- Uploads stored in: server/uploads/
- SQLite DB stored in: server/data/meter_ocr.db
