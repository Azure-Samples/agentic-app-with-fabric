#!/bin/bash
set -euo pipefail

echo "=== Installing Python dependencies ==="
if [ ! -x .venv/bin/python ]; then
  python3 -m venv --system-site-packages .venv
fi
. .venv/bin/activate
requirements_hash=$(sha256sum requirements.txt | awk '{print $1}')
installed_hash=""

if [ -f .venv/.requirements.sha256 ]; then
  installed_hash=$(cat .venv/.requirements.sha256)
fi

python -m pip install --upgrade pip

if [ "$requirements_hash" != "$installed_hash" ]; then
  python -m pip install --prefer-binary -r requirements.txt
  printf '%s\n' "$requirements_hash" > .venv/.requirements.sha256
else
  echo "Python requirements are unchanged; skipping pip install."
fi

echo "=== Installing Node.js dependencies ==="
npm install --no-audit --no-fund

echo "=== Copying .env.sample to .env (if not present) ==="
if [ ! -f backend/.env ]; then
  cp backend/.env.sample backend/.env
  echo "Created backend/.env from .env.sample — please fill in your credentials."
fi

echo "=== Post-create setup complete ==="
echo "To activate Python:   source .venv/bin/activate"
echo "To start the backend:  cd backend && python launcher.py"
echo "To start the frontend: npm run dev"
