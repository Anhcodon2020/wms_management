# wms_management

Flask app for WMS management.

## Deployment note

This project is a **Flask backend app**, so it cannot run directly on GitHub Pages
(GitHub Pages is for static HTML/CSS/JS only).

Recommended setup:
- Source code on GitHub
- Auto deploy from GitHub Actions to Render

## Deploy from GitHub to Render

1. Create a Render Web Service from this repository.
2. Keep these commands on Render:
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn app:app`
3. In Render, copy your **Deploy Hook URL**.
4. In GitHub repository:
   - Go to `Settings` -> `Secrets and variables` -> `Actions`
   - Add secret `RENDER_DEPLOY_HOOK_URL` with the Deploy Hook URL value.
5. Push to `main`:
   - GitHub Actions workflow `.github/workflows/static.yml` will call Render deploy hook automatically.

## Required environment variables on Render

Set these variables in Render service settings:
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- `MAIL_USERNAME`
- `MAIL_PASSWORD`
- `MAIL_RECIPIENTS`

Python version is configured via `render.yaml` (`3.11.9`).
