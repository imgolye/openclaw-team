# Contributing to OpenClaw Team

Thanks for your interest in contributing! Here's how to get started.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/imgolye/openclaw-team.git
cd openclaw-team

# Start the backend (Python 3.9+)
python3 -m backend.presentation.http.runtime --serve --port 18890

# Start the frontend (Node 18+)
cd apps/frontend
npm install
npm run dev
```

Frontend runs at `http://localhost:5173`, backend at `http://localhost:18890`.

## Project Structure

```
apps/frontend/    React + Vite web app
backend/          Python HTTP server (no framework)
platform/         Deploy scripts, themes, skills, tests
docs/             Architecture and design docs
```

## How to Contribute

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Test locally — make sure the frontend builds and backend starts
4. Submit a pull request with a clear description of what you changed and why

## Code Style

- **Python**: Follow existing patterns. No external linter enforced — just keep it readable.
- **JavaScript/JSX**: Consistent with existing React code. Ant Design for UI components.
- **CSS**: Follow the layered architecture in `apps/frontend/src/styles/`.

## What We're Looking For

- Bug fixes with clear reproduction steps
- New skills in `platform/skills/`
- Documentation improvements
- i18n translations
- Frontend UX improvements

## Reporting Issues

Open an issue with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Your environment (OS, Python version, Node version)

## License

By contributing, you agree that your contributions will be licensed under the AGPL-3.0 License.
