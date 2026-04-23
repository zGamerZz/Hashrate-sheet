# Deployment: `/opt/gomining/multiplier_logger`

This repo is deployed on the server as the `mw-multi-sheet-logger.service` systemd service.

## Expected Layout

```text
/opt/gomining/multiplier_logger/
  backfill_*.py
  deploy/
  hashrate_hunter/
  main.py
  requirements.txt
  service_acc.json
  sync_state.sqlite
  sync_debug.log
```

Runtime files such as `sync_state.sqlite`, `sync_debug.log`, `sync.lock`, `__pycache__/`, and `.venv/` stay local to the server and are ignored by Git.

## Install Or Update

Run as a sudo-capable user on the server:

```bash
cd /opt/gomining/multiplier_logger

sudo systemctl stop mw-multi-sheet-logger.service || true
git pull origin main

python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

cp -n deploy/systemd/mw-multi-sheet-logger.env.example .env
sudo install -m 0644 deploy/systemd/mw-multi-sheet-logger.service /etc/systemd/system/mw-multi-sheet-logger.service
sudo systemctl daemon-reload
sudo systemctl enable --now mw-multi-sheet-logger.service
```

Edit `/opt/gomining/multiplier_logger/.env` after the first copy to set secrets and sheet IDs.

## Clean Server Working Tree

Only run cleanup while the service is stopped:

```bash
cd /opt/gomining/multiplier_logger
sudo systemctl stop mw-multi-sheet-logger.service

bash deploy/clean_server_runtime.sh
git status --short --ignored
sudo systemctl start mw-multi-sheet-logger.service
```

Do not delete `service_acc.json` or `sync_state.sqlite` unless you intentionally want to reset credentials or local sync state.

## Service Checks

```bash
systemctl status mw-multi-sheet-logger.service --no-pager
journalctl -u mw-multi-sheet-logger.service -f
```
