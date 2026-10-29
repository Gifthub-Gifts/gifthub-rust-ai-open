# Deploying gifthub-rust-ai

Runs on the **same host as the Django app** (so it reaches the local Redis) as a systemd unit,
alongside the other `/srv/gifthub*` services. CI (`.github/workflows/deploy.yml`) builds the release
binary, scp's it over SSH, restarts the unit, health-checks journald for the ready marker, and rolls
back to the previous binary on failure.

**The deploy ships only the binary.** Provider API keys and `REDIS_URL` live in a `.env` *on the
host* (read by the systemd unit) — never in the repo or in CI.

---

## 1. The only CI secret: `SSH_PRIVATE_KEY`

Host and user come from the repo variables `PROD_HOST` and `PROD_USER` (the deploy job is skipped
while `PROD_HOST` is unset), and the host key is scanned live at deploy time — so the **one** secret
CI needs is the SSH private key.

Add it at **repo → Settings → Secrets and variables → Actions → New repository secret**, name
`SSH_PRIVATE_KEY`.

Two ways to fill it:

- **Reuse the gifthub deploy key** (one key for both repos): paste the same value as `noxur05/gifthub`'s
  `SSH_PRIVATE_KEY` secret. Its public half (`gifthub-gha-deploy-key`) is already in the host's
  `authorized_keys`, so nothing to change on the server. You must copy the value yourself (GitHub
  secret values can't be read back out).
- **Dedicated key** (matches the box's per-project convention — `gifthub-admin-gha-deploy-key`,
  `gifthub-client-gha-deploy-key`, …): generate one, authorize it on the host, set the secret:
  ```bash
  ssh-keygen -t ed25519 -C "gifthub-rust-ai-gha-deploy" -f ./gha_deploy_key -N ""
  ssh "$PROD_USER@$PROD_HOST" "echo '$(cat gha_deploy_key.pub)' >> ~/.ssh/authorized_keys"
  gh secret set SSH_PRIVATE_KEY --repo Gifthub-Gifts/gifthub-rust-ai < gha_deploy_key
  rm gha_deploy_key gha_deploy_key.pub
  ```

---

## 2. One-time host setup (on the production host)

The deploy user needs passwordless sudo for `systemctl` and `journalctl`.

```bash
# 2.1  Deploy dir (owned by the deploy user so scp can write the binary; the unit runs it as root)
sudo mkdir -p /srv/gifthub-rust-ai
sudo chown "$PROD_USER:$PROD_USER" /srv/gifthub-rust-ai

# 2.2  Runtime .env — provider keys + Redis + the Django callback. REDIS_URL MUST be DB 3 (matches
#      Django's AI_GATEWAY_REDIS_URL). DJANGO_CALLBACK_URL + AI_GATEWAY_WEBHOOK_SECRET are REQUIRED:
#      the binary fails to boot without them (a deploy then health-checks red and rolls back), so set
#      them BEFORE the first deploy of the callback build. The secret must be byte-identical to
#      Django's AI_GATEWAY_WEBHOOK_SECRET. See .env.example for the full list.
sudo tee /srv/gifthub-rust-ai/.env >/dev/null <<'ENV'
REDIS_URL=redis://127.0.0.1:6379/3
# Where finished jobs are POSTed (local Granian; bypasses nginx). nginx must still deny external /internal/.
DJANGO_CALLBACK_URL=http://127.0.0.1:8000/internal/ai/callback/
AI_GATEWAY_WEBHOOK_SECRET=<same value as Django's AI_GATEWAY_WEBHOOK_SECRET>
AI_PROVIDERS=cloudflare,openrouter,google
CLOUDFLARE_BASE_URL=https://api.cloudflare.com/client/v4/accounts/<ACCOUNT_ID>/ai/v1/chat/completions
CLOUDFLARE_API_KEY=<token>
CLOUDFLARE_MODEL=@cf/meta/llama-3.1-8b-instruct
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1/chat/completions
OPENROUTER_API_KEY=<key>
OPENROUTER_MODEL=google/gemma-2-9b-it:free
GOOGLE_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/chat/completions
GOOGLE_API_KEY=<key>
GOOGLE_MODEL=gemini-1.5-flash
ENV
sudo chmod 600 /srv/gifthub-rust-ai/.env

# 2.3  Install + enable the unit (don't start yet — no binary until the first deploy)
sudo cp deploy/gifthub-rust-ai.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable gifthub-rust-ai
```

> Obtain the provider API keys from Cloudflare / OpenRouter / Google AI Studio. **Rotate the
> Cloudflare dev token** before using it in prod if it was ever shared. Keep `.env` `chmod 600`.

---

## 3. Deploy

- **Automatic:** every push/merge to `main`.
- **Manual (recommended first):** Actions → *Deploy to production* → *Run workflow* → type `DEPLOY`.

The first deploy is safe before any binary exists (the backup step tolerates it). Verify:

```bash
sudo systemctl status gifthub-rust-ai
sudo journalctl -u gifthub-rust-ai -n 50 --no-pager   # look for: 🚀 gifthub-rust-ai started
```

## 4. Turn it on (Django side)

Once the gateway is up and consuming `ai_jobs`, set `OPENROUTER_API_KEY` (the on/off switch) in the
Django env. Until then the Django AI tasks skip cleanly.

## 5. Cutover to the callback protocol (one-time, coordinated with Django)

The async build changes the contract: results are **POSTed to Django** (not pushed over Redis) and the
gateway **publishes `ai_budget`** that Django paces against. There is no compatibility flag, so deploy
in this order — it's safe because each step degrades gracefully:

1. **Set the shared secret on both hosts.** Put the *same* `AI_GATEWAY_WEBHOOK_SECRET` in
   `/srv/gifthub-rust-ai/.env` (here) and `/srv/gifthub/.env` (Django), and `DJANGO_CALLBACK_URL` here.
   Generate one with e.g. `openssl rand -hex 32`.
2. **Deploy Django first.** The new Django enqueues jobs but **pauses dispatch until it sees an
   `ai_budget` key** — and the old gateway doesn't publish one — so no jobs are created and nothing is
   wasted. (Callbacks it can't yet receive don't exist yet either.)
3. **Deploy this gateway.** It starts publishing `ai_budget` and POSTing callbacks; Django immediately
   resumes, paced to the budget. Any job whose callback is lost is reclaimed by Django's reaper.

Verify after step 3:

```bash
redis-cli -n 3 get ai_budget        # -> {"rpm":...,"rpd":...,"remaining_today":...}
sudo journalctl -u gifthub-rust-ai -n 50 --no-pager | grep -E "ok \(|waiting .* rpm slot"
```

> Confirm nginx denies external `/internal/` (the callback path) — the gateway reaches Django on
> `127.0.0.1:8000` directly, so `/internal/` never needs to be internet-reachable:
> `location /internal/ { allow 127.0.0.1; deny all; }`
