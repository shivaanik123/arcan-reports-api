# Deploying the Arcan Reports API on Railway

## Steps

1. **Go to your Railway dashboard** → your project that has the Postgres database

2. **Click "New Service" → "GitHub Repo"** (or "Empty Service" if you prefer manual deploy)

3. **If using GitHub:**
   - Create a new repo (e.g. `arcan-reports-api`)
   - Push the 4 files in this folder: `main.py`, `requirements.txt`, `Procfile`, `railway.toml`
   - Connect the repo to Railway

4. **If using manual deploy (CLI):**
   ```bash
   cd api
   railway login
   railway link    # select your project
   railway up
   ```

5. **Set environment variables** in Railway's service settings:
   - `DATABASE_URL` → your Postgres connection string (Railway can auto-inject this if the DB is in the same project — look for "Reference Variables" and use `${{Postgres.DATABASE_URL}}`)
   - `API_KEY` → pick something secure (e.g. `arcan-weekly-reports-2026` or generate a random string)

6. **Railway will give you a public URL** like `https://arcan-reports-api-production.up.railway.app`

7. **Test it:** Visit `https://your-url/health` in your browser — you should see:
   ```json
   {"status": "ok", "database": "connected"}
   ```

8. **Share the URL and API key with me** so I can connect to it from here.

## Important

- The API key in the `x-api-key` header protects your data from unauthorized access
- Railway's free tier gives you $5/month of usage — this tiny API will use almost none of it
- The API only runs when called (once a week), so costs are minimal
