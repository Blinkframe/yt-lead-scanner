# yt_prospect — Setup Guide

## What you need
- Python 3.8+
- A Google account with access to your prospect spreadsheet
- Two things from Google Cloud Console (steps below)

---

## Step 1 — Install dependencies

```
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib python-dotenv
```

---

## Step 2 — Google Cloud Console setup

Go to: https://console.cloud.google.com

### Enable APIs
1. Create a new project (or use an existing one)
2. Go to **APIs & Services > Library**
3. Enable **YouTube Data API v3**
4. Enable **Google Sheets API**

### Get your YouTube API Key
1. Go to **APIs & Services > Credentials**
2. Click **Create Credentials > API Key**
3. Copy the key — this goes in `config.env` as `YOUTUBE_API_KEY`

### Get your OAuth2 credentials (for Sheets)
1. Go to **APIs & Services > Credentials**
2. Click **Create Credentials > OAuth client ID**
3. Application type: **Desktop app**
4. Download the JSON file and rename it `credentials.json`
5. Place `credentials.json` in the same folder as `yt_prospect.py`

---

## Step 3 — Create config.env

Create a file called `config.env` in the same folder as the script:

```
YOUTUBE_API_KEY=paste_your_api_key_here
GOOGLE_SHEET_ID=paste_your_sheet_id_here
GOOGLE_CREDENTIALS_FILE=credentials.json
```

**Finding your Sheet ID:**  
It's the long string in your Google Sheet URL:  
`https://docs.google.com/spreadsheets/d/THIS_PART_HERE/edit`

---

## Step 4 — Set up your sheet headers

Make sure row 1 of your sheet contains exactly these headers  
(order doesn't matter — the script maps by name):

```
channelName | channelURL | subscriberCount | podcast? | found_date | email_date | contact | email | portfolio? | response?
```

If a header is missing, the script will tell you which one and exit cleanly.

---

## Step 5 — Run it

```
python yt_prospect.py
```

The first run will open a browser window for Google OAuth — just log in and approve.  
After that, auth is cached in `token.pickle` and runs silently.

---

## Files created by the script

| File | Purpose |
|---|---|
| `config.env` | Your API keys (never share this) |
| `credentials.json` | OAuth client secrets (never share this) |
| `token.pickle` | Cached auth token (auto-refreshes) |
| `prospect_runs.log` | Log of every run with counts |

---

## Notes

- YouTube Data API free tier = 10,000 units/day. Each search costs ~100 units, channel lookups cost ~1 unit each. You have headroom for many runs per day.
- The script adds random 1–3 second delays between API calls.
- Duplicate channels (already in your sheet) are logged to console but not added.
- `podcast?`, `email_date`, `contact`, `email`, `portfolio?`, `response?` are left blank for manual entry.
