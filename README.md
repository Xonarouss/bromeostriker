# BromeStriker (Python) — NL

## Lokaal draaien
```bat
py -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python -m bromestriker
```

## Commands
- /mute user reden?  -> Strike 1 = 24u dempen, Strike 2 = 7 dagen dempen, Strike 3 = ban (strikes gewist)
- /unmute user reden?
- /strikes user  (publiek)
- /resetstrikes user reden?

## Gedempt zichtbaarheid
Gedempt mag alle kanalen zien zoals een normale member, behalve categorie IDs in `MUTED_HIDDEN_CATEGORY_IDS`.
