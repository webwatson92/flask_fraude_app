# FPM - Détection de Fraude (Flask)

## Installation
```bash
python -m venv .venv
source .venv/bin/activate  # sous Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## upd

## Lancement
```bash
export FLASK_APP=app.py
flask run --debug
```
Ou:
```bash
python app.py
```

## Configuration (optionnel)
Le script de traitement par défaut (`fraude_detect_admi.py`) consomme les variables:
- `ADMI_WINDOW_DAYS` : taille de la fenêtre temporelle (en jours)
- `ADMI_OUT_DIR` : dossier de sortie des rapports CSV
- Variables de connexion MySQL (si nécessaire) : `ADMI_DB_HOST`, `ADMI_DB_PORT`, `ADMI_DB_USER`, `ADMI_DB_PASS`, `ADMI_DB_NAME`, etc.

L'application utilise exclusivement le script local `fraude_detect_admi.py`.

Les rapports sont stockés dans `reports/run_YYYYmmdd_HHMMSS/`.

> Remarque : `fraude_detect_admi.py` s'appuie sur `ADMI_OUT_DIR` pour écrire ses CSV et sur `ADMI_WINDOW_DAYS` pour la fenêtre d'analyse.
"# flask_fraude_app" 
