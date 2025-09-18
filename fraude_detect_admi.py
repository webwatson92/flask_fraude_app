#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Détection automatique des fraudes T1..T4 depuis MySQL 'admi'
- Aucune vue SQL requise
- Enrichissement libellés (structures/prestataires, bénéficiaires, type prestation, acte)
- Exports CSV dans ./reports
"""

import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

# =========================
# 1) PARAMÈTRES
# =========================
DB_HOST = os.getenv("ADMI_DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("ADMI_DB_PORT", "3306"))
DB_USER = os.getenv("ADMI_DB_USER", "root")
DB_PASS = os.getenv("ADMI_DB_PASS", "")
DB_NAME = os.getenv("ADMI_DB_NAME", "admi")

WINDOW_DAYS       = int(os.getenv("ADMI_WINDOW_DAYS", "90"))
T2_TOLERANCE_PCT  = float(os.getenv("ADMI_T2_TOL_PCT", "0.00"))   # 0.10 = 10%
T3_MIN_ACTES_7J   = int(os.getenv("ADMI_T3_MIN_ACTES_7J", "4"))
T3_MIN_ECARTS_7J  = int(os.getenv("ADMI_T3_MIN_ECARTS_7J", "2"))
T4_MIN_STRUCTS_J  = int(os.getenv("ADMI_T4_MIN_STRUCTS_JOUR", "2"))

OUT_DIR = os.getenv("ADMI_OUT_DIR", "reports")
os.makedirs(OUT_DIR, exist_ok=True)
DATE_MIN = (datetime.today() - timedelta(days=WINDOW_DAYS)).date()

# =========================
# 2) CONNEXION
# =========================
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_recycle=3600, pool_pre_ping=True
)

def q(conn, sql, params=None, name=""):
    """SELECT helper robuste. Autorise q(conn, sql, 'nom')."""
    if isinstance(params, str) and not name:
        name, params = params, None
    df = pd.read_sql(text(sql), conn, params=params or {})
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {name or sql.splitlines()[0][:65]} -> {len(df)} lignes")
    return df

# =========================
# 3) LECTURE DES TABLES
# =========================
with engine.begin() as conn:
    cols_at = q(conn, "SHOW COLUMNS FROM acte_trans", name="cols_acte_trans")
    has_benef = "id_beneficiaire" in cols_at["Field"].str.lower().tolist()

    df_at = q(conn, f"""
        SELECT id_acte_trans, id_structure, id_type_prest, date_soin,
               { 'id_beneficiaire' if has_benef else 'NULL AS id_beneficiaire' }
        FROM acte_trans
        WHERE date_soin >= :dmin
    """, {"dmin": DATE_MIN}, "acte_trans (90j)")

    df_la = q(conn, """
        SELECT id_list_acte_acte_trans, id_acte_trans, id_acte,
               quantite, date_execution_acte, montant_acte
        FROM list_acte_acte_trans
        WHERE id_acte_trans IN (
            SELECT id_acte_trans FROM acte_trans WHERE date_soin >= :dmin
        )
    """, {"dmin": DATE_MIN}, "list_acte_acte_trans (90j)")

    df_ac = q(conn, "SELECT id_avenant, id_acte, forfait_acte_convention FROM actes_convention", name="actes_convention")

    # Référentiels (DIM) pour libellés
    df_ss  = q(conn, "SELECT id_structure, str_id_structure, id_type_str_sante FROM structure_sante", name="structure_sante")
    df_s   = q(conn, "SELECT code_structure, nom_structure FROM structure", name="structure")
    df_tss = q(conn, "SELECT id_type_str_sante, libelle_type_structure_sante FROM type_str_sante", name="type_str_sante")
    df_ad  = q(conn, "SELECT id_adherent, num_bnf, matricule, nom, prenoms, telephone FROM adherent", name="adherent")
    df_tp  = q(conn, "SELECT id_type_prest, libelle_type_prestation, code_prestation FROM type_prestation", name="type_prestation")
    df_act = q(conn, "SELECT id_acte, code_acte, libelle_acte FROM acte", name="acte")

if df_at.empty or df_la.empty:
    print("Aucune donnée sur la période -> arrêt.")
    raise SystemExit(0)

# =========================
# 4) DIM (libellés)
# =========================
dim_struct = (df_ss
              .merge(df_s, left_on="str_id_structure", right_on="code_structure", how="left")
              .merge(df_tss, on="id_type_str_sante", how="left"))
dim_struct = dim_struct.rename(columns={
    "id_structure": "id_structure",
    "str_id_structure": "structure_code",
    "nom_structure": "structure_nom",
    "libelle_type_structure_sante": "type_structure_libelle"
})[["id_structure","structure_code","structure_nom","type_structure_libelle"]]

dim_benef = df_ad.rename(columns={
    "id_adherent": "id_beneficiaire",
    "num_bnf": "num_beneficiaire",
    "matricule": "mecano",
    "nom": "beneficiaire_nom",
    "prenoms": "beneficiaire_prenom",
    "telephone": "beneficiaire_contact"
})[["id_beneficiaire","num_beneficiaire","mecano","beneficiaire_nom","beneficiaire_prenom","beneficiaire_contact"]]

dim_tp = df_tp.rename(columns={
    "id_type_prest": "id_type_prestation",
    "libelle_type_prestation": "type_prestation_libelle"
})[["id_type_prestation","type_prestation_libelle","code_prestation"]]

dim_acte = df_act.rename(columns={
    "id_acte":"id_acte","code_acte":"acte_code","libelle_acte":"acte_libelle"
})[["id_acte","acte_code","acte_libelle"]]

# =========================
# 5) Dernier tarif par acte
# =========================
if df_ac.empty:
    print("ATTENTION: 'actes_convention' est vide -> T2 ne comparera pas au tarif.")
df_ac_last = (df_ac.sort_values(["id_acte","id_avenant"])
                   .groupby("id_acte", as_index=False)
                   .tail(1)[["id_acte","forfait_acte_convention"]]
                   .rename(columns={"forfait_acte_convention":"tarif_officiel"}))

# =========================
# 6) Flux transaction × acte enrichi
# =========================
df_tx = (df_la
         .merge(df_at, on="id_acte_trans", how="inner")
         .merge(df_ac_last, on="id_acte", how="left")
         .merge(dim_struct, on="id_structure", how="left")
         .merge(dim_benef, on="id_beneficiaire", how="left")
         .merge(dim_tp, left_on="id_type_prest", right_on="id_type_prestation", how="left")
         .merge(dim_acte, on="id_acte", how="left"))

df_tx["montant_execute"] = df_tx["montant_acte"]
df_tx["ecart_montant"]   = df_tx["montant_execute"] - df_tx["tarif_officiel"].fillna(0)

# =========================
# 7) Détections
# =========================

# --- T2 : Surfacturation
cond_tarif = df_tx["tarif_officiel"].notna()
cond_surf  = df_tx["montant_execute"] > df_tx["tarif_officiel"] * (1 + T2_TOLERANCE_PCT)
df_T2 = df_tx.loc[cond_tarif & cond_surf].copy()
df_T2["typologie_code"] = "T2"
df_T2["raison"] = f"Surfacturation: montant > tarif_officiel x (1+{T2_TOLERANCE_PCT:.2f})"

# --- T1a : Transactions sans lignes
ids_with_lines = set(df_la["id_acte_trans"].unique())
df_T1a = df_at.loc[~df_at["id_acte_trans"].isin(ids_with_lines)].copy()
df_T1a["id_acte"] = None
df_T1a["montant_execute"] = None
df_T1a["tarif_officiel"] = None
df_T1a["ecart_montant"] = None
df_T1a["typologie_code"] = "T1"
df_T1a["raison"] = "Transaction sans actes rattachés"
df_T1a = (df_T1a
          .merge(dim_struct, on="id_structure", how="left")
          .merge(dim_benef, on="id_beneficiaire", how="left")
          .merge(dim_tp, left_on="id_type_prest", right_on="id_type_prestation", how="left"))

# --- T1b : Lignes sans date d’exécution
df_T1b = df_tx.loc[df_tx["date_execution_acte"].isna()].copy()
df_T1b["typologie_code"] = "T1"
df_T1b["raison"] = "Acte sans date d'exécution"

# --- T3 : Collusion (structure × bénéficiaire, fenètre 7 jours)
if df_tx["id_beneficiaire"].isna().all():
    df_T3 = pd.DataFrame(columns=[
        "id_structure","structure_code","structure_nom",
        "id_beneficiaire","beneficiaire_nom","beneficiaire_prenom",
        "periode_debut","periode_fin","nb_actes_7j","nb_ecarts_pos_7j",
        "typologie_code","raison"
    ])
else:
    base = df_tx[[
        "id_structure","structure_code","structure_nom",
        "id_beneficiaire","beneficiaire_nom","beneficiaire_prenom",
        "date_soin","ecart_montant"
    ]].dropna(subset=["id_beneficiaire"]).copy()

    # Auto-corrélation 7j : on limite la table de droite aux colonnes minimales pour éviter les suffixes de libellés
    a = base.merge(
        base[["id_structure","id_beneficiaire","date_soin","ecart_montant"]],
        on=["id_structure","id_beneficiaire"], suffixes=("_1","_2")
    )
    a = a[(a["date_soin_2"] >= a["date_soin_1"] - pd.Timedelta(days=7)) &
          (a["date_soin_2"] <= a["date_soin_1"])]

    g = a.groupby(
        ["id_structure","structure_code","structure_nom",
         "id_beneficiaire","beneficiaire_nom","beneficiaire_prenom"],
        as_index=False
    ).agg(
        periode_debut=("date_soin_2","min"),
        periode_fin=("date_soin_2","max"),
        nb_actes_7j=("date_soin_2","size"),
        nb_ecarts_pos_7j=("ecart_montant_2", lambda s: (s > 0).sum())
    )

    df_T3 = g[(g["nb_actes_7j"] >= T3_MIN_ACTES_7J) &
              (g["nb_ecarts_pos_7j"] >= T3_MIN_ECARTS_7J)].copy()
    df_T3["typologie_code"] = "T3"
    df_T3["raison"] = f"Collusion 7j (N>={T3_MIN_ACTES_7J}, écarts+>={T3_MIN_ECARTS_7J})"

# --- T4 : Usurpation d’identité (même bénéficiaire, même jour, >= N structures)
if df_tx["id_beneficiaire"].isna().all():
    df_T4 = pd.DataFrame(columns=[
        "id_beneficiaire","beneficiaire_nom","beneficiaire_prenom",
        "jour","nb_structures","structures","typologie_code","raison"
    ])
else:
    d = df_tx[[
        "id_beneficiaire","beneficiaire_nom","beneficiaire_prenom",
        "id_structure","date_soin"
    ]].dropna(subset=["id_beneficiaire"]).copy()
    d["jour"] = pd.to_datetime(d["date_soin"]).dt.date
    g = d.groupby(["id_beneficiaire","beneficiaire_nom","beneficiaire_prenom","jour"]).agg(
        nb_structures=("id_structure","nunique"),
        structures=("id_structure", lambda x: ",".join(sorted(map(str, set(x)))))
    ).reset_index()
    df_T4 = g[g["nb_structures"] >= T4_MIN_STRUCTS_J].copy()
    df_T4["typologie_code"] = "T4"
    df_T4["raison"] = f"Même bénéficiaire dans ≥{T4_MIN_STRUCTS_J} structures le même jour"

# =========================
# 8) EXPORTS CSV
# =========================
def dump(df, name, cols=None):
    path = os.path.join(OUT_DIR, f"{name}.csv")
    if cols:
        cols = [c for c in cols if c in df.columns]
        df = df[cols]
    df.to_csv(path, index=False)
    print("Écrit ->", path)

cols_T2 = [
    "typologie_code","raison","date_soin",
    "id_structure","structure_code","structure_nom","type_structure_libelle",
    "id_beneficiaire","beneficiaire_nom","beneficiaire_prenom","mecano","beneficiaire_contact",
    "id_type_prest","id_type_prestation","type_prestation_libelle",
    "id_acte","acte_code","acte_libelle",
    "montant_execute","tarif_officiel","ecart_montant"
]
cols_T1 = cols_T2
cols_T3 = [
    "typologie_code","raison",
    "id_structure","structure_code","structure_nom",
    "id_beneficiaire","beneficiaire_nom","beneficiaire_prenom",
    "periode_debut","periode_fin","nb_actes_7j","nb_ecarts_pos_7j"
]
cols_T4 = [
    "typologie_code","raison",
    "id_beneficiaire","beneficiaire_nom","beneficiaire_prenom",
    "jour","nb_structures","structures"
]

dump(df_T1a, "auto_T1a", cols_T1)
dump(df_T1b, "auto_T1b", cols_T1)
dump(df_T2,  "auto_T2",  cols_T2)
dump(df_T3,  "auto_T3",  cols_T3)
dump(df_T4,  "auto_T4",  cols_T4)

# =========================
# 9) CONSOLIDÉ HOMOGÈNE
# =========================
cols_all = [
    "typologie_code","raison","date_soin","jour","periode_debut","periode_fin",
    "id_acte_trans","id_acte","acte_code","acte_libelle",
    "id_structure","structure_code","structure_nom","type_structure_libelle",
    "id_beneficiaire","beneficiaire_nom","beneficiaire_prenom","mecano","beneficiaire_contact",
    "id_type_prest","id_type_prestation","type_prestation_libelle",
    "montant_execute","tarif_officiel","ecart_montant",
    "nb_actes_7j","nb_ecarts_pos_7j","nb_structures","structures"
]
def norm(df, add=None):
    if df is None or df.empty:
        return pd.DataFrame(columns=cols_all)
    out = df.copy()
    if add:
        for k,v in add.items():
            out[k] = v
    for c in cols_all:
        if c not in out.columns:
            out[c] = None
    return out[cols_all]

df_all = pd.concat([
    norm(df_T1a, {"typologie_code":"T1"}),
    norm(df_T1b, {"typologie_code":"T1"}),
    norm(df_T2,  {"typologie_code":"T2"}),
    norm(df_T3,  {"typologie_code":"T3"}),
    norm(df_T4,  {"typologie_code":"T4"}),
], ignore_index=True)

dump(df_all, "auto_all", cols_all)

print("OK. Détection enrichie terminée.")
