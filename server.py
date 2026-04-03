#!/usr/bin/env python3
"""
Intent Analytics — MCP Server (FastMCP)
Serveur MCP pour SAHAR Conseil v2, deployable sur Railway en mode HTTP/SSE.

8 outils :
  - estimate          : Estimation prix immobilier (RPC Supabase)
  - enrich            : Enrichissement commune (geo + DPE + marche + POI)
  - get_commune_profile : Profil complet depuis mart.kpi_cache
  - search            : Recherche multicriteres de communes
  - search_nearby     : Recherche PostGIS autour d'un point
  - get_renovation_aids : Aides renovation par commune/revenus
  - query             : SQL lecture seule sur Supabase
  - health            : Health check

Deploiement Railway :
  PORT=8000 python server.py
"""

import os
import sys
import json
import re
import logging
from typing import Optional
from datetime import datetime

try:
    from fastmcp import FastMCP
except ImportError:
    print("pip install fastmcp", file=sys.stderr)
    sys.exit(1)

import requests

# Config
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://wwvdpixzfaviaapixarb.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
PORT = int(os.getenv("PORT", "8000"))

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

GEO_API = "https://geo.api.gouv.fr"
BAN_API = "https://api-adresse.data.gouv.fr"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("intent-analytics")

# MCP Server
mcp = FastMCP(
    name="intent-analytics",
    instructions="SAHAR Conseil v2 - Enrichissement immobilier open data francais. DVF, DPE ADEME, BAN, INSEE, POI. 8 outils d analyse territoriale et d estimation.",
    version="2.0.0",
)


# Helpers
def _sb_rest(table, params, schema="public"):
    headers = {**SB_HEADERS}
    if schema != "public":
        headers["Accept-Profile"] = schema
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def _sb_rpc(fn_name, payload):
    url = f"{SUPABASE_URL}/rest/v1/rpc/{fn_name}"
    r = requests.post(url, headers=SB_HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def _geo_commune(code_postal, nom=""):
    params = {"codePostal": code_postal, "fields": "nom,code,codeDepartement,codeRegion,codesPostaux,population,surface,centre"}
    if nom:
        params["nom"] = nom
        params["boost"] = "population"
    r = requests.get(f"{GEO_API}/communes", params=params, timeout=10)
    r.raise_for_status()
    communes = r.json()
    if not communes:
        return {}
    return max(communes, key=lambda c: c.get("population", 0))


def _resolve_code_insee(code_postal):
    commune = _geo_commune(code_postal)
    return commune.get("code") if commune else None


DPE_ADJUSTMENTS = {"A": 0.10, "B": 0.05, "C": 0.0, "D": -0.03, "E": -0.08, "F": -0.13, "G": -0.18}


# Tool 1: estimate
@mcp.tool()
def estimate(code_postal: str, surface: float, type_local: str, dpe: str = "C", nb_pieces: Optional[float] = None) -> dict:
    """Estime le prix d un bien immobilier via RPC Supabase."""
    try:
        code_insee = _resolve_code_insee(code_postal)
        if not code_insee:
            return {"erreur": f"Code postal {code_postal} non reconnu."}
        payload = {"p_code_insee": code_insee, "p_surface": surface, "p_type_bien": type_local}
        if nb_pieces:
            payload["p_nb_pieces"] = int(nb_pieces)
        try:
            result = _sb_rpc("estimate_with_quality", payload)
        except Exception:
            result = _sb_rpc("estimate_property", payload)
        if not result:
            return {"erreur": "Estimation impossible - pas assez de donnees."}
        if isinstance(result, list) and result:
            result = result[0]
        prix_base = result.get("prix_estime") or result.get("estimated_price", 0)
        prix_m2_base = result.get("prix_m2") or result.get("price_per_m2", 0)
        dpe_upper = (dpe or "C").upper()
        adj = DPE_ADJUSTMENTS.get(dpe_upper, 0.0)
        prix_final = round(prix_base * (1 + adj))
        prix_m2_final = round(prix_m2_base * (1 + adj))
        return {
            "code_postal": code_postal, "code_insee": code_insee, "surface_m2": surface,
            "type_local": type_local, "dpe": dpe_upper, "prix_estime": prix_final,
            "prix_m2": prix_m2_final, "ajustement_dpe": f"{adj:+.0%}",
            "nb_pieces": nb_pieces, "source": "Supabase RPC + DVF",
            "qualite": result.get("quality_grade") or result.get("confidence"),
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        log.exception("estimate error")
        return {"erreur": str(e)}


# Tool 2: enrich
@mcp.tool()
def enrich(code_postal: str, nom_commune: str = "") -> dict:
    """Enrichit les donnees d une commune en croisant geo.api.gouv.fr + Supabase (DPE, marche, POI)."""
    try:
        geo = _geo_commune(code_postal, nom_commune)
        if not geo:
            return {"erreur": f"Commune non trouvee pour CP {code_postal}"}
        code_insee = geo.get("code", "")
        centre = geo.get("centre", {}).get("coordinates", [None, None])
        result = {
            "code_postal": code_postal, "code_insee": code_insee, "nom": geo.get("nom"),
            "departement": geo.get("codeDepartement"), "region": geo.get("codeRegion"),
            "population": geo.get("population"),
            "superficie_km2": round(geo.get("surface", 0) / 1_000_000, 2),
            "centre": {"lat": centre[1], "lon": centre[0]} if centre[0] else None,
        }
        try:
            dpe_rows = _sb_rest("dpe_commune", {"city_code": f"eq.{code_insee}", "select": "*", "limit": "1"}, schema="mart")
            if dpe_rows:
                d = dpe_rows[0]
                result["dpe"] = {"pct_passoires_fg": round((d.get("pct_f", 0) or 0) + (d.get("pct_g", 0) or 0), 1), "conso_moy_kwh_m2": d.get("conso_moy_kwh_m2"), "score_energie": d.get("score_energie_0_100"), "nb_dpe": d.get("nb_dpe_total")}
        except Exception as e:
            result["dpe"] = {"erreur": str(e)}
        try:
            market_rows = _sb_rest("price_commune", {"city_code": f"eq.{code_insee}", "select": "*", "limit": "1"}, schema="mart")
            if market_rows:
                m = market_rows[0]
                result["marche"] = {"prix_m2_median": m.get("prix_median_m2"), "prix_m2_q1": m.get("prix_q1_m2"), "prix_m2_q3": m.get("prix_q3_m2"), "nb_ventes_12m": m.get("nb_transactions_12m"), "evolution_12m": m.get("evolution_12m_pct"), "trend": m.get("trend")}
        except Exception as e:
            result["marche"] = {"erreur": str(e)}
        try:
            poi_rows = _sb_rest("poi_scores", {"city_code": f"eq.{code_insee}", "select": "*", "limit": "1"}, schema="mart")
            if poi_rows:
                p = poi_rows[0]
                result["poi"] = {"score_global": p.get("score_global"), "education": p.get("score_education"), "sante": p.get("score_sante"), "commerce": p.get("score_commerce"), "transport": p.get("score_transport"), "sport_loisirs": p.get("score_sport_loisirs")}
        except Exception as e:
            result["poi"] = {"erreur": str(e)}
        result["timestamp"] = datetime.utcnow().isoformat()
        return result
    except Exception as e:
        log.exception("enrich error")
        return {"erreur": str(e)}


# Tool 3: get_commune_profile
@mcp.tool()
def get_commune_profile(code_postal: str) -> dict:
    """Retourne le profil complet d une commune depuis mart.kpi_cache."""
    try:
        code_insee = _resolve_code_insee(code_postal)
        if not code_insee:
            return {"erreur": f"Code postal {code_postal} non reconnu."}
        rows = _sb_rest("kpi_cache", {"city_code": f"eq.{code_insee}", "select": "*", "limit": "1"}, schema="mart")
        if rows:
            profile = rows[0]
            profile["_source"] = "mart.kpi_cache"
            profile["_code_postal"] = code_postal
            return profile
        try:
            rows2 = _sb_rest("commune_profile", {"city_code": f"eq.{code_insee}", "select": "*", "limit": "1"}, schema="api")
            if rows2:
                profile = rows2[0]
                profile["_source"] = "api.commune_profile"
                profile["_code_postal"] = code_postal
                return profile
        except Exception:
            pass
        return {"erreur": f"Profil non disponible pour {code_postal} ({code_insee})."}
    except Exception as e:
        log.exception("get_commune_profile error")
        return {"erreur": str(e)}


# Tool 4: search
@mcp.tool()
def search(nom: str = "", departement: str = "", prix_m2_min: Optional[float] = None, prix_m2_max: Optional[float] = None, population_min: Optional[float] = None, pct_passoires_max: Optional[float] = None, limit: int = 20) -> dict:
    """Recherche multicriteres de communes dans Supabase SAHAR v2."""
    try:
        limit = min(int(limit), 100)
        params = {"select": "city_code,city_name,dept_code,population,prix_median_m2,nb_transactions_12m,evolution_12m_pct,trend,pct_passoires_fg,score_energie,score_global,confidence", "order": "population.desc.nullslast", "limit": str(limit)}
        if nom:
            params["city_name"] = f"ilike.*{nom}*"
        if departement:
            params["dept_code"] = f"eq.{departement}"
        if prix_m2_min is not None:
            params["prix_median_m2"] = f"gte.{prix_m2_min}"
        if prix_m2_max is not None:
            if "prix_median_m2" in params:
                params["and"] = f"(prix_median_m2.gte.{prix_m2_min},prix_median_m2.lte.{prix_m2_max})"
                del params["prix_median_m2"]
            else:
                params["prix_median_m2"] = f"lte.{prix_m2_max}"
        if population_min is not None:
            params["population"] = f"gte.{int(population_min)}"
        if pct_passoires_max is not None:
            params["pct_passoires_fg"] = f"lte.{pct_passoires_max}"
        rows = _sb_rest("kpi_cache", params, schema="mart")
        return {"results": rows, "total": len(rows), "filters": {"nom": nom or None, "departement": departement or None, "prix_m2_min": prix_m2_min, "prix_m2_max": prix_m2_max, "population_min": population_min, "pct_passoires_max": pct_passoires_max}, "limit": limit}
    except Exception as e:
        log.exception("search error")
        return {"erreur": str(e), "results": [], "total": 0}


# Tool 5: search_nearby
@mcp.tool()
def search_nearby(lat: float, lon: float, type: str, radius: int = 1000) -> dict:
    """Recherche des points d interet, DPE ou bornes autour d un point via PostGIS."""
    try:
        radius = min(int(radius), 10000)
        rpc_map = {"poi": "search_nearby_poi", "dpe": "search_nearby_dpe", "borne_recharge": "search_nearby_bornes", "transaction": "search_nearby_transactions"}
        rpc_fn = rpc_map.get(type)
        if not rpc_fn:
            return {"erreur": f"Type inconnu: {type}. Valeurs possibles: {list(rpc_map.keys())}"}
        payload = {"p_lat": lat, "p_lon": lon, "p_radius": radius}
        try:
            results = _sb_rpc(rpc_fn, payload)
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 404:
                try:
                    results = _sb_rpc("search_nearby", {**payload, "p_type": type})
                except Exception:
                    return {"erreur": f"RPC {rpc_fn} non disponible.", "results": [], "count": 0}
            else:
                raise
        if isinstance(results, list):
            return {"results": results[:50], "count": len(results), "params": {"lat": lat, "lon": lon, "type": type, "radius": radius}}
        return {"results": [], "count": 0, "raw": results}
    except Exception as e:
        log.exception("search_nearby error")
        return {"erreur": str(e), "results": [], "count": 0}


# Tool 6: get_renovation_aids
@mcp.tool()
def get_renovation_aids(code_insee: str, revenu_classe: str) -> dict:
    """Retourne les aides a la renovation disponibles (MaPrimeRenov, CEE, etc.)."""
    try:
        params = {"select": "*", "or": f"(code_insee.eq.{code_insee},code_insee.is.null)", "order": "montant_max.desc.nullslast"}
        rows = _sb_rest("ref_renovation_aids", params)
        filtered = []
        for row in rows:
            eligible = row.get("revenus_eligibles", [])
            if not eligible or revenu_classe in (eligible if isinstance(eligible, list) else []):
                filtered.append(row)
        if not filtered:
            try:
                rpc_result = _sb_rpc("get_renovation_aids", {"p_code_insee": code_insee, "p_revenu_classe": revenu_classe})
                if isinstance(rpc_result, list):
                    filtered = rpc_result
            except Exception:
                pass
        return {"code_insee": code_insee, "revenu_classe": revenu_classe, "nb_aides": len(filtered), "aides": filtered, "sources": ["ANAH", "MaPrimeRenov", "CEE", "Eco-PTZ", "Collectivites locales"], "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        log.exception("get_renovation_aids error")
        return {"erreur": str(e)}


# Tool 7: query
@mcp.tool()
def query(sql: str) -> dict:
    """Execute une requete SQL en lecture seule sur Supabase SAHAR v2. Seuls les SELECT sont autorises."""
    try:
        sql_clean = sql.strip().rstrip(";")
        sql_upper = sql_clean.upper()
        forbidden = ["DELETE", "DROP", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE"]
        for kw in forbidden:
            if re.search(rf"\b{kw}\b", sql_upper):
                return {"erreur": f"Requete non autorisee: {kw} interdit."}
        if not sql_upper.lstrip().startswith("SELECT"):
            return {"erreur": "Seuls les SELECT sont autorises."}
        result = _sb_rpc("execute_readonly_query", {"query_text": sql_clean})
        if isinstance(result, list):
            return {"data": result, "count": len(result)}
        elif isinstance(result, dict) and "error" in result:
            return {"erreur": result["error"]}
        else:
            return {"data": result, "count": 1 if result else 0}
    except requests.exceptions.HTTPError as e:
        if e.response and e.response.status_code == 404:
            return {"erreur": "RPC execute_readonly_query non disponible."}
        return {"erreur": str(e)}
    except Exception as e:
        log.exception("query error")
        return {"erreur": str(e)}


# Tool 8: health
@mcp.tool()
def health() -> dict:
    """Verifie la sante du serveur MCP et de Supabase SAHAR v2."""
    checks = {}
    status = "healthy"
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/", headers=SB_HEADERS, timeout=10)
        checks["supabase"] = "ok" if r.status_code in (200, 204) else f"status {r.status_code}"
    except Exception as e:
        checks["supabase"] = f"error: {str(e)}"
        status = "degraded"
    tables_to_check = [("mart", "kpi_cache"), ("mart", "price_commune"), ("mart", "dpe_commune"), ("core", "communes")]
    volumes = {}
    for schema, table in tables_to_check:
        try:
            headers = {**SB_HEADERS, "Prefer": "count=exact"}
            if schema != "public":
                headers["Accept-Profile"] = schema
            r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers, params={"select": "city_code", "limit": "0"}, timeout=10)
            count_header = r.headers.get("content-range", "*/0")
            total = count_header.split("/")[-1]
            volumes[f"{schema}.{table}"] = int(total) if total.isdigit() else total
        except Exception as e:
            volumes[f"{schema}.{table}"] = f"error: {str(e)}"
            status = "degraded"
    checks["volumes"] = volumes
    try:
        _sb_rpc("estimate_property", {"p_code_insee": "75056", "p_surface": 50, "p_type_bien": "Appartement"})
        checks["rpc_estimate"] = "ok"
    except Exception as e:
        checks["rpc_estimate"] = f"unavailable: {str(e)[:100]}"
    try:
        r = requests.get(f"{GEO_API}/communes/75056", timeout=5)
        checks["geo_api"] = "ok" if r.status_code == 200 else f"status {r.status_code}"
    except Exception as e:
        checks["geo_api"] = f"error: {str(e)}"
    return {"status": status, "version": "2.0.0", "server": "intent-analytics", "deployment": "railway", "checks": checks, "timestamp": datetime.utcnow().isoformat()}


# Entry point
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Intent Analytics MCP Server")
    parser.add_argument("--stdio", action="store_true", help="Mode stdio (Claude Desktop)")
    parser.add_argument("--port", type=int, default=PORT, help=f"Port HTTP (defaut: {PORT})")
    args = parser.parse_args()
    if args.stdio:
        log.info("Intent Analytics MCP -> stdio mode")
        mcp.run(transport="stdio")
    else:
        log.info(f"Intent Analytics MCP -> SSE on http://0.0.0.0:{args.port}")
        mcp.run(transport="sse", host="0.0.0.0", port=args.port)
