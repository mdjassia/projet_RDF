from rdflib import Graph, URIRef, Namespace, RDF, Literal, XSD
from SPARQLWrapper import SPARQLWrapper, JSON
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time

# -----------------------------
# Config
# -----------------------------
INPUT_RDF = "schema/data/player.ttl"
OUTPUT_RDF = "schema/data/players_enriched.ttl"
BATCH_SIZE = 100
SLEEP_TIME = 1
MAX_WORKERS = 3

# Namespaces
EX = Namespace("http://example.org/football/")
DBO = Namespace("http://dbpedia.org/ontology/")

# -----------------------------
# Connexion à DBpedia
# -----------------------------
sparql = SPARQLWrapper("https://dbpedia.org/sparql")
sparql.setReturnFormat(JSON)

# -----------------------------
# Charger RDF local
# -----------------------------
g = Graph()
g.parse(INPUT_RDF, format="ttl")
players = list(g.subjects(RDF.type, EX.Footballer))
print(f"{len(players)} joueurs trouvés.")

# -----------------------------
# Helpers
# -----------------------------
def get_first(res, *keys):
    """Retourne la première valeur trouvée dans le résultat DBpedia"""
    for k in keys:
        if k in res:
            return res[k]["value"]
    return None

def safe_date_literal(value):
    """Crée un Literal xsd:date si possible, sinon simple Literal"""
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return Literal(value, datatype=XSD.date)
    except:
        return Literal(value)

def safe_uri_or_literal(value):
    """Renvoie URIRef si c'est une URI, sinon un Literal"""
    if value.startswith("http://") or value.startswith("https://"):
        return URIRef(value)
    else:
        # On nettoie les accents et espaces pour créer une URI propre
        clean = value.replace(" ", "_").replace("é", "e").replace("ó", "o").replace("í", "i")
        return URIRef(f"http://dbpedia.org/resource/{clean}")

# -----------------------------
# Fonction pour interroger DBpedia
# -----------------------------
def query_dbpedia(player_name):
    dbp_iri = "http://dbpedia.org/resource/" + player_name.replace(" ", "_")
    query = f"""
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX dbp: <http://dbpedia.org/property/>
    PREFIX dbr: <http://dbpedia.org/resource/>

    SELECT ?birthDate ?birthDateRaw ?birthPlace ?birthPlaceRaw ?position ?positionRaw 
           ?deathDate ?deathDateRaw ?deathPlace ?deathPlaceRaw ?team ?teamRaw
    WHERE {{
      OPTIONAL {{ <{dbp_iri}> dbo:birthDate ?birthDate. }}
      OPTIONAL {{ <{dbp_iri}> dbp:birthDate ?birthDateRaw. }}
      OPTIONAL {{ <{dbp_iri}> dbo:birthPlace ?birthPlace. }}
      OPTIONAL {{ <{dbp_iri}> dbp:birthPlace ?birthPlaceRaw. }}
      OPTIONAL {{ <{dbp_iri}> dbo:position ?position. }}
      OPTIONAL {{ <{dbp_iri}> dbp:position ?positionRaw. }}
      OPTIONAL {{ <{dbp_iri}> dbo:deathDate ?deathDate. }}
      OPTIONAL {{ <{dbp_iri}> dbp:deathDate ?deathDateRaw. }}
      OPTIONAL {{ <{dbp_iri}> dbo:deathPlace ?deathPlace. }}
      OPTIONAL {{ <{dbp_iri}> dbp:deathPlace ?deathPlaceRaw. }}
      OPTIONAL {{ <{dbp_iri}> dbo:team ?team. }}
      OPTIONAL {{ <{dbp_iri}> dbp:team ?teamRaw. }}
    }}
    """
    sparql.setQuery(query)
    try:
        results = sparql.query().convert()
        return results["results"]["bindings"]
    except Exception as e:
        print(f"Erreur DBpedia pour {player_name}: {e}")
        return []

# -----------------------------
# Fonction pour traiter un joueur
# -----------------------------
def process_player(player):
    player_name = str(player).split("/")[-1]  # nom simple pour DBpedia
    g_temp = Graph()
    g_temp.bind("ex", EX)
    g_temp.bind("dbo", DBO)

    # Ajouter type Footballer
    g_temp.add((player, RDF.type, EX.Footballer))

    # Ajouter la NationalTeam locale déjà dans le TTL
    for nt in g.objects(player, DBO.NationalTeam):
        if isinstance(nt, Literal):
            g_temp.add((player, DBO.NationalTeam, safe_uri_or_literal(str(nt))))
        else:
            g_temp.add((player, DBO.NationalTeam, nt))

    # Compléter avec DBpedia pour les autres propriétés
    results = query_dbpedia(player_name)
    for res in results:
        # birthDate
        val = get_first(res, "birthDate", "birthDateRaw")
        if val:
            g_temp.add((player, DBO.birthDate, safe_date_literal(val)))

        # birthPlace
        val = get_first(res, "birthPlace", "birthPlaceRaw")
        if val:
            g_temp.add((player, DBO.birthPlace, safe_uri_or_literal(val)))

        # position
        val = get_first(res, "position", "positionRaw")
        if val:
            g_temp.add((player, DBO.position, safe_uri_or_literal(val)))

        # deathDate
        val = get_first(res, "deathDate", "deathDateRaw")
        if val:
            g_temp.add((player, DBO.deathDate, safe_date_literal(val)))

        # deathPlace
        val = get_first(res, "deathPlace", "deathPlaceRaw")
        if val:
            g_temp.add((player, DBO.deathPlace, safe_uri_or_literal(val)))

        # team
        val = get_first(res, "team", "teamRaw")
        if val:
            g_temp.add((player, DBO.team, safe_uri_or_literal(val)))

    return g_temp

# -----------------------------
# Traitement par lots avec parallélisation
# -----------------------------
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for i in range(0, len(players), BATCH_SIZE):
        batch = players[i:i+BATCH_SIZE]
        print(f"Traitement lot {i//BATCH_SIZE + 1}: {len(batch)} joueurs")

        futures = [executor.submit(process_player, p) for p in batch]
        g_batch = Graph()
        g_batch.bind("ex", EX)
        g_batch.bind("dbo", DBO)

        for f in futures:
            g_batch += f.result()

        # Écriture immédiate du lot
        with open(OUTPUT_RDF, "ab") as f:
            f.write(g_batch.serialize(format="ttl").encode("utf-8"))

        time.sleep(SLEEP_TIME)

print(f"RDF enrichi sauvegardé dans {OUTPUT_RDF}")
