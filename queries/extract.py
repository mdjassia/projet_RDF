from rdflib import Graph, URIRef, Namespace, RDF
from SPARQLWrapper import SPARQLWrapper, JSON
from concurrent.futures import ThreadPoolExecutor
import time

# -----------------------------
# Config
# -----------------------------
INPUT_RDF = "schema/player.ttl"
OUTPUT_RDF = "schema/players_enriched.ttl"
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
# Fonction pour interroger DBpedia
# -----------------------------
def query_dbpedia(player_name):
    dbp_iri = "http://dbpedia.org/resource/" + player_name.replace(" ", "_")
    query = f"""
    SELECT ?birthDate ?birthPlace ?position ?deathDate ?deathPlace ?team
    WHERE {{
      OPTIONAL {{ <{dbp_iri}> dbo:birthDate ?birthDate. }}
      OPTIONAL {{ <{dbp_iri}> dbo:birthPlace ?birthPlace. }}
      OPTIONAL {{ <{dbp_iri}> dbo:position ?position. }}
      OPTIONAL {{ <{dbp_iri}> dbo:deathDate ?deathDate. }}
      OPTIONAL {{ <{dbp_iri}> dbo:deathPlace ?deathPlace. }}
      OPTIONAL {{ <{dbp_iri}> dbo:team ?team. }}
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

    # Ajouter type et NationalTeam locale
    g_temp.add((player, RDF.type, EX.Footballer))
    for team in g.objects(player, DBO.NationalTeam):
        g_temp.add((player, DBO.NationalTeam, team))

    # Compléter avec DBpedia pour les autres propriétés
    results = query_dbpedia(player_name)
    for res in results:
        if "birthDate" in res:
            g_temp.add((player, DBO.birthDate, URIRef(res["birthDate"]["value"])))
        if "birthPlace" in res:
            g_temp.add((player, DBO.birthPlace, URIRef(res["birthPlace"]["value"])))
        if "position" in res:
            g_temp.add((player, DBO.position, URIRef(res["position"]["value"])))
        if "deathDate" in res:
            g_temp.add((player, DBO.deathDate, URIRef(res["deathDate"]["value"])))
        if "deathPlace" in res:
            g_temp.add((player, DBO.deathPlace, URIRef(res["deathPlace"]["value"])))
        if "team" in res:
            g_temp.add((player, DBO.team, URIRef(res["team"]["value"])))

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

        # Écriture immédiate du lot pour visualiser le progrès
        with open(OUTPUT_RDF, "ab") as f:
            f.write(g_batch.serialize(format="ttl").encode("utf-8"))

        time.sleep(SLEEP_TIME)

print(f"RDF enrichi sauvegardé dans {OUTPUT_RDF}")
