from rdflib import Graph, Namespace, RDF, URIRef

# Charger le TTL généré par Tarql ou ARQ
g = Graph()
g.parse("schema/players.ttl", format="turtle")

# Créer un nouveau graphe (optionnel)
clean_graph = Graph()
clean_graph.bind("ex", Namespace("http://example.org/football/"))
clean_graph.bind("dbo", Namespace("http://dbpedia.org/ontology/"))
clean_graph.bind("rdf", RDF)

# Ajouter tous les triples (les doublons seront ignorés)
for s, p, o in g:
    clean_graph.add((s, p, o))

# Sauvegarder le graphe propre
clean_graph.serialize("schema/players_clean.ttl", format="turtle")

print(f"✅ Graphe nettoyé : {len(clean_graph)} triples uniques")
