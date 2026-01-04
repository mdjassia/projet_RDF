import argparse
from rdflib import Graph
from owlrl import DeductiveClosure, RDFS_Semantics


def load_files(g: Graph, paths: list[str]) -> None:
    for p in paths:
        g.parse(p)  # rdflib détecte le format via l'extension (ttl, nt, rdf, ...)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", required=False, help="ontology.ttl (recommandé si --rdfs)")
    parser.add_argument("--data", action="append", required=True, help="RDF data file (repeatable)")
    parser.add_argument("--query", required=True, help="SPARQL query file")
    parser.add_argument("--rdfs", choices=["default", "none"], default="default")
    parser.add_argument("--newfacts", action="store_true", help="Only inferred triples")
    args = parser.parse_args()

    # 1) Base facts
    base = Graph()
    load_files(base, args.data)

    # 2) Schema
    schema = Graph()
    if args.schema:
        load_files(schema, [args.schema])

    # 3) Reasoning
    if args.rdfs == "none":
        query_graph = base
    else:
        # union(base, schema) dans un graph de travail
        enriched = Graph()
        for t in base:
            enriched.add(t)
        for t in schema:
            enriched.add(t)

        # applique le raisonnement RDFS (subClassOf, subPropertyOf, domain, range, etc.)
        DeductiveClosure(RDFS_Semantics).expand(enriched)

        if not args.newfacts:
            query_graph = enriched
        else:
            # newfacts = enriched - (base ∪ schema)
            union = Graph()
            for t in base:
                union.add(t)
            for t in schema:
                union.add(t)

            new_only = Graph()
            for t in enriched:
                if t not in union:
                    new_only.add(t)

            query_graph = new_only

    # 4) Run SPARQL query
    sparql = open(args.query, "r", encoding="utf-8").read()
    res = query_graph.query(sparql)

    # Print results
    if res.type == "ASK":
        print(bool(res))
    elif res.type == "SELECT":
        # simple output table
        vars_ = [str(v) for v in res.vars]
        print("\t".join(vars_))
        for row in res:
            print("\t".join("" if v is None else str(v) for v in row))
    else:
        # CONSTRUCT/DESCRIBE -> serialize as Turtle
        out_g = Graph()
        for triple in res.graph:
            out_g.add(triple)
        print(out_g.serialize(format="turtle"))


if __name__ == "__main__":
    main()
