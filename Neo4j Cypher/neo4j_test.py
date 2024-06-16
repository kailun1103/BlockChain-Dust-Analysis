from neo4j import GraphDatabase, Driver

URI = "bolt://localhost:7687"
AUTH = ("kailun1103", "00000000")

def neo4j_address_node(driver: Driver, database):
    with driver.session(database=database) as session:
        result = session.run(
            f"""
            MATCH (a:Address)
            WHERE a.address CONTAINS '3PUe6B3D36Y8EPE63j1hSZAsiJrrMK3kov'
            RETURN a
            """
        ).data()
        if result == '':
            pass
        else:
            print(123)
            print(result)


with GraphDatabase.driver(URI, auth=AUTH) as neo4j_driver:
    neo4j_address_node(neo4j_driver, 'btc')