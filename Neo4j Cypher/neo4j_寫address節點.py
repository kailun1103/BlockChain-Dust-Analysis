from neo4j import GraphDatabase, Driver
import json

URI = "bolt://localhost:7687"
AUTH = ("kailun1103", "00000000")

def neo4j_address_node(driver: Driver, database):
    with driver.session(database=database) as session:
        # 读取JSON文件
        with open('unique_addresses.json', 'r', encoding='utf-8') as jsonfile:
            addresses = json.load(jsonfile)
            count = 1
            # 遍历JSON文件中的每个地址
            for address in addresses:
                print(count)
                count += 1
                session.run(f"CREATE (:Address {{address: '{address}'}})")


with GraphDatabase.driver(URI, auth=AUTH) as neo4j_driver:
    neo4j_address_node(neo4j_driver, 'btc')