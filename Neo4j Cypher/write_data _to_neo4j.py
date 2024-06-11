import json
import csv
import time
from neo4j import GraphDatabase, Driver

URI = "bolt://localhost:7687"
AUTH = ("kailun1103", "00000000")

def neo4j_input_node(driver: Driver, database, input_data, txn_hash):
    with driver.session(database=database) as session:
        count = 0
        txn_hash = txn_hash[-3:]
        for data in input_data:
            cypher = f'CREATE (:InputAddress {{address: "%s", amount: "%s BTC", id: "{txn_hash}_in{count}"}})' % (data["inputHash"], data["amount"])
            session.run(cypher)
            count += 1

def neo4j_output_node(driver: Driver, database, output_data, txn_hash):
    with driver.session(database=database) as session:
        count = 0
        txn_hash = txn_hash[-3:]
        for data in output_data:
            cypher = f'CREATE (:OutputAddress {{address: "%s", amount: "%s BTC", id: "{txn_hash}_out{count}"}})' % (data["outputHash"], data["amount"])
            session.run(cypher)
            count += 1

def neo4j_txn_node(driver: Driver, database, txn_data):
    with driver.session(database=database) as session:
        cypher = f"""
            CREATE (:Transaction {{
                hash: '{txn_data[0]}',
                time: '{txn_data[1]}',
                input_volume: {txn_data[2]},
                output_volume: {txn_data[3]},
                input_count: {txn_data[4]},
                output_count: {txn_data[5]},
                fees: {txn_data[6]},
                total_txn: {txn_data[7]},
                final_txn_time: '{txn_data[8]}',
                state: '{txn_data[9]}',
                verification_time: '{txn_data[10]}',
                block: '{txn_data[11]}',
                block_url: 'https://explorer.btc.com/btc/block/{txn_data[11]}',
                hash_url: 'https://explorer.btc.com/btc/transaction/{txn_data[0]}'
            }})
        """
        session.run(cypher)

def neo4j_txn_relation(driver: Driver, database, txn_relation):
    with driver.session(database=database) as session:

        transaction_hash = txn_relation[0]
        inputs = txn_relation[1]
        outputs = txn_relation[2]
        txn_hash = transaction_hash[-3:]

        # Start building Cypher query
        cypher = f"MATCH "

        # Matching input addresses
        for index, inp in enumerate(inputs):
            input_address = inp['inputHash']
            input_amount = inp['amount']
            cypher += f"(input{index}:InputAddress {{address: '{input_address}', amount: '{input_amount} BTC', id: '{txn_hash}_in{index}'}}), "

        # Matching output addresses
        for index, out in enumerate(outputs):
            output_address = out['outputHash']
            output_amount = out['amount']
            cypher += f"(output{index}:OutputAddress {{address: '{output_address}', amount: '{output_amount} BTC', id: '{txn_hash}_out{index}'}}), "

        cypher += f"(transaction:Transaction {{hash: '{transaction_hash}'}})"

        cypher += "\nCREATE "
        for index, inp in enumerate(inputs):
            input_amount = inp['amount']
            cypher += f"(input{index})-[:SENT {{amount: '{input_amount} BTC'}}]->(transaction), "
        for index, out in enumerate(outputs):
            output_amount = out['amount']
            cypher += f"(transaction)-[:RECEIVED {{amount: '{output_amount} BTC'}}]->(output{index}), "

        cypher = cypher[:-2]
        session.run(cypher)
        
def neo4j_process(neo4j_driver, input_details, output_details, txn_hash, txn_data, txn_relation):
    neo4j_input_node(neo4j_driver, 'btc', input_details, txn_hash)
    neo4j_output_node(neo4j_driver, 'btc', output_details, txn_hash)
    neo4j_txn_node(neo4j_driver, 'btc', txn_data)
    neo4j_txn_relation(neo4j_driver, 'btc', txn_relation)

csv_file_path = 'test.csv'
start_time = time.time()
with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    
    next(reader, None)
    with GraphDatabase.driver(URI, auth=AUTH) as neo4j_driver:
        for row in reader:
            print(time.time() - start_time)
            
            txn_hash, txn_date, input_volume, output_volume, input_count, output_count, fees, total_txn_amount, final_txn_date, state, txn_time, height, input_details, output_details = row
            input_volume = "{:.8f}".format(float(input_volume))
            output_volume = "{:.8f}".format(float(output_volume))
            fees = "{:.8f}".format(float(fees))

            input_details = json.loads(input_details)
            output_details = json.loads(output_details)
            txn_data = [txn_hash, txn_date, input_volume, output_volume, input_count, output_count, fees, total_txn_amount, final_txn_date, state, txn_time, height, input_details, output_details]
            txn_relation = [txn_hash, input_details, output_details]
            
            neo4j_process(neo4j_driver, input_details, output_details, txn_hash, txn_data, txn_relation)