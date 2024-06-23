import csv
import os
import json
import time
from datetime import datetime
from neo4j import GraphDatabase, Driver

csv.field_size_limit(2147483647)


def neo4j_address_node(driver: Driver, database, Txn_Details, hash_index):
    with driver.session(database=database) as session:
        for data in Txn_Details:
            address_check = session.run(
                f"""
                    MATCH (a:Address)
                    WHERE a.address CONTAINS '{data[hash_index]}'
                    RETURN a
                """
            ).data()
            if address_check == []:
                session.run(f"CREATE (:Address {{address: '{data[hash_index]}'}})")

def neo4j_txn_node(driver: Driver, database, txn_data):
    with driver.session(database=database) as session:
        cypher = f"""
            CREATE (:Transaction {{
                TxnHash: '{txn_data[0]}',
                TxnInitiationDate: '{txn_data[1]}',
                TxnInputAmount: '{txn_data[2]}',
                TxnOutputAmount: '{txn_data[3]}',
                TxnInputAddress: '{txn_data[4]}',
                TxnOutputAddress: '{txn_data[5]}',
                TxnFees: '{txn_data[6]}',
                MempoolCount: '{txn_data[7]}',
                MempoolFinalTxnDate: '{txn_data[8]}',
                TxnState: '{txn_data[9]}',
                TxnVerificationDate: '{txn_data[10]}',
                BlockHeight: '{txn_data[11]}',
                MinerVerificationTime: '{txn_data[14]}',
                DustBool: '{txn_data[15]}',
                BlockHash: '{txn_data[16]}',
                BlockValidator: '{txn_data[17]}',
                BlockDate: '{txn_data[18]}',
                BlockTxnCount: '{txn_data[19]}',
                BlockTxnAmount: '{txn_data[20]}',
                BlockSize: '{txn_data[21]}',
                MinerReward: '{txn_data[22]}',
                BlockTxnFees: '{txn_data[23]}',
                BlockMerkleRootHash: '{txn_data[24]}',
                BlockMinerHash: '{txn_data[25]}',
                BlockDifficulty: '{txn_data[26]}',
                BlockNonce: '{txn_data[27]}',
                BlockConfirm: '{txn_data[28]}'
            }})
        """
        session.run(cypher)



def neo4j_txn_relation(driver: Driver, database, txn_data, Txn_Input_Details, Txn_Output_Details):
    with driver.session(database=database) as session:

        txn_hash = txn_data[0]

        # Start building Cypher query
        cypher = f"MATCH "

        # Matching input addresses
        for index, inp in enumerate(Txn_Input_Details):
            input_address = inp['inputHash']
            cypher += f"(input{index}:Address {{address: '{input_address}'}}), "

        # Matching output addresses
        for index, out in enumerate(Txn_Output_Details):
            output_address = out['outputHash']
            cypher += f"(output{index}:Address {{address: '{output_address}'}}), "

        cypher += f"(transaction:Transaction {{TxnHash: '{txn_hash}'}})"

        cypher += "\nCREATE "
        for index, inp in enumerate(Txn_Input_Details):
            input_amount = inp['amount']
            cypher += f"(input{index})-[:SENT {{amount: '{input_amount}'}}]->(transaction), "
        for index, out in enumerate(Txn_Output_Details):
            output_amount = out['amount']
            cypher += f"(transaction)-[:RECEIVED {{amount: '{output_amount}'}}]->(output{index}), "

        cypher = cypher[:-2]
        print(cypher)
        session.run(cypher)



def neo4j_process(neo4j_driver, txn_data, Txn_Input_Details, Txn_Output_Details):
    neo4j_address_node(neo4j_driver, 'btc', Txn_Input_Details, 'inputHash')
    neo4j_address_node(neo4j_driver, 'btc', Txn_Output_Details, 'outputHash')
    neo4j_txn_node(neo4j_driver, 'btc', txn_data)
    neo4j_txn_relation(neo4j_driver, 'btc', txn_data, Txn_Input_Details, Txn_Output_Details)


URI = "bolt://localhost:7687"
AUTH = ("kailun1103", "00000000")

csv_file_path = 'test'

with GraphDatabase.driver(URI, auth=AUTH) as neo4j_driver:
    for root, dirs, files in os.walk(csv_file_path):
        csv_files = [file for file in files if file.endswith('.csv')]
        for csv_file in csv_files:
            csv_path = os.path.join(root, csv_file)
            with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
                print(csv_file)
                reader = csv.reader(infile)
                header = next(reader)
                for row in reader:
                    if int(row[4]) > 75 or int(row[5]) > 75:
                        continue
                    else:
                        Txn_Hash, Txn_Initiation_Date, Txn_Input_Amount, Txn_Output_Amount,	Txn_Input_Address, Txn_Output_Address, Txn_Fees, Mempool_Count, Mempool_Final_Txn_Date, Txn_State, Txn_Verification_Date, Block_Height, Txn_Input_Details, Txn_Output_Details, Miner_Verification_Time, Dust_Bool, Block_Hash, Block_Validator, Block_Date, Block_Txn_Count, Block_Txn_Amount, Block_Size, Miner_Reward, Block_Txn_Fees, Block_Merkle_Root_Hash, Block_Miner_Hash, Block_Difficulty, Block_Nonce, Block_Confirm = row
                        print(row[0])
                        Txn_Input_Amount = "{:.8f}".format(float(Txn_Input_Amount))
                        Txn_Output_Amount = "{:.8f}".format(float(Txn_Output_Amount))
                        Txn_Fees = "{:.8f}".format(float(Txn_Fees))
                        Txn_Input_Details = json.loads(Txn_Input_Details)
                        Txn_Output_Details = json.loads(Txn_Output_Details)

                        txn_data = [Txn_Hash, Txn_Initiation_Date, Txn_Input_Amount, Txn_Output_Amount,	Txn_Input_Address, Txn_Output_Address, Txn_Fees, Mempool_Count, Mempool_Final_Txn_Date, Txn_State, Txn_Verification_Date, Block_Height, Txn_Input_Details, Txn_Output_Details, Miner_Verification_Time, Dust_Bool, Block_Hash, Block_Validator, Block_Date, Block_Txn_Count, Block_Txn_Amount, Block_Size, Miner_Reward, Block_Txn_Fees, Block_Merkle_Root_Hash, Block_Miner_Hash, Block_Difficulty, Block_Nonce, Block_Confirm]
                        
                        neo4j_process(neo4j_driver, txn_data, Txn_Input_Details, Txn_Output_Details)


print('DONE')
                    


