from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict, List
import uvicorn
import numpy as np
import time
import os
import json
import logging 

from web3 import Web3
from dotenv import load_dotenv
import requests
from eth_account.messages import encode_defunct
import asyncio

from eth_account import Account

app = FastAPI()
load_dotenv()
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
RPC_URL = os.getenv("RPC_URL")
NODE_CONTRACT_ADDRESS = os.getenv("NODE_MANAGER_CONTRACT")
AGGREGATOR_CONTRACT_ADDRESS = os.getenv("AGGREGATOR_CONTRACT")


w3 = Web3(Web3.HTTPProvider(RPC_URL))
account = w3.eth.account.from_key(PRIVATE_KEY)
wallet = account.address

with open("nodemanager_abi.json", "r") as node_abi_file:
    NODE_CONTRACT_ABI = json.load(node_abi_file)

node_contract = w3.eth.contract(address = NODE_CONTRACT_ADDRESS, abi = NODE_CONTRACT_ABI)

with open("aggregator_abi.json", "r") as aggregator_abi_file:
    AGGREGATOR_CONTRACT_ABI = json.load(aggregator_abi_file)

aggregator_contract = w3.eth.contract(address = AGGREGATOR_CONTRACT_ADDRESS, abi = AGGREGATOR_CONTRACT_ABI)

oracle_data_store = {}

trusted_prices: List[int] = []

def setup_logger():
    logging.basicConfig(
        level= logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger("Oracle Node")

logger = setup_logger()

# ================
# === Settings ===
# ================

ANOMALY_ZSCORE_THRESHOLD = 2.0
REPUTATION_INITIAL = 100
REPUTATION_MIN = 0
REPUTATION_MAX = 100
REPUTATION_PENALTY = 10
REPUTATION_REWARD = 1


connected_nodes: Dict[str, WebSocket] = {}
node_prices: Dict[str, int] = {}
reputation_scores: Dict[str, int] = {}


def detect_anomalies(prices: Dict[str, int]) -> List[str]:
    if len(prices) < 3:
        return[] #not enough data
    
    values = list(prices.values())
    mean = np.mean(values)
    std = np.std(values)    
    anomalies = []

    for node_id, price in prices.items():
        z_score = (price - mean) / std if std != 0 else 0
        if abs(z_score) > ANOMALY_ZSCORE_THRESHOLD:
            anomalies.append(node_id)

    return anomalies


def update_reputation(node_id: str, is_anamoly: bool):
    if node_id not in reputation_scores:
        reputation_scores[node_id] = REPUTATION_INITIAL
    
    if is_anamoly:
        reputation_scores[node_id] -= REPUTATION_PENALTY
    else:
        reputation_scores[node_id] += REPUTATION_PENALTY

    reputation_scores[node_id] = max(REPUTATION_MIN, min(REPUTATION_MAX, reputation_scores[node_id]))


# ================
# === oracle test ===
# ================


def is_registered_oracle(nodeaddress):
    try:
        return node_contract.functions.registeredOracles(nodeaddress).call()
    except Exception as e:
        print(f"[ERROR] Checking registration: {e}")
        return False


def verify_signature(symbol, price, timestamp, signature, address, message_hash_hex):
    try:
        message = f"{symbol}:{price}:{timestamp}"

        message_encoded = encode_defunct(text= message)
        message_hash = w3.keccak(message_encoded.body)

        if message_hash.hex() != message_hash_hex:
            logger.warning(f"⚠️ Hash mismatch for address {address}")
            return False 
        
        recovered = w3.eth.account.recover_message(message_encoded, signature= signature)
        logger.info(f"Recovered address: {recovered}")

        return recovered.lower() == address.lower()

    except Exception as e:
        logger.error(f"[Signature Error] {e}")
        return False


connected_nodes_ip: Dict[str, WebSocket] = {}
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    node_ip = websocket.client.host 
    connected_nodes_ip[node_ip] = websocket 
    print(f"[CONNECTED] Node {node_ip}")

    try:
        while True:
            received_data = await websocket.receive_text()
            
            try:
                data = json.loads(received_data)

                symbol = data.get("symbol")
                price = data.get("price")
                timestamp = data.get("timestamp")
                signature = data.get("signature")
                node_address = data.get("address")
                message_hash = data.get("message_hash")


                if not is_registered_oracle(node_address):
                    await websocket.close()
                    return

                if not verify_signature(symbol, price, timestamp, signature, node_address, message_hash):
                    logger.warning(f"[❌] Invalid signature from {node_address}")
                    continue
                
                oracle_data_store[node_address] = {
                    "symbol": symbol,
                    "price": price,
                    "timestamp": timestamp
                }
                node_prices[node_address] = price 

                anomalies = detect_anomalies(node_prices)

                for nid in node_prices:
                    update_reputation(nid, nid in anomalies)

                print(f"[REPUTATION] {reputation_scores}")

                global trusted_prices
                trusted_prices = [p for nid, p in node_prices.items() if nid not in anomalies]
               
                logger.info(f"[✅] Received from {node_address} | {symbol} = {price}")
                
            except Exception as e:
                print(f"[ERROR] Failed to parse data: {e}")
                continue


    except WebSocketDisconnect:
        print(f"[DISCONNECTED] Node {node_ip}")
        del connected_nodes_ip[node_ip]
    

def current_timestamp():
    return int(time.time())



def sign_price(symbol, price):
    if not PRIVATE_KEY:
        raise Exception("Private key not found")
    
    timestamp = current_timestamp()
    message = f"{symbol}:{price}:{timestamp}"

    message_encoded = encode_defunct(text= message)
    message_hash = w3.keccak(message_encoded.body)
    signed = w3.eth.account.sign_message(message_encoded, private_key= PRIVATE_KEY)

    signer_address = Account.from_key(PRIVATE_KEY).address

    return{
        "symbol": symbol,
        "price": price,
        "timestamp": timestamp,
        "signature": signed.signature.hex(),
        "address": signer_address,
        "message_hash": message_hash.hex()
    }


async def aggregator_task():
    global trusted_prices
    while True:
        if trusted_prices:
            prices = trusted_prices
            if len(trusted_prices) >= 2:
                aggregated_price = int(np.median(prices))
                print(f"[AGGREGATED PRICE]: ${aggregated_price}")

                if aggregated_price and should_submit(aggregated_price):
                    try:
                        signed_data = sign_price("BTCUSD", aggregated_price)

                        nonce = w3.eth.get_transaction_count(wallet)
                        txn = aggregator_contract.functions.reportPrice(
                            current_timestamp(),
                            aggregated_price,
                            bytes.fromhex(signed_data["signature"][2:])
                        ).build_transaction({
                            'from': wallet,
                            'nonce': nonce,
                            'gas': 300000,
                            'gasPrice': w3.to_wei('30', 'gwei')
                        })

                        signed_txn = w3.eth.account.sign_transaction(txn, private_key = PRIVATE_KEY)
                        tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
                        print(f"[📤] Submitted price: ${aggregated_price} | TX: {tx_hash.hex()}")
                    except Exception as e:
                        print(f"[ERROR] Submitting price: {e}")


            else:
                print("[INFO] Waiting for more nodes to calculate median...")
        else:
            print("[INFO] No price data received yet.")
        
        await asyncio.sleep(60)  # aggregate every 60 seconds


last_submitted_price = None
def should_submit(price):
    global last_submitted_price
    if last_submitted_price is None or abs(price - last_submitted_price) > 1:
        last_submitted_price = price 
        return True 
    return False

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(aggregator_task())

if __name__ == "__main__":
    uvicorn.run("aggregator:app", host= "0.0.0.0", port=8000)
