from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict, List
import uvicorn
import numpy as np
import time
import os
import json
import logging
import asyncio

from web3 import Web3
from dotenv import load_dotenv
from eth_account.messages import encode_defunct
from eth_account import Account

# === FastAPI Setup ===
app = FastAPI()
load_dotenv()

# === Environment Variables ===
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
RPC_URL = os.getenv("RPC_URL")
NODE_CONTRACT_ADDRESS = os.getenv("NODE_MANAGER_CONTRACT")
AGGREGATOR_CONTRACT_ADDRESS = os.getenv("AGGREGATOR_CONTRACT")

# === Web3 Setup ===
w3 = Web3(Web3.HTTPProvider(RPC_URL))
account = w3.eth.account.from_key(PRIVATE_KEY)
wallet = account.address

# === Load ABIs ===
with open("nodemanager_abi.json", "r") as f:
    NODE_CONTRACT_ABI = json.load(f)
with open("aggregator_abi.json", "r") as f:
    AGGREGATOR_CONTRACT_ABI = json.load(f)

# === Contract Setup ===
node_contract = w3.eth.contract(address=NODE_CONTRACT_ADDRESS, abi=NODE_CONTRACT_ABI)
aggregator_contract = w3.eth.contract(address=AGGREGATOR_CONTRACT_ADDRESS, abi=AGGREGATOR_CONTRACT_ABI)

# === Logger ===
def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger("OracleAggregator")

logger = setup_logger()

# === Global Data Stores ===
oracle_data_store = {}
connected_nodes: Dict[str, WebSocket] = {}
connected_nodes_ip: Dict[str, WebSocket] = {}
node_prices: Dict[str, int] = {}
reputation_scores: Dict[str, int] = {}
trusted_prices: List[int] = []

# === Reputation and Anomaly Detection Settings ===
ANOMALY_ZSCORE_THRESHOLD = 2.0
REPUTATION_INITIAL = 100
REPUTATION_MIN = 0
REPUTATION_MAX = 100
REPUTATION_PENALTY = 10
REPUTATION_REWARD = 1

# === Helper Functions ===
def detect_anomalies(prices: Dict[str, int]) -> List[str]:
    if len(prices) < 3:
        return []

    values = list(prices.values())
    mean = np.mean(values)
    std = np.std(values)
    return [
        node_id
        for node_id, price in prices.items()
        if abs((price - mean) / std) > ANOMALY_ZSCORE_THRESHOLD if std != 0 else False
    ]

def update_reputation(node_id: str, is_anomaly: bool):
    if node_id not in reputation_scores:
        reputation_scores[node_id] = REPUTATION_INITIAL
    delta = -REPUTATION_PENALTY if is_anomaly else REPUTATION_REWARD
    reputation_scores[node_id] = max(REPUTATION_MIN, min(REPUTATION_MAX, reputation_scores[node_id] + delta))

def is_registered_oracle(node_address: str) -> bool:
    try:
        return node_contract.functions.registeredOracles(node_address).call()
    except Exception as e:
        logger.error(f"[ERROR] Checking oracle registration: {e}")
        return False

def verify_signature(symbol, price, timestamp, signature, address, message_hash_hex):
    try:
        message = f"{symbol}:{price}:{timestamp}"
        message_encoded = encode_defunct(text=message)
        message_hash = w3.keccak(message_encoded.body)

        if message_hash.hex() != message_hash_hex:
            logger.warning(f"⚠️ Hash mismatch for address {address}")
            return False

        recovered = w3.eth.account.recover_message(message_encoded, signature=signature)
        return recovered.lower() == address.lower()
    except Exception as e:
        logger.error(f"[Signature Error] {e}")
        return False

def current_timestamp():
    return int(time.time())

def sign_price(symbol, price):
    timestamp = current_timestamp()
    message = f"{symbol}:{price}:{timestamp}"
    message_encoded = encode_defunct(text=message)
    message_hash = w3.keccak(message_encoded.body)
    signed = w3.eth.account.sign_message(message_encoded, private_key=PRIVATE_KEY)

    return {
        "symbol": symbol,
        "price": price,
        "timestamp": timestamp,
        "signature": signed.signature.hex(),
        "address": wallet,
        "message_hash": message_hash.hex()
    }

# === WebSocket Endpoint ===
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    node_ip = websocket.client.host
    connected_nodes_ip[node_ip] = websocket
    logger.info(f"[CONNECTED] Node {node_ip}")

    try:
        while True:
            try:
                raw = await websocket.receive_text()
                data = json.loads(raw)

                symbol = data.get("symbol")
                price = data.get("price")
                timestamp = data.get("timestamp")
                signature = data.get("signature")
                node_address = data.get("address")
                message_hash = data.get("message_hash")

                if not is_registered_oracle(node_address):
                    logger.warning(f"[❌] Unregistered node: {node_address}")
                    await websocket.close()
                    return

                if not verify_signature(symbol, price, timestamp, signature, node_address, message_hash):
                    logger.warning(f"[❌] Invalid signature from {node_address}")
                    continue

                oracle_data_store[node_address] = {"symbol": symbol, "price": price, "timestamp": timestamp}
                node_prices[node_address] = price

                anomalies = detect_anomalies(node_prices)
                for nid in node_prices:
                    update_reputation(nid, nid in anomalies)

                global trusted_prices
                trusted_prices = [p for nid, p in node_prices.items() if nid not in anomalies]

                logger.info(f"[✅] Valid price received from {node_address} | {symbol} = {price}")

            except Exception as e:
                logger.error(f"[ERROR] Invalid payload or processing error: {e}")

    except WebSocketDisconnect:
        logger.info(f"[DISCONNECTED] Node {node_ip}")
        del connected_nodes_ip[node_ip]

# === Aggregator Logic ===
last_submitted_price = None

def should_submit(price):
    global last_submitted_price
    if last_submitted_price is None or abs(price - last_submitted_price) > 1:
        last_submitted_price = price
        return True
    return False

async def aggregator_task():
    global trusted_prices
    while True:
        if trusted_prices and len(trusted_prices) >= 2:
            aggregated_price = int(np.median(trusted_prices))
            logger.info(f"[AGGREGATED PRICE]: ${aggregated_price}")

            if should_submit(aggregated_price):
                try:
                    signed_data = sign_price("BTCUSD", aggregated_price)

                    txn = aggregator_contract.functions.reportPrice(
                        current_timestamp(),
                        aggregated_price,
                        bytes.fromhex(signed_data["signature"][2:])
                    ).build_transaction({
                        "from": wallet,
                        "nonce": w3.eth.get_transaction_count(wallet),
                        "gas": 300000,
                        "gasPrice": w3.to_wei("30", "gwei")
                    })

                    signed_txn = w3.eth.account.sign_transaction(txn, private_key=PRIVATE_KEY)
                    tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)

                    logger.info(f"[📤] Price submitted: ${aggregated_price} | TX: {tx_hash.hex()}")
                except Exception as e:
                    logger.error(f"[ERROR] Submitting price to contract: {e}")
        else:
            logger.info("[INFO] Waiting for enough trusted prices...")

        await asyncio.sleep(60)  # every 60 seconds

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(aggregator_task())

# === Entry Point for Local Run ===
if __name__ == "__main__":
    uvicorn.run("aggregator:app", host="0.0.0.0", port=8000)
