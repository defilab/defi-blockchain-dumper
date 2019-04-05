import os
import sys
import json
import time
import base64
from flask import Flask, request, make_response
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import bulk

from points.entities import Entity
from points.ledgers import OntologyLedger, HyperLedger

es = Elasticsearch([os.getenv('ES_ENDPOINT')])

app = Flask(__name__)


server_entity = Entity(
    os.getenv("ACCOUNT_NAME"),
    ledger_class=OntologyLedger if os.getenv('LEDGER_TYPE', 'OntologyLedger') == 'OntologyLedger' else HyperLedger,
    channel=os.getenv("CHANNEL_ENDPOINT"),
    postman_endpoint=os.getenv("POSTMAN_ENDPOINT"),
    registry_endpoint=os.getenv('REGISTRY_ENDPOINT'),
    rpc_address=os.getenv('BLOCKCHAIN_RPC_ADDRESS'),
    channel_name=os.getenv('CHANNEL_NAME', 'pts-exchange'),
    chaincode=os.getenv("CHAINCODE", 'pts-exchange'),
    peers=os.getenv('PEERS').split(','),
)

@app.route('/health_check', methods=['GET'])
def health_check():
    return "OK"

@app.route('/scan', methods=['GET'])
def scan():
    try:
        status = es.get(index="blockchain_state", doc_type='state', id=1)['_source']
    except NotFoundError as e:
        status = {"current_height": -1}
        es.index(index="blockchain_state", doc_type='state', id=1, body=status)

    current_block_height = server_entity.ledger.get_blockchain_height()
    scanned_blocks = status['current_height']
    print("Current block: %s, scanned block: %s" % (current_block_height - 1, scanned_blocks))
    i = scanned_blocks
    for i in range(scanned_blocks + 1, current_block_height):
        print("scanning block %s" % i)
        try:
            transactions = server_entity.ledger.get_transactions_from_block(i)
        except Exception as e:
            print("Failed to get transactions from block %s (%s)" % (i, e))
            import traceback
            traceback.print_exc()
            break
        es_ops = []
        for tx in transactions:
            if not tx.get('transaction_id'):
                continue
            if tx.get('offer_id'):
                body = {
                    "offer_id": tx['offer_id'],
                }
                if tx['action'] == 'AcceptOffer':
                    body['accept_offer_tx'] = tx
                    body['responded_at'] = tx['offer_body']['responded_at']
                    body['responded_by'] = tx['offer_body']['responded_by']
                    body['responded_by_account'] = tx['offer_body'].get('responded_by_account')
                elif tx['action'] == 'PutOffer':
                    body['put_offer_tx'] = tx
                    body['created_at'] = tx['offer_body']['created_at']
                    body['created_by'] = tx['offer_body']['created_by']
                    body['created_by_account'] = tx['offer_body'].get('created_by_account')
                es_ops.append({
                    "_op_type": "update",
                    "_index": "blockchain_offer_%s" % (int(float(time.time()) / 3600) * 3600),
                    "_type": "offer",
                    "doc_as_upsert": True,
                    "_id": tx['offer_id'],
                    "doc": body,
                })
            es_ops.append({
                "_op_type": "update",
                "_index": "blockchain_tx_%s" % (int(float(time.time()) / 3600) * 3600),
                "_type": "transaction",
                "doc_as_upsert": True,
                "_id": tx['transaction_id'],
                "doc": tx,
            })

        bulk(es, es_ops)
        status['current_height'] = i
        es.index(index="blockchain_state", doc_type='state', id=1, body=status)
    return "scanned to block %s" % i


if __name__ == '__main__':
    scan()
