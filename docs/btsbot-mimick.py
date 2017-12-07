#! /usr/bin/env python3

from bitshares import BitShares
from bitshares.price import Price
from bitshares.market import Market
from bitshares.instance import set_shared_bitshares_instance

import os
import time
import json
import decimal
import sys

import tinys3


# BTS:USD price
# https://btsbots.com/api/order?max_results=100&where=a_s==BTS;a_b==USD (now defunct)
#
S3_ACCESS_KEY = "XXX_YOUR_S3_KEY_HERE"
S3_SECRET_KEY = "XXX_YOUR_S3_SEC_KEY_HERE"
aws_bucket = "roelandp-nl"

#Setting Bitshares node
websocket = "wss://eu.openledger.info/ws" #"wss://node.bitshares.eu"
bitshares = BitShares(websocket)
set_shared_bitshares_instance(bitshares)

basedir = os.path.dirname(os.path.realpath(__file__))

#Creating S3 Connection
conn = tinys3.Connection(S3_ACCESS_KEY, S3_SECRET_KEY, tls=True)

bases_to_check = ['BTS']
markets_to_check = ['USD','CHF','JPY','CNY','EUR','KRW','GOLD','SILVER','BTC','OPEN.BTC','CAD','ARS','MXN','GBP','HKD','SGD','NZD','AUD','SEK','RUB','GOLOS','ALTCAP.XDR','ALTCAP.X','RUBLE','HERO','BTWTY']

for basesymbol in bases_to_check:
    print('now checking markets for '+basesymbol)

    for marketsymbol in markets_to_check:
        marketpair = basesymbol+":"+marketsymbol
        marketpairrev = marketsymbol+":"+basesymbol
        now = time.time()

        try:

            market = Market(marketpair).orderbook(limit=50)

            meta = {
                'marketpair': marketpair,
                'created_at': now
            }

            # SHOOTING THE 'MARKET'-pair (asks) to AMZ - e.g. BTS-USD.json

            items = []
            for item in market['asks']:

                order_to_append = {
                                    'a_b': str(item['base'].asset.asset),
                                    'b_b': item['base'].amount,
                                    'a_s': str(item['quote'].asset.asset),
                                    'b_s': item['quote'].amount,
                                    'p': item['price']
                                    }

                items.append(order_to_append)

            tojson = {
                "_meta": meta,
                "_items": items
            }


            jsonned = json.dumps(tojson)
            fname = "btsbots-api-result/"+marketpair.replace(":","-")+".json"
            fnamedir = basedir+"/"+fname

            with open(fnamedir, 'w') as f:
                f.seek(0)
                f.write(jsonned)
                f.truncate()
                f.close()
            with open(fnamedir, 'rb') as f:
                conn.upload(fname,f,bucket=aws_bucket,content_type='application/json',expires=250)

            print('uploaded => '+fname)
            # SHOOTING THE 'REVERSE MARKET'-pair (bids) to AMZ - e.g. USD-BTS.json as the btsprice tool expects it that way.
            #
            #

            meta = {
                'marketpair': marketpairrev,
                'created_at': now
            }

            items = []

            for item in market['bids']:
                
                #NOTE here we reverse the a_ & b_ as the alt api originally gave the base / quote for the bids in their originals.

                order_to_append = {
                                    'a_b': str(item['quote'].asset.asset),
                                    'b_b': item['quote'].amount,
                                    'a_s': str(item['base'].asset.asset),
                                    'b_s': item['base'].amount,
                                    'p': (1/item['price'])
                                    }

                items.append(order_to_append)

            tojson = {
                "_meta": meta,
                "_items": items
            }


            jsonned = json.dumps(tojson)

            fname = "btsbots-api-result/"+marketpairrev.replace(":","-")+".json";
            fnamedir = basedir+"/"+fname

            with open(fnamedir, 'w') as f:
                f.seek(0)
                f.write(jsonned)
                f.truncate()
                f.close()
            with open(fnamedir, 'rb') as f:
                conn.upload(fname,f,bucket=aws_bucket,content_type='application/json',expires=250)

            print('uploaded => '+fname)

        except:
            print("Unexpected error:", sys.exc_info()[0])

