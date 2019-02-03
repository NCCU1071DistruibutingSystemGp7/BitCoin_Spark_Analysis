import os
import json
from datetime import datetime
import chainquery as cq

CODE_NUMBER = 1
START_HEIGHT = 546707
END_HEIGHT = 546700

### NO NEED TO CHANGE CODE BELOW ###
# Create nessesary folder
try:
    os.mkdir('block')
except:
    pass

try:
    os.mkdir('tx_%s/' % CODE_NUMBER)
except:
    pass

try:
    log_file = open('./logs/tx_%s.txt' % CODE_NUMBER, 'a')
except:
    pass


for height in range(START_HEIGHT, END_HEIGHT - 1, -1):
    block_hash = cq.getblockhash(height)
    print 'block: %s' % block_hash
    log_file.write(str(datetime.now()) + ' start downloading block_height: %s, block_hash: %s' % (height, block_hash) + '\n')
    block = cq.getblock(block_hash)
    open('block/%s.json' % block_hash, 'wb').write(json.dumps(block))
    txs = block['tx']
    num_txs = len(txs)
    count = 0
    for txid in txs:
        count += 1
        print 'block: %d (%d/%d) txid: %s' % (height, count, num_txs, txid)
        tx = cq.getrawtransaction(txid)
        open('tx_%s/%s.json' % (CODE_NUMBER, txid), 'wb').write(json.dumps(tx))
    log_file.write(str(datetime.now()) + 'finish downloading.\n')
log_file.close()
