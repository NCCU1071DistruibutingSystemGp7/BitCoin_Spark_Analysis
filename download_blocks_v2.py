import os
import json
import time
import sys
from datetime import datetime
import chainquery as cq

CODE_NUMBER = 12
START_HEIGHT = 543491
END_HEIGHT = 543400

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
    time_1 = 0
    time_2 = 0
    time_cost = 0
    percentage = .0
    time_left = 0
    for txid in txs:
        count += 1
        time_1 = time.time()
        #print 'block: %d (%d/%d) txid: %s' % (height, count, num_txs, txid)
        tx = cq.getrawtransaction(txid)
        open('tx_%s/%s.json' % (CODE_NUMBER, txid), 'wb').write(json.dumps(tx))
        time_2 = time.time()

        # count processing time cost every 3 tx
        if count % 3 == 1:
            time_cost = time_2 - time_1
            time_left = time_cost * (num_txs - count)
            time_left = time_left / 60

        percentage = count / float(num_txs)
        progress_bar_stars = ''.join(['*' * int(percentage * 100 / 2)])
        progress_bar_space = ''.join([' ' * (50 - len(progress_bar_stars))])

        sys.stdout.write('\r' + 'block: {0} ({1}/{2}) |{3}{4}| {5:.1%} time_left: {6:.1f}minutes'.format(height, count, num_txs, progress_bar_stars, progress_bar_space, percentage, time_left))
        sys.stdout.flush()
    log_file.write(str(datetime.now()) + 'finish downloading.\n')
log_file.close()
