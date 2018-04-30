#!/bin/bash

export PILOSA_IP=10.0.1.46

time curl $PILOSA_IP:10101/index/genome/query -d'Count(Intersect(Bitmap(frame=sequences, row=1), Bitmap(frame=sequences, row=0)))'
time curl $PILOSA_IP:10101/index/genome/query -d'Count(Union(Bitmap(frame=sequences, row=1), Bitmap(frame=sequences, row=0)))'
time curl $PILOSA_IP:10101/index/genome/query -d'Count(Difference(Bitmap(frame=sequences, row=1), Bitmap(frame=sequences, row=0)))'
time curl $PILOSA_IP:10101/index/genome/query -d'Count(Xor(Bitmap(frame=sequences, row=1), Bitmap(frame=sequences, row=0)))'
time curl $PILOSA_IP:10101/index/genome/query -d'Count(Difference(Bitmap(frame=sequences, row=0), Bitmap(frame=sequences, row=1)))'
time curl $PILOSA_IP:10101/index/genome/query -d'Count(Difference(Bitmap(frame=sequences, row=2), Bitmap(frame=sequences, row=1)))'
time curl $PILOSA_IP:10101/index/genome/query -d'TopN(Bitmap(frame=sequences, row=0), frame=sequences)'


# {"results":[2858674656]}
# real	0m0.515s

# {"results":[24637]}
# real	0m0.235s

# {"results":[32046]}
# real	0m0.330s

# {"results":[[{"id":0,"count":2858674662},{"id":3,"count":2858674661},{"id":9,"count":2858674661},{"id":8,"count":2858674659},{"id":7,"count":2858674657},{"id":5,"count":2858674657},{"id":4,"count":2858674656},{"id":6,"count":2858674656},{"id":1,"count":2858674656},{"id":2,"count":2858674655}]]}
# real	0m1.720s
