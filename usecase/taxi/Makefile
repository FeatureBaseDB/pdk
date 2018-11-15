PILOSA_IP=""

install:
	cd ../..; \
	make install

run: install
	nohup pdk taxi -f ./greenAndYellowUrls.txt --pilosa $PILOSA_IP:10101 -b 1000000 -c 6 -e 2 &> taxi.out &
