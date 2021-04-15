#!/bin/bash

sudo docker run --rm -it mp_vx bash -c '../../Downloads/kafka/bin/kafka-console-consumer.sh --bootstrap-server hadoopdn-gsi-prod05.mpmg.mp.br:6667 --topic crawler_status --property print.key=true --property key.separator=" : " --from-beginning --timeout-ms 10000' | grep -v TimeoutException | sort -t' ' -k 1,1n | tac | sort -s -u -t'"' -k 2,2n | sort -t' ' -k 1,1n
