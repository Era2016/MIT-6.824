#!bin/bash
#
#for j in `seq 0 1`; do for i in {2A,2B,2C}; do time go test -run $i >> result2.log; done; done &
#
tests=(2a 2b 2c 2d)
for i in ${tests[*]}; do
    if [ ! -d "/tmp/$i" ]; then
        mkdir -p /tmp/$i
    fi
done

(for i in `seq 0 99`; do time go test -run 2A >> /tmp/2a/result_$i; done) &
(for i in `seq 0 99`; do time go test -run 2B >> /tmp/2b/result_$i; done) &
(for i in `seq 0 99`; do time go test -run 2C >> /tmp/2c/result_$i; done) &
(for i in `seq 0 99`; do time go test -run 2D >> /tmp/2d/result_$i; done) &

echo "wait ..."
