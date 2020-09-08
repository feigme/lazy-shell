#!/bin/bash

pods=$(sudo kubectl get pod -o wide | grep '172.16.70.218' | awk '{print $1}')
cnt=0
for pod in $pods; do
    echo -e "\033[36m${pod}\033[0m"
    num=$(sudo kubectl exec -it $pod -- ps -eLf | wc -l)
    cnt=$(( cnt + num ))
    echo "thread count: $num"
done

echo "total count: $cnt"