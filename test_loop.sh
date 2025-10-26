#!/bin/bash

count=1
while true; do
    echo "=== Test run #$count at $(date +%H:%M:%S) ==="
    
    if ! ./test.sh > /tmp/test_run_${count}.log 2>&1; then
        echo ""
        echo "❌ FAILURE on run #$count"
        echo ""
        cat /tmp/test_run_${count}.log
        echo ""
        echo "Full output saved to: /tmp/test_run_${count}.log"
        exit 1
    fi
    
    echo "✓ Pass"
    rm -f /tmp/test_run_${count}.log
    ((count++))
done
