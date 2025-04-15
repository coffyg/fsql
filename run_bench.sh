#!/bin/bash
set -e

# Create a results directory if it doesn't exist
mkdir -p benchmark_results

# Run specific benchmark and save results
run_benchmark() {
    local name=$1
    local benchmark_name=$2
    echo "Running benchmark: $name..."
    go test -bench="$benchmark_name" -benchmem -count=5 -timeout=30m | tee "benchmark_results/${name}_results.txt"
}

# Function to run all benchmarks
run_all_benchmarks() {
    echo "Running all benchmarks..."
    go test -bench="." -benchmem -count=5 -timeout=30m | tee "benchmark_results/all_benchmarks.txt"
}

# Function to run all tests
run_tests() {
    echo "Running all tests..."
    go test -v | tee "benchmark_results/tests.txt"
}

# Function to run a specific optimization test
run_optimization_test() {
    local name=$1
    echo "Running benchmark for optimization: $name..."
    go test -bench="." -benchmem -count=5 -timeout=30m | tee "benchmark_results/${name}_benchmark.txt"
}

# Default behavior: run all
if [ $# -eq 0 ]; then
    run_tests
    run_all_benchmarks
    exit 0
fi

# Handle command-line arguments
case "$1" in
    "all")
        run_tests
        run_all_benchmarks
        ;;
    "tests")
        run_tests
        ;;
    "bench")
        run_all_benchmarks
        ;;
    "optimization")
        if [ -z "$2" ]; then
            echo "Please provide a name for the optimization test"
            exit 1
        fi
        run_optimization_test "$2"
        ;;
    "specific")
        if [ -z "$2" ]; then
            echo "Please provide a benchmark name"
            exit 1
        fi
        if [ -z "$3" ]; then
            echo "Please provide a benchmark pattern"
            exit 1
        fi
        run_benchmark "$2" "$3"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Usage: $0 [all|tests|bench|optimization <name>|specific <name> <pattern>]"
        exit 1
        ;;
esac

echo "Done!"