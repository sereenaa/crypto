#!/bin/sh

# Run the main function for opcodes - fetch current data
# python main.py default &

# Fetch the historical data with multi processing 
python main.py historical 53296497 54999999 100 1 &
python main.py historical 55000000 55999999 100 2 &
python main.py historical 56000000 56999999 100 3 &
python main.py historical 57000000 57999999 100 4 &
python main.py historical 58000000 58999999 100 5 &

# Wait for all background processes to finish
wait


# 53296497 (latest block) 