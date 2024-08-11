#!/bin/sh

# Run the main function for opcodes - fetch current data
# python main.py default &

# Fetch the historical data with multi processing 
python main.py historical 53294400 55057990 100 1 &
python main.py historical 55057990 56819482 100 2 &
python main.py historical 56821282 58580974 100 3 &
python main.py historical 58588974 60342466 100 4 &
python main.py historical 60342666 62103959 100 5 &

# Wait for all background processes to finish
wait


# 53296497 (latest block) 