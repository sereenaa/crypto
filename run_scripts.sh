#!/bin/sh

# Run the main function for opcodes - fetch current data
# python main.py default &

# Fetch the historical data with multi processing 
python main.py historical 54008340 55057990 100 1 &
python main.py historical 55253389 56819482 100 2 &
python main.py historical 57373582 58580974 100 3 &
python main.py historical 59181874 60342466 100 4 &
python main.py historical 60349967 62103959 100 5 &

# Wait for all background processes to finish
wait