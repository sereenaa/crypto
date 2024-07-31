#!/bin/sh

# Run the main function for transactions
python main.py transactions &

# Run the main function for logs
python main.py logs &

# Wait for all background processes to finish
wait