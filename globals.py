# globals.py

from queue import Queue 

global_failed_blocks_list_prev = []
global_failed_blocks_list_curr = []

global_failed_blocks_queue_prev = Queue()
global_failed_blocks_queue_curr = Queue()