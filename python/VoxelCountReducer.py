#!/usr/bin/env python

import sys


prev_key = None
value_total = 0

for input_line in sys.stdin:
	
        current_key, value = input_line.split("\t", 1)
        
        value = int(value)

        #if current key is the same, increment value
	if prev_key == current_key:
		key_count += value
        #if current key does not match previous key, print key and count 
        #and 'reset' key_count to current key's value 
	else:
		if prev_key:
			print("%s\t%d" % (prev_key, key_count))
		key_count = value
		prev_key = current_key

#if last line in stdin, print key and value
if prev_key == current_key:
	print( "%s\t%d" % (prev_key, key_count))
			
