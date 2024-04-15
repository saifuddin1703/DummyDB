whenever a write takes place put it into memcache which is a redblack tree
run a process in background which will convert this memcache into a sstable in disk whenever the size of tree crosses 1mb
sstable is a sorted string table which constains all the keys in sorted order and for each 100kb form a block store the byte offset of the block start into the hash map in memory which the starting key 
whenever the sstable crosses 5mb size combine the sstable with the previous one using merge sorted fiel logic 
