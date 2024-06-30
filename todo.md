whenever a write takes place put it into memcache which is a redblack tree
run a process in background which will convert this memcache into a sstable in disk whenever the size of tree crosses 1mb
sstable is a sorted string table which constains all the keys in sorted order and for each 100kb form a block store the byte offset of the block start into the hash map in memory which the starting key
whenever the sstable crosses 5mb size combine the sstable with the previous one using merge sorted fiel logic

process- p0

1. combile the segments -- done
2. add bloom filter

functions - p2

1. remove key -- done
2. list all the keys -- done

functions - p3

1. range queyies 4. key count
