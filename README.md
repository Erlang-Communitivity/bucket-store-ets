README.md - bucket-store-ets

bucket-store-ets - An implementation of the bucket-store callbacks on top of an ETS backend

Bucket-store callbacks:
 * Standard gen_server callbacks, with handle_call({store, Bucket, Key, Doc})
 * Standard start_link/0 and start/0
 * store(Bucket, Key, Doc) - client function that calls the gen_server
 * fetch(Bucket, Key) - Retrieves the Doc stored at that Bucket and Keyy
 * list_keys(Bucket) - Lists the keys in the given bucket
 * list_buckets() - Lists the existing buckets
 * map(Bucket, MapFn) - Maps the function over the entries in the bucket 
 * foldl(Fold_Fn, Acc0, Bucket) - Folds over the entries in the bucket using the fold function
 * find(Bucket, DocSpec) - Finds entries in the bucket with Docs matching specs

