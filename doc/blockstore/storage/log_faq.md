# Accepted abbreviations in the prefix of the log message

## Volume actor
```
[v:72075186234033792 g:12 d:sample-disk-id t:10.05s]
```
* v - volume TabletId
* g - volume tablet generation
* d - disk id
* t - time since actor start

## Partition actor
```
[p:72075186234033792 g:12 d:sample-disk-id t:10.05s]
[p0:72075186234033794 g:4 d:sample-disk-id2 t:10.05s]
[p1:72075186234033795 g:5 d:sample-disk-id2 t:10.05s]
```
* p - partition TabletId
* p0 - partition TabletId #0 for multipartition volume
* p1 - partition TabletId #1 for multipartition volume
* g - partition tablet generation
* d - disk id
* t - time since actor start

## Volume proxy
```
[vp:72075186234033792 d:sample-disk-id pg:1 t:1m 0s]
[~vp:72075186234033792 d:sample-disk-id pg:1 t:1m 0s]
```
* vp - volume TabletId
* d - disk id
* pg - partition tablet generation
* ~ - means temporary server (NBS-2)

## Volume session
```
[vs:72075186234033792 d:sample-disk-id s:d33af69c-5e388492-fa9dd967-3444b26f t:13.664ms]
[~vs:72075186234033792 d:sample-disk-id s:d33af69c-5e388492-fa9dd967-3444b26f t:13.664ms]
```
* vs - volume TabletId
* d - disk id
* s - session id
* t - time since actor start
* ~ - means temporary server (NBS-2)

## Volume client
```
[vc:72075186234033792 d:sample-disk-id s:d33af69c-5e388492-fa9dd967-3444b26f c:example-1@example.net pg:1 t:10.979ms]
[~vc:72075186234033792 d:sample-disk-id s:d33af69c-5e388492-fa9dd967-3444b26f c:example-1@example.net pg:1 t:10.979ms]
```
* vc - volume TabletId
* d - disk id
* s - session id
* c - client id
* pg - pipe generation
* t - time since actor start
* ~ - means temporary server (NBS-2)
