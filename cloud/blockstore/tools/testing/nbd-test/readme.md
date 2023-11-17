# run NBD target
```bash
./nbd-test \
    --test Target \
    --socket-path /tmp/nbd.sock
```

# run NBD initiator
```bash
./nbd-test \
    --test Initiator \
    --socket-path /tmp/nbd.sock
```
