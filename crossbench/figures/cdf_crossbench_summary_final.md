| suite | kind | data GiB | attempted | complete | partial | failed | instances | median | P90 | P99 | max | spread |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Prod-DS (default, 107) | read | 11.43 | 107 | 107 | 0 | 0 | 107 | 106 ms | 491 ms | 91591 ms | 168.5 s | 9081x |
| Prod-DS, 99 templates | read | 11.43 | 99 | 99 | 0 | 0 | 99 | 103 ms | 312 ms | 1104 ms | 20.7 s | 1113x |
| TPC-DS | read | 11.36 | 99 | 99 | 0 | 0 | 99 | 76 ms | 285 ms | 790 ms | 0.9 s | 68x |
| DSB | read | 11.79 | 104 | 104 | 0 | 0 | 104 | 49 ms | 113 ms | 223 ms | 0.2 s | 22x |
| JOB | read | 3.61 | 113 | 113 | 0 | 0 | 113 | 60 ms | 128 ms | 201 ms | 0.5 s | 49x |
| Redbench (Krid et al.) | read | 3.61 | 763 | 763 | 0 | 0 | 8,784 | 95 ms | 189 ms | 309 ms | 0.5 s | 30x |
| SQLBarber | read | 3.61 | 1000 | 1000 | 0 | 0 | 1,000 | 7 ms | 20 ms | 25 ms | 0.0 s | 20x |
| Redbench (Wehrstein et al.), reads | read | 3.61 | 62 | 62 | 0 | 0 | 62 | 48 ms | 78 ms | 107 ms | 0.1 s | 4x |
| Redbench (Wehrstein et al.), writes | write | 3.61 | 938 | 938 | 0 | 0 | 938 | 2 ms | 4 ms | 7 ms | 0.0 s | 8x |
