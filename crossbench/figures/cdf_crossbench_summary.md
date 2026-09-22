| suite | kind | data (GiB, logical / on disk) | queries | ok | err | t/o | median | P90 | P99 | max | spread |
|---|---|---|---|---|---|---|---|---|---|---|---|
| prodds | read | 11.43 / 4.75 | 107 | 107 | 0 | 0 | 103 ms | 529 ms | 90858 ms | 170.5 s | 8158x |
| prodds_templates | read (subset) | 11.43 / 4.75 | 99 | 99 | 0 | 0 | 98 ms | 323 ms | 1157 ms | 21.1 s | 1011x |
| tpcds | read | 11.36 / 5.97 | 99 | 99 | 0 | 0 | 75 ms | 282 ms | 792 ms | 0.9 s | 58x |
| dsb | read | 11.79 / 6.12 | 104 | 104 | 0 | 0 | 49 ms | 115 ms | 223 ms | 0.3 s | 23x |
| job | read | 3.61 / 2.01 | 113 | 109 | 4 | 0 | 62 ms | 136 ms | 209 ms | 0.5 s | 43x |
| redbench | write | 3.61 / 2.01 | 1000 | 1000 | 0 | 0 | 2 ms | 5 ms | 78 ms | 0.1 s | 115x |
