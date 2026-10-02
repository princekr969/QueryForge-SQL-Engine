# QueryForge verification report

Status: **passed**

Generated: 2026-10-01T13:57:43.019Z

- Differential: 100 queries
- Join: 3 queries, local
- Approximation: uniform, Zipfian, and adversarial gates passed
- Scaling: 1/2/4/8 workers, 5 measured runs after warm-up
- Adaptive skew: 3748 ms static → 3116 ms adaptive p50
- Chaos: 5 injected modes
- Worker crash checksum: d46abbd4e4f640721d5eed677f788241abf1f6f7b574b188a9590de80f2a9a55
- Coordinator restart checksum: d916efcaafb0176a3ec8de3d7c34357ae5c9f97818f200725bed0b35d6faec64

![Storage latency](storage-latency.svg)

![Scaling latency](scaling-latency.svg)
