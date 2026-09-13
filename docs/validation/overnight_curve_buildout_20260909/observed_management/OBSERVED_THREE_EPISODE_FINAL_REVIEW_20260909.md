# Three fixed paper episodes — final verified result

All three predeclared GBP/USD episodes reached their original targets with all five virtual arms flat. The unchanged verifier reconciled178steps and2,727 retained files, with no missing slots, missed slots or verification issues. All20 registered source bindings matched.

| Separate virtual arm | Round trips | Gross midpoint USD | Spread USD | Slippage USD | Net USD |
|---|---:|---:|---:|---:|---:|
| Hold initial curve | 3 | 3.15903 | 0.94932 | 0.14990 | 2.05981 |
| Curve manager | 11 | -1.63018 | 3.49384 | 0.54980 | -5.67382 |
| Matched momentum manager | 49 | 3.73361 | 16.10708 | 2.44909 | -14.82256 |
| Legacy selector / new momentum input | 82 | 2.25846 | 26.94956 | 4.09855 | -28.78965 |
| No trade | 0 | 0.00000 | 0.00000 | 0.00000 | 0.00000 |

The curve manager beat matched momentum in each episode, by a combined virtual$9.14874. That combines worse gross midpoint P&L of$5.36379 with lower spread/slippage costs of$14.51253. It still lost in all three episodes. Holding the initial curve position was the only active arm with positive aggregate net P&L.

The evidence supports lower turnover as the reason for the relative improvement over momentum. It does not establish a profitable predictor or a reliable management advantage. Three consecutive one-pair episodes are a small, dependent sample.

Each arm used the same predeclared USD2,500 research sizing scenario, clocks, quote rules and original target. Sums reset sizing per episode and are not actual account P&L. Costs include recorded bid/ask half-spreads and fixed0.1bps slippage per leg; no broker fills, financing, commissions or market impact were observed.

Original targets:07:43:50,08:42:55 and09:42:55 UTC on September9. Verification ran09:43:35.998–09:44:58.787 UTC. Original publication, consumption, plan, quote, settlement and evidence-read clocks remain in the bound reports.

Result JSON SHA256: b31bfe23669a440a71ff884dc29ac04e4ff22131f9a2216d7a49c8cfd0b617cb.
