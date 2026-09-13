# Data Insights

Analysis of 900 user-days (10 users × 90 days) of synthetic wearable data processed through the FitLake pipeline. All numbers below are computed from the Gold layer output and are reproducible — the generator is seeded (`SEED = 42`), so `make run-local` regenerates these figures exactly.

---

## Dataset Overview

| Metric | Value |
|---|---|
| Users | 10 (3 athletes, 4 intermediate, 3 beginners) |
| Days tracked | 90 per user |
| Total records | 900 per table (HRV, sleep, activity) |
| Avg recovery score | **69.8 / 100** |
| Recovery distribution | 552 Green (61.3%) · 346 Yellow (38.4%) · 2 Red (0.2%) |
| Avg HRV (rMSSD) | 50.3 ms |
| Avg sleep | 7.51 hours |

---

## Finding 1: Sleep Duration Is the Strongest Behavioural Lever

| Sleep Duration | Avg Recovery | Avg HRV | Sample Size |
|---|---|---|---|
| **< 6 hours** | **58.2** | 45.4 ms | 49 nights |
| 7–9 hours | 70.6 | 50.1 ms | 630 nights |
| > 9 hours | 75.9 | 49.0 ms | 35 nights |

**Insight:** Nights under 6 hours averaged **12.5 points lower** recovery than the 7–9 hour zone. HRV moves in the same direction — **45.4 ms** on short nights vs **50.0 ms** on 7h+ nights, a **4.6 ms** drop that feeds directly into the next morning's score.

Note the >9h band scores *highest* here (75.9), which runs against the sleep-science expectation that oversleeping signals illness. That is an artifact of the scoring function rather than a finding: `_sleep_performance_score` only begins penalising duration past 9h, and the 35-night sample is small.

---

## Finding 2: Recovery Follows a Clear Day-of-Week Pattern

| Day | Avg Recovery |
|---|---|
| **Sunday** | **78.3** (highest) |
| Wednesday | 72.5 |
| Monday | 71.9 |
| Tuesday | 71.0 |
| Thursday | 68.3 |
| Friday | 66.1 |
| **Saturday** | **61.5** (lowest) |

**Insight:** Recovery peaks on **Sunday** (the rest day in the training simulation) and bottoms out on **Saturday**. The **16.7-point spread** reflects cumulative training load: Friday and Saturday are the high-strain days in the simulated blocks, and autonomic recovery hasn't caught up by Saturday morning's HRV reading.

This mirrors real-world wearable data — athletes training hard Thursday–Saturday consistently post their lowest recovery on Saturday morning.

---

## Finding 3: Training Blocks Clearly Suppress Recovery

| Training Phase | Avg Recovery |
|---|---|
| Training weeks (days 1–21 of cycle) | **67.1** |
| Recovery week (days 22–28 of cycle) | **79.0** |

**Insight:** The simulated 4-week periodization (3 weeks progressive overload → 1 week deload) produces a **12.0-point gap** between phases. This validates the recovery score's sensitivity to training load: accumulated stress suppresses autonomic recovery, and a deload week lets the body catch up.

---

## Finding 4: Fitness Level Affects HRV But Not Recovery

| Fitness Level | Avg Recovery | Avg HRV (rMSSD) |
|---|---|---|
| Athlete | 69.8 | **56.9 ms** |
| Intermediate | 69.3 | 52.3 ms |
| Beginner | 70.6 | **41.0 ms** |

**Insight:** Recovery scores are nearly **identical across fitness levels** (within 1.3 points) even though athletes carry **39% higher** absolute HRV than beginners. This is by design: the algorithm scores against **personal rolling baselines**, not population norms. An athlete at 57 ms is at their baseline exactly as a beginner at 41 ms is at theirs.

This is the core of the WHOOP-style approach — recovery is relative to **you**. A 41 ms reading would alarm an athlete and is unremarkable for a beginner.

---

## Finding 5: HRV Is the Strongest Predictor of Recovery

**Pearson correlation between HRV rMSSD and recovery score: r = 0.534**

Correlation of each component against the final score:

| Component | Weight | r vs final score |
|---|---|---|
| HRV | 40% | **0.951** |
| Resting HR | 25% | 0.541 |
| Sleep performance | 25% | 0.295 |
| SpO₂ | 10% | 0.284 |

The HRV *component* tracks the final score at r = 0.951, far above its 40% weight, because it carries the widest dynamic range of the four. Raw rMSSD correlates more loosely (r = 0.534) since the z-score step normalises it against each user's own baseline before scoring — which is exactly the intent.

---

## Finding 6: Sleep Quality Distribution

| Sleep Quality Tier | Nights | Percentage |
|---|---|---|
| Excellent | 483 | **53.7%** |
| Good | 320 | 35.6% |
| Fair | 97 | 10.8% |
| Poor | 0 | 0.0% |

**Sleep debt accumulation:** average 14-day rolling sleep debt is **7.0 hours** (just under 1 hour/night). Maximum observed was **29.8 hours** — nearly 2 hours/night sustained over two weeks, a level associated with measurable cognitive impairment in the sleep literature.

No night lands in the Poor tier, which reflects the generator's conservative sleep model rather than a property of real populations.

---

## Finding 7: The Quarantine Layer Catches Injected Sensor Faults

The quality checks flagged **8 activity records** (0.9%) with out-of-bounds heart rate:

- 5 records with `avg_hr_bpm` outside the 30–220 range
- 7 records with `peak_hr_bpm` outside the 30–225 range
- (4 records violate both bounds simultaneously)

These come from the generator's deliberate 2% anomaly injection, which simulates two real optical-HR failure modes: **strap contact loss** (HR reads 12–28 bpm) and **motion artifact / cadence lock** (peak spikes to 230–265 bpm). 14 records total are flagged `is_anomaly`; the remainder fell on rest days, where there is no workout HR to corrupt.

Zone minutes are computed *before* the fault is applied, so the workout's strain remains coherent while the reported HR does not — matching how a real strap misreports a session that physically happened.

---

## Implications for Wearable Analytics

1. **Personalization matters more than absolutes.** Population-level HRV norms are meaningless for individual recovery assessment. Each user needs their own baseline — Finding 4 is the direct evidence.

2. **Sleep is the biggest controllable lever.** Strain and HRV are largely outcomes; sleep duration is a behaviour users can actively manage, and it moves recovery 12.5 points.

3. **Weekly periodicity is real.** Any model or dashboard ignoring day-of-week effects will produce misleading insights — the spread here is 16.7 points.

4. **Data quality is non-negotiable.** The 8 quarantined records would have corrupted strain calculations and HR-zone aggregates if passed through unchecked.

5. **Recovery weeks work.** The 12.0-point lift during deload weeks is quantitative evidence for periodized training.

---

## Caveats

This is **synthetic data from a seeded generator**, not a clinical dataset. The findings demonstrate that the pipeline and scoring algorithm behave correctly and sensitively; they are not evidence about human physiology. Several patterns above (the >9h sleep result, the absence of Poor sleep nights, only 2 Red days across 900 user-days) are properties of the generator's model rather than discoveries.
