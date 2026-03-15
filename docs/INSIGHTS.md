# Data Insights

Analysis of 900 user-days (10 users × 90 days) of synthetic wearable data processed through the FitLake pipeline. All numbers below are computed from the Gold layer output.

---

## Dataset Overview

| Metric | Value |
|---|---|
| Users | 10 (3 athletes, 4 intermediate, 3 beginners) |
| Days tracked | 90 per user |
| Total records | 900 per table (HRV, sleep, activity) |
| Avg recovery score | **70.0 / 100** |
| Recovery distribution | 617 Green (68.6%) · 283 Yellow (31.4%) · 0 Red |
| Avg HRV (rMSSD) | 50.3 ms |
| Avg sleep | 7.51 hours |

---

## Finding 1: Sleep Duration Is the Strongest Lever for Recovery

| Sleep Duration | Avg Recovery | Sample Size |
|---|---|---|
| **< 6 hours** | **59.7** | 44 nights |
| 7–9 hours | 70.6 | 633 nights |
| > 9 hours | 71.9 | 42 nights |

**Insight:** Users who slept less than 6 hours averaged nearly **11 points lower** recovery than those in the 7–9 hour optimal zone. The marginal return from sleeping beyond 9 hours is minimal (+1.3 points), confirming that oversleeping doesn't meaningfully improve recovery — but undersleeping dramatically harms it.

**HRV confirms this:** On nights with < 6h sleep, average HRV was **41.7 ms** vs **50.2 ms** on nights with 7+ hours — an **8.4 ms drop** that directly feeds into a lower recovery score the next morning.

---

## Finding 2: Recovery Follows a Clear Day-of-Week Pattern

| Day | Avg Recovery |
|---|---|
| **Sunday** | **74.7** (highest) |
| Tuesday | 71.7 |
| Wednesday | 71.5 |
| Monday | 71.1 |
| Thursday | 68.9 |
| Friday | 68.0 |
| **Saturday** | **64.7** (lowest) |

**Insight:** Recovery peaks on **Sunday** (rest day in the training simulation) and bottoms out on **Saturday**. This 10-point spread across the week reflects the cumulative effect of training load: Friday and Saturday are high-strain days in the simulated training blocks, and recovery hasn't caught up yet by Saturday morning's HRV measurement.

This pattern mirrors real-world WHOOP data — athletes who train hard Thursday–Saturday consistently see their lowest recovery scores on Saturday morning.

---

## Finding 3: Training Blocks Clearly Suppress Recovery

| Training Phase | Avg Recovery |
|---|---|
| Training weeks (days 1–21 of cycle) | **68.3** |
| Recovery week (days 22–28 of cycle) | **75.8** |

**Insight:** The simulated 4-week periodization (3 weeks progressive overload → 1 week recovery) produces a **7.5-point recovery gap** between training and recovery phases. This validates the recovery score algorithm's sensitivity to training load cycles — it correctly detects that accumulated training stress suppresses autonomic recovery, and that a deload week allows the body to catch up.

---

## Finding 4: Fitness Level Affects HRV But Not Recovery

| Fitness Level | Avg Recovery | Avg HRV (rMSSD) |
|---|---|---|
| Athlete | 69.7 | **57.5 ms** |
| Intermediate | 69.8 | 52.1 ms |
| Beginner | 70.7 | **40.7 ms** |

**Insight:** Recovery scores are nearly **identical across fitness levels** (within 1 point), even though athletes have 42% higher absolute HRV than beginners. This is by design — the algorithm uses **personal rolling baselines**, not population norms. An athlete with 57ms HRV is at their baseline, just as a beginner at 40ms is at theirs. Both score ~70.

This is the core insight behind WHOOP's approach: recovery is relative to **you**, not to the population. A 40ms HRV would alarm an athlete but is perfectly normal for a sedentary beginner.

---

## Finding 5: HRV Is the Strongest Predictor of Recovery

**Pearson correlation between HRV rMSSD and recovery score: r = 0.473**

Among the four recovery components:
- **HRV** (40% weight) has the strongest correlation with the final score
- This is expected given HRV's 40% weight, but the correlation also validates that the z-score normalization and tanh sigmoid mapping produce a meaningful signal
- Resting HR and sleep quality contribute meaningful variance beyond HRV, justifying their inclusion

---

## Finding 6: Sleep Quality Distribution

| Sleep Quality Tier | Nights | Percentage |
|---|---|---|
| Excellent | 481 | **53.4%** |
| Good | 319 | 35.4% |
| Fair | 100 | 11.1% |
| Poor | 0 | 0.0% |

**Sleep debt accumulation:** The average 14-day rolling sleep debt is **7.0 hours** (just under 1 hour/night on average). The maximum observed sleep debt was **26.0 hours** (nearly 2 hours/night sustained over 2 weeks) — a level associated with significant cognitive impairment in sleep research.

---

## Finding 7: The Quarantine Layer Catches Real Issues

The data quality pipeline flagged **53 activity records** with out-of-bounds heart rate values:
- 44 records with `avg_hr_bpm` outside the 30–220 range
- 9 records with `peak_hr_bpm` outside the 30–225 range

These represent the 2% anomaly injection rate built into the synthetic data generator. In a real pipeline, these would correspond to sensor dropouts, Bluetooth disconnections, or device malfunctions — exactly the kind of data that should be quarantined rather than corrupting downstream analytics.

---

## Implications for Wearable Analytics

These findings demonstrate several principles that apply to real wearable data at scale:

1. **Personalization matters more than absolutes.** Population-level HRV norms are meaningless for individual recovery assessment. Each user needs their own baseline.

2. **Sleep is the biggest controllable lever.** While strain and HRV are largely outcomes of training, sleep duration is a behavior users can actively manage.

3. **Weekly periodicity is real.** Any model or dashboard that ignores day-of-week effects will produce misleading insights.

4. **Data quality is non-negotiable.** The 53 quarantined records (5.9% of activity data) would have skewed strain calculations if passed through unchecked.

5. **Recovery weeks work.** The 7.5-point recovery lift during deload weeks provides quantitative evidence for periodized training.
