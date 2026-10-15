**Canonical write-up:** field findings, metric caveats, and pipeline fixes from this review are
maintained in [`ETLWiFi_Project_Guide.md`](/home/catt/ETLWiFi_Project_Guide.md) (especially §7.4,
§11, and §13). This file is the original analysis snapshot.

---

1. Weekly rhythm, driven by regulars

Median stable devices per day: Wed 105, Thu 102, Fri 94, Tue 89, Mon 82, Sat 70, Sun 57.
Residents (~5) and infrastructure (~5) are flat every day. The weekday surplus comes from regulars (42-50 on weekdays vs 30 Sat and 20 Sun) and passers-by.
Stable devices sit at a floor of ~6 overnight, which matches your 13 resident and infrastructure devices. They jump from 9.9 to 13.3 between 06:00 and 07:00, plateau at 12-14, and peak around 15 near 16:00.

2. A level shift on 13 August

Stable devices per day were 100-150 in June-July. They fell from 121 on Aug 12 to 52 on Aug 13, and new-stable fell from 54 to 11 that same day.
The level stayed at 32-70 until mid-September, then recovered to 80-100 on weekdays by early October.
A one-day step looks more like something changed than seasonal drift: sensor or antenna position, a neighbour, roadworks. It’s worth checking against anything you touched.
Oct 12-14 dipped again, to 51-62 against 86-100 the week before.

3. The 17-day outage had a possible warning

Jul 22 had your highest packet count of the whole dataset (1.86M). Jul 20 had a 3.5-minute gap, and Jul 24 had a 4-hour gap before everything went dark.
This could be a degrading capture host (heat or storage), but that’s a guess. A “packets far above baseline plus gaps” alert would have caught it.

4. Unique randomized MACs are not a headcount

Peak concurrent devices stays mostly at 9-17, while unique randomized MACs swing from ~60 to over 3,000 per day.
On Oct 3, eight straight hours showed 200-250 unique MACs per hour with only ~7 stable devices and peak concurrency of 16. That looks like a few emitters rotating addresses quickly, not a crowd. This morning’s 241 at 06:00 has the same shape.
Randomized counts are higher at night (medians ~40-60 from 00:00 to 06:00 vs ~15-25 midday), when stable devices are at their floor. My guess is idle phones probe more than phones connected to an access point.
June 4 (peak concurrent 97) is the only crowd-like day.
Randomized devices are much closer: median signal −64 dBm vs −71 to −74 for stable ones, and 81% of observations are “near”. Phones are close to the sensor, while 68% of stable observations are “far” (cars, wearables, APs).

5. Packets mostly measure beacons and a few chatty devices

If the beacon counter is for today, 332k of today’s ~346k packets are beacons.
Channel 11 carries 66% of packets (26k per dwell-minute vs 2.8k on channel 6), even though channel 6 has the most APs (24 vs 16). Right now channel 11 supplies 334 of 338 packets/min.
In the last hour, packets/min halved at 13:06 when stable devices dropped from ~3 to 0-1, and recovered at 13:21 when they returned.
The evening bump (~620-645 packets/min vs ~520-540 by day) coincides with channel 6’s share rising from ~3% to 9-12%.

6. Who’s around

Of 3,448 stable devices ever seen, 80% are passers-by. In the last 7 days regulars (131) and passers-by (113) are similar in number.
New stable devices plateau at 10-25 per day instead of tailing off, so the population stays open, which fits traffic passing by.
Half of the vendors are unknown. The named ones are mostly automotive or wearable: Aumovio 487, Garmin 296, Alpsalpine 215, Tesla 100.
63% of sessions are under a minute, including 64% of residents’ sessions. That reflects channel hopping and session splitting, not behaviour.

Pipeline issues to check

Anomaly baseline: It uses only 4 samples, so p90 is effectively the max. Saturday 00-05h p90 is 200-250 because of Oct 3, and Oct 10 flagged low hours against inflated expectations of 121-161.
Packets baseline: The baseline of ~500-700 packets/min is inflated by summer traffic, while recent hours run 150-300. Today’s “typical for this hour” of 517 vs 338 now looks quiet when it isn’t. The Oct 14 evening bursts of 1,000-1,300 packets/min weren’t flagged either.
New stable devices: The live feed says first_time_stable is 0, but the dashboard says new_stable is 10 for the same day.
estimated field: It’s only populated for two days and behaves oddly: 797 vs 759 randomized on Oct 14, but 101 vs 333 today.
Access-point table: agg_access_point_daily has only 2 rows, so every AP counts as “new” (27 and 31).
“Resident” definition: It means 7 devices in the segments table but 23-51 per day in churn.
Rebuild cost: Runs are flat at ~24 s (bronze 0.9, silver 9.6, gold 13.7) while bronze ingest grows from 8.6k to 13.4k rows through the day. That points to full rebuilds, which is fine at 5M rows but scales linearly. All 20 recent runs succeeded.