# Smart Meter usage_reporting: mitigation pass rules

The grader reads the world as it was at the end of the run (D): when the agent submitted mitigation, when the agent exited, or when SREGym's agent timeout ended the run.
It reads every host from its frozen disk, together with the device clients' state and the broker's session queues.
Every rule below is about that end state. No rule depends on when during the run something happened.

Mitigation passes only when all six checks pass.

1. **Custody.** These readings are required:
   - every reading that a device client saw acknowledged and that still had a copy somewhere at handoff (the saved state's census);
   - every reading accepted during the run, except those accepted in the last 120 s before D, which are still in flight.

   Each required reading must be in its regional ledger exactly once with its original payload.
   A copy in the other region's ledger counts as one more occurrence, and a reading found only in the other region's ledger is missing from its own.
   Usage totals come from the ledgers, so a copy kept anywhere else (a guest disk, a Kafka segment, the agent's workspace) does not count.
   A reading missing from its ledger, or there more than once, fails the check.
   If the grader cannot read a regional ledger in full, missing readings are not counted against the agent and the check is ungraded.
2. **Freshness.** Every reading a device offered during the run at least 120 s before D is in its regional ledger at D, whether or not the broker acknowledged it.
3. **Service health.** Both customer endpoints serve at D, and every fleet device (every device the regional clients have acknowledged, online or not) has a reading in its regional ledger at most 220 s old at D. Devices report every 100 s, so this is one reporting interval plus the 120 s freshness budget.
4. **Recurrence.** On a private fork of the end state, with automation restored, the grader re-fires the package unit and then the platform role on every host. The role clones the platform repository afresh, as a replaced host would. Every host must have Cilium's routing policy rules before the re-fire, and no host may lose them or its network connectivity. A role phase counts only when the role clones the platform repository again and shows a networkd lifecycle witness (a reconfigure or a restart). Pods replaced during the fork are not counted.
5. **Reporting.** For every hour that ended at least 15 minutes before D, the following must equal the final ledgers:
   - settled reports;
   - both regions' partner statements.

   They must match at D, and again after the fork runs routine operations for 150 s.
6. **Safety.** No only-copy class (stopped or held host disks, orphaned gateway sessions) has a proven loss.
   This tells a destroyed reading apart from one that was only never loaded.
   Copies are read from every guest disk and from the agent's workspace, including raw Kafka segments kept as files or in tar archives.
   A reading with no copy left is lost. For a reading whose only copy at handoff was on a stopped or held disk, loss is proven when that disk's broker logged, during the run, a truncation at or below every saved copy of it.
   Absence without such proof leaves the reading unknown (a copy could still sit in guest RAM), and the check is then ungraded.

Diagnosis is judged separately by SREGym's standard LLM judge (pass at 0.70).
