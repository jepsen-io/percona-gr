# Jepsen Percona Group Replication Test

A test harness for Percona Server with Group Replication, verifying isolation
levels up to strict serializable.

## Usage

For example, try:

```
lein run test --time-limit 300 --isolation repeatable-read --expected-consistency repeatable-read --rate 1000 --concurrency 3n --select-for share --nemesis partition,recover
```

## Workloads

`-w list-append` runs an Elle list-append workload, where each list is stored
in a row like `(primary-key, secondary-key, value)`. Primary and secondary key
are identical; access by secondary key tests index consistency. Values are
strings, and appending elements to them is done with SQL `CONCAT`. See `jepsen.percona-gr.list-append` for details.

`-w rw-register` is a read-write register--in most respects it's just like
list-append, but its value is an integer which is overwritten by blind writes,
rather than appended to using string concat. Inference here is weaker than
list-append, but it may expose different behavior.

## Nemeses

The usual `pause`, `kill`, `partition,` and `clock` introduce process pauses,
crashes, network partitions, and clock skew, respectively. `recover`
periodically checks to see if the cluster has fallen over, and tries to recover
it by restarting group replication from scratch. You'll probably have to mix
`recover` with most fault modes, because GR is fragile. Recovery is also
incredibly slow--300+ seconds is common. Make sure to choose a time limit long
enough.

## Options

See `lein run test --help` for full options.

`abort-probability` is the probability that we choose to spuriously abort a
transaction while executing it. We do this to detect aborted reads.

`--concurrency 3n` means "run three worker threads for each node".

There are two strategies for writing values. `--[no-]on-dup-key` enables or
disables writing using `INSERT ... ON DUPLICATE KEY`. `--[no-]update-insert`
enables or disables writing using an `UPDATE` first, and backing off to an
`INSERT` if that fails, and if THAT fails, trying an `UPDATE` again.

`--expected-consistency-model` controls the level of consistency we check for.
By default this is `strict-serializable`, but you could also choose any Elle
consistency model, like `strong-session-snapshot-isolation` or
`read-committed`.

`--isolation` controls the isolation level we request from Percona during transactions. This should be `read-uncommitted`, `read-committed`, `repeatable-read`, or `serializable`.

`--innodb-flush-method METHOD` allows changing how InnoDB flushes data to disk.

`--lazyfs` mounts the Percona data directory in a lazyfs filesystem, and changes the `kill` nemesis fault to also lose un-fsynced writes.

`--max-txn-length LEN` and `--max-writes-per-key COUNT` control the upper
bounds on micro-ops per transaction and how many writes we attempt on a given
key before choosing a new key.

`--nemesis FAULTS` takes a comma-separated list of faults to inject, like
`kill,partition,recover`. `--nemesis-interval SECONDS` controls roughly how
long it takes between nemesis operations (in a given fault class).

`--no-db` skips database teardown and setup. This will almost certainly result
in corrupt results: false positives, etc. However, Percona is so slow to set up
that this option is helpful for debugging workloads.

`--[no-]predicate-reads` allows disabling accessing rows by secondary key, rather than primary key.

`--table-count NUM` controls the number of tables we split rows across.

`--rate HZ` is the upper bound on how many ops/sec Jepsen tries to perform.

`--recover-interval` is how often the recovery nemesis checks to see if it needs to recover the cluster.

`--recovery-time` controls how long we wait for the cluster to heal at the
end of the test.

`--select-for MODE`, if set, uses SELECT ... FOR UPDATE or FOR SHARE on
selects. Using `share` or `update` are applied to every select operation.
`share+update` does a `FOR UPDATE` on rows that the transaction will write
later, and `FOR SHARE` otherwise.

`--single-node` runs just one Percona node, and skips all the group replication
setup.

`--time-limit SECONDS` controls how long the test runs for.

`--inter-mop-delay MS` introduces randomized delays (exponentially distributed) between each micro-op in a transaction. Helpful for increasing the window during which we can observe isolation violations.

`--workload NAME` chooses which workload to run.

## License

Copyright © 2022 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
