// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors.Tests.Runtime.Volatile
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Actors.Runtime;
    using Xunit;
    using ActorStateTable = Microsoft.ServiceFabric.Actors.Runtime.VolatileActorStateTable<
        Actors.Runtime.VolatileActorStateProvider.ActorStateType,
        string,
        Actors.Runtime.VolatileActorStateProvider.ActorStateData>;

    public class VolatileActorStateTableTests : VolatileStateProviderTestBase
    {
        [Fact]
        public void TestPrepareCommit()
        {
            this.TestPrepareCommitInternal(GetStatesPerReplication());
            this.TestPrepareCommitInternal(GetStatesPerReplication(3));
        }

        [Fact]
        public void TestEnumerateMaxSequenceNumber()
        {
            this.TestEnumerateMaxSequenceNumberInternal(GetStatesPerReplication());
            this.TestEnumerateMaxSequenceNumberInternal(GetStatesPerReplication(3));
        }

        [Fact]
        public void TestApply()
        {
            this.TestApplyInternal(GetStatesPerReplication());
            this.TestApplyInternal(GetStatesPerReplication(3));
        }

        [Fact]
        public void TestUpdateApply()
        {
            this.TestUpdateApplyInternal(GetStatesPerReplication());
            this.TestUpdateApplyInternal(GetStatesPerReplication(3));
        }

        [Fact]
        public void TestUpdateCommit()
        {
            this.TestUpdateCommitInternal(GetStatesPerReplication());
            this.TestUpdateCommitInternal(GetStatesPerReplication(3));
        }

        [Fact]
        public void TestSnapshot()
        {
            this.TestSnapshotInternal(GetStatesPerReplication());
            this.TestSnapshotInternal(GetStatesPerReplication(3));
        }

        [Fact]
        public void TestSnapshotScale()
        {
            TestCase("#########################");
            TestCase("### TestSnapshotScale ###");
            TestCase("#########################");

            long sequenceNumber = 0;
            var stateTable = new ActorStateTable();

            int targetReplicationCount = 1 * 1000;
            Dictionary<VolatileActorStateProvider.ActorStateType, int> statesPerReplication = GetStatesPerReplication(10);

            VerifyStateTableSnapshot(stateTable, statesPerReplication, long.MaxValue, 0, 0, 0);

            TestLog("Generating {0} keys...", targetReplicationCount * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

            var stopwatch = new Stopwatch();

            stopwatch.Start();

            for (var ix = 0; ix < targetReplicationCount; ++ix)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    ix.ToString(),
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    1);

                TestApply(stateTable, replicationUnit, false);
            }

            stopwatch.Stop();

            TestLog(
                "Generated {0} keys in {1}",
                targetReplicationCount * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                stopwatch.Elapsed);

            stopwatch.Restart();

            ActorStateTable.ActorStateEnumerator snapshot = stateTable.GetShallowCopiesEnumerator(long.MaxValue);

            stopwatch.Stop();

            TestLog(
                "Snapshot {0} keys in {1}: committed={2} uncommitted={3}",
                targetReplicationCount * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                stopwatch.Elapsed,
                snapshot.CommittedCount,
                snapshot.UncommittedCount);

            TestCase("# Passed");
        }

        [Fact]
        public void TestMultipleTypes()
        {
            this.TestMultipleTypesInternal(GetStatesPerReplication());
            this.TestMultipleTypesInternal(GetStatesPerReplication(3));
        }

        [Fact]
        public void TestDelete()
        {
            this.TestDeleteInternal(GetStatesPerReplication());
            this.TestDeleteInternal(GetStatesPerReplication(3));
        }

        internal void TestPrepareCommitInternal(Dictionary<VolatileActorStateProvider.ActorStateType, int> statesPerReplication)
        {
            TestCase("#########################");
            TestCase("### TestPrepareCommit ###");
            TestCase("#########################");

            TestCase(
                "### StatesPerReplication (Actor:{0}, TimeStamp:{1}, Reminder:{2}) ###",
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder]);

            long sequenceNumber = 0;
            var stateTable = new ActorStateTable();
            VerifyStateTableSnapshot(stateTable, statesPerReplication, long.MaxValue, 0, 0, 0);

            TestCase("# Testcase 1: In order prepare, commit, prepare, commit ...");

            foreach (string keyPrefix in new[] {"a", "b", "c"})
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    sequenceNumber);

                TestPrepareUpdate(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

                TestCommitUpdate(stateTable, sequenceNumber);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            TestCase("# Testcase 2: In order prepare, prepare, ... commit, commit ...");

            var keyPrefixList = new[] {"d", "e", "f"};
            var replicationUnitDict = new Dictionary<string, ReplicationUnit>();

            long commitSequenceNumber = sequenceNumber;

            foreach (string keyPrefix in keyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    sequenceNumber * 2);

                replicationUnitDict.Add(keyPrefix, replicationUnit);

                TestPrepareUpdate(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    commitSequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            foreach (string keyPrefix in keyPrefixList)
            {
                ++commitSequenceNumber;

                TestCommitUpdate(stateTable, commitSequenceNumber);

                VerifyReads(
                    stateTable,
                    replicationUnitDict[keyPrefix],
                    statesPerReplication,
                    true,
                    commitSequenceNumber * 2,
                    commitSequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            TestCase("# Testcase 3: Out of order commits");

            keyPrefixList = new[] {"g", "h", "i"};
            replicationUnitDict = new Dictionary<string, ReplicationUnit>();

            long preCommitSequenceNumber = sequenceNumber;
            foreach (string keyPrefix in keyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    keyPrefix.ToCharArray()[0]);

                replicationUnitDict.Add(keyPrefix, replicationUnit);

                TestPrepareUpdate(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    preCommitSequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            long commitSequenceNumber1 = sequenceNumber;
            Task.Factory.StartNew(() => { TestCommitUpdate(stateTable, commitSequenceNumber1); });
            Thread.Sleep(500);
            VerifyReads(
                stateTable,
                replicationUnitDict["i"],
                statesPerReplication,
                false,
                0,
                preCommitSequenceNumber,
                sequenceNumber,
                sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

            long commitSequenceNumber2 = commitSequenceNumber1 - 1;
            Task.Factory.StartNew(() => { TestCommitUpdate(stateTable, commitSequenceNumber2); });
            Thread.Sleep(500);
            VerifyReads(
                stateTable,
                replicationUnitDict["h"],
                statesPerReplication,
                false,
                0,
                preCommitSequenceNumber,
                sequenceNumber,
                sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

            long commitSequenceNumber3 = commitSequenceNumber2 - 1;
            TestCommitUpdate(stateTable, commitSequenceNumber3);
            Thread.Sleep(500);

            foreach (string keyPrefix in keyPrefixList)
            {
                VerifyReads(
                    stateTable,
                    replicationUnitDict[keyPrefix],
                    statesPerReplication,
                    true,
                    keyPrefix[0],
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            TestCase("# Passed");
        }

        internal void TestEnumerateMaxSequenceNumberInternal(Dictionary<VolatileActorStateProvider.ActorStateType, int> statesPerReplication)
        {
            TestCase("######################################");
            TestCase("### TestEnumerateMaxSequenceNumber ###");
            TestCase("######################################");

            TestCase(
                "### StatesPerReplication (Actor:{0}, TimeStamp:{1}, Reminder:{2}) ###",
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder]);

            long sequenceNumber = 0;
            var stateTable = new ActorStateTable();
            VerifyStateTableSnapshot(stateTable, statesPerReplication, long.MaxValue, 0, 0, 0);

            TestCase("# Testcase 1: Commmitted values only");

            var committedKeyPrefixList = new[] {"apple", "orange", "banana"};
            foreach (string keyPrefix in committedKeyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    keyPrefix.Length);

                TestPrepareUpdate(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

                TestCommitUpdate(stateTable, sequenceNumber);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    keyPrefix.Length,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            VerifyStateTableSnapshot(
                stateTable,
                statesPerReplication,
                long.MaxValue,
                sequenceNumber,
                sequenceNumber,
                sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

            TestCase("# Testcase 2: Commmitted + uncommitted values");

            var uncommittedKeyPrefixList = new[] {"grape", "pear", "kiwi"};
            foreach (string keyPrefix in uncommittedKeyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    keyPrefix.Length);

                TestPrepareUpdate(stateTable, replicationUnit);

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    committedKeyPrefixList.Length,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            VerifyStateTableSnapshot(
                stateTable,
                statesPerReplication,
                long.MaxValue,
                sequenceNumber - uncommittedKeyPrefixList.Length,
                sequenceNumber,
                sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

            TestCase("# Passed");
        }

        internal void TestApplyInternal(Dictionary<VolatileActorStateProvider.ActorStateType, int> statesPerReplication)
        {
            TestCase("#################");
            TestCase("### TestApply ###");
            TestCase("#################");

            TestCase(
                "### StatesPerReplication (Actor:{0}, TimeStamp:{1}, Reminder:{2}) ###",
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder]);

            long sequenceNumber = 0;
            var stateTable = new ActorStateTable();
            VerifyStateTableSnapshot(stateTable, statesPerReplication, long.MaxValue, 0, 0, 0);

            TestCase("# Testcase 1: Singleton apply");

            var keyPrefixList = new[] {"f", "fo", "foo"};
            foreach (string keyPrefix in keyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    keyPrefix.Length);

                TestApply(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    keyPrefix.Length,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            TestCase("# Testcase 2: Batch apply");

            var keyPrefixBatchList = new[]
            {
                new[] {"b", "ba", "barr"},
                new[] {"x", "xy", "xyz"},
                new[] {"a", "ab", "abc"}
            };

            foreach (string[] keyPrefixBatch in keyPrefixBatchList)
            {
                var replicationUnitBatch = new List<ReplicationUnit>();
                var replicationUnitDict = new Dictionary<string, ReplicationUnit>();

                foreach (string keyPrefix in keyPrefixBatch)
                {
                    ++sequenceNumber;

                    ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                        sequenceNumber,
                        keyPrefix,
                        statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                        keyPrefix.Length);

                    replicationUnitBatch.Add(replicationUnit);
                    replicationUnitDict.Add(keyPrefix, replicationUnit);
                }

                TestApplyBatch(stateTable, replicationUnitBatch);

                foreach (string keyPrefix in keyPrefixBatch)
                {
                    VerifyReads(
                        stateTable,
                        replicationUnitDict[keyPrefix],
                        statesPerReplication,
                        true,
                        keyPrefix.Length,
                        sequenceNumber,
                        sequenceNumber,
                        sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
                }
            }

            TestCase("# Passed");
        }

        internal void TestUpdateApplyInternal(Dictionary<VolatileActorStateProvider.ActorStateType, int> statesPerReplication)
        {
            TestCase("#######################");
            TestCase("### TestUpdateApply ###");
            TestCase("#######################");

            TestCase(
                "### StatesPerReplication (Actor:{0}, TimeStamp:{1}, Reminder:{2}) ###",
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder]);

            long sequenceNumber = 0;
            var stateTable = new ActorStateTable();
            VerifyStateTableSnapshot(stateTable, statesPerReplication, long.MaxValue, 0, 0, 0);

            var keyPrefixList = new[] {"a-apply", "b-apply", "c-apply"};
            foreach (string keyPrefix in keyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    keyPrefix.Length);

                TestApply(stateTable, replicationUnit);

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    keyPrefix.Length,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            long commitSequenceNumber = sequenceNumber;
            var replicationUnitDict = new Dictionary<string, ReplicationUnit>();

            foreach (string keyPrefix in keyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    keyPrefix.Length * 2);

                replicationUnitDict.Add(keyPrefix, replicationUnit);

                TestPrepareUpdate(stateTable, replicationUnit);

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    keyPrefix.Length,
                    keyPrefixList.Length,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            foreach (string keyPrefix in keyPrefixList)
            {
                ++commitSequenceNumber;

                TestCommitUpdate(stateTable, commitSequenceNumber);

                VerifyReads(
                    stateTable,
                    replicationUnitDict[keyPrefix],
                    statesPerReplication,
                    true,
                    keyPrefix.Length * 2,
                    commitSequenceNumber,
                    sequenceNumber,
                    (sequenceNumber + (keyPrefixList.Length - commitSequenceNumber)) * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            TestCase("# Passed");
        }

        internal void TestUpdateCommitInternal(Dictionary<VolatileActorStateProvider.ActorStateType, int> statesPerReplication)
        {
            TestCase("########################");
            TestCase("### TestUpdateCommit ###");
            TestCase("########################");

            TestCase(
                "### StatesPerReplication (Actor:{0}, TimeStamp:{1}, Reminder:{2}) ###",
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder]);

            long sequenceNumber = 0;
            var stateTable = new ActorStateTable();
            VerifyStateTableSnapshot(stateTable, statesPerReplication, long.MaxValue, 0, 0, 0);

            var keyPrefixList = new[] {"a-commit", "b-commit", "c-commit"};
            foreach (string keyPrefix in keyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    keyPrefix.Length);

                TestPrepareUpdate(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

                TestCommitUpdate(stateTable, sequenceNumber);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    keyPrefix.Length,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            long commitSequenceNumber = sequenceNumber;
            var replicationUnitDict = new Dictionary<string, ReplicationUnit>();

            foreach (string keyPrefix in keyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    keyPrefix.Length * 2);

                replicationUnitDict.Add(keyPrefix, replicationUnit);

                TestPrepareUpdate(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    keyPrefix.Length,
                    keyPrefixList.Length,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            foreach (string keyPrefix in keyPrefixList)
            {
                ++commitSequenceNumber;

                TestCommitUpdate(stateTable, commitSequenceNumber);

                VerifyReads(
                    stateTable,
                    replicationUnitDict[keyPrefix],
                    statesPerReplication,
                    true,
                    keyPrefix.Length * 2,
                    commitSequenceNumber,
                    sequenceNumber,
                    (sequenceNumber + (keyPrefixList.Length - commitSequenceNumber)) * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            TestCase("# Passed");
        }

        internal void TestSnapshotInternal(Dictionary<VolatileActorStateProvider.ActorStateType, int> statesPerReplication)
        {
            TestCase("####################");
            TestCase("### TestSnapshot ###");
            TestCase("####################");

            TestCase(
                "### StatesPerReplication (Actor:{0}, TimeStamp:{1}, Reminder:{2}) ###",
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder]);

            long sequenceNumber = 0;
            var stateTable = new ActorStateTable();
            VerifyStateTableSnapshot(stateTable, statesPerReplication, long.MaxValue, 0, 0, 0);

            var committedKeyPrefixList = new[] {"w", "x", "y", "z"};
            var replicationUnitDict = new Dictionary<string, ReplicationUnit>();

            foreach (string commitedkeyPrefix in committedKeyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    commitedkeyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    commitedkeyPrefix.Length);

                replicationUnitDict[commitedkeyPrefix] = replicationUnit;

                TestApply(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    commitedkeyPrefix.Length,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            var updatedKeyPrefixList = new[] {"w", "y"};
            foreach (string updatedkeyPrefix in updatedKeyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    updatedkeyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    updatedkeyPrefix.Length * 2);

                replicationUnitDict[updatedkeyPrefix] = replicationUnit;

                TestPrepareUpdate(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    updatedkeyPrefix.Length,
                    committedKeyPrefixList.Length,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            var uncommittedKeyPrefixList = new[] {"a", "b", "c"};
            foreach (string uncommittedkeyPrefix in uncommittedKeyPrefixList)
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    uncommittedkeyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    uncommittedkeyPrefix.Length * 2);

                replicationUnitDict[uncommittedkeyPrefix] = replicationUnit;

                TestPrepareUpdate(stateTable, replicationUnit);
                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    committedKeyPrefixList.Length,
                    sequenceNumber,
                    sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);
            }

            ActorStateTable.ActorStateEnumerator committedSnapshot = stateTable.GetShallowCopiesEnumerator(committedKeyPrefixList.Length);
            ActorStateTable.ActorStateEnumerator knownSnapshot = stateTable.GetShallowCopiesEnumerator(long.MaxValue);

            int updateSequenceNumber = committedKeyPrefixList.Length;

            foreach (string key in updatedKeyPrefixList)
            {
                ++updateSequenceNumber;
                TestCommitUpdate(stateTable, updateSequenceNumber);
            }

            TestCommitUpdate(stateTable, ++updateSequenceNumber);

            int expectedCount = (committedKeyPrefixList.Length +
                                 uncommittedKeyPrefixList.Length) *
                                statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

            var keyPrefixes = new[] {"x", "z", "w", "y", "a", "b", "c"};
            var expectedResults = new[] {true, true, true, true, true, false, false};
            var expectedLengths = new[] {1, 1, 2, 2, 2, 0, 0};

            for (var i = 0; i < keyPrefixes.Length; i++)
            {
                VerifyReads(
                    stateTable,
                    replicationUnitDict[keyPrefixes[i]],
                    statesPerReplication,
                    expectedResults[i],
                    expectedLengths[i],
                    updateSequenceNumber,
                    sequenceNumber,
                    expectedCount);
            }

            VerifyStateTableSnapshot(
                stateTable,
                statesPerReplication,
                committedSnapshot,
                updateSequenceNumber,
                sequenceNumber,
                committedKeyPrefixList.Length * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

            VerifyStateTableSnapshot(
                stateTable,
                statesPerReplication,
                knownSnapshot,
                updateSequenceNumber,
                sequenceNumber,
                sequenceNumber * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

            TestCase("# Passed");
        }

        internal void TestMultipleTypesInternal(Dictionary<VolatileActorStateProvider.ActorStateType, int> statesPerReplication)
        {
            TestCase("#######################################################");
            TestCase("### TestMultipleTypes ###");
            TestCase("#######################################################");

            TestCase(
                "### StatesPerReplication (Actor:{0}, TimeStamp:{1}, Reminder:{2}) ###",
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder]);

            long sequenceNumber = 0;
            var stateTable = new ActorStateTable();
            VerifyStateTableSnapshot(stateTable, statesPerReplication, long.MaxValue, 0, 0, 0);

            var committedEntriesCount = 0;
            var uncommittedEntriesCount = 0;

            TestCase("# Testcase 1: In order prepare, commit, prepare, commit ...");

            {
                ++sequenceNumber;

                var keyPrefix = "a";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    sequenceNumber);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            {
                ++sequenceNumber;

                var key = "L";
                TimeSpan timestamp = TimeSpan.FromSeconds(sequenceNumber);

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateTimeStamp(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                    timestamp);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    timestamp,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    timestamp,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            {
                ++sequenceNumber;

                var key = "rem";
                var reminderName = "Rem-Name1";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateReminder(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder],
                    reminderName);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    reminderName,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    reminderName,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            TestCase("# Testcase 2: Duplicate keys per type ...");

            long expectedCount = sequenceNumber;

            {
                ++sequenceNumber;

                var keyPrefix = "a";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    sequenceNumber);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    sequenceNumber - expectedCount,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            {
                ++sequenceNumber;

                var key = "L";
                TimeSpan oldTimestamp = TimeSpan.FromSeconds(sequenceNumber - expectedCount);
                TimeSpan timestamp = TimeSpan.FromSeconds(sequenceNumber);

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateTimeStamp(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                    timestamp);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    oldTimestamp,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    timestamp,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            {
                ++sequenceNumber;

                var key = "rem";
                var oldReminderName = "Rem-Name1";
                var reminderName = "Rem-Name2";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateReminder(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder],
                    reminderName);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    oldReminderName,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    reminderName,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            TestCase("# Testcase 3: Duplicate keys across types ...");

            var duplicateKey = "Dupe";

            {
                ++sequenceNumber;

                string keyPrefix = duplicateKey;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    sequenceNumber);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            {
                ++sequenceNumber;

                string key = duplicateKey;
                TimeSpan timestamp = TimeSpan.FromSeconds(sequenceNumber);

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateTimeStamp(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                    timestamp);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    timestamp,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    timestamp,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            {
                ++sequenceNumber;

                string key = duplicateKey;
                var reminderName = "Rem-Name3";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateReminder(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder],
                    reminderName);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    reminderName,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    reminderName,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            TestCase("# Testcase 4: Enumeration by type ...");

            var baseCountPerType = 2;

            int baseCountActorType = baseCountPerType;
            foreach (string keyPrefix in new[] {"x", "y"})
            {
                ++sequenceNumber;
                ++baseCountActorType;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    sequenceNumber);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    sequenceNumber,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            VerifyStateTableSnapshot(
                stateTable,
                statesPerReplication,
                stateTable.GetShallowCopiesEnumerator(VolatileActorStateProvider.ActorStateType.Actor),
                sequenceNumber,
                sequenceNumber,
                baseCountActorType * statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

            {
                ++sequenceNumber;

                var key = "MyTimestamp";
                TimeSpan timestamp = TimeSpan.FromSeconds(sequenceNumber);

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateTimeStamp(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                    timestamp);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    timestamp,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    timestamp,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            VerifyStateTableSnapshot(
                stateTable,
                statesPerReplication,
                stateTable.GetShallowCopiesEnumerator(VolatileActorStateProvider.ActorStateType.LogicalTimestamp),
                sequenceNumber,
                sequenceNumber,
                baseCountPerType + 1);

            VerifyStateTableSnapshot(
                stateTable,
                statesPerReplication,
                stateTable.GetShallowCopiesEnumerator(VolatileActorStateProvider.ActorStateType.Reminder),
                sequenceNumber,
                sequenceNumber,
                baseCountPerType);

            TestCase("# Passed");
        }

        internal void TestDeleteInternal(Dictionary<VolatileActorStateProvider.ActorStateType, int> statesPerReplication)
        {
            TestCase("##################");
            TestCase("### TestDelete ###");
            TestCase("##################");

            TestCase(
                "### StatesPerReplication (Actor:{0}, TimeStamp:{1}, Reminder:{2}) ###",
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder]);

            long sequenceNumber = 0;
            long committedEntriesCount = 0;
            long uncommittedEntriesCount = 0;

            var stateTable = new ActorStateTable();
            VerifyStateTableSnapshot(stateTable, statesPerReplication, long.MaxValue, 0, 0, 0);

            var actorReplicationUnitDict = new Dictionary<string, ReplicationUnit>();
            var timeStampReplicationUnitDict = new Dictionary<string, ReplicationUnit>();
            var reminderReplicationUnitDict = new Dictionary<string, ReplicationUnit>();

            TestCase("# Testcase 1: Single create/delete ...");

            {
                ++sequenceNumber;

                var keyPrefix = "x";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    sequenceNumber);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    1,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                ++sequenceNumber;

                replicationUnit = ReplicationUnit.CreateForDeleteActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    1,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor] - 1;

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                ++sequenceNumber;

                replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    1);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor] - 1;

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    1,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }

            TestCase("# Testcase 2: Multiple create/delete ...");

            foreach (string keyPrefix in new[] {"a", "b", "c"})
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    1);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    1,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }

            var firstIteration = true;
            foreach (string keyPrefix in new[] {"a", "b", "c"})
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForDeleteActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    1,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                if (firstIteration)
                {
                    firstIteration = false;
                    committedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor] - 1;
                }
                else
                {
                    committedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                }

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }

            TestCase("# Testcase 3: Interleaved create/delete ...");

            foreach (string keyPrefix in new[] {"d", "e", "f"})
            {
                ++sequenceNumber;

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    1);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                ++sequenceNumber;

                replicationUnit = ReplicationUnit.CreateForDeleteActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 2,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber - 1);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor] - 1;

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    1,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor] - 1;

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }

            TestCase("# Testcase 4: Delete non-existent key ...");

            {
                ++sequenceNumber;

                var keyPrefix = "NotFound";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForDeleteActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }

            {
                ++sequenceNumber;

                var keyPrefix = "Exists";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor],
                    1);

                actorReplicationUnitDict[keyPrefix] = replicationUnit;

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor] - 1;

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    1,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }

            {
                ++sequenceNumber;

                var keyPrefix = "NotFound";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForDeleteActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber - 1,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount += 1;

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    0,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);

                VerifyReads(
                    stateTable,
                    actorReplicationUnitDict["Exists"],
                    statesPerReplication,
                    true,
                    1,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }

            TestCase("# Testcase 5: Delete same key different types ...");

            TimeSpan timestamp = TimeSpan.FromSeconds(42);
            var reminderName = "Reminder-Exists";

            {
                ++sequenceNumber;

                var key = "Exists";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateTimeStamp(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp],
                    timestamp);

                timeStampReplicationUnitDict[key] = replicationUnit;

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    timestamp,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp] - 1;

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    timestamp,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            {
                ++sequenceNumber;

                var key = "Exists";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForUpdateReminder(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder],
                    reminderName);

                reminderReplicationUnitDict[key] = replicationUnit;

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    false,
                    reminderName,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];
                committedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    reminderName,
                    sequenceNumber,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);
            }

            {
                ++sequenceNumber;

                var keyPrefix = "Exists";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForDeleteTimeStamp(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp]);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    timestamp,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp];
                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.LogicalTimestamp] - 1;

                TryReadAndVerify(stateTable, timeStampReplicationUnitDict[keyPrefix], false, timestamp);
                TryReadAndVerify(stateTable, reminderReplicationUnitDict[keyPrefix], true, reminderName);
                TryReadAndVerify(stateTable, actorReplicationUnitDict[keyPrefix], true, 1);

                VerifyStateTableSnapshot(
                    stateTable,
                    statesPerReplication,
                    long.MaxValue,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }

            {
                ++sequenceNumber;

                var key = "Exists";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForDeleteReminder(
                    sequenceNumber,
                    key,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder]);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    reminderName,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];
                committedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Reminder];

                TryReadAndVerify(stateTable, timeStampReplicationUnitDict[key], false, timestamp);
                TryReadAndVerify(stateTable, reminderReplicationUnitDict[key], false, reminderName);
                TryReadAndVerify(stateTable, actorReplicationUnitDict[key], true, 1);

                VerifyStateTableSnapshot(
                    stateTable,
                    statesPerReplication,
                    long.MaxValue,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }

            {
                ++sequenceNumber;

                var keyPrefix = "Exists";

                ReplicationUnit replicationUnit = ReplicationUnit.CreateForDeleteActor(
                    sequenceNumber,
                    keyPrefix,
                    statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor]);

                TestPrepareUpdate(stateTable, replicationUnit);

                uncommittedEntriesCount += statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                VerifyReads(
                    stateTable,
                    replicationUnit,
                    statesPerReplication,
                    true,
                    1,
                    sequenceNumber - 1,
                    sequenceNumber,
                    uncommittedEntriesCount + committedEntriesCount);

                TestCommitUpdate(stateTable, sequenceNumber);

                uncommittedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];
                committedEntriesCount -= statesPerReplication[VolatileActorStateProvider.ActorStateType.Actor];

                TryReadAndVerify(stateTable, timeStampReplicationUnitDict[keyPrefix], false, timestamp);
                TryReadAndVerify(stateTable, reminderReplicationUnitDict[keyPrefix], false, reminderName);
                TryReadAndVerify(stateTable, actorReplicationUnitDict[keyPrefix], false, 1);

                VerifyStateTableSnapshot(
                    stateTable,
                    statesPerReplication,
                    long.MaxValue,
                    sequenceNumber,
                    sequenceNumber,
                    committedEntriesCount + uncommittedEntriesCount);
            }
        }
    }
}