using System;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using NUnit.Framework;

namespace EventStore.Core.Tests.TransactionLog
{
    [TestFixture]
    public class when_sequentially_reading_a_db_with_replication_checkpoint: SpecificationWithDirectoryPerTestFixture
    {
        private const int RecordsCount = 8;
        private const int ExpectedCount = 5;

        private TFChunkDb _db;
        private LogRecord[] _records;
        private RecordWriteResult[] _results;

        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            _db = new TFChunkDb(new TFChunkDbConfig(PathName,
                                                    new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                                                    4096,
                                                    0,
                                                    new InMemoryCheckpoint(),
                                                    new InMemoryCheckpoint(),
                                                    new InMemoryCheckpoint(-1),
                                                    new InMemoryCheckpoint(-1),
                                                    new InMemoryCheckpoint(-1)));
            _db.Open();

            var chunk = _db.Manager.GetChunk(0);

            _records = new LogRecord[RecordsCount];
            _results = new RecordWriteResult[RecordsCount];

            var pos = 0;
            for (int i = 0; i < RecordsCount; ++i)
            {
                if (i > 0 && i % 3 == 0)
                {
                    pos = i/3 * _db.Config.ChunkSize;
                    chunk.Complete();
                    chunk = _db.Manager.AddNewChunk();
                }

                _records[i] = LogRecord.SingleWrite(pos,
                                                    Guid.NewGuid(), Guid.NewGuid(), "es1", ExpectedVersion.Any, "et1",
                                                    new byte[1200], new byte[] { 5, 7 });
                _results[i] = chunk.TryAppend(_records[i]);

                pos += _records[i].GetSizeWithLengthPrefixAndSuffix();
            }

            chunk.Flush();
            _db.Config.WriterCheckpoint.Write((RecordsCount / 3) * _db.Config.ChunkSize + _results[RecordsCount - 1].NewPosition);
            _db.Config.WriterCheckpoint.Flush();

            _db.Config.ReplicationCheckpoint.Write(_records[ExpectedCount - 1].LogPosition);
            Console.WriteLine("Written replication checkpoint at {0}", _records[4].LogPosition);
        }

        public override void TestFixtureTearDown()
        {
            _db.Dispose();

            base.TestFixtureTearDown();
        }

        private TFChunkReader GetTFChunkReader(long from)
        {
            return new TFChunkReader(_db, _db.Config.WriterCheckpoint, _db.Config.ReplicationCheckpoint, from);
        }

        [Test]
        public void all_records_were_written()
        {
            var pos = 0;
            for (int i = 0; i < RecordsCount; ++i)
            {
                if (i % 3 == 0)
                    pos = 0;

                Assert.IsTrue(_results[i].Success);
                Assert.AreEqual(pos, _results[i].OldPosition);

                pos += _records[i].GetSizeWithLengthPrefixAndSuffix();
                Assert.AreEqual(pos, _results[i].NewPosition);
            }
        }

        [Test]
        public void only_records_before_checkpoint_can_be_read_with_forward_pass()
        {
            var seqReader = GetTFChunkReader(0);

            SeqReadResult res;
            int count = 0;
            while ((res = seqReader.TryReadNext()).Success)
            {
                var rec = _records[count];
                Assert.AreEqual(rec, res.LogRecord);
                Assert.AreEqual(rec.LogPosition, res.RecordPrePosition);
                Assert.AreEqual(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

                ++count;
            }
            Assert.AreEqual(ExpectedCount, count);
        }

        [Test]
        public void only_records_before_checkpoint_can_be_read_with_backward_pass()
        {
            var seqReader = GetTFChunkReader(_db.Config.WriterCheckpoint.Read());

            SeqReadResult res;
            int count = 0;
            for(var i = RecordsCount - 1; i >= 0; i--)
            {
                res = seqReader.TryReadPrev();
                if(i > ExpectedCount - 1) {
                    Assert.IsFalse(res.Success);
                    continue;
                }
                var rec = _records[i];
                Assert.AreEqual(rec, res.LogRecord);
                Assert.AreEqual(rec.LogPosition, res.RecordPrePosition);
                Assert.AreEqual(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

                ++count;
            }
            Assert.AreEqual(ExpectedCount, count);
        }

        [Test]
        public void only_records_before_checkpoint_can_be_read_doing_forward_backward_pass()
        {
            var seqReader = GetTFChunkReader(0);

            SeqReadResult res;
            int count1 = 0;
            while ((res = seqReader.TryReadNext()).Success)
            {
                var rec = _records[count1];
                Assert.AreEqual(rec, res.LogRecord);
                Assert.AreEqual(rec.LogPosition, res.RecordPrePosition);
                Assert.AreEqual(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

                ++count1;
            }
            Assert.AreEqual(ExpectedCount, count1);

            int count2 = 0;
            while ((res = seqReader.TryReadPrev()).Success)
            {
                var rec = _records[ExpectedCount - count2 - 1];
                Assert.AreEqual(rec, res.LogRecord);
                Assert.AreEqual(rec.LogPosition, res.RecordPrePosition);
                Assert.AreEqual(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

                ++count2;
            }
            Assert.AreEqual(ExpectedCount, count2);
        }

        [Test]
        public void records_before_checkpoint_can_be_read_forward_starting_from_any_position()
        {
            for (int i = 0; i < RecordsCount; ++i)
            {
                var seqReader = GetTFChunkReader(_records[i].LogPosition);

                SeqReadResult res;
                int count = 0;
                while ((res = seqReader.TryReadNext()).Success)
                {
                    var rec = _records[i + count];
                    Assert.AreEqual(rec, res.LogRecord);
                    Assert.AreEqual(rec.LogPosition, res.RecordPrePosition);
                    Assert.AreEqual(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

                    ++count;
                }
                Assert.AreEqual(Math.Max(ExpectedCount - i, 0), count);
            }
        }

        [Test]
        public void records_before_checkpoint_can_be_read_backward_starting_from_any_position()
        {
            for (int i = 0; i < RecordsCount; ++i)
            {
                var seqReader = GetTFChunkReader(_records[i].LogPosition);

                SeqReadResult res;
                int count = 0;
                while ((res = seqReader.TryReadPrev()).Success)
                {
                    var rec = _records[i - count - 1];
                    Assert.AreEqual(rec, res.LogRecord);
                    Assert.AreEqual(rec.LogPosition, res.RecordPrePosition);
                    Assert.AreEqual(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

                    ++count;
                }
                var assertCount = i > ExpectedCount ? 0 : i;
                Assert.AreEqual(assertCount, count);
            }
        }
    }
}