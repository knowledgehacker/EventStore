using System;
using EventStore.Core.Data;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.LogRecords;
using NUnit.Framework;

namespace EventStore.Core.Tests.TransactionLog
{
    [TestFixture]
    public class when_reading_a_single_record : SpecificationWithDirectoryPerTestFixture
    {
        private const int RecordsCount = 8;

        private TFChunkDb _db;
        private LogRecord[] _records;
        private RecordWriteResult[] _results;

        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            _db = new TFChunkDb(TFChunkDbConfigHelper.Create(PathName, 0));
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
        public void all_records_can_be_read()
        {
            var reader = GetTFChunkReader(0);

            RecordReadResult res;
            for(var i = 0; i < RecordsCount; i++)
            {
                var rec = _records[i];
                res = reader.TryReadAt(rec.LogPosition);

                Assert.IsTrue(res.Success);
                Assert.AreEqual(rec, res.LogRecord);
            }
        }

        [Test]
        public void only_records_before_checkpoint_can_be_read()
        {
            var expectedCount = 5;
            _db.Config.ReplicationCheckpoint.Write(_records[expectedCount - 1].LogPosition);

            var reader = GetTFChunkReader(0);

            RecordReadResult res;
            for(var i = 0; i < RecordsCount; i++)
            {
                var rec = _records[i];
                res = reader.TryReadAt(rec.LogPosition);

                if(i > expectedCount - 1)
                {
                    Assert.IsFalse(res.Success);
                    continue;
                }

                Assert.IsTrue(res.Success);
                Assert.AreEqual(rec, res.LogRecord);
            }
        }
    }
}