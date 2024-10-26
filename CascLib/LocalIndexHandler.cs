using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace CASCLib
{
    public class LocalIndexHandler
    {
        private Dictionary<MD5Hash, IndexEntry> LocalIndexData = new Dictionary<MD5Hash, IndexEntry>(MD5HashComparer9.Instance);
        private Dictionary<byte, uint> BucketIndexVersion = new Dictionary<byte, uint>();
        public List<LocalIndexHeader> LocalIndices = new List<LocalIndexHeader>();
        public ShmemHeader LocalShmems = new ShmemHeader();

        public int Count => LocalIndexData.Count;

        private LocalIndexHandler()
        {

        }

        public static LocalIndexHandler Initialize(CASCConfig config, BackgroundWorkerEx worker)
        {
            var handler = new LocalIndexHandler();

            handler.ParseShmem(config);

            var idxFiles = handler.GetIdxFiles(config);

            worker?.ReportProgress(0, "Loading \"local indexes\"...");

            int idxIndex = 0;

            foreach (var idx in idxFiles)
            {
                handler.ParseIndex(idx);

                worker?.ReportProgress((int)(++idxIndex / (float)idxFiles.Count * 100));
            }

            Logger.WriteLine("LocalIndexHandler: loaded {0} indexes", handler.Count);

            return handler;
        }

        private void ParseShmem(CASCConfig config)
        {
            string dataFolder = CASCGame.GetDataFolder(config.GameType);
            string dataPath = Path.Combine(dataFolder, "data");
            string BasePath = config.BasePath == null ? AppContext.BaseDirectory : config.BasePath;
            var fileShmem = Path.Combine(Path.Combine(BasePath, dataPath), "shmem");

            byte idxFileCount = 16;

            using (var fs = new FileStream(fileShmem, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var br = new BinaryReader(fs))
            {
                // header
                LocalShmems.BlockType = br.ReadUInt32();
                LocalShmems.NextBlock = br.ReadUInt32();
                LocalShmems.DataPath = br.ReadChars(0x100);

                uint numBlocks = (uint)(LocalShmems.NextBlock - 264 - idxFileCount * 4) / 8;

                // Console.WriteLine($"ParseShmem BlockType {LocalShmems.BlockType} NextBlock {LocalShmems.NextBlock} Position {br.BaseStream.Position} DataPath {new string(LocalShmems.DataPath)} numBlocks {numBlocks}");

                // entries
                for (uint i = 0; i < numBlocks; i++)
                {
                    ShmemEntry info = new ShmemEntry()
                    {
                        Size = br.ReadUInt32(),
                        Offset = br.ReadUInt32()
                    };

                    // Console.WriteLine($"ParseShmem Size {info.Size} Offset {info.Offset}");

                    LocalShmems.Entries.Add(info);
                }

                // Console.WriteLine($"ParseShmem Version Position {br.BaseStream.Position}");

                for (byte i = 0; i < idxFileCount; i++)
                {
                    var version = br.ReadUInt32();
                    BucketIndexVersion.Add(i, version);
                    Console.WriteLine($"ParseShmem idx {i} Version {version}");
                }

                if (LocalShmems.BlockType == 5)
                    return;

                // Console.WriteLine($"ParseShmem Structure Position {br.BaseStream.Position}");

                var Size = br.ReadUInt32();
                var CountUseBlock = br.ReadUInt32();
                br.ReadBytes(0x18); // Padding

                // Console.WriteLine($"ParseShmem DataBlock1 Size {Size} CountUseBlock {CountUseBlock} Position {br.BaseStream.Position}");

                for (uint i = 1; i <= 1090; i++)
                {
                    var DataNumber = br.ReadByte();
                    var Count = br.ReadUInt32();
                    // Console.WriteLine($"ParseShmem 0 DataNumber{i} {DataNumber} Count {Count}");
                }

                // Console.WriteLine($"ParseShmem DataBlock2 Size {Size} CountUseBlock {CountUseBlock} Position {br.BaseStream.Position}");

                for (uint i = 1; i <= 1090; i++)
                {
                    var DataNumber = br.ReadByte();
                    var Offset = br.ReadUInt32();
                    // Console.WriteLine($"ParseShmem 1 DataNumber{i} {DataNumber} Offset {Offset}");
                }

                var unk0 = br.ReadUInt32();
                var unk1 = br.ReadUInt32();
                var unk2 = br.ReadUInt32();

                if (fs.Position != fs.Length)
                   throw new Exception($"idx file under read fs.Position {fs.Position} fs.Length {fs.Length}");
            }
        }

        private void ParseIndex(string idx)
        {
            using (var fs = new FileStream(idx, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var br = new BinaryReader(fs))
            {
                // header
                LocalIndexHeader index = new LocalIndexHeader()
                {
                    BaseFile = idx,

                    HeaderHashSize = br.ReadUInt32(),
                    HeaderHash = br.ReadUInt32(),
                    _2 = br.ReadUInt16(),
                    BucketIndex = br.ReadByte(),
                    _4 = br.ReadByte(),
                    EntrySizeBytes = br.ReadByte(),
                    EntryOffsetBytes = br.ReadByte(),
                    EntryKeyBytes = br.ReadByte(),
                    ArchiveFileHeaderBytes = br.ReadByte(),
                    ArchiveTotalSizeMaximum = br.ReadUInt64(),
                    Padding = br.ReadBytes(8),
                    EntriesSize = br.ReadUInt32(),
                    EntriesHash = br.ReadUInt32()
                };

                Console.WriteLine($"ParseIndex HeaderHash {index.HeaderHash} BucketIndex {index.BucketIndex} EntriesSize {index.EntriesSize} EntriesHash {index.EntriesHash}");
                // Console.WriteLine($"ParseIndex EntrySizeBytes {index.EntrySizeBytes} EntryOffsetBytes {index.EntryOffsetBytes} EntryKeyBytes {index.EntryKeyBytes} ArchiveFileHeaderBytes {index.ArchiveFileHeaderBytes} ArchiveTotalSizeMaximum {index.ArchiveTotalSizeMaximum}");

                Jenkins96 hasher = new Jenkins96();
                br.BaseStream.Position = 0x28;
                byte[] entryhash = br.ReadBytes((int)index.EntriesSize);
                hasher.HashCore(entryhash);
                ulong hashValue = hasher.GetHashValue();

                Console.WriteLine($"ParseIndex entryhash new {(uint)(hashValue >> 32) & 0x00000000FFFFFFFF} {(uint)hashValue & 0x00000000FFFFFFFF} EntriesHash old {index.EntriesHash} entryhash.Length {entryhash.Length}");

                // test generation HeaderHash
                br.BaseStream.Position = 8;
                byte[] h2 = br.ReadBytes((int)index.HeaderHashSize);
                hasher.HashCore(h2);
                hashValue = hasher.GetHashValue();

                Console.WriteLine($"ParseIndex headerhash cheack {(uint)(hashValue >> 32) & 0x00000000FFFFFFFF} HeaderHash read {index.HeaderHash}");

                uint numBlocks = index.EntriesSize / 18;

                // entries
                br.BaseStream.Position = 0x28;
                for (uint i = 0; i < numBlocks; i++)
                {
                    IndexEntry info = new IndexEntry();
                    byte[] keyBytes = br.ReadBytes(9);
                    info.Key = keyBytes;
                    Array.Resize(ref keyBytes, 16);

                    MD5Hash key = keyBytes.ToMD5();

                    info.IndexOffset = br.ReadUInt40BE();
                    info.Size = br.ReadInt32();

                    // Logger.WriteLine($"ParseIndex key {info.Key.ToHexString()} Index {info.Index} Offset {info.Offset} Size {info.Size}");

                    if (!LocalIndexData.ContainsKey(key)) // use first key
                        LocalIndexData.Add(key, info);

                    index.Entries.Add(info);
                }

                LocalIndices.Add(index);
                //if (fs.Position != fs.Length)
                //    throw new Exception("idx file under read");
            }
        }

        public void AddEntry(IndexEntry info)
        {
            return;
            var idx = LocalIndices.First(x => x.BucketIndex == info.Key.GetBucket());
            var existing = idx.Entries.FirstOrDefault(x => x.Key.SequenceEqual(info.Key)); // check for existing

            if (existing != null)
                existing = info;
            else
                idx.Entries.Add(info);

            // Console.WriteLine($"AddEntry info.Key {info.Key.ToHexString()} BucketIndex {idx.BucketIndex}");
            idx.Changed = true;
            LocalShmems.Changed = true;
        }

        public void SaveIndex(string basePath, bool fastSave = false)
        {
            if (!fastSave)
                SaveShmem(basePath);

            // Console.WriteLine($"SaveIndex basePath {basePath}");
            foreach (var index in LocalIndices)
            {
                // if (!index.Changed)
                    // continue;

                // Console.WriteLine($"SaveIndex BaseFile {index.BaseFile} BucketIndex {index.BucketIndex}");

                Jenkins96 hasher = new Jenkins96();

                using (var ms = new MemoryStream())
                using (var bw = new BinaryWriter(ms))
                {
                    bw.Write(index.HeaderHashSize);
                    bw.Write((uint)0); // HeaderHash
                    bw.Write(index._2);
                    bw.Write(index.BucketIndex);
                    bw.Write(index._4);
                    bw.Write(index.EntrySizeBytes);
                    bw.Write(index.EntryOffsetBytes);
                    bw.Write(index.EntryKeyBytes);
                    bw.Write(index.ArchiveFileHeaderBytes);
                    bw.Write(index.ArchiveTotalSizeMaximum);
                    bw.Write(new byte[8]);
                    bw.Write((uint)index.Entries.Count * 18);
                    bw.Write((uint)0); // EntriesHash

                    // entries
                    index.Entries.Sort(new HashComparer());
                    foreach (var entry in index.Entries)
                    {
                        bw.Write(entry.Key);
                        bw.WriteUInt40BE(entry.IndexOffset);
                        bw.Write(entry.Size);
                        // Console.WriteLine($"SaveIndex key {entry.Key.ToHexString()} IndexOffset {entry.IndexOffset} Size {entry.Size}");
                    }

                    // update EntriesHash
                    bw.BaseStream.Position = 0x28;

                    byte[] entryhash = new byte[18];
                    bw.BaseStream.Read(entryhash, 0, entryhash.Length);
                    hasher.HashCore(entryhash);
                    ulong hashValue = hasher.GetHashValue();

                    Console.WriteLine($"SaveIndex entryhash new {(uint)(hashValue >> 32) & 0x00000000FFFFFFFF} EntriesHash old {index.EntriesHash}");

                    bw.BaseStream.Position = 0x24;
                    if (index.Entries.Count == 0)
                        bw.Write((uint)0);
                    else
                        bw.Write((uint)(hashValue >> 32) & 0x00000000FFFFFFFF);

                    // update HeaderHash
                    bw.BaseStream.Position = 8;
                    byte[] headerhash = new byte[index.HeaderHashSize];
                    bw.BaseStream.Read(headerhash, 0, headerhash.Length);
                    hasher.HashCore(headerhash);
                    hashValue = hasher.GetHashValue();

                    Console.WriteLine($"SaveIndex headerhash new {(uint)(hashValue >> 32) & 0x00000000FFFFFFFF} HeaderHash old {index.HeaderHash}");

                    bw.BaseStream.Position = 4;
                    bw.Write((uint)(hashValue >> 32) & 0x00000000FFFFFFFF);

                    // minimum file length constraint
                    var Length = (Convert.ToInt32((float)bw.BaseStream.Length / (float)0x10000) + 1) * 0x10000;
                    if (bw.BaseStream.Length < Length)
                        bw.BaseStream.SetLength(Length);

                    // save file to output
                    var bucket = index.BucketIndex.ToString("X2");
                    uint version = 1;
                    BucketIndexVersion.TryGetValue((byte)index.BucketIndex, out version);
                    string filename = bucket + version.ToString("X8") + ".idx";

                    Console.WriteLine($"SaveIndex bucket {bucket} version {version} filename {filename}");

                    var path = Path.Combine(Path.Combine(basePath, Path.Combine("Data", "data")), filename.ToLowerInvariant());

                    // Console.WriteLine($"SaveIndex path {path}");

                    File.Delete(path);
                    using (var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
                    {
                        fs.Seek(0, SeekOrigin.End);
                        ms.Position = 0;
                        ms.CopyTo(fs);
                        fs.Flush();
                    }

                    index.Changed = false;
                }
            }
        }

        public void SaveShmem(string basePath)
        {
            if (!LocalShmems.Changed)
                return;

            Console.WriteLine($"SaveShmem BlockType {LocalShmems.BlockType} NextBlock {LocalShmems.NextBlock}");

            using (var ms = new MemoryStream())
            using (var bw = new BinaryWriter(ms))
            {
                bw.Write(LocalShmems.BlockType);
                bw.Write(LocalShmems.NextBlock);
                bw.Write(LocalShmems.DataPath);

                // entries
                foreach (var entry in LocalShmems.Entries)
                {
                    bw.Write(entry.Size);
                    bw.Write(entry.Offset);
                }

                foreach (var version in BucketIndexVersion)
                {
                    Console.WriteLine($"SaveShmem version {version}");
                    bw.Write(version.Value);
                }

                var spaceList = GetEmptySpace();
                int CountUseBlock = spaceList.Count;

                bw.Write((uint)1); // Size
                bw.Write((uint)CountUseBlock); // CountUseBlock
                bw.Write(new byte[0x18]);

                foreach (var index in spaceList)
                {
                    bw.Write((byte)index.Archive);
                    bw.Write((uint)index.Size);
                }

                for (int i = CountUseBlock; i < 1090; i++)
                {
                    bw.Write((byte)0);
                    bw.Write((uint)0);
                }

                foreach (var index in spaceList)
                {
                    bw.Write((byte)index.Archive);
                    bw.Write((uint)index.Offset);
                }

                for (int i = CountUseBlock; i < 1090; i++)
                {
                    bw.Write((byte)0);
                    bw.Write((uint)0);
                }

                bw.Write((uint)0); // unk0
                bw.Write((uint)0); // unk1
                bw.Write((uint)0); // unk2

                var path = Path.Combine(Path.Combine(basePath, Path.Combine("Data", "data")), "shmem");

                // Console.WriteLine($"SaveShmem path {path}");

                File.Delete(path);
                using (var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
                {
                    fs.Seek(0, SeekOrigin.End);
                    ms.Position = 0;
                    ms.CopyTo(fs);
                    fs.Flush();
                }

                LocalShmems.Changed = false;
            }

            // Test code for BlockType = 5
            // using (var ms = new MemoryStream())
            // using (var bw = new BinaryWriter(ms))
            // {
                // LocalShmems.BlockType = 5;
                // LocalShmems.NextBlock = 4096;

                // bw.Write(LocalShmems.BlockType);
                // bw.Write(LocalShmems.NextBlock);
                // bw.Write(LocalShmems.DataPath);

                // // entries
                // foreach (var entry in LocalShmems.Entries)
                // {
                    // bw.Write(entry.Size);
                    // bw.Write(entry.Offset);
                // }

                // foreach (var version in BucketIndexVersion)
                // {
                    // Console.WriteLine($"SaveShmem version {version}");
                    // bw.Write(version.Value);
                // }

                // var spaceList = GetEmptySpace();
                // int CountUseBlock = spaceList.Count;

                // bw.Write((uint)1); // Size
                // bw.Write((uint)CountUseBlock); // CountUseBlock
                // bw.Write(new byte[0x18]);

                // foreach (var index in spaceList)
                // {
                    // bw.Write((byte)index.Archive);
                    // bw.Write((uint)index.Size);
                // }

                // for (int i = CountUseBlock; i < 1090; i++)
                // {
                    // bw.Write((byte)0);
                    // bw.Write((uint)0);
                // }

                // foreach (var index in spaceList)
                // {
                    // bw.Write((byte)index.Archive);
                    // bw.Write((uint)index.Offset);
                // }

                // for (int i = CountUseBlock; i < 1090; i++)
                // {
                    // bw.Write((byte)0);
                    // bw.Write((uint)0);
                // }

                // bw.Write((uint)0); // unk0
                // bw.Write((uint)0); // unk1
                // bw.Write((uint)0); // unk2

                // if (bw.BaseStream.Length < 0x4000)
                    // bw.BaseStream.SetLength(0x4000);

                // var path = Path.Combine(Path.Combine(basePath, Path.Combine("Data", "data")), "shmem");

                // // Console.WriteLine($"SaveShmem path {path}");

                // File.Delete(path);
                // using (var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
                // {
                    // fs.Seek(0, SeekOrigin.End);
                    // ms.Position = 0;
                    // ms.CopyTo(fs);
                    // fs.Flush();
                // }

                // LocalShmems.Changed = false;
            // }
        }

        private List<string> GetIdxFiles(CASCConfig config)
        {
            List<string> latestIdx = new List<string>();

            string dataFolder = CASCGame.GetDataFolder(config.GameType);
            string dataPath = Path.Combine(dataFolder, "data");
            string BasePath = config.BasePath == null ? AppContext.BaseDirectory : config.BasePath;

            for (byte i = 0; i < 0x10; i++)
            {
                uint version = 1;
                if (!BucketIndexVersion.TryGetValue(i, out version))
                {
                    BucketIndexVersion.Add(i, version);
                    CreateIdxFiles(i, BasePath);
                }

                var bucket = i.ToString("X2");
                string filename = bucket + version.ToString("X8") + ".idx";

                Console.WriteLine($"GetIdxFiles bucket {bucket} version {version} filename {filename}");

                var curFile = Path.Combine(Path.Combine(BasePath, dataPath), filename.ToLowerInvariant());
                if (!File.Exists(curFile))
                    CreateIdxFiles(i, BasePath);
                latestIdx.Add(curFile);
            }

            return latestIdx;
        }

        private void CreateIdxFiles(int i, string basePath)
        {
            List<string> latestIdx = new List<string>();

            var bucket = i.ToString("X2");
            string filename = bucket + i.ToString("X8") + ".idx";

            LocalIndexHeader index = new LocalIndexHeader()
            {
                BaseFile = filename,
                HeaderHash = 0,
                BucketIndex = (byte)i,
            };

            LocalIndices.Add(index);

            index.Changed = true;
            LocalShmems.Changed = true;
            SaveIndex(basePath, true);
        }

        public List<LocalIndexSpace> GetEmptySpace(ulong minsize = 0x100)
        {
			// implementation of shmem

            List<LocalIndexSpace> space = new List<LocalIndexSpace>();

            var datagroups = LocalIndices.SelectMany(x => x.Entries).GroupBy(x => x.Index).Select(x => x.OrderBy(y => y.Offset).ToList());
            foreach (var group in datagroups)
            {
                for (int i = 0; i < group.Count - 1; i++)
                {
                    var current = group[i];
                    ulong nextOffset = (ulong)group[i + 1].Offset;

                    if ((ulong)current.Offset + (ulong)current.Size + (ulong)minsize < nextOffset)
                    {
                        space.Add(new LocalIndexSpace()
                        {
                            Archive = current.Index,
                            Offset = current.Offset,
                            Size = (int)nextOffset - (int)(current.Offset - current.Size)
                        });
                    }
                }
            }

            return space;
        }

        public IndexEntry GetIndexInfo(in MD5Hash eKey)
        {
            if (!LocalIndexData.TryGetValue(eKey, out IndexEntry result))
                Logger.WriteLine("LocalIndexHandler: missing EKey: {0}", eKey.ToHexString());

            return result;
        }

        public void Clear()
        {
            LocalIndexData.Clear();
            LocalIndexData = null;
        }
    }
}
