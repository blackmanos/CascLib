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
        public List<LocalIndexHeader> LocalIndices = new List<LocalIndexHeader>();

        public int Count => LocalIndexData.Count;
        private const int CHUNK_SIZE = 0xC0000;

        private LocalIndexHandler()
        {

        }

        public static LocalIndexHandler Initialize(CASCConfig config, BackgroundWorkerEx worker)
        {
            var handler = new LocalIndexHandler();

            var idxFiles = GetIdxFiles(config);

            if (idxFiles.Count == 0)
                throw new FileNotFoundException("idx files are missing!");

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

                    // Console.WriteLine($"ParseIndex key {key.ToHexString()} IndexOffset {info.IndexOffset} Size {info.Size}");

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
            var idx = LocalIndices.First(x => x.BucketIndex == info.Key.GetBucket());
            var existing = idx.Entries.FirstOrDefault(x => x.Key.SequenceEqual(info.Key)); // check for existing

            if (existing != null)
                existing = info;
            else
                idx.Entries.Add(info);

            // Console.WriteLine($"AddEntry info.Key {info.Key.ToHexString()} BucketIndex {idx.BucketIndex}");
            idx.Changed = true;
        }

        public void SaveIndex(string basePath)
        {
            // Console.WriteLine($"SaveIndex basePath {basePath}");
            foreach (var index in LocalIndices)
            {
                if (!index.Changed)
                    continue;

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

                    // Console.WriteLine($"SaveIndex Entries {index.Entries.Count}");

                    // update EntriesHash
                    bw.BaseStream.Position = 0x28;

                    byte[] entryhash = new byte[18];
                    bw.BaseStream.Read(entryhash, 0, entryhash.Length);

                    // Console.WriteLine($"SaveIndex entryhash.Length {entryhash.Length}");

                    bw.BaseStream.Position = 0x24;
                    bw.Write(hasher.ComputeHash(entryhash));

                    // update HeaderHash
                    bw.BaseStream.Position = 8;
                    byte[] headerhash = new byte[index.HeaderHashSize];
                    bw.BaseStream.Read(headerhash, 0, headerhash.Length);

                    // Console.WriteLine($"SaveIndex headerhash.Length {headerhash.Length} HeaderHashSize {index.HeaderHashSize}");

                    bw.BaseStream.Position = 4;
                    bw.Write(hasher.ComputeHash(headerhash));

                    // minimum file length constraint
                    if (bw.BaseStream.Length < CHUNK_SIZE)
                        bw.BaseStream.SetLength(CHUNK_SIZE);

                    // save file to output
                    var bucket = index.BucketIndex.ToString("X2");
                    var version = long.Parse(Path.GetFileNameWithoutExtension(index.BaseFile).Substring(2), NumberStyles.HexNumber);
                    string filename = bucket + version.ToString("X8") + ".idx";

                    // Console.WriteLine($"SaveIndex bucket {bucket} version {version} filename {filename}");

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

        private static List<string> GetIdxFiles(CASCConfig config)
        {
            List<string> latestIdx = new List<string>();

            string dataFolder = CASCGame.GetDataFolder(config.GameType);
            string dataPath = Path.Combine(dataFolder, "data");
            string BasePath = config.BasePath == null ? AppContext.BaseDirectory : config.BasePath;

            for (int i = 0; i < 0x10; i++)
            {
                var files = Directory.EnumerateFiles(Path.Combine(BasePath, dataPath), string.Format($"{i:x2}*.idx"));

                if (files.Any())
                    latestIdx.Add(files.Last());
            }

            return latestIdx;
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
