﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace CASCLib
{
	public enum DownloadFlags : byte
	{
		None = 0,
		Plugin = 1,
		PluginData = 2,
	}

    public class DownloadEntry
    {
        public int Index;
        public MD5Hash EKey;
        public ulong FileSize;
        public byte Priority;
		public uint Checksum;
		public DownloadFlags[] Flags; // V2 only

        public IEnumerable<KeyValuePair<string, DownloadTag>> Tags;
    }

    public class DownloadTag
    {
        public short Type;
        public BitArray Bits;
    }

    public class DownloadHandler
    {
        public Dictionary<MD5Hash, DownloadEntry> DownloadData = new Dictionary<MD5Hash, DownloadEntry>(MD5HashComparer.Instance);
        private Dictionary<string, DownloadTag> Tags = new Dictionary<string, DownloadTag>();
        private Dictionary<byte, bool> PriorityData = new Dictionary<byte, bool>();

        public int Count => DownloadData.Count;

        public DownloadHandler(BinaryReader stream, BackgroundWorkerEx worker)
        {
            worker?.ReportProgress(0, "Loading \"download\"...");

            stream.Skip(2); // DL

            byte Version = stream.ReadByte();
            byte ChecksumSize = stream.ReadByte();
            byte HasChecksum = stream.ReadByte();
            byte NumFlags = 0;

            int numFiles = stream.ReadInt32BE();
            short numTags = stream.ReadInt16BE();
            int numMaskBytes = (numFiles + 7) / 8;

            if (Version >= 2)
                NumFlags = stream.ReadByte();

            for (int i = 0; i < numFiles; i++)
            {
                var entry = new DownloadEntry()
                {
                    Index = i,
                    EKey = stream.Read<MD5Hash>(),
                    FileSize = stream.ReadUInt40BE(),
                    Priority = stream.ReadByte() // 0 (file count 3345), 1 (file count 824864), 2 (file count 679997)
                };

                //byte[] unk = stream.ReadBytes(0xA);
                // stream.Skip(0xA);

                if (HasChecksum != 0)
                    entry.Checksum = stream.ReadUInt32BE();

                if (Version >= 2)
                    entry.Flags = (DownloadFlags[])(object)stream.ReadBytes(NumFlags);

                // Logger.WriteLine($"DownloadHandler EKey {entry.EKey.ToHexString()} FileSize {entry.FileSize} Priority {entry.Priority}");

                if (!DownloadData.ContainsKey(entry.EKey))
                    DownloadData.Add(entry.EKey, entry);

                worker?.ReportProgress((int)((i + 1) / (float)numFiles * 100));
            }

            for (int i = 0; i < numTags; i++)
            {
                DownloadTag tag = new DownloadTag();
                string name = stream.ReadCString();
                tag.Type = stream.ReadInt16BE();

                byte[] bits = stream.ReadBytes(numMaskBytes);

                for (int j = 0; j < numMaskBytes; j++)
                    bits[j] = (byte)((bits[j] * 0x0202020202 & 0x010884422010) % 1023);

                tag.Bits = new BitArray(bits);

                Tags.Add(name, tag);
            }
        }

        public void Dump()
        {
            foreach (var entry in DownloadData)
            {
                if (entry.Value.Tags == null)
                    entry.Value.Tags = Tags.Where(kv => kv.Value.Bits[entry.Value.Index]);

                Logger.WriteLine("{0} {1}", entry.Key.ToHexString(), string.Join(",", entry.Value.Tags.Select(tag => tag.Key)));
            }
        }

        public DownloadEntry GetEntry(in MD5Hash key)
        {
            DownloadData.TryGetValue(key, out DownloadEntry entry);

            if (entry != null && entry.Tags == null)
                entry.Tags = Tags.Where(kv => kv.Value.Bits[entry.Index]);

            return entry;
        }

        public void Clear()
        {
            Tags.Clear();
            Tags = null;
            DownloadData.Clear();
            DownloadData = null;
        }
    }
}
