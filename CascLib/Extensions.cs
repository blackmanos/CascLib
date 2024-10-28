using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Linq;
using System.Security.Cryptography;

namespace CASCLib
{
    public static class Extensions
    {
        public static int ReadInt32BE(this BinaryReader reader)
        {
            int val = reader.ReadInt32();
            int ret = (val >> 24 & 0xFF) << 0;
            ret |= (val >> 16 & 0xFF) << 8;
            ret |= (val >> 8 & 0xFF) << 16;
            ret |= (val >> 0 & 0xFF) << 24;
            return ret;
        }

        public static long ReadInt40BE(this BinaryReader reader)
        {
            byte[] val = reader.ReadBytes(5);
            return val[4] | val[3] << 8 | val[2] << 16 | val[1] << 24 | val[0] << 32;
        }

        public static void Skip(this BinaryReader reader, int bytes)
        {
            reader.BaseStream.Position += bytes;
        }

        public static ushort ReadUInt16BE(this BinaryReader reader)
        {
            byte[] val = reader.ReadBytes(2);
            return (ushort)(val[1] | val[0] << 8);
        }

        public static uint ReadUInt32BE(this BinaryReader reader)
        {
            byte[] val = reader.ReadBytes(4);
            return (uint)(val[3] | val[2] << 8 | val[1] << 16 | val[0] << 24);
        }

        public static ulong ReadUInt40BE(this BinaryReader reader)
        {
            byte[] array = new byte[8];
            for (int i = 0; i < 5; i++)
                array[4 - i] = reader.ReadByte();

            return BitConverter.ToUInt64(array, 0);
        }

        public static void WriteUInt40BE(this BinaryWriter writer, ulong v)
        {
            byte[] bytes = BitConverter.GetBytes(v);
            for (int i = 3; i < bytes.Length; i++)
                writer.Write(bytes[bytes.Length - i - 1]);
        }

        public static Action<T, V> GetSetter<T, V>(this FieldInfo fieldInfo)
        {
            var paramExpression = Expression.Parameter(typeof(T));
            var fieldExpression = Expression.Field(paramExpression, fieldInfo);
            var valueExpression = Expression.Parameter(fieldInfo.FieldType);
            var assignExpression = Expression.Assign(fieldExpression, valueExpression);

            return Expression.Lambda<Action<T, V>>(assignExpression, paramExpression, valueExpression).Compile();
        }

        public static Func<T, V> GetGetter<T, V>(this FieldInfo fieldInfo)
        {
            var paramExpression = Expression.Parameter(typeof(T));
            var fieldExpression = Expression.Field(paramExpression, fieldInfo);

            return Expression.Lambda<Func<T, V>>(fieldExpression, paramExpression).Compile();
        }

        public static T Read<T>(this BinaryReader reader) where T : unmanaged
        {
            byte[] result = reader.ReadBytes(Unsafe.SizeOf<T>());

            return Unsafe.ReadUnaligned<T>(ref result[0]);
        }

        public static T[] ReadArray<T>(this BinaryReader reader) where T : unmanaged
        {
            int numBytes = (int)reader.ReadInt64();

            byte[] source = reader.ReadBytes(numBytes);

            if (source.Length != numBytes)
                throw new Exception("source.Length != numBytes");

            reader.BaseStream.Position += (0 - numBytes) & 0x07;

            return source.CopyTo<T>();
        }

        public static T[] ReadArray<T>(this BinaryReader reader, int size) where T : unmanaged
        {
            int numBytes = Unsafe.SizeOf<T>() * size;

            byte[] source = reader.ReadBytes(numBytes);

            if (source.Length != numBytes)
                throw new Exception("source.Length != numBytes");

            return source.CopyTo<T>();
        }

        public static unsafe T[] CopyTo<T>(this byte[] src) where T : unmanaged
        {
            //T[] result = new T[src.Length / Unsafe.SizeOf<T>()];

            //if (src.Length > 0)
            //    Unsafe.CopyBlockUnaligned(Unsafe.AsPointer(ref result[0]), Unsafe.AsPointer(ref src[0]), (uint)src.Length);

            //return result;

            Span<T> result = MemoryMarshal.Cast<byte, T>(src);
            return result.ToArray();
        }

        public static short ReadInt16BE(this BinaryReader reader)
        {
            byte[] val = reader.ReadBytes(2);
            return (short)(val[1] | val[0] << 8);
        }

        public static void CopyBytes(this Stream input, Stream output, int bytes)
        {
            byte[] buffer = new byte[0x1000];
            int read;
            while (bytes > 0 && (read = input.Read(buffer, 0, Math.Min(buffer.Length, bytes))) > 0)
            {
                output.Write(buffer, 0, read);
                bytes -= read;
            }
        }

        public static void CopyBytesFromPos(this Stream input, Stream output, int offset, int bytes)
        {
            byte[] buffer = new byte[0x1000];
            int read;
            int pos = 0;
            while (pos < offset && (read = input.Read(buffer, 0, Math.Min(buffer.Length, offset - pos))) > 0)
            {
                pos += read;
            }
            while (bytes > 0 && (read = input.Read(buffer, 0, Math.Min(buffer.Length, bytes))) > 0)
            {
                output.Write(buffer, 0, read);
                bytes -= read;
            }
        }

        public static void CopyToStream(this Stream src, Stream dst, long len, BackgroundWorkerEx progressReporter = null)
        {
            long done = 0;

#if NET6_0_OR_GREATER
            Span<byte> buf = stackalloc byte[0x1000];
#else
            byte[] buf = new byte[0x1000];
#endif
            int count;
            do
            {
                if (progressReporter != null && progressReporter.CancellationPending)
                    return;
#if NET6_0_OR_GREATER
                count = src.Read(buf);
                dst.Write(buf.Slice(0, count));
#else
                count = src.Read(buf, 0, buf.Length);
                dst.Write(buf, 0, count);
#endif
                done += count;

                progressReporter?.ReportProgress((int)(done / (float)len * 100));
            } while (count > 0);
        }

        public static void ExtractToFile(this Stream input, string path, string name)
        {
            string fullPath = Path.Combine(path, name);
            string dir = Path.GetDirectoryName(fullPath);

            DirectoryInfo dirInfo = new DirectoryInfo(dir);
            if (!dirInfo.Exists)
                dirInfo.Create();

            using (var fileStream = File.Open(fullPath, FileMode.Create))
            {
                input.Position = 0;
                input.CopyTo(fileStream);
            }
        }

        public static void ExtractToData(this Stream ms, string path, in MD5Hash eKey, LocalIndexHandler LocalIndex)
        {
            // Console.WriteLine($"ExtractToData eKey {eKey.ToHexString()} path {path}");

            byte[] hash = eKey.ToHexString().FromHexString();
            string filename = GetDataFile(ms.Length + 30, path);
            Directory.CreateDirectory(path);

            IndexEntry info = new IndexEntry();
            info.Key = eKey.Take(9).ToArray();
            info.Index = int.Parse(Path.GetExtension(filename).TrimStart('.'));
            info.Size = (int)ms.Length + 30;

            // Console.WriteLine($"ExtractToData eKey {eKey.ToHexString()} info.Key {info.Key.ToHexString()} filename {filename}");

			using (MemoryStream msb = new MemoryStream())
			using (BinaryWriter bw = new BinaryWriter(msb, Encoding.ASCII))
            using (FileStream fs = new FileStream(Path.Combine(path, filename), FileMode.OpenOrCreate, FileAccess.ReadWrite))
            {
                fs.Seek(0, SeekOrigin.End);
                info.Offset = (int)fs.Position;

				bw.Write(hash.Reverse().ToArray()); // MD5 hash
				bw.Write((uint)ms.Length + 30); // Size
				bw.Write(new byte[0xA]); // Unknown

                ms.Position = 0;
                msb.Position = 0;
                msb.CopyTo(fs);
                ms.CopyTo(fs);
                fs.Flush();
            }
            // Console.WriteLine($"ExtractToData LocalIndex hash {hash.ToHexString()} info.Key {info.Key.ToHexString()} filename {filename} Size {info.Size}");
            LocalIndex.AddEntry(info);
            ms.Position = 0;
        }

        public static void RestoreToData(this Stream ms, string filename, in MD5Hash eKey, IndexEntry info)
        {
            // Console.WriteLine($"ExtractToData eKey {eKey.ToHexString()} filename {filename}");

            byte[] hash = eKey.ToHexString().FromHexString();

            // Console.WriteLine($"ExtractToData eKey {eKey.ToHexString()} info.Key {info.Key.ToHexString()} filename {filename}");

			using (MemoryStream msb = new MemoryStream())
			using (BinaryWriter bw = new BinaryWriter(msb, Encoding.ASCII))
            using (FileStream fs = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite))
            {
                fs.Position = info.Offset;

				bw.Write(hash.Reverse().ToArray()); // MD5 hash
				bw.Write((uint)ms.Length + 30); // Size
				bw.Write(new byte[0xA]); // Unknown

                ms.Position = 0;
                msb.Position = 0;
                msb.CopyTo(fs);
                ms.CopyTo(fs);
                fs.Flush();
            }
            // Console.WriteLine($"ExtractToData LocalIndex hash {hash.ToHexString()} info.Key {info.Key.ToHexString()} filename {filename} Size {info.Size}");
            ms.Position = 0;
        }

		private static string GetDataFile(long bytes, string path)
		{
			string pathData = Path.Combine(path, "Data", "data");
            string prevDataFile = Path.Combine(pathData, "data.000");

            var files = Directory.EnumerateFiles(pathData, "data.*");
            if (files.Any())
                prevDataFile = files.OrderByDescending(x => x).First();

            if (!File.Exists(prevDataFile))
				return prevDataFile;

			long remaining = (0x40000000L - new FileInfo(prevDataFile).Length);

			if (remaining > bytes) // < 1GB space check
			{
				return prevDataFile;
			}
			else
			{
				int ext = int.Parse(Path.GetExtension(prevDataFile).TrimStart('.')) + 1;
				return Path.Combine(pathData, "data." + ext.ToString("D3")); // make a new .data file
			}
		}

        public static string ToHexString(this byte[] data)
        {
#if NET6_0_OR_GREATER
            return Convert.ToHexString(data);
#else
            if (data == null)
                throw new ArgumentNullException(nameof(data));
            if (data.Length == 0)
                return string.Empty;
            if (data.Length > int.MaxValue / 2)
                throw new ArgumentOutOfRangeException(nameof(data), "SR.ArgumentOutOfRange_InputTooLarge");
            return HexConverter.ToString(data, HexConverter.Casing.Upper);
#endif
        }

        public static bool EqualsTo(this in MD5Hash key, byte[] array)
        {
            if (array.Length != 16)
                return false;

            ref MD5Hash other = ref Unsafe.As<byte, MD5Hash>(ref array[0]);

            if (key.lowPart != other.lowPart || key.highPart != other.highPart)
                return false;

            return true;
        }

        public static bool EqualsTo9(this in MD5Hash key, byte[] array)
        {
            if (array.Length != 16)
                return false;

            ref MD5Hash other = ref Unsafe.As<byte, MD5Hash>(ref array[0]);

            return EqualsTo9(key, other);
        }

        public static bool EqualsTo9(this in MD5Hash key, in MD5Hash other)
        {
            if (key.lowPart != other.lowPart)
                return false;

            if ((key.highPart & 0xFF) != (other.highPart & 0xFF))
                return false;

            return true;
        }

        public static bool EqualsTo(this in MD5Hash key, in MD5Hash other)
        {
            return key.lowPart == other.lowPart && key.highPart == other.highPart;
        }

        public static byte[] Take(this in MD5Hash key, int counter)
        {
            var keyBytes = key.ToHexString().FromHexString();
            Array.Resize(ref keyBytes, counter);
            return keyBytes;
        }

        public static unsafe string ToHexString(this in MD5Hash key)
        {
#if NET6_0
            ref MD5Hash md5ref = ref Unsafe.AsRef(in key);
            var md5Span = MemoryMarshal.CreateReadOnlySpan(ref md5ref, 1);
            var span = MemoryMarshal.AsBytes(md5Span);
            return Convert.ToHexString(span);
#elif NET7_0_OR_GREATER
            var md5Span = new ReadOnlySpan<MD5Hash>(in key);
            var span = MemoryMarshal.AsBytes(md5Span);
            return Convert.ToHexString(span);
#else
            byte[] array = new byte[16];
            fixed (byte* aptr = array)
            {
                *(MD5Hash*)aptr = key;
            }
            return array.ToHexString();
#endif
        }

        public static MD5Hash ToMD5(this byte[] array)
        {
            if (array.Length != 16)
                throw new ArgumentException("array size != 16", nameof(array));

            return Unsafe.As<byte, MD5Hash>(ref array[0]);
        }

        public static byte GetBucket(this byte[] key)
        {
            byte a = key.Aggregate((x, y) => (byte)(x ^ y));
            return (byte)((a & 0xf) ^ (a >> 4));
        }

        public static MD5Hash ToMD5(this Span<byte> array)
        {
            if (array.Length != 16)
                throw new ArgumentException("array size != 16", nameof(array));

            return Unsafe.As<byte, MD5Hash>(ref array[0]);
        }

        public static byte[] ToByteArray(this string hex, int count = 32)
        {
            Func<char, int> CharToHex = (h) => h - (h < 0x3A ? 0x30 : 0x57);

            count = Math.Min(hex.Length / 2, count);

            var arr = new byte[count];
            for (var i = 0; i < count; i++)
                arr[i] = (byte)((CharToHex(hex[i << 1]) << 4) + CharToHex(hex[(i << 1) + 1]));

            return arr;
        }

        public static string ToMD5String(this byte[] bytes) => BitConverter.ToString(bytes).Replace("-", "").ToLowerInvariant();
    }

    public static class CStringExtensions
    {
        /// <summary> Reads the NULL terminated string from 
        /// the current stream and advances the current position of the stream by string length + 1.
        /// <seealso cref="BinaryReader.ReadString"/>
        /// </summary>
        public static string ReadCString(this BinaryReader reader)
        {
            return reader.ReadCString(Encoding.UTF8);
        }

        /// <summary> Reads the NULL terminated string from 
        /// the current stream and advances the current position of the stream by string length + 1.
        /// <seealso cref="BinaryReader.ReadString"/>
        /// </summary>
        public static string ReadCString(this BinaryReader reader, Encoding encoding)
        {
            var bytes = new List<byte>();
            byte b;
            while ((b = reader.ReadByte()) != 0)
                bytes.Add(b);
            return encoding.GetString(bytes.ToArray());
        }

        public static void WriteCString(this BinaryWriter writer, string str)
        {
            var bytes = Encoding.UTF8.GetBytes(str);
            writer.Write(bytes);
            writer.Write((byte)0);
        }

        public static byte[] FromHexString(this string str)
        {
#if NET6_0_OR_GREATER
            return Convert.FromHexString(str);
#else
            if (str == null)
                throw new ArgumentNullException(nameof(str));
            if (str.Length == 0)
                return Array.Empty<byte>();
            if ((uint)str.Length % 2 != 0)
                throw new FormatException("SR.Format_BadHexLength");

            byte[] result = new byte[str.Length >> 1];

            if (!HexConverter.TryDecodeFromUtf16(str, result))
                throw new FormatException("SR.Format_BadHexChar");

            return result;
#endif
        }
    }

    class HashComparer : IComparer<byte[]>, IComparer<IndexEntry>, IComparer<MD5Hash>, IComparer<string>
    {
        public int Compare(MD5Hash x, MD5Hash y) => Compare(x.ToHexString(), y.ToHexString());
        public int Compare(IndexEntry x, IndexEntry y) => Compare(x.Key, y.Key);
        public int Compare(string x, string y) => Compare(x.ToByteArray(), y.ToByteArray());

        public int Compare(byte[] x, byte[] y)
        {
            if (x == y)
                return 0;

            int length = Math.Min(x.Length, y.Length);
            for (int i = 0; i < length; i++)
            {
                int c = x[i].CompareTo(y[i]);
                if (c != 0)
                    return c;
            }

            return 0;
        }
    }
}
