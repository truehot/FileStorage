using System.Text;
using System.Text.RegularExpressions;

namespace FileStorage.Application.Validator
{
    internal static class TableValidator
    {
        public const int MaxTableNameBytes = 256;

        public static void Validate(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Table name cannot be empty.", nameof(name));

            if (name.Length > MaxTableNameBytes)
                throw new ArgumentException($"Table name is too long. Max {MaxTableNameBytes} bytes allowed.");

            int byteCount = Encoding.UTF8.GetByteCount(name);
            if (byteCount > MaxTableNameBytes)
                throw new ArgumentException($"Table name exceeds {MaxTableNameBytes} bytes when encoded in UTF8.");

            if (name.Any(char.IsControl))
                throw new ArgumentException("Table name contains invalid control characters.");
        }
    }
}
