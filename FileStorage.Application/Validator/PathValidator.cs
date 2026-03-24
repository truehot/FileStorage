namespace FileStorage.Application.Validator
{
    internal static class PathValidator
    {
        public static void Validate(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("File path cannot be empty.");

            char[] invalidChars = Path.GetInvalidPathChars();
            if (filePath.IndexOfAny(invalidChars) >= 0)
                throw new ArgumentException("File path contains invalid characters.");

            string fileName = Path.GetFileNameWithoutExtension(filePath);
            if (IsReservedWindowsName(fileName))
                throw new ArgumentException($"File name '{fileName}' is a reserved system name.");
        }

        private static bool IsReservedWindowsName(string name)
        {
            string[] reserved = { "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4",
                             "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2",
                             "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9" };
            return reserved.Contains(name.ToUpperInvariant());
        }
    }
}
