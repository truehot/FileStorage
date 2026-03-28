namespace FileStorage.Application.Validator
{
    internal static class PathValidator
    {
        public static void Validate(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("File path cannot be empty.", nameof(filePath));

            try
            {
                _ = Path.GetFullPath(filePath);
            }
            catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
            {
                throw new ArgumentException("File path is invalid.", nameof(filePath), ex);
            }

            string? directoryPath = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directoryPath) && directoryPath.IndexOfAny(Path.GetInvalidPathChars()) >= 0)
                throw new ArgumentException("File path contains invalid directory characters.", nameof(filePath));

            string fileName = Path.GetFileName(filePath);
            if (string.IsNullOrWhiteSpace(fileName))
                throw new ArgumentException("File path must include a file name.", nameof(filePath));

            if (fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                throw new ArgumentException("File name contains invalid characters.", nameof(filePath));

            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
            if (string.IsNullOrWhiteSpace(fileNameWithoutExtension))
                throw new ArgumentException("File path must include a valid file name.", nameof(filePath));

            if (IsReservedWindowsName(fileNameWithoutExtension))
                throw new ArgumentException(
                    $"File name '{fileNameWithoutExtension}' is a reserved system name.",
                    nameof(filePath));
        }

        private static bool IsReservedWindowsName(string name)
        {
            string normalized = name.TrimEnd(' ', '.').ToUpperInvariant();

            string[] reserved =
            [
                "CON", "PRN", "AUX", "NUL",
                "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
                "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
            ];

            return reserved.Contains(normalized);
        }
    }
}
