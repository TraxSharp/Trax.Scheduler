namespace Trax.Scheduler.Tests.Meta.Infrastructure;

internal static class SourceText
{
    /// <summary>
    /// Blanks out comment and string-literal content so a keyword scan cannot match inside one.
    /// </summary>
    /// <remarks>
    /// A single left-to-right pass, not a sequence of regex replacements. Comments and string
    /// literals are mutually exclusive contexts and cannot be resolved independently: stripping
    /// <c>//</c> first ate the rest of any line holding a URL, the dangling quote then swallowed
    /// everything to the next quote in the file, and whole test files went invisible to the guards.
    ///
    /// <para>
    /// Output is the same length as the input, character for character, with newlines preserved.
    /// Delimiters are kept and only the content between them is blanked, so every offset and line
    /// number in the result still points at the same place in the original.
    /// </para>
    /// </remarks>
    public static string StripCommentsAndStrings(string source)
    {
        var output = source.ToCharArray();
        var i = 0;

        // Blanks source[from..to) in place, keeping line structure intact.
        void Blank(int from, int to)
        {
            for (var k = from; k < to && k < output.Length; k++)
            {
                if (output[k] != '\n' && output[k] != '\r')
                    output[k] = ' ';
            }
        }

        while (i < source.Length)
        {
            var c = source[i];

            if (c == '/' && i + 1 < source.Length && source[i + 1] == '/')
            {
                var end = source.IndexOf('\n', i);
                if (end < 0)
                    end = source.Length;
                Blank(i, end);
                i = end;
                continue;
            }

            if (c == '/' && i + 1 < source.Length && source[i + 1] == '*')
            {
                var end = source.IndexOf("*/", i + 2, StringComparison.Ordinal);
                end = end < 0 ? source.Length : end + 2;
                Blank(i, end);
                i = end;
                continue;
            }

            // Raw string literal: three or more quotes, closed by the same count.
            if (c == '"' && i + 2 < source.Length && source[i + 1] == '"' && source[i + 2] == '"')
            {
                var open = 0;
                while (i + open < source.Length && source[i + open] == '"')
                    open++;

                var scan = i + open;
                while (scan < source.Length)
                {
                    if (source[scan] != '"')
                    {
                        scan++;
                        continue;
                    }

                    var run = 0;
                    while (scan + run < source.Length && source[scan + run] == '"')
                        run++;
                    if (run >= open)
                        break;
                    scan += run;
                }

                var end = scan >= source.Length ? source.Length : scan + open;
                Blank(i + open, Math.Min(scan, end));
                i = end;
                continue;
            }

            // Verbatim string, optionally interpolated: @"...", $@"...", @$"...".
            var verbatim = c == '@' && i + 1 < source.Length && source[i + 1] == '"';
            var interpolatedVerbatim =
                (c == '$' || c == '@')
                && i + 2 < source.Length
                && (
                    (source[i + 1] == '@' && source[i + 2] == '"')
                    || (source[i + 1] == '$' && source[i + 2] == '"')
                );

            if (verbatim || interpolatedVerbatim)
            {
                var quote = source.IndexOf('"', i);
                var scan = quote + 1;
                while (scan < source.Length)
                {
                    if (source[scan] != '"')
                    {
                        scan++;
                        continue;
                    }

                    // "" is an escaped quote inside a verbatim string.
                    if (scan + 1 < source.Length && source[scan + 1] == '"')
                    {
                        scan += 2;
                        continue;
                    }

                    break;
                }

                Blank(quote + 1, scan);
                i = scan < source.Length ? scan + 1 : source.Length;
                continue;
            }

            if (c == '"' || c == '\'')
            {
                var scan = i + 1;
                while (scan < source.Length && source[scan] != c)
                {
                    // A backslash escapes the next character, including the delimiter.
                    scan += source[scan] == '\\' ? 2 : 1;
                }

                Blank(i + 1, Math.Min(scan, source.Length));
                i = scan < source.Length ? scan + 1 : source.Length;
                continue;
            }

            i++;
        }

        return new string(output);
    }

    public static IReadOnlyList<(int LineNumber, string Line)> MatchingLines(
        string source,
        Regex pattern
    )
    {
        var hits = new List<(int, string)>();
        var lines = source.Replace("\r\n", "\n").Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (pattern.IsMatch(lines[i]))
                hits.Add((i + 1, lines[i]));
        }
        return hits;
    }
}
