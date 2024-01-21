using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Newtonsoft.Json.Linq;

public class JsonComparator
{
    public static bool AreFilesIdentical(string filePath1, string filePath2)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        // Read JSON data from the files
        string json1 = File.ReadAllText(filePath1);
        string json2 = File.ReadAllText(filePath2);

        // Parse JSON data into JArray (assuming the data is an array of objects)
        JArray jsonArray1 = JArray.Parse(json1);
        JArray jsonArray2 = JArray.Parse(json2);

        // Measure the time taken to compare the arrays

        // Check if the two arrays have the same count of identical objects
        bool areIdentical = AreArraysIdentical(jsonArray1, jsonArray2);

        stopwatch.Stop();

        Console.WriteLine($"Time taken to compare files: {stopwatch.ElapsedMilliseconds} milliseconds");

        return areIdentical;
    }

    private static bool AreArraysIdentical(JArray array1, JArray array2)
    {
        if (array1.Count != array2.Count)
        {
            return false;
        }

        HashSet<JToken> set1 = new HashSet<JToken>(array1, JToken.EqualityComparer);
        HashSet<JToken> set2 = new HashSet<JToken>(array2, JToken.EqualityComparer);

        return set1.SetEquals(set2);
    }
}