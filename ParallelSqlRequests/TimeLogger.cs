using System;
using System.Collections.Generic;
using System.Diagnostics;

public class TimeLogger
{
    private readonly Dictionary<string, Stopwatch> timers;
    private readonly Dictionary<string, long> maxElapsedTimes;

    public TimeLogger()
    {
        timers = new Dictionary<string, Stopwatch>();
        maxElapsedTimes = new Dictionary<string, long>();
    }

    public void StartTimer(string timerName)
    {
        if (!timers.ContainsKey(timerName))
        {
            timers[timerName] = Stopwatch.StartNew();
        }
        else
        {
            Console.WriteLine($"Timer '{timerName}' is already running.");
        }
    }

    public void StopTimer(string timerName)
    {
        if (timers.TryGetValue(timerName, out var stopwatch))
        {
            stopwatch.Stop();
            UpdateMaxElapsedTime(timerName, stopwatch.ElapsedMilliseconds);
            Console.WriteLine($"Timer '{timerName}' stopped. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
        }
        else
        {
            Console.WriteLine($"Timer '{timerName}' is not running.");
        }
    }

    public long GetLogTime(string timerName)
    {
        if (timers.TryGetValue(timerName, out var stopwatch))
        {
            return stopwatch.ElapsedMilliseconds;
        }

        return 0;
    }

    public long GetMaxElapsedTime(string timerName)
    {
        return maxElapsedTimes.TryGetValue(timerName, out var maxTime) ? maxTime : 0;
    }

    private void UpdateMaxElapsedTime(string timerName, long elapsedTime)
    {
        if (!maxElapsedTimes.ContainsKey(timerName))
        {
            maxElapsedTimes[timerName] = elapsedTime;
        }
        else
        {
            maxElapsedTimes[timerName] = Math.Max(maxElapsedTimes[timerName], elapsedTime);
        }
    }
}