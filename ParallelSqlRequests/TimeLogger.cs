using System.Diagnostics;

public class TimeLogger
{
    private readonly Dictionary<string, Stopwatch> timers;

    public TimeLogger()
    {
        timers = new Dictionary<string, Stopwatch>();
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
}
