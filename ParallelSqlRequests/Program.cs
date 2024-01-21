using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using MPI;
using Newtonsoft.Json;
using ZstdSharp.Unsafe;


public static class JsonSerializerHelper
{
    public static string SerializeObject<T>(T obj)
    {
        return JsonConvert.SerializeObject(obj);
    }

    public static T DeserializeObject<T>(string json)
    {
        return JsonConvert.DeserializeObject<T>(json);
    }

    public static List<ComputerHardware> DeserializeGatheredData(List<string> gatheredData)
    {
        var deserializedData = new List<ComputerHardware>();

        foreach (var jsonData in gatheredData)
        {
            var hardware = JsonSerializerHelper.DeserializeObject<ComputerHardware>(jsonData);
            deserializedData.Add(hardware);
        }

        return deserializedData;
    }
}


[Table("computer_hardware")]
public class ComputerHardware
{
    [Key] [Column("computer_hardware_id")] public int Id { get; set; }

    [Required] [Column("cpu_name")] public string CPU { get; set; }

    [Required] [Column("gpu_name")] public string GPU { get; set; }

    [Required] [Column("ram_name")] public string RAM { get; set; }

    [Required]
    [Column("motherboard_name")]
    public string Motherboard { get; set; }

    [Required] [Column("psu_name")] public string PSU { get; set; }
}

public class SampleDbContext : DbContext
{
    public DbSet<ComputerHardware> ComputerHardwares { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        var connectionString = "Server=DESKTOP-B3JFO7O;Database=course_db;Integrated Security=True;";
        optionsBuilder.UseSqlServer(connectionString);
    }
}

class Program
{
    // Function to validate JSON format (optional)
    public static bool IsValidJson(string json)
    {
        try
        {
            Newtonsoft.Json.JsonConvert.DeserializeObject(json);
            return true;
        }
        catch (Newtonsoft.Json.JsonException)
        {
            return false;
        }
    }

    static void CreateTable(int numProcesses)
    {
        using (var dbContext = new SampleDbContext())
        {
            for (int i = 0; i < numProcesses - 1; i++)
            {
                bool tableExists = dbContext.Database.ExecuteSqlRaw(
                    $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'computer_hardware_{i}'"
                ) > 0;

                if (!tableExists)
                {
                    Console.WriteLine($"Creating table computer_hardware_{i}...");
                    var stopwatch = Stopwatch.StartNew();

                    dbContext.Database.ExecuteSqlRaw(
                        $"EXEC('SELECT * INTO computer_hardware_{i} FROM computer_hardware WHERE computer_hardware_id % {numProcesses - 1} = {i};')"
                    );

                    stopwatch.Stop();
                    Console.WriteLine(
                        $"Table computer_hardware_{i} created. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
                }
                else
                {
                    Console.WriteLine($"Table computer_hardware_{i} already exists.");
                }
            }
        }
    }

    static void DeleteTables(int rank, int numProcesses)
    {
        using (var dbContext = new SampleDbContext())
        {
            if (rank == 0)
            {
                for (int i = 0; i < numProcesses - 1; i++)
                {
                    var stopwatch = Stopwatch.StartNew();
                    dbContext.Database.ExecuteSqlRaw($"DROP TABLE IF EXISTS computer_hardware_{i}");
                    stopwatch.Stop();
                    Console.WriteLine(
                        $"Dropped table computer_hardware_{i}. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
                }
            }
        }
    }

    static List<string> GetLocalData(int rank)
    {
        using (var dbContext = new SampleDbContext())
        {
            var tableName = $"computer_hardware_{rank - 1}";
            var stopwatchRetrieveData = Stopwatch.StartNew();
            var localData = dbContext.ComputerHardwares
                .FromSqlRaw($"SELECT * FROM {tableName}");
            stopwatchRetrieveData.Stop();


            var stopwatchSerializeData = Stopwatch.StartNew();
            var serializedLocalData =
                localData.Select(hardware => JsonSerializerHelper.SerializeObject(hardware)).ToList();
            stopwatchSerializeData.Stop();

            Console.WriteLine(
                $"Retrieved local data for process {rank}/{Communicator.world.Size}. Retrive Time: {stopwatchRetrieveData.ElapsedMilliseconds} ms, Serialize time: {stopwatchSerializeData.ElapsedMilliseconds}");

            return serializedLocalData;
        }
    }

    static void ExecuteMainTableRequest()
    {
        using (var dbContext = new SampleDbContext())
        {
            var stopwatch = Stopwatch.StartNew();


            Console.WriteLine($"[MAIN REQUEST]: Main table request executing...");
            var result = dbContext.ComputerHardwares.ToArray();
            for (int i = 0; i < result.Length; i++)
            {
                Console.WriteLine(result[i].Id);
            }

            stopwatch.Stop();

            Console.WriteLine(
                $"[MAIN REQUEST]: Main table request executed. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
            Console.WriteLine($"[MAIN REQUEST]: Main table consist of {result.Length} records");
            // You can process or print the result as needed
        }
    }

    static List<ComputerHardware> DeserializeData(List<string> gatheredData)
    {
        var deserializedData = new List<ComputerHardware>();
        string jsonArray = "[" + string.Join(",", gatheredData) + "]";


        if (IsValidJson(jsonArray))
        {
            var hardwareList = JsonSerializerHelper.DeserializeObject<List<ComputerHardware>>(jsonArray);

            foreach (var hardware in hardwareList)
            {
                deserializedData.Add(hardware);
            }
        }
        else
        {
            Console.WriteLine($"Invalid JSON format for array: {jsonArray}");
        }

        return deserializedData;
    }

    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            int rank = Communicator.world.Rank;
            int numProcesses = Communicator.world.Size;

            // if (rank == 0)
            // {
            //     var stopwatch = Stopwatch.StartNew();
            //     CreateTable(numProcesses);
            //     stopwatch.Stop();
            //     Console.WriteLine($"Table creation for rank 0. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
            // }

            if (rank == 0)
            {
                ExecuteMainTableRequest();
            }

            Communicator.world.Barrier();
            if (rank != 0)
            {
                // Non-zero ranks send their local data to rank 0
                var localData = GetLocalData(rank);
                string serializedData = string.Join(",", localData);
                Console.WriteLine($"Process {rank}/{numProcesses} sending data...");
                var stopwatch = Stopwatch.StartNew();
                Communicator.world.Send(serializedData, 0, 0);
                stopwatch.Stop();
                Console.WriteLine(
                    $"Process {rank}/{numProcesses} sent data. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
            }
            else
            {
                var gatheredData = new List<string>();
                var stopwatchGatherData = Stopwatch.StartNew();
                for (int i = 1; i < numProcesses; i++)
                {
                    Console.WriteLine($"Process {rank}/{numProcesses} receiving data from process {i}/{numProcesses}");
                    var receivedData = Communicator.world.Receive<string>(i, 0);
                    Console.WriteLine($"Process {rank}/{numProcesses} received data from process {i}/{numProcesses}.");
                    gatheredData.Add(receivedData);
                }
                

                stopwatchGatherData.Stop();
                Console.WriteLine(
                    $"Rank 0 gathered data from other processes. Elapsed Time: {stopwatchGatherData.ElapsedMilliseconds} ms");
                
                var stopwatchDeserializeData = Stopwatch.StartNew();
                var deserializedData = DeserializeData(gatheredData);
                stopwatchDeserializeData.Stop();
                Console.WriteLine(
                    $"Rank 0 deserialized data. Elapsed Time: {stopwatchDeserializeData.ElapsedMilliseconds} ms");
            }

            //
            Communicator.world.Barrier(); // Synchronize all processes before exiting
            // DeleteTables(rank, numProcesses);
        }
    }
}