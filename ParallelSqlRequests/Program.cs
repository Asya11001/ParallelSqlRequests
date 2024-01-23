using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using MPI;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

public class ObjectPool<T>
{
    private readonly Func<T> objectFactory;
    private readonly Queue<T> objects;

    public ObjectPool(Func<T> objectFactory, int initialSize)
    {
        this.objectFactory = objectFactory ?? throw new ArgumentNullException(nameof(objectFactory));
        this.objects = new Queue<T>(initialSize);

        for (int i = 0; i < initialSize; i++)
        {
            this.objects.Enqueue(this.objectFactory());
        }
    }

    public T GetObject()
    {
        lock (objects)
        {
            if (objects.Count > 0)
            {
                return objects.Dequeue();
            }
        }

        return objectFactory();
    }

    public void PutObject(T item)
    {
        lock (objects)
        {
            objects.Enqueue(item);
        }
    }
}

public static class JsonSerializerHelper
{
    private static readonly ObjectPool<JsonSerializer> SerializerPool =
        new ObjectPool<JsonSerializer>(() => new JsonSerializer(), 10);

    public static string SerializeObject<T>(T obj)
    {
        using (var writer = new StringWriter())
        using (var jsonWriter = new JsonTextWriter(writer))
        {
            var serializer = SerializerPool.GetObject();
            serializer.Serialize(jsonWriter, obj);
            SerializerPool.PutObject(serializer);

            return writer.ToString();
        }
    }

    public static T DeserializeObject<T>(string json)
    {
        using (var reader = new StringReader(json))
        using (var jsonReader = new JsonTextReader(reader))
        {
            var serializer = SerializerPool.GetObject();
            var result = serializer.Deserialize<T>(jsonReader);
            SerializerPool.PutObject(serializer);

            return result;
        }
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

[Serializable]
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
    private const int MIN_PROCESSES = 2;
    private const int MAX_PROCESSES = 100;
    private static readonly TimeLogger timeLogger = new TimeLogger();

    private static JsonSerializerSettings serializerSettings = new JsonSerializerSettings
    {
        TypeNameHandling = Newtonsoft.Json.TypeNameHandling.None,
        MetadataPropertyHandling = Newtonsoft.Json.MetadataPropertyHandling.Ignore
    };

    static void SaveToFile(ComputerHardware[] data, string fileName)
    {
        try
        {
            string json = JsonSerializerHelper.SerializeObject(data);
            File.WriteAllText(fileName, json);
            Console.WriteLine($"Data saved to file: {fileName}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error saving data to file: {ex.Message}");
        }
    }

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

    static bool CompareFiles()
    {
        string filePath1 = "one_process_response.txt";
        string filePath2 = "multi_process_response.txt";

        bool areFilesIdentical = JsonComparator.AreFilesIdentical(filePath1, filePath2);

        Console.WriteLine($"Are the files identical? {areFilesIdentical}");
        return areFilesIdentical;
    }


    static void CreateTables(int rank, int numProcesses)
    {
        if (rank == 0)
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

                        dbContext.Database.ExecuteSqlRaw(
                            $"EXEC('SELECT * INTO computer_hardware_{i} FROM computer_hardware WHERE computer_hardware_id % {numProcesses - 1} = {i};')"
                        );
                    }
                    else
                    {
                        Console.WriteLine($"Table computer_hardware_{i} already exists.");
                    }
                }
            }
        }
    }

    static void DropAllTables(int rank)
    {
        using (var dbContext = new SampleDbContext())
        {
            if (rank == 0)
            {
                for (int i = 0; i < MAX_PROCESSES; i++)
                {
                    dbContext.Database.ExecuteSqlRaw($"DROP TABLE IF EXISTS computer_hardware_{i}");
                }
            }
        }
    }


    static ComputerHardware[] GetLocalData(int rank)
    {
        using (var dbContext = new SampleDbContext())
        {
            var tableName = $"computer_hardware_{rank - 1}";
            timeLogger.StartTimer($"GetLocalData");
            var localData = dbContext.ComputerHardwares
                .FromSqlRaw($"SELECT * FROM {tableName}");
            timeLogger.StopTimer($"GetLocalData");

            // timeLogger.StartTimer($"SerializedLocalData");
            // var serializedLocalData =
                // localData.Select(hardware => JsonSerializerHelper.SerializeObject(hardware)).ToList();
            // timeLogger.StopTimer($"SerializedLocalData");

            return localData.ToArray();
        }
    }


    static void ExecuteMainTableRequest()
    {
        using (var dbContext = new SampleDbContext())
        {
            Console.WriteLine($"[MAIN REQUEST]: Main table request executing...");
            timeLogger.StartTimer("MainTableRequest");
            var result = dbContext.ComputerHardwares.ToArray();
            timeLogger.StopTimer("MainTableRequest");

            Console.WriteLine($"[MAIN REQUEST]: Main table consist of {result.Length} records");

            SaveToFile(result, "one_process_response.txt");
        }
    }

    static ComputerHardware[] DeserializeData(string[] gatheredData)
    {
        var deserializedData = new List<ComputerHardware>();

        try
        {
            timeLogger.StartTimer("SerializedDataToArray");
            string jsonArray = "[" + string.Join(",", gatheredData) + "]";
            timeLogger.StopTimer("SerializedDataToArray");

            if (IsValidJson(jsonArray))
            {
                timeLogger.StartTimer("DeserializeGatheredData");
                deserializedData = JsonSerializerHelper.DeserializeObject<List<ComputerHardware>>(jsonArray);
                timeLogger.StopTimer("DeserializeGatheredData");
            }
            else
            {
                Console.WriteLine($"Invalid JSON format for array: {jsonArray}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception during deserialization: {ex.Message}");
        }

        timeLogger.StartTimer("DeserializedDataToArray");
        var deserializedDataArray = deserializedData.ToArray();
        timeLogger.StopTimer("DeserializedDataToArray");

        return deserializedDataArray;
    }

    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            if (Communicator.world.Size < MIN_PROCESSES)
            {
                throw new Exception("Error. Provide more processes to work");
            }

            if (Communicator.world.Size > MAX_PROCESSES)
            {
                throw new Exception("Error. Provide fewer processes to work");
            }

            int rank = Communicator.world.Rank;
            int numProcesses = Communicator.world.Size;

            Communicator.world.Barrier();
            if (rank == 0)
            {
                // ExecuteMainTableRequest();
            }

            Communicator.world.Barrier();
            if (rank == 0)
            {
                timeLogger.StartTimer("DropAllTablesBeforeWork");
                DropAllTables(rank);
                timeLogger.StopTimer("DropAllTablesBeforeWork");
            }

            if (rank == 0)
            {
                timeLogger.StartTimer("CreateTables");
                CreateTables(rank, numProcesses);
                timeLogger.StopTimer("CreateTables");
            }


            Communicator.world.Barrier();
            if (rank != 0)
            {
                var localData = GetLocalData(rank);
                // string serializedData = string.Join(",", localData);
                // Console.WriteLine($"Process {rank}/{numProcesses} sending data...");

                timeLogger.StartTimer($"SentData");
                Communicator.world.Send<ComputerHardware[]>(localData, 0, 0);
                timeLogger.StopTimer($"SentData");
            }
            else
            {
                var gatheredData = new ComputerHardware[0];

                timeLogger.StartTimer("GatherData");
                for (int i = 1; i < numProcesses; i++)
                {
                    timeLogger.StartTimer("ReceiveData");
                    Console.WriteLine($"Process {rank}/{numProcesses} receiving data from process {i}/{numProcesses}");
                    foreach (var computerHardware in Communicator.world.Receive<ComputerHardware[]>(i, 0))
                    {
                       gatheredData.Append(computerHardware);
                    }
                    Console.WriteLine($"Process {rank}/{numProcesses} received data from process {i}/{numProcesses}.");
                    timeLogger.StopTimer("ReceiveData");
                }

                timeLogger.StopTimer("GatherData");

                // Console.WriteLine(gatheredData.Length);
                // timeLogger.StartTimer("DeserializeData");
                // var deserializedData = DeserializeData(gatheredData);
                // timeLogger.StopTimer("DeserializeData");

                // SaveToFile(deserializedData, "multi_process_response.txt");
            }

            Communicator.world.Barrier();

            if (rank == 0)
            {
                timeLogger.StartTimer("DropAllTablesAfterWork");
                DropAllTables(rank);
                timeLogger.StopTimer("DropAllTablesAfterWork");
            }

            // Communicator.world.Barrier();

            // if (rank == 0)
            // {
            //     CompareFiles();
            // }

            Communicator.world.Barrier();
            if (rank == 0)
            {
                long totalTParallelRequestsTime =
                    // timeLogger.GetMaxElapsedTime("ReceiveData") +
                    // timeLogger.GetMaxElapsedTime("GetLocalData") +
                    // timeLogger.GetMaxElapsedTime("SerializeLocalData") +
                    timeLogger.GetLogTime("CreateTables") +
                    timeLogger.GetLogTime("GatherData") +
                    timeLogger.GetLogTime("DeserializeGatheredData") +
                    timeLogger.GetLogTime("DropAllTablesBeforeWork") +
                    timeLogger.GetLogTime("DropAllTablesAfterWork");
                
                Console.WriteLine($"DeserializeData: {timeLogger.GetLogTime("DeserializeData")}");
                Console.WriteLine($"Non-parallel request: {timeLogger.GetLogTime("MainTableRequest")}");
                Console.WriteLine($"Parallel requests : {timeLogger.GetLogTime("CreateTables")}+" +
                                  $"{timeLogger.GetLogTime("GatherData")}+" +
                                  $"{timeLogger.GetLogTime("DeserializeGatheredData")}+" +
                $"{timeLogger.GetLogTime("DropAllTablesBeforeWork")}+" +
                                  $"{timeLogger.GetLogTime("DropAllTablesAfterWork")}=" +
                                  $"{totalTParallelRequestsTime}");
            }
        }
    }
}
