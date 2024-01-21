using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using MPI;
using Newtonsoft.Json;

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
    private const int MIN_PROCESSES = 2;
    private const int MAX_PROCESSES = 100;
    private static readonly TimeLogger timeLogger = new TimeLogger();

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


    static List<string> GetLocalData(int rank)
    {
        using (var dbContext = new SampleDbContext())
        {
            var tableName = $"computer_hardware_{rank - 1}";
            timeLogger.StartTimer($"GetLocalData");
            var localData = dbContext.ComputerHardwares
                .FromSqlRaw($"SELECT * FROM {tableName}");
            timeLogger.StopTimer($"GetLocalData");

            timeLogger.StartTimer($"SerializedLocalData");
            var serializedLocalData =
                localData.Select(hardware => JsonSerializerHelper.SerializeObject(hardware)).ToList();
            timeLogger.StopTimer($"SerializedLocalData");
            
            return serializedLocalData;
        }
    }


    static void ExecuteMainTableRequest()
    {
        using (var dbContext = new SampleDbContext())
        {

            Console.WriteLine($"[MAIN REQUEST]: Main table request executing...");
            var result = dbContext.ComputerHardwares.ToArray();


            Console.WriteLine($"[MAIN REQUEST]: Main table consist of {result.Length} records");

            SaveToFile(result, "one_process_response.txt");
            timeLogger.StopTimer("MainTableRequest");
        }
    }

    static ComputerHardware[] DeserializeData(string[] gatheredData)
    {
        var deserializedData = new List<ComputerHardware>();

        try
        {
            string jsonArray = "[" + string.Join(",", gatheredData) + "]";

            if (IsValidJson(jsonArray))
            {
                deserializedData = JsonSerializerHelper.DeserializeObject<List<ComputerHardware>>(jsonArray);
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

        return deserializedData.ToArray();
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
                timeLogger.StartTimer("MainTableRequest");
                ExecuteMainTableRequest();
                timeLogger.StopTimer("MainTableRequest");
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
                string serializedData = string.Join(",", localData);
                Console.WriteLine($"Process {rank}/{numProcesses} sending data...");

                timeLogger.StartTimer($"SentData");
                Communicator.world.Send(serializedData, 0, 0);
                timeLogger.StopTimer($"SentData");
            }
            else
            {
                var gatheredData = new string[numProcesses - 1];
                
                timeLogger.StartTimer("GatherData");
                for (int i = 1; i < numProcesses; i++)
                {
                    
                    timeLogger.StartTimer("ReceiveData");
                    Console.WriteLine($"Process {rank}/{numProcesses} receiving data from process {i}/{numProcesses}");
                    gatheredData[i - 1] = Communicator.world.Receive<string>(i, 0);
                    Console.WriteLine($"Process {rank}/{numProcesses} received data from process {i}/{numProcesses}.");
                    timeLogger.StopTimer("ReceiveData");
                }
                timeLogger.StopTimer("GatherData");

                timeLogger.StartTimer("DeserializeGatheredData");
                var deserializedData = DeserializeData(gatheredData);
                timeLogger.StopTimer("DeserializeGatheredData");
                
                SaveToFile(deserializedData, "multi_process_response.txt");
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
                                                  timeLogger.GetLogTime("DropAllTables") +
                                                  timeLogger.GetLogTime("DeserializeGatheredData");
            
                Console.WriteLine($"One single request: {timeLogger.GetLogTime("MainTableRequest")}");
                Console.WriteLine($"Parallel requests : {totalTParallelRequestsTime}");

            }
        }
    }
}