using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using MPI;
using Newtonsoft.Json;
using Environment = MPI.Environment;

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
}

[Table("computer_hardware")]
public class ComputerHardware
{
    [Key] public int Id { get; set; }

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

// class Program
// {
//     static void CreateTable(int rank, int numProcesses)
//     {
//         using (var dbContext = new SampleDbContext())
//         {
//             if (rank == 0)
//             {
//                 for (int i = 0; i < numProcesses - 1; i++)
//                 {
//                     bool tableExists = dbContext.Database.ExecuteSqlRaw(
//                         $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'computer_hardware_{i}'"
//                     ) > 0;
//
//                     if (!tableExists)
//                     {
//                         Console.WriteLine($"Creating table computer_hardware_{i}...");
//
//                         dbContext.Database.ExecuteSqlRaw(
//                             $"EXEC('SELECT * INTO computer_hardware_{i} FROM computer_hardware WHERE Id % {numProcesses - 1} = {i};')"
//                         );
//
//                         Console.WriteLine($"Table computer_hardware_{i} created.");
//                     }
//                     else
//                     {
//                         Console.WriteLine($"Table computer_hardware_{i} already exists.");
//                     }
//                 }
//             }
//         }
//     }
//
//     static void DeleteTables(int rank, int numProcesses)
//     {
//         using (var dbContext = new SampleDbContext())
//         {
//             if (rank == 0)
//             {
//                 for (int i = 0; i < numProcesses - 1; i++)
//                 {
//                     dbContext.Database.ExecuteSqlRaw($"DROP TABLE IF EXISTS computer_hardware_{i}");
//                 }
//             }
//         }
//     }
//
//     static List<string> GetLocalData(int rank)
//     {
//         using (var dbContext = new SampleDbContext())
//         {
//             var tableName = $"computer_hardware_{rank}";
//             var localData = dbContext.ComputerHardwares
//                 .FromSqlRaw($"SELECT * FROM {tableName}")
//                 .Select(hardware => JsonSerializerHelper.SerializeObject(hardware))
//                 .ToList();
//
//             return localData;
//         }
//     }
//     static void Main(string[] args)
//     {
//         using (new MPI.Environment(ref args))
//         {
//             int rank = Communicator.world.Rank;
//             int numProcesses = Communicator.world.Size;
//
//             CreateTable(rank, numProcesses);
//
//             if (rank != 0)
//             {
//                 // Non-zero ranks send their local data to rank 0
//                 var localData = GetLocalData(rank);
//                 string serializedData = string.Join(",", localData);
//                 Console.WriteLine($"Process {rank}/{numProcesses} sending data: {serializedData}");
//                 Communicator.world.Send(serializedData, 0, 0);
//                 Console.WriteLine($"Process {rank}/{numProcesses} sent data");
//             }
//             else
//             {
//                 // Rank 0 gathers data from other processes
//                 var gatheredData = new List<string>();
//                 for (int i = 1; i < numProcesses; i++)
//                 {
//                     Console.WriteLine($"Process {rank}/{numProcesses} receiving data from process {i}/{numProcesses}");
//                     string receivedData = Communicator.world.Receive<string>(i, 0);
//                     Console.WriteLine($"Process {rank}/{numProcesses} received data from process {i}/{numProcesses}: {receivedData}");
//                     gatheredData.Add(receivedData);
//                 }
//
//                 Console.WriteLine($"Process {rank}/{numProcesses} received a lot of data as the result!");
//
//                 // Print the gathered data
//                 foreach (var data in gatheredData)
//                 {
//                     Console.WriteLine(data);
//                 }
//
//             }
//
//             Communicator.world.Barrier(); // Synchronize all processes before exiting
//             DeleteTables(rank, numProcesses);
//         }
//     }
//
// }


class Program
{
    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            if (Communicator.world.Rank != 0)
            {
                int rank = Communicator.world.Rank;

                Console.WriteLine($"Process {rank} started.");

                // Load data
                Stopwatch sw = new Stopwatch();
                sw.Start();
                List<ComputerHardware> allData = LoadData();
                sw.Stop();
                Console.WriteLine($"Process {rank} loaded data in {sw.ElapsedMilliseconds} ms.");

                // Divide the workload
                int chunkSize = allData.Count / Communicator.world.Size;
                int startIndex = rank * chunkSize;
                int endIndex = (rank == Communicator.world.Size - 1) ? allData.Count : startIndex + chunkSize;

                // Process the local portion of the workload
                sw.Reset();
                sw.Start();
                List<ComputerHardware> localData = allData.GetRange(startIndex, endIndex - startIndex);
                List<string> results = ProcessData(localData);
                sw.Stop();
                Console.WriteLine($"Process {rank} processed data in {sw.ElapsedMilliseconds} ms.");

                // Send results to the master process
                string serializedResults = JsonSerializerHelper.SerializeObject(results);
                Console.WriteLine($"Process {rank} sending results to master process.");
                Communicator.world.Send(serializedResults, 0, 0);

                Console.WriteLine($"Process {rank} done.");
            }
            else
            {
                Console.WriteLine("Main process waiting to receive results:");

                List<string> allResults = new List<string>();
                for (int i = 1; i < Communicator.world.Size; i++)
                {
                    Console.WriteLine($"Main process waiting to receive results from process {i}.");
                    string receivedResults = Communicator.world.Receive<string>(i, 0);
                    List<string> deserializedResults =
                        JsonSerializerHelper.DeserializeObject<List<string>>(receivedResults);
                    allResults.AddRange(deserializedResults);
                }

                Console.WriteLine("Aggregated Results:");
                foreach (string result in allResults)
                {
                    Console.WriteLine(result);
                }
            }
        }
    }

    static List<ComputerHardware> LoadData()
    {
        using (var dbContext = new SampleDbContext())
        {
            return dbContext.ComputerHardwares.ToList();
        }
    }

    static List<string> ProcessData(List<ComputerHardware> data)
    {
        List<string> results = new List<string>();

        foreach (var item in data)
        {
            // Process each item and convert to a string
            string result = $"{item.Id}: {item.CPU}, {item.GPU}, {item.RAM}, {item.Motherboard}, {item.PSU}";
            results.Add(result);
        }

        return results;
    }
}