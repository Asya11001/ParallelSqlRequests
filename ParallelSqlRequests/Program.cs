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
}

[Table("computer_hardware")]
public class ComputerHardware
{
    [Key]
    [Column("computer_hardware_id")]
    public int Id { get; set; }

    [Required]
    [Column("cpu_name")]
    public string CPU { get; set; }

    [Required]
    [Column("gpu_name")]
    public string GPU { get; set; }

    [Required]
    [Column("ram_name")]
    public string RAM { get; set; }

    [Required]
    [Column("motherboard_name")]
    public string Motherboard { get; set; }

    [Required]
    [Column("psu_name")]
    public string PSU { get; set; }
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
                    Console.WriteLine($"Table computer_hardware_{i} created. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
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
                    Console.WriteLine($"Dropped table computer_hardware_{i}. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
                }
            }
        }
    }

    static List<string> GetLocalData(int rank)
    {
        using (var dbContext = new SampleDbContext())
        {
            var tableName = $"computer_hardware_{rank - 1}";
            var stopwatch = Stopwatch.StartNew();
            var localData = dbContext.ComputerHardwares
                .FromSqlRaw($"SELECT * FROM {tableName}")
                .Select(hardware => JsonSerializerHelper.SerializeObject(hardware))
                .ToList();
            stopwatch.Stop();
            Console.WriteLine($"Retrieved local data for process {rank}/{Communicator.world.Size}. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
            return localData;
        }
    }
    static void ExecuteMainTableRequest()
    {
        using (var dbContext = new SampleDbContext())
        {
            var stopwatch = Stopwatch.StartNew();

            // Replace the following query with your actual query on the main table
            var result = dbContext.ComputerHardwares.ToList();

            stopwatch.Stop();

            Console.WriteLine($"[MAIN REQUEST]: Main table request executed. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
            // You can process or print the result as needed
        }
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
                Console.WriteLine($"Process {rank}/{numProcesses} sent data. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
            }
            else
            {
                // Rank 0 gathers data from other processes
                var gatheredData = new List<string>();
                var stopwatch = Stopwatch.StartNew();
                for (int i = 1; i < numProcesses; i++)
                {
                    Console.WriteLine($"Process {rank}/{numProcesses} receiving data from process {i}/{numProcesses}");
                    var receivedData = Communicator.world.Receive<string>(i, 0);
                    Console.WriteLine($"Process {rank}/{numProcesses} received data from process {i}/{numProcesses}.");
                    gatheredData.Add(receivedData);
                }
                stopwatch.Stop();
                Console.WriteLine($"Rank 0 gathered data from other processes. Elapsed Time: {stopwatch.ElapsedMilliseconds} ms");
            }
            //
            // Communicator.world.Barrier(); // Synchronize all processes before exiting
            // DeleteTables(rank, numProcesses);
        }
    }

}