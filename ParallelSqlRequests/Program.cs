using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using MPI;
using Microsoft.EntityFrameworkCore;
using MySql.Data.MySqlClient;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
        string connectionString = "Server=DESKTOP-B3JFO7O;Database=course_db;Integrated Security=True;";
        optionsBuilder.UseSqlServer(connectionString);
    }
}

class Program
{
    static void CreateTable(int rank, int numProcesses)
    {
        using (var dbContext = new SampleDbContext())
        {
            if (rank == 0)
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
                            $"EXEC('SELECT * INTO computer_hardware_{i} FROM computer_hardware WHERE Id % {numProcesses - 1} = {i};')"
                        );

                        Console.WriteLine($"Table computer_hardware_{i} created.");
                    }
                    else
                    {
                        Console.WriteLine($"Table computer_hardware_{i} already exists.");
                    }
                }
            }
        }
    }
    static void SendData(int rank, int numProcesses, List<ComputerHardware> data)
    {
        if (rank != 0)
        {
            string serializedData = JsonSerializerHelper.SerializeObject(data);

            Communicator.world.Send(serializedData, 0, 0);
        }
    }

    static List<ComputerHardware> ReceiveData(int numProcesses)
    {
        List<ComputerHardware> receivedData = new List<ComputerHardware>();

        for (int i = 1; i < numProcesses; i++)
        {
            string serializedData = Communicator.world.Receive<string>(i, 0);

            List<ComputerHardware> deserializedData = JsonSerializerHelper.DeserializeObject<List<ComputerHardware>>(serializedData);

            receivedData.AddRange(deserializedData);
        }

        return receivedData;
    }

    static void GatherResults(int rank, int numProcesses, List<long> processTimes)
    {
        using (var dbContext = new SampleDbContext())
        {
            if (rank == 0)
            {
                List<(int ProcessId, int NumRecords, List<ComputerHardware> Results)> gatheredResults =
                    new List<(int, int, List<ComputerHardware>)>();

                for (int i = 0; i < numProcesses - 1; i++)
                {
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();

                    var result = dbContext.ComputerHardwares.FromSqlRaw($"SELECT * FROM computer_hardware_{i}")
                        .ToList();
                    gatheredResults.Add((i + 1, result.Count, result));

                    SendData(rank, numProcesses, result);

                    stopwatch.Stop();

                    processTimes.Add(stopwatch.ElapsedMilliseconds);

                    Console.WriteLine($"Process {i + 1} has finished its work.");
                }
                Console.WriteLine("Gathered Results:");
                foreach (var result in gatheredResults)
                {
                    Console.WriteLine($"From Process {result.ProcessId}: {result.NumRecords} records, Time taken: {processTimes[result.ProcessId - 1]} milliseconds");
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
                    dbContext.Database.ExecuteSqlRaw($"DROP TABLE IF EXISTS computer_hardware_{i}");
                }
            }
        }
    }


    static void SingleRequestToMainTable()
    {
        using (var dbContext = new SampleDbContext())
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            var result = dbContext.ComputerHardwares.ToList();

            stopwatch.Stop();

            Console.WriteLine("Single Request to Main Table:");
            Console.WriteLine($"Number of records: {result.Count}");
            // foreach (var hardware in result)
            // {
            //     Console.WriteLine($"{hardware.Id}: {hardware.CPU}, {hardware.GPU}, {hardware.RAM}, {hardware.Motherboard}, {hardware.PSU}");
            // }

            Console.WriteLine(
                $"Time taken for single request to main table: {stopwatch.ElapsedMilliseconds} milliseconds");
        }
    }
// Modify ComparePerformance method to receive data
    static void ComparePerformance(int rank, int numProcesses)
    {
        if (rank == 0)
        {
            Console.WriteLine($"Comparing Performance for {numProcesses - 1} Slave Processes:");
        }

        List<long> processTimes = new List<long>();

        GatherResults(rank, numProcesses, processTimes);

        Communicator.world.Barrier();

        if (rank == 0)
        {
            // Receive data from all slave processes
            List<ComputerHardware> receivedData = ReceiveData(numProcesses);

            // Display the received data or perform further processing
            Console.WriteLine("Received Data:");
            foreach (var hardware in receivedData)
            {
                Console.WriteLine($"{hardware.Id}: {hardware.CPU}, {hardware.GPU}, {hardware.RAM}, {hardware.Motherboard}, {hardware.PSU}");
            }

            // Perform any additional processing with the received data
        }

        Communicator.world.Barrier();

        // SingleRequestToMainTable();
    }
    
    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            int rank = Communicator.world.Rank;
            int numProcesses = Communicator.world.Size;
            if (numProcesses < 2)
            {
                Console.WriteLine("Provide at least 2 processes to work with");
                return;
            }

            Communicator.world.Barrier();

            CreateTable(rank, numProcesses);

            Communicator.world.Barrier();

            ComparePerformance(rank, numProcesses);
            
            Communicator.world.Barrier();

            DeleteTables(rank, numProcesses);
        }
    }
}