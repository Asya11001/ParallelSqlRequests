using System;
using System.Diagnostics;
using System.Linq;
using MPI;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
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

[Table("computer_hardware")] // Specify the actual table name
[Serializable]
public class ComputerHardware
{
    [Key]
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


public class ComputerHardwares
{
    public int Id { get; set; }
    public string CPU { get; set; }
    public string GPU { get; set; }
    public string RAM { get; set; }
    public string Motherboard { get; set; }
    public string PSU { get; set; } 
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
    static void Main(string[] args)
    {
        Stopwatch stopwatch = new Stopwatch();

        using (new MPI.Environment(ref args))
        {
            Intracommunicator comm = MPI.Communicator.world;
            if (comm.Size < 2)
            {
                return;
            }

            try
            {
                using (var dbContext = new SampleDbContext())
                {
                    // Only rank 0 creates tables
                    if (comm.Rank == 0)
                    {
                        for (int i = 1; i < comm.Size; i++)
                        {
                            var tableName = $"computer_hardware_{i}";

                            // Check if the table exists before creating it
                            var tableExists = dbContext.Database.ExecuteSqlRaw($"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}'") > 0;

                            if (!tableExists)
                            {
                                // Use a transaction to ensure atomic table creation
                                using (var transaction = dbContext.Database.BeginTransaction())
                                {
                                    try
                                    {
                                        // Create the table if it doesn't exist
                                        dbContext.Database.ExecuteSqlRaw($"SELECT * INTO {tableName} FROM computer_hardware WHERE ABS(CAST(Id AS INT)) % {comm.Size} = ({comm.Rank} - 1 + {comm.Size}) % {comm.Size}");
                                        transaction.Commit();
                                    }
                                    catch (Exception)
                                    {
                                        transaction.Rollback();
                                    }
                                }
                            }
                        }

                        comm.Barrier(); // Ensure all processes wait for table creation
                    }
                    else
                    {
                        comm.Barrier(); // Other processes wait for table creation
                    }

                    // Each process performs the query on its own table
                    var tableNameForQuery = $"computer_hardware_{comm.Rank}";

                    List<ComputerHardware> result = null;

                    // Check if the table exists before querying it
                    var queryTableExists = dbContext.Database.ExecuteSqlRaw($"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableNameForQuery}'") > 0;

                    if (queryTableExists)
                    {
                        result = dbContext.Set<ComputerHardware>().FromSqlRaw($"SELECT * FROM {tableNameForQuery}").ToList();

                        // Process and print the results here
                        foreach (var hardware in result)
                        {
                            Console.WriteLine($"Process {comm.Rank}: ID: {hardware.Id}, CPU: {hardware.CPU}, GPU: {hardware.GPU}, RAM: {hardware.RAM}, Motherboard: {hardware.Motherboard}, PSU: {hardware.PSU}");
                        }
                    }

                    // Wait for all processes to complete the query
                    comm.Barrier();

                    if (comm.Rank == 0)
                    {
                        stopwatch.Start();
                    }

                    // Main process gathers results from all processes
                    List<ComputerHardware>[] allResults = comm.Gather(result, 0);

                    // Main process prints the gathered results
                    if (comm.Rank == 0)
                    {
                        // Process and print the gathered results
                        foreach (var processResults in allResults)
                        {
                            if (processResults != null)
                            {
                                foreach (var hardware in processResults)
                                {
                                    Console.WriteLine($"Gathered Result: ID: {hardware.Id}, CPU: {hardware.CPU}, GPU: {hardware.GPU}, RAM: {hardware.RAM}, Motherboard: {hardware.Motherboard}, PSU: {hardware.PSU}");
                                }
                            }
                        }

                        stopwatch.Stop();
                        Console.WriteLine($"Program executed in {stopwatch.ElapsedMilliseconds} milliseconds.");
                    }

                    // Each process drops its own table
                    for (int i = 1; i < comm.Size; i++)
                    {
                        var tableNameForDeletion = $"computer_hardware_{i}";

                        // Check if the table exists before dropping it
                        var tableExists = dbContext.Database.ExecuteSqlRaw($"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableNameForDeletion}'") > 0;

                        if (tableExists)
                        {
                            // Use a transaction to ensure atomic table deletion
                            using (var transaction = dbContext.Database.BeginTransaction())
                            {
                                try
                                {
                                    // Drop the table if it exists
                                    dbContext.Database.ExecuteSqlRaw($"DROP TABLE {tableNameForDeletion}");
                                    transaction.Commit();
                                }
                                catch (Exception)
                                {
                                    transaction.Rollback();
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }
    }
}
