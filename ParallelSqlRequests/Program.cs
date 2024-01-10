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
                    // Check if the table exists
                    bool tableExists = dbContext.Database.ExecuteSqlRaw(
                        $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'computer_hardware_{i}'"
                    ) > 0;

                    if (!tableExists)
                    {
                        // Create the table and insert data based on the specified condition
                        dbContext.Database.ExecuteSqlRaw(
                            $"EXEC('SELECT * INTO computer_hardware_{i} FROM computer_hardware WHERE Id % {numProcesses - 1} = {i};')"
                        );
                    }
                }
            }
        }
    }


    static void GatherResults(int rank, int numProcesses)
    {
        using (var dbContext = new SampleDbContext())
        {
            if (rank == 0)
            {
                List<ComputerHardware> gatheredResults = new List<ComputerHardware>();

                // Gather results from each slave process
                for (int i = 1; i < numProcesses; i++)
                {
                    var result = dbContext.ComputerHardwares.FromSqlRaw($"SELECT * FROM ComputerHardware_{i}").ToList();
                    gatheredResults.AddRange(result);
                }

                // Process gathered results as needed
                Console.WriteLine("Gathered Results:");
                foreach (var hardware in gatheredResults)
                {
                    Console.WriteLine($"{hardware.Id}: {hardware.CPU}, {hardware.GPU}, {hardware.RAM}, {hardware.Motherboard}, {hardware.PSU}");
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
                    // Delete tables for each slave process
                    dbContext.Database.ExecuteSqlRaw($"DROP TABLE IF EXISTS ComputerHardware_{i}");
                }
            }
        }
    }

    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            int rank = Communicator.world.Rank;
            int numProcesses = Communicator.world.Size;

            CreateTable(rank, numProcesses);

            // Perform other computation or data processing tasks here...

            // GatherResults(rank, numProcesses);

            // DeleteTables(rank, numProcesses);
        }
    }
}
