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
                Console.WriteLine("Please run the program with at least 2 processes.");
                return;
            }

            try
            {
                using (var dbContext = new SampleDbContext())
                {
                    // Create the table if it doesn't exist
                    dbContext.Database.EnsureCreated();

                    // Wait for all processes to ensure table creation
                    comm.Barrier();

                    // Each process performs the query on the entire table
                    List<ComputerHardware> result = dbContext.Set<ComputerHardware>().ToList();

                    // Serialize the result
                    var serializedResult = JsonSerializerHelper.SerializeObject(result);

                    // Gather results to process in the root process
                    string[] gatheredResults = comm.Gather(serializedResult, 0);

                    if (comm.Rank == 0)
                    {
                        // Process and print the gathered results
                        foreach (var gatheredResult in gatheredResults)
                        {
                            var deserializedResult = JsonSerializerHelper.DeserializeObject<List<ComputerHardware>>(gatheredResult);
                            foreach (var hardware in deserializedResult)
                            {
                                Console.WriteLine($"Gathered Result: ID: {hardware.Id}, CPU: {hardware.CPU}, GPU: {hardware.GPU}, RAM: {hardware.RAM}, Motherboard: {hardware.Motherboard}, PSU: {hardware.PSU}");
                            }
                        }

                        stopwatch.Stop();
                        Console.WriteLine($"Program executed in {stopwatch.ElapsedMilliseconds} milliseconds.");
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
