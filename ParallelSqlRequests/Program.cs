/*
using System;
using System.Data;
using System.Data.SqlClient;

class Program
{
    static void Main(string[] args)
    {
        string connectionString = "Server=DESKTOP-B3JFO7O;Database=course_db;Integrated Security=True;";

        try
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                // SQL query to select all rows from the computer_hardware table
                string query = "SELECT TOP (1) * FROM computer_hardware";

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        // Check if there are rows returned
                        if (reader.HasRows)
                        {
                            // Iterate through the rows and access the data
                            while (reader.Read())
                            {
                                // Access columns by name or index
                                int id = reader.GetInt32(reader.GetOrdinal("id"));
                                string CPU = reader.GetString(reader.GetOrdinal("cpu_name"));
                                string GPU = reader.GetString(reader.GetOrdinal("gpu_name"));
                                string RAM = reader.GetString(reader.GetOrdinal("ram_name"));
                                string Motherboard = reader.GetString(reader.GetOrdinal("motherboard_name"));
                                string PSU = reader.GetString(reader.GetOrdinal("psu_name"));

                                // Output the data (you can do whatever you need with it)
                                Console.WriteLine($"ID: {id}, CPU: {CPU}, GPU: {GPU}, RAM: {RAM}, Motherboard: {Motherboard}, PSU: {PSU}");
                            }
                        }
                        else
                        {
                            Console.WriteLine("No rows found.");
                        }
                    }
                }

                connection.Close();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: " + ex.Message);
        }
    }
}*/

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

[Table("computer_hardware")] // Specify the actual table name
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

            try
            {
                if (comm.Rank == 0)
                {
                    Console.WriteLine("Choose MPI Rank 0 operation (1-7): ");
                    int selectedQuery = int.Parse(Console.ReadLine());
                    stopwatch.Start();

                    using (var dbContext = new SampleDbContext())
                    {
                        // Assuming ComputerHardware is your entity representing the 'computer_hardware' table
                        var result = dbContext.ComputerHardwares.Take(1).ToList();

                        // Process and print the results here
                        foreach (var hardware in result)
                        {
                            Console.WriteLine($"ID: {hardware.Id}, CPU: {hardware.CPU}, GPU: {hardware.GPU}, RAM: {hardware.RAM}, Motherboard: {hardware.Motherboard}, PSU: {hardware.PSU}");
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
