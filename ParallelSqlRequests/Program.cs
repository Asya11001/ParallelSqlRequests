
using System;
using System.Data;
using System.Data.SqlClient;

class Program
{
    static void Main(string[] args)
    {
        string connectionString = "Server=DESKTOP-VU5HLAS\\SQLEXPRESS;Database=course_db;Integrated Security=True;";

        try
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                // SQL query to select all rows from the computer_hardware table
                string query = "SELECT * FROM computer_hardware";

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
}