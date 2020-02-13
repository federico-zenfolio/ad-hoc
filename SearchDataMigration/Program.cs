using System;

namespace SearchDataMigration
{
    class Program
    {
        static void Main(string[] args)
        {
            var migrationProcess = new MigrationProcess();
            migrationProcess.ProcessAsync().GetAwaiter().GetResult();
            Console.ReadLine();
        }
    }
}
