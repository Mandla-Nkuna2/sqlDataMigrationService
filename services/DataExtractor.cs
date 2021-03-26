using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data;
using Google.Cloud.Firestore;
using System.Data.SqlClient;
using Quartz.Impl;
using Quartz;
using System.Threading;

namespace dataMigrationService.services
{
    public class DataExtractor
    {
        string projectId;
        FirestoreDb fireStoreDb;
        public async System.Threading.Tasks.Task FirebaseSave(String tableName, String id, Dictionary<string, object> data, String companyName)
        {
            // initiallising db 
            //service key
            string filepath = "../../../fas.json";
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", filepath);
            projectId = "fleet-administration-system";
            fireStoreDb = FirestoreDb.Create(projectId);
            //................................................
            //saving to db
            await fireStoreDb.Collection(companyName).Document(tableName).Collection("tables").Document(id).SetAsync(data);
        }
        public int FirebaseCheck(String tableName, String companyName)
        {
            // initiallising db 
            //service key
            string filepath = "../../../fas.json";
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", filepath);
            projectId = "fleet-administration-system";
            fireStoreDb = FirestoreDb.Create(projectId);
            //................................................
            //saving to db
            int number = fireStoreDb.Collection(companyName).Document(tableName).Collection("tables").GetSnapshotAsync().Result.Documents.Count;
            return number;
        }
        public void getData(string connectionString, string companyName)
        {
            DataTable myTable = new DataTable("tables");
            //var connectionString = "Data Source=ROB-PC\\SQLEXPRESS;Database=AKHATECH_FLEET;Integrated Security=false;User ID=FasTestDbUser1;Password=qwerty";
            var sqlConnection = new System.Data.SqlClient.SqlConnection(connectionString);

            sqlConnection.Open();

            DataTable dt = sqlConnection.GetSchema("Tables");
            int i = 0;
            foreach (DataRow row in dt.Rows)
            {

                string tablename = (string)row[2];
                var s = row.Table.Rows;
                Console.WriteLine("..........................................................");
                Console.WriteLine("this is the table name");
                Console.WriteLine(tablename);


                string oString = "Select * from " + tablename;

                SqlDataAdapter sda = new SqlDataAdapter(oString, sqlConnection);

                var rows = row.Table.Rows;
                DataTable dt1 = new DataTable();
                sda.Fill(dt1);

                DataRowCollection realRows = dt1.Rows;
                int a = 0;
                int firebaseCount = FirebaseCheck(tablename, companyName);
                Console.WriteLine("started table " + tablename);
                Console.WriteLine("IN FIREBASE: " + firebaseCount + " ACTUAL : " + realRows.Count);
                if (realRows.Count <= firebaseCount)
                {
                    Console.WriteLine("skipped");
                    continue;
                }
                var tasks = new List<Task>();
                foreach (DataRow realRow in realRows)
                {

                    int count = 0;
                    Dictionary<string, object> docData = new Dictionary<string, object> { };

                    realRow.ItemArray.ToList().ForEach(col =>
                    {
                        var property = realRow.Table.Columns[count].ToString();
                        var value = col;
                        if (value != null)
                        {
                            docData.Add(property, value.ToString());
                        }
                        else
                        {
                            docData.Add(property, null);
                        }
                        count++;
                    });
                    string id = "000000f" + a;
                    var storing = FirebaseSave(tablename, id, docData, companyName);
                    tasks.Add(storing);
                    long modulus = a % 100;
                    if (modulus == 0)
                    {
                        Console.WriteLine(a);
                        Console.WriteLine("waiting for 100 rows to save...");
                        Task.WhenAll(tasks).Wait();
                        Console.WriteLine("100 rows saved");

                    }
                    a++;
                }
                Console.WriteLine("waiting for data to save for table...");
                Task.WhenAll(tasks).Wait();
                Console.WriteLine("table " + tablename + "complete");
                i++;
            }
            Console.WriteLine(i);
            sqlConnection.Close();

        }
        public  string startDataMigration(string connString, string companyName) {
            Program program = new Program();
            try
            {
                getData(connString, companyName);
                return "started";
            }
            catch (Exception e)
            {
                Console.WriteLine("Exceptions caught");
                scheduleTest(connString, companyName).Wait();
                return "started wait";
            }
        }
        public async System.Threading.Tasks.Task scheduleTest(string connString, string companyName)
        {
            StdSchedulerFactory factory = new StdSchedulerFactory();

            // get a scheduler
            IScheduler scheduler = await factory.GetScheduler();
            await scheduler.Start();
            // define the job and tie it to our HelloJob class
            IJobDetail job = JobBuilder.Create<Job>()
                .WithIdentity("myJob", "group1").WithDescription(connString+"%%"+ companyName)
                .Build();

            // Trigger the job to run now, and then every 40 seconds
            ITrigger trigger = TriggerBuilder.Create()
               .WithIdentity("trigger3", "group1")
               .WithCronSchedule("0 * * ? * *")
               .ForJob("myJob", "group1")
               .Build();

            await scheduler.ScheduleJob(job, trigger);
        }
        public async Task streamTest() {
            CancellationTokenSource source = new CancellationTokenSource();
            await Task.Delay(1000, source.Token);

        }
    }
    public class Job : IJob
    {
        DataExtractor extractor = new DataExtractor();
        public async Task Execute(IJobExecutionContext context)
        {
            await Console.Out.WriteLineAsync("STARTING AGAIN");
            await CancelJob();
            await Console.Out.WriteLineAsync("job cancelled");

        }
        public async Task CancelJob()
        {
            StdSchedulerFactory factory = new StdSchedulerFactory();
            IScheduler scheduler = await factory.GetScheduler();
            JobKey key = JobKey.Create("myJob", "group1");
            var desc = scheduler.GetJobDetail(key).Result.Description;
            var messageArray =  desc.Split("%%");
            string connString = messageArray[0];
            string companyName = messageArray[1];
            await Console.Out.WriteLineAsync(desc);
            await scheduler.Shutdown();
            extractor.startDataMigration(connString, companyName);
        }
    }
}
