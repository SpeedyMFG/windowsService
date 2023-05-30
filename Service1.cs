using Nest;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace WindowsService1
{
    public partial class Service1 : ServiceBase
    {
        private Stopwatch stopwatch = new Stopwatch();
        private System.Timers.Timer timer = new System.Timers.Timer();
        private string connectionString = "Server=localhost;Database=Localhost_Login;Trusted_Connection=True;";
        private string[] logins; // logins dizisini tanımladık
        private HashSet<string> processedLogins = new HashSet<string>();

        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            WriteToFile("Service is started at " + DateTime.Now);
            stopwatch.Start();
            timer.Elapsed += new ElapsedEventHandler(OnElapsedTime);
            timer.Interval = 1800000;
            timer.Enabled = true;
        }

        protected override void OnStop()
        {
            WriteToFile("Service is stopped at " + DateTime.Now);
            stopwatch.Stop();
            TimeSpan elapsedTime = stopwatch.Elapsed; // Ölçülen süreyi al
            WriteToFile("Ölçülen Süre: " + elapsedTime.ToString()); // Log dosyasına yaz
            processedLogins.Clear();
        }

        private void OnElapsedTime(object source, ElapsedEventArgs e)
        {
            DateTime currentTime = DateTime.Now;
            DateTime startTime = currentTime.AddMinutes(-30);
            DateTime endTime = currentTime;

            while (startTime < endTime)
            {
                WriteToFile("Service is recall at " + DateTime.Now);

                // SuccessLogin tablosundan verileri çek
                GetLogins(startTime, endTime, "SuccessLogin", true);
                foreach (string loginInfo in logins)
                {
                    if (!processedLogins.Contains(loginInfo))
                    {
                        WriteToFile("scc satırı:" + loginInfo);
                        SendMessageToRabbitMQ(loginInfo);
                        ListenToRabbitMQ();
                        processedLogins.Add(loginInfo);
                    }
                }

                // ErrorLogin tablosundan verileri çek
                GetLogins(startTime, endTime, "ErrorLogin", false);
                foreach (string loginInfo in logins)
                {
                    if (!processedLogins.Contains(loginInfo))
                    {
                        WriteToFile("err satırı:" + loginInfo);
                        SendMessageToRabbitMQ(loginInfo);
                        ListenToRabbitMQ();
                        processedLogins.Add(loginInfo);
                    }
                }

                startTime = startTime.AddMinutes(30);
                endTime = endTime.AddMinutes(30);
            }
        }


        private void GetLogins(DateTime startTime, DateTime endTime, string tableName, bool isSuccessLogin)
        {
            // SQL bağlantısı ve sorgu
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                string query = $"SELECT * FROM {tableName} WHERE LoginTarihi >= @StartTime AND LoginTarihi < @EndTime";
                SqlCommand command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@StartTime", startTime);
                command.Parameters.AddWithValue("@EndTime", endTime);
                SqlDataReader reader = command.ExecuteReader();

                // Kullanıcıları listeye ekleme
                List<string> loginList = new List<string>();
                while (reader.Read())
                {
                    string userName = reader["Ad"].ToString();
                    DateTime loginTime = Convert.ToDateTime(reader["LoginTarihi"]);
                    string loginInfo = "Kullanıcı: " + userName;
                    string loginTimeInfo = "Giriş Zamanı: " + loginTime.ToString();

                    if (isSuccessLogin)
                    {
                        loginInfo = "[SUCCESS] " + loginInfo;
                    }
                    else
                    {
                        loginInfo = "[ERROR] " + loginInfo;
                    }

                    string combinedMessage = loginInfo + " | " + loginTimeInfo;
                    loginList.Add(combinedMessage);
                }
                logins = loginList.ToArray();
                reader.Close();
                connection.Close();
            }
        }

        public void WriteToFile(string Message)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "/Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "/Logs/ServiceLog.txt";
            if (!File.Exists(filepath))
            {
                // Create a file to write to.   
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
        }

        private void SendMessageToRabbitMQ(string message)
        {
            var factory = new ConnectionFactory()
            {
                Uri = new Uri("amqps://eheogzhu:Em7mmWVpyg6mUGLKZv_xVntnBJcPfrcQ@chimpanzee.rmq.cloudamqp.com/eheogzhu")
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare("hello-queue", true, false, false);

                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(string.Empty, "hello-queue", null, body);
            }
        }

        private void ListenToRabbitMQ()
        {
            var factory = new ConnectionFactory()
            {
                Uri = new Uri("amqps://eheogzhu:Em7mmWVpyg6mUGLKZv_xVntnBJcPfrcQ@chimpanzee.rmq.cloudamqp.com/eheogzhu")
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare("hello-queue", true, false, false);

                var consumer = new EventingBasicConsumer(channel);
                channel.BasicConsume("hello-queue", true, consumer);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    WriteToLogsFile(message);

                    if (message.StartsWith("[SUCCESS]"))
                    {
                        string userName = GetUserNameFromMessage(message);
                        DateTime loginTime = GetLoginTimeFromMessage(message);
                        SaveToElasticsearch("SuccessLogin", userName, loginTime);
                    }
                    else if (message.StartsWith("[ERROR]"))
                    {
                        string userName = GetUserNameFromMessage(message);
                        DateTime loginTime = GetLoginTimeFromMessage(message);
                        SaveToElasticsearch("ErrorLogin", userName, loginTime);
                    }

                    channel.BasicAck(ea.DeliveryTag, true);
                };

            }
        }

        private void WriteToLogsFile(string message)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "/Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "/Logs/Received.txt";
            if (!File.Exists(filepath))
            {
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(message);
                }
            }
        }

        private static string GetUserNameFromMessage(string message)
        {
            // Mesajın kullanıcı adını içerdiği bölümü ayıklama
            string[] parts = message.Split(new string[] { "|" }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length > 0)
            {
                string userName = parts[0].Trim();
                // Sadece kullanıcı adını döndürme
                if (userName.StartsWith("Kullanıcı:"))
                {
                    userName = userName.Substring("Kullanıcı:".Length).Trim();
                }
                return userName;
            }
            return string.Empty;
        }

        private DateTime GetLoginTimeFromMessage(string message)
        {
            // Mesajın giriş zamanını içerdiği bölümü ayıklama
            string[] parts = message.Split(new string[] { "Giriş Zamanı: " }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length > 1)
            {
                DateTime loginTime;
                if (DateTime.TryParse(parts[1].Trim(), out loginTime))
                {
                    return loginTime;
                }
            }
            return DateTime.MinValue;
        }

        private static void SaveToElasticsearch(string indexName, string userName, DateTime loginTime)
        {
            var node = new Uri("http://10.12.149.11:9200");
            var settings = new ConnectionSettings(node)
                .DefaultIndex(indexName)
                .BasicAuthentication("elastic", "2021cj")
                .DisableDirectStreaming()
                .PrettyJson();

            var client = new ElasticClient(settings);

            // Örnek veri
            var loginData = new
            {
                Ad = userName,
                LoginTarihi = loginTime
            };

            // İndeksi kontrol etme
            var indexExistsResponse = client.Indices.Exists("successlogin");

            if (!indexExistsResponse.IsValid)
            {
                Console.WriteLine($"İndeks varlık kontrolü hatası: {indexExistsResponse.ServerError?.Error?.Reason}");
                return;
            }

            if (!indexExistsResponse.Exists)
            {
                // İndeks oluşturma
                var createIndexResponse = client.Indices.Create("errorlogin", c => c
                    .Map<LoginData>(m => m.AutoMap())
                );

                if (!createIndexResponse.IsValid)
                {
                    Console.WriteLine($"İndeks oluşturma hatası: {createIndexResponse.ServerError?.Error?.Reason}");
                    return;
                }
            }

            // Veri ekleme
            var indexResponse = client.IndexDocument(loginData);

            if (!indexResponse.IsValid)
            {
                Console.WriteLine($"Veri ekleme hatası: {indexResponse.ServerError?.Error?.Reason}");
                return;
            }

            Console.WriteLine("Veri başarıyla eklendi.");
        }

    }
    public class LoginData
    {
        public string Ad { get; set; }
        public DateTime LoginTarihi { get; set; }
    }
}