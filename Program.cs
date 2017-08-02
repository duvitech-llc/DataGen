using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System.IO;
using System.Collections.Generic;

namespace ConsoleApp1
{
    class Program
    {
        private static EventHubClient eventHubClient;
        private const string EhConnectionString = "Endpoint=sb://edgespaceevents.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=b48xeYvn62Y4b8txH0k1Kxx9imjLZKLZFdTKSHZsOAM=";
        private const string EhEntityPath = "carhub";
        static Random random = new Random();
        static List<string> VinList = new List<string>();

        public static void Main(string[] args)
        {
            GetVINMasterList();
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            // Creates an EventHubsConnectionStringBuilder object from a the connection string, and sets the EntityPath.
            // Typically the connection string should have the Entity Path in it, but for the sake of this simple scenario
            // we are using the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString)
            {
                EntityPath = EhEntityPath
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            await SendMessagesToEventHub(150);

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();
        }

        // Creates an Event Hub client and sends 100 messages to the event hub.
        private static async Task SendMessagesToEventHub(int numMessagesToSend)
        {
            for (var i = 0; i < numMessagesToSend; i++)
            {
                try
                {
                    var city = GetLocation();
                    var info = new CarEvent()
                    {

                        vin = GetRandomVIN(),
                        city = city,
                        outsideTemperature = GetOutsideTemp(city),
                        engineTemperature = GetEngineTemp(city),
                        speed = GetSpeed(city),
                        fuel = random.Next(0, 40),
                        engineoil = GetOil(city),
                        tirepressure = GetTirePressure(city),
                        odometer = random.Next(0, 200000),
                        accelerator_pedal_position = random.Next(0, 100),
                        parking_brake_status = GetRandomBoolean(),
                        headlamp_status = GetRandomBoolean(),
                        brake_pedal_status = GetRandomBoolean(),
                        transmission_gear_position = GetGearPos(),
                        ignition_status = GetRandomBoolean(),
                        windshield_wiper_status = GetRandomBoolean(),
                        abs = GetRandomBoolean(),
                        timestamp = DateTime.UtcNow
                    };

                    var messageString = JsonConvert.SerializeObject(info);
                    Console.WriteLine($"Sending message: {messageString}");
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(messageString)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(5000);
            }

            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }

        static int GetSpeed(string city)
        {
            if (city.ToLower() == "bellevue")
            {
                return GetRandomWeightedNumber(100, 0, Convert.ToDouble(0.5));
            }
            return GetRandomWeightedNumber(100, 0, Convert.ToDouble(0.9));
        }

        static int GetOil(string city)
        {
            if (city.ToLower() == "seattle")
            {
                return GetRandomWeightedNumber(50, 0, Convert.ToDouble(0.3));
            }
            return GetRandomWeightedNumber(50, 0, Convert.ToDouble(1.2));
        }

        static int GetTirePressure(string city)
        {
            if (city.ToLower() == "seattle")
            {
                return GetRandomWeightedNumber(50, 0, Convert.ToDouble(0.5));
            }
            return GetRandomWeightedNumber(50, 0, Convert.ToDouble(1.7));
        }
        static int GetEngineTemp(string city)
        {
            if (city.ToLower() == "seattle")
            {
                return GetRandomWeightedNumber(500, 0, Convert.ToDouble(0.3));
            }
            return GetRandomWeightedNumber(500, 0, Convert.ToDouble(1.2));
        }
        static int GetOutsideTemp(string city)
        {
            if (city.ToLower() == "seattle")
            {
                return GetRandomWeightedNumber(100, 0, Convert.ToDouble(0.3));
            }
            return GetRandomWeightedNumber(100, 0, Convert.ToDouble(1.2));
        }


        private static int GetRandomWeightedNumber(int max, int min, double probabilityPower)
        {
            var randomizer = new Random();
            var randomDouble = randomizer.NextDouble();

            var result = Math.Floor(min + (max + 1 - min) * (Math.Pow(randomDouble, probabilityPower)));
            return (int)result;
        }

        static void GetVINMasterList()
        {
            var reader = new StreamReader(File.OpenRead(@"VINMasterList.csv"));
            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                var values = line.Split(';');

                VinList.Add(values[0]);

            }
        }
        static string GetRandomVIN()
        {
            int RandomIndex = random.Next(1, VinList.Count - 1);
            return VinList[RandomIndex];
        }

        static string GetLocation()
        {
            List<string> list = new List<string>() { "Seattle", "Redmond", "Bellevue", "Sammamish", "Bellevue", "Bellevue", "Seattle", "Seattle", "Seattle", "Redmond", "Bellevue", "Redmond" };
            int l = list.Count;
            Random r = new Random();
            int num = r.Next(l);
            return list[num];
        }
        static string GetGearPos()
        {
            List<string> list = new List<string>() { "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eight" };
            int l = list.Count;
            Random r = new Random();
            int num = r.Next(l);
            return list[num];
        }
        static bool GetRandomBoolean()
        {
            return new Random().Next(100) % 2 == 0;
        }
    }
}