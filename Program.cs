using System;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using static System.Net.Mime.MediaTypeNames;

//database config
SqlConnection sqlConnection;
var connectionString = "Data Source=192.168.150.15;Initial Catalog=TestNew;User ID=soltani;Password=@Soltani123#";
sqlConnection = new SqlConnection(connectionString);

// rabbitmq config
ConnectionFactory factory = new();
factory.Uri = new Uri("amqp://guest:guest@localhost:5672");
factory.ClientProvidedName = "RabbitReceiver";

IConnection cnn = factory.CreateConnection();
IModel channel = cnn.CreateModel();

var exchangeName = "TcpExchange";
var routingKey = "tcp-routing-key";
var queueName = "TcpQueue";

channel.ExchangeDeclare(exchangeName, ExchangeType.Direct);
channel.QueueDeclare(queueName, true, false, false, null);
channel.QueueBind(queueName, exchangeName, routingKey, null);
channel.BasicQos(0, 1, false);

var consumer = new EventingBasicConsumer(channel);
consumer.Received += (sender, args) =>
{
    Task.Delay(TimeSpan.FromSeconds(5)).Wait();
    var body = args.Body.ToArray();
    string message = Encoding.UTF8.GetString(body);
    Console.WriteLine($"message received:{message}");
    var meesageId = message[message.Length - 2];

    //var timestamp = args.BasicProperties.Timestamp;
    //var queueDate = DateTimeOffset.FromUnixTimeSeconds(timestamp.UnixTime).UtcDateTime;

    var timestamp = args.BasicProperties.Timestamp.UnixTime;
    var milliseconds = (int)args.BasicProperties.Headers["Milliseconds"];
    var queueDate = DateTimeOffset.FromUnixTimeSeconds(timestamp).AddMilliseconds(milliseconds).UtcDateTime;


    //send data to database
    try
    {
        // Open the connection
        sqlConnection.Open();
        Console.WriteLine("connection established successfully");
        // Create a SqlCommand object with the INSERT statement

        // Retrieve data from the table
        var rabbitMessage = RetrieveData(sqlConnection, meesageId.ToString());
        var procDate = DateTime.UtcNow;
        // Insert data into the table
        InsertData(sqlConnection, rabbitMessage, queueDate , procDate);
        
    }
    catch (Exception ex)
    {
        Console.WriteLine("Error: " + ex.Message);
    }
    finally
    {
        // can handle message if database fail if evrything is ok then basicack will run
        //channel.BasicAck(args.DeliveryTag, false);
        sqlConnection.Close();
    }
};

static void InsertData(SqlConnection sqlConnection, string message,DateTime queueDate,DateTime procDate)
{
    try
    {
        var insertQuery = "INSERT INTO [TestNew].[dbo].[Storage] (Message, RecDate , ProcDate , EndPocDate) VALUES (@message, @RecDate ,@ProcDate ,@EndPocDate )";
        SqlCommand command = new SqlCommand(insertQuery, sqlConnection);
        command.Parameters.AddWithValue("@message", message);
        command.Parameters.AddWithValue("@RecDate", queueDate);
        command.Parameters.AddWithValue("@ProcDate", procDate);
        command.Parameters.AddWithValue("@EndPocDate", DateTime.UtcNow);
        command.ExecuteNonQuery();
    }
    catch (Exception ex)
    {
        Console.WriteLine("Error: " + ex.Message);
    }
    Console.WriteLine($"Message inserted into the database:{message}");
}


static string RetrieveData(SqlConnection sqlConnection, string messageId)
{
    var finalMessage = "";

    try
    {
        var commandQuery = "SELECT * FROM [dbo].[MiddleTable] where status=@Id";
        SqlCommand command = new SqlCommand(commandQuery, sqlConnection);
        command.Parameters.AddWithValue("@Id", messageId);
        SqlDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            finalMessage = reader.GetString(reader.GetOrdinal("Message"));
        }

        reader.Close();
    }
    catch (Exception ex)
    {
        Console.WriteLine("Error: " + ex.Message);
    }

    return finalMessage;
}

var consumerTag = channel.BasicConsume(queueName,true,consumer);
Console.ReadLine();
channel.BasicCancel(consumerTag);
channel.Close();
cnn.Close();