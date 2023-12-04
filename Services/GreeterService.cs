using Grpc.Core;
using MySqlConnector;
using PRNET_Unity;

namespace PRNET_Unity.Services
{
    public class GreeterService : Greeter.GreeterBase
    {
        private readonly ILogger<GreeterService> _logger;
        private readonly string _connectionString;

        public GreeterService(ILogger<GreeterService> logger)
        {
            _logger = logger;
            _connectionString = "";

            try
            {
                using var sr = new StreamReader("connection.txt");
                _connectionString = sr.ReadToEnd();
                _logger.LogInformation("Reading file successful");
            }
            catch (IOException e)
            {
                _logger.LogError("The file could not be read: {message}", e.Message);
            }
        }

        public override Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context)
        {
            _logger.LogInformation("SayHello called with name: {name}", request.Name);
            if (request.Name.Length > 20)
            {
                _logger.LogWarning("Name too long (max 20)");
                return Task.FromResult(new HelloReply
                {
                    Message = "Name too long (max 20)", Value = 0
                });
            }

            using var connection = new MySqlConnection(_connectionString);
            connection.Open();

            using var insertCommand = new MySqlCommand("insert into players(nick) values (@Nick);", connection);
            insertCommand.Parameters.AddWithValue("@Nick", request.Name);
            insertCommand.ExecuteNonQuery();

            using var selectCommand = new MySqlCommand("select nick from players;", connection);
            using var reader = selectCommand.ExecuteReader();

            uint number = 0;
            var reply = "";
            while (reader.Read())
            {
                reply += reader.GetString(0) + "\n";
                ++number;
            }

            connection.Close();
            _logger.LogInformation("SayHello successful");
            return Task.FromResult(new HelloReply
            {
                Message = reply, Value = number
            });
        }
    }
}