using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MySqlConnector;
using PRNET_Unity;

namespace PRNET_Unity.Services
{
    public class HighScoresService : HighScores.HighScoresBase
    {
        private readonly ILogger<HighScoresService> _logger;
        private readonly string _connectionString;

        public HighScoresService(ILogger<HighScoresService> logger)
        {
            _logger = logger;
            _connectionString = ReadConnectionString();
        }

        private string ReadConnectionString()
        {
            try
            {
                using var sr = new StreamReader("connection.txt");
                var fileContent = sr.ReadToEnd();
                _logger.LogInformation("Reading file successful");
                return fileContent;
            }
            catch (IOException e)
            {
                _logger.LogError("The file could not be read: {message}", e.Message);
                return "";
            }
        }

        public override Task<SubmitReply> Submit(SubmitRequest request, ServerCallContext context)
        {
            _logger.LogInformation("Submit called with name: {name}, time: {time}", request.Name, request.SurvivedTime);
            if (request.Name.Length > 20)
            {
                _logger.LogWarning("Name too long (max 20)");
                return Task.FromResult(new SubmitReply { Success = false });
            }

            using var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var playerId = GetPlayerId(request.Name, connection);
            if (playerId == null)
            {
                _logger.LogWarning("Inserting new player failed");
                return Task.FromResult(new SubmitReply { Success = false });
            }

            InsertScore(playerId, request.SurvivedTime, connection);

            connection.Close();
            _logger.LogInformation("Submit successful");
            return Task.FromResult(new SubmitReply { Success = true });
        }

        public override async Task GetAll(GetAllRequest request, IServerStreamWriter<GetReply> responseStream,
            ServerCallContext context)
        {
            await using var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var reader = await SelectAllScores(connection);
            await StreamScores(responseStream, reader);

            await connection.CloseAsync();
            _logger.LogInformation("GetAll successful");
        }

        public override async Task GetMy(GetMyRequest request, IServerStreamWriter<GetReply> responseStream,
            ServerCallContext context)
        {
            _logger.LogInformation("GetMy called with name: {name}", request.Name);
            if (request.Name.Length > 20)
            {
                _logger.LogWarning("Name too long (max 20)");
                return;
            }

            await using var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var reader = await SelectPlayerScores(request, connection);

            await StreamScores(responseStream, reader);
            await connection.CloseAsync();
            _logger.LogInformation("GetMy successful");
        }

        private static void InsertScore(object playerId, Duration survivedTime, MySqlConnection connection)
        {
            using var command = new MySqlCommand(
                "INSERT INTO scores (player_id, survived_time, played_date) " +
                "VALUES (@PlayerId, @SurvivedTime, @PlayedDate);",
                connection);
            command.Parameters.AddWithValue("@PlayerId", (int)playerId);
            command.Parameters.AddWithValue("@SurvivedTime", survivedTime.ToTimeSpan());
            command.Parameters.AddWithValue("@PlayedDate", DateTime.Now);
            command.ExecuteNonQuery();
        }

        private static object? GetPlayerId(string nick, MySqlConnection connection)
        {
            var playerId = SelectPlayerId(nick, connection);
            if (playerId != null)
            {
                return playerId;
            }

            InsertPlayer(nick, connection);
            playerId = SelectPlayerId(nick, connection);

            return playerId;
        }

        private static object? SelectPlayerId(string nick, MySqlConnection connection)
        {
            using var command = new MySqlCommand(
                "SELECT player_id " +
                "FROM players " +
                "WHERE nick = @Nick;",
                connection);
            command.Parameters.AddWithValue("@Nick", nick);
            return command.ExecuteScalar();
        }

        private static void InsertPlayer(string nick, MySqlConnection connection)
        {
            using var insertNickCommand = new MySqlCommand(
                "INSERT INTO players (nick) " +
                "VALUES (@Nick);",
                connection);
            insertNickCommand.Parameters.AddWithValue("@Nick", nick);
            insertNickCommand.ExecuteNonQuery();
        }

        private static async Task<MySqlDataReader> SelectAllScores(MySqlConnection connection)
        {
            await using var selectCommand = new MySqlCommand(
                "SELECT players.nick, scores.survived_time, scores.played_date " +
                "FROM scores " +
                "INNER JOIN players " +
                "ON scores.player_id = players.player_id " +
                "ORDER BY scores.survived_time DESC;",
                connection);
            return await selectCommand.ExecuteReaderAsync();
        }

        private static async Task<MySqlDataReader> SelectPlayerScores(GetMyRequest request, MySqlConnection connection)
        {
            await using var selectCommand = new MySqlCommand(
                "SELECT players.nick, scores.survived_time, scores.played_date " +
                "FROM scores " +
                "INNER JOIN players " +
                "ON scores.player_id = players.player_id " +
                "WHERE players.nick = @Nick " +
                "ORDER BY scores.survived_time DESC;",
                connection);
            selectCommand.Parameters.AddWithValue("@Nick", request.Name);
            return await selectCommand.ExecuteReaderAsync();
        }

        private static async Task StreamScores(IAsyncStreamWriter<GetReply> responseStream, MySqlDataReader reader)
        {
            while (reader.Read())
            {
                var reply = new GetReply
                {
                    Name = reader.GetString(0),
                    SurvivedTime = Duration.FromTimeSpan(reader.GetTimeSpan(1)),
                    PlayedDate = Timestamp.FromDateTime(DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc))
                };
                await responseStream.WriteAsync(reply);
            }
        }
    }
}