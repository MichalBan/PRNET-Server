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

            using var selectNickCommand = new MySqlCommand(
                "SELECT player_id " +
                "FROM players " +
                "WHERE nick = @Nick;",
                connection);
            selectNickCommand.Parameters.AddWithValue("@Nick", request.Name);
            var playerId = selectNickCommand.ExecuteScalar();

            if (playerId == null)
            {
                using var insertNickCommand = new MySqlCommand(
                    "INSERT INTO players (nick) " +
                    "VALUES (@Nick);",
                    connection);
                insertNickCommand.Parameters.AddWithValue("@Nick", request.Name);
                insertNickCommand.ExecuteNonQuery();

                playerId = selectNickCommand.ExecuteScalar();
            }

            if (playerId == null)
            {
                _logger.LogWarning("Inserting new player failed");
                return Task.FromResult(new SubmitReply { Success = false });
            }

            using var insertScoreCommand = new MySqlCommand(
                "INSERT INTO scores (player_id, survived_time, played_date) " +
                "VALUES (@PlayerId, @SurvivedTime, @PlayedDate);",
                connection);
            insertScoreCommand.Parameters.AddWithValue("@PlayerId", (int)playerId);
            insertScoreCommand.Parameters.AddWithValue("@SurvivedTime", request.SurvivedTime.ToTimeSpan());
            insertScoreCommand.Parameters.AddWithValue("@PlayedDate", DateTime.Now);
            insertScoreCommand.ExecuteNonQuery();

            connection.Close();
            _logger.LogInformation("Submit successful");
            return Task.FromResult(new SubmitReply { Success = true });
        }

        public override async Task GetAll(GetAllRequest request, IServerStreamWriter<GetReply> responseStream,
            ServerCallContext context)
        {
            await using var connection = new MySqlConnection(_connectionString);
            connection.Open();

            await using var selectCommand = new MySqlCommand(
                "SELECT players.nick, scores.survived_time, scores.played_date " +
                "FROM scores " +
                "INNER JOIN players " +
                "ON scores.player_id = players.player_id " +
                "ORDER BY scores.survived_time DESC;",
                connection);
            await using var reader = await selectCommand.ExecuteReaderAsync();

            await StreamGetReplies(responseStream, reader);
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

            await using var selectCommand = new MySqlCommand(
                "SELECT players.nick, scores.survived_time, scores.played_date " +
                "FROM scores " +
                "INNER JOIN players " +
                "ON scores.player_id = players.player_id " +
                "WHERE players.nick = @Nick " +
                "ORDER BY scores.survived_time DESC;",
                connection);
            selectCommand.Parameters.AddWithValue("@Nick", request.Name);
            await using var reader = await selectCommand.ExecuteReaderAsync();

            await StreamGetReplies(responseStream, reader);
            await connection.CloseAsync();
            _logger.LogInformation("GetMy successful");
        }

        private static async Task StreamGetReplies(IServerStreamWriter<GetReply> responseStream, MySqlDataReader reader)
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