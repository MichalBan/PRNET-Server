using Grpc.Core;
using PRNET_Unity;

namespace PRNET_Unity.Services
{
    public class GreeterService : Greeter.GreeterBase
    {
        private readonly ILogger<GreeterService> _logger;

        public GreeterService(ILogger<GreeterService> logger)
        {
            _logger = logger;
        }

        public override Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context)
        {
            var reply = "Hello " + request.Name;
            Console.WriteLine(reply);
            return Task.FromResult(new HelloReply
            {
                Message = reply, Value = 33
            });
        }
    }
}