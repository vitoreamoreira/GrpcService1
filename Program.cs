using GrpcService1;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);

// Adicione serviços ao contêiner.
builder.Services.AddGrpc();
builder.Services.AddSingleton<RabbitMQ.Client.IConnectionFactory, RabbitMQ.Client.ConnectionFactory>(sp => new RabbitMQ.Client.ConnectionFactory { HostName = "localhost" });

var app = builder.Build();

// Configure o pipeline de solicitação HTTP.
app.MapGrpcService<MobilityServiceImplementation>();
app.MapGet("/", () => "Comunicação gRPC");

app.Run();