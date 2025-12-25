using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using PaymentsService.Data;
using RabbitMQ.Client;
using System.Text;

namespace PaymentsService.Messaging;

public sealed class PaymentsOutboxWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _cfg;
    private readonly ILogger<PaymentsOutboxWorker> _logger;

    private IConnection? _connection;
    private RabbitMQ.Client.IModel? _rmqChannel;


    public PaymentsOutboxWorker(IServiceScopeFactory scopeFactory, IConfiguration cfg, ILogger<PaymentsOutboxWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _cfg = cfg;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = _cfg["RabbitMq:Host"] ?? "rabbitmq",
                    Port = int.TryParse(_cfg["RabbitMq:Port"], out var p) ? p : 5672,
                    UserName = _cfg["RabbitMq:User"] ?? "guest",
                    Password = _cfg["RabbitMq:Pass"] ?? "guest",
                    AutomaticRecoveryEnabled = true,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
                };

                _connection = factory.CreateConnection();
                _rmqChannel = _connection.CreateModel();

                _rmqChannel.QueueDeclare("orders.payment.result", durable: true, exclusive: false, autoDelete: false, arguments: null);

                while (!stoppingToken.IsCancellationRequested && _connection.IsOpen)
                {
                    try
                    {
                        await PublishBatch(stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Outbox publish failed, will retry...");
                        await Task.Delay(2000, stoppingToken);
                    }

                    await Task.Delay(1000, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RabbitMQ not ready for PaymentsOutboxWorker. Retry in 3s...");
                try { _rmqChannel?.Close(); } catch { }
                try { _connection?.Close(); } catch { }
                _rmqChannel = null;
                _connection = null;

                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }

    private async Task PublishBatch(CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();

        var batch = await db.Outbox
            .Where(x => x.SentAtUtc == null)
            .OrderBy(x => x.OccurredAtUtc)
            .Take(20)
            .ToListAsync(ct);

        if (batch.Count == 0) return;

        foreach (var msg in batch)
        {
            var body = Encoding.UTF8.GetBytes(msg.PayloadJson);
            var props = _rmqChannel!.CreateBasicProperties();
            props.Persistent = true;
            props.MessageId = msg.Id.ToString();

            _rmqChannel.BasicPublish("", "orders.payment.result", props, body);
            msg.SentAtUtc = DateTime.UtcNow;
        }

        await db.SaveChangesAsync(ct);
    }


    public override void Dispose()
    {
        try { _rmqChannel?.Close(); } catch { }
        try { _connection?.Close(); } catch { }
        base.Dispose();
    }
}
