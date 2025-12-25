using Microsoft.EntityFrameworkCore;
using OrdersService.Data;
using OrdersService.Data.Entities;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedContracts.Enums;
using SharedContracts.Events;
using System.Text;
using System.Text.Json;

namespace OrdersService.Messaging;

public sealed class PaymentResultConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _cfg;
    private readonly ILogger<PaymentResultConsumer> _logger;

    private IConnection? _connection;
    private IModel? _channel;

    public PaymentResultConsumer(IServiceScopeFactory scopeFactory, IConfiguration cfg, ILogger<PaymentResultConsumer> logger)
    {
        _scopeFactory = scopeFactory;
        _cfg = cfg;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // небольшой стартовый лаг
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
                _channel = _connection.CreateModel();
                _channel.BasicQos(0, 10, false);

                _channel.QueueDeclare("orders.payment.result", durable: true, exclusive: false, autoDelete: false, arguments: null);

                var consumer = new AsyncEventingBasicConsumer(_channel);
                consumer.Received += Handle;

                _channel.BasicConsume("orders.payment.result", autoAck: false, consumer: consumer);

                // держим сервис живым пока не отменят токен
                while (!stoppingToken.IsCancellationRequested && _connection.IsOpen)
                    await Task.Delay(1000, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RabbitMQ not ready for PaymentResultConsumer. Retry in 3s...");
                try { _channel?.Close(); } catch { }
                try { _connection?.Close(); } catch { }
                _channel = null;
                _connection = null;

                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }


    private async Task Handle(object sender, BasicDeliverEventArgs ea)
    {
        var deliveryTag = ea.DeliveryTag;
        var messageIdStr = ea.BasicProperties?.MessageId;

        if (!Guid.TryParse(messageIdStr, out var messageId))
        {
            _channel!.BasicReject(deliveryTag, false);
            return;
        }

        var json = Encoding.UTF8.GetString(ea.Body.ToArray());

        PaymentResult? evt;
        try { evt = JsonSerializer.Deserialize<PaymentResult>(json); }
        catch { evt = null; }

        if (evt is null)
        {
            _channel!.BasicReject(deliveryTag, false);
            return;
        }

        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();

            if (await db.Inbox.AnyAsync(x => x.Id == messageId))
            {
                _channel!.BasicAck(deliveryTag, false);
                return;
            }

            await using var tx = await db.Database.BeginTransactionAsync();

            db.Inbox.Add(new InboxMessage { Id = messageId, ReceivedAtUtc = DateTime.UtcNow });

            var order = await db.Orders.FirstOrDefaultAsync(x => x.Id == evt.OrderId);
            if (order is not null)
            {
                order.Status = evt.Status == PaymentStatus.Succeeded ? OrderStatus.Finished : OrderStatus.Cancelled;
            }

            await db.SaveChangesAsync();
            await tx.CommitAsync();

            _channel!.BasicAck(deliveryTag, false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PaymentResult handle failed");
            _channel!.BasicNack(deliveryTag, false, true);
        }
    }

    public override void Dispose()
    {
        try { _channel?.Close(); } catch { }
        try { _connection?.Close(); } catch { }
        base.Dispose();
    }
}
