using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using PaymentsService.Data;
using PaymentsService.Data.Entities;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedContracts.Enums;
using SharedContracts.Events;
using System.Text;
using System.Text.Json;

namespace PaymentsService.Messaging;

public sealed class PaymentRequestConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _cfg;
    private readonly ILogger<PaymentRequestConsumer> _logger;

    private IConnection? _connection;
    private RabbitMQ.Client.IModel? _rmqChannel;


    public PaymentRequestConsumer(IServiceScopeFactory scopeFactory, IConfiguration cfg, ILogger<PaymentRequestConsumer> logger)
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
                _rmqChannel.BasicQos(0, 10, false);

                _rmqChannel.QueueDeclare("orders.payment.request", durable: true, exclusive: false, autoDelete: false, arguments: null);
                _rmqChannel.QueueDeclare("orders.payment.result", durable: true, exclusive: false, autoDelete: false, arguments: null);

                var consumer = new AsyncEventingBasicConsumer(_rmqChannel);
                consumer.Received += Handle;

                _rmqChannel.BasicConsume("orders.payment.request", autoAck: false, consumer: consumer);

                while (!stoppingToken.IsCancellationRequested && _connection.IsOpen)
                    await Task.Delay(1000, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RabbitMQ not ready for PaymentRequestConsumer. Retry in 3s...");
                try { _rmqChannel?.Close(); } catch { }
                try { _connection?.Close(); } catch { }
                _rmqChannel = null;
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
            _logger.LogWarning("Message without valid MessageId, reject.");
            _rmqChannel!.BasicReject(deliveryTag, requeue: false);
            return;
        }

        var json = Encoding.UTF8.GetString(ea.Body.ToArray());

        OrderCreated? evt;
        try
        {
            evt = JsonSerializer.Deserialize<OrderCreated>(json);
        }
        catch
        {
            _rmqChannel!.BasicReject(deliveryTag, requeue: false);
            return;
        }

        if (evt is null)
        {
            _rmqChannel!.BasicReject(deliveryTag, requeue: false);
            return;
        }

        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();

            // Inbox (idempotency)
            var already = await db.Inbox.AnyAsync(x => x.Id == messageId);
            if (already)
            {
                _rmqChannel!.BasicAck(deliveryTag, multiple: false);
                return;
            }

            await using var tx = await db.Database.BeginTransactionAsync();

            db.Inbox.Add(new InboxMessage { Id = messageId, ReceivedAtUtc = DateTime.UtcNow });

            // Аккаунт (для ДЗ можно авто-создавать)
            var acc = await db.Accounts.FirstOrDefaultAsync(x => x.UserId == evt.UserId);
            if (acc is null)
            {
                acc = new Account { UserId = evt.UserId, Balance = 1000m, UpdatedAtUtc = DateTime.UtcNow };
                db.Accounts.Add(acc);
            }

            var payment = new Payment
            {
                Id = Guid.NewGuid(),
                OrderId = evt.OrderId,
                UserId = evt.UserId,
                Amount = evt.Amount,
                CreatedAtUtc = DateTime.UtcNow
            };

            PaymentResult result;

            if (acc.Balance >= evt.Amount)
            {
                acc.Balance -= evt.Amount;
                acc.UpdatedAtUtc = DateTime.UtcNow;

                payment.Status = PaymentStatus.Succeeded;
                result = new PaymentResult(Guid.NewGuid(), evt.OrderId, PaymentStatus.Succeeded, null, DateTime.UtcNow);
            }
            else
            {
                payment.Status = PaymentStatus.Failed;
                payment.Reason = "Insufficient funds";
                result = new PaymentResult(Guid.NewGuid(), evt.OrderId, PaymentStatus.Failed, "Insufficient funds", DateTime.UtcNow);
            }

            db.Payments.Add(payment);

            db.Outbox.Add(new OutboxMessage
            {
                Id = result.MessageId,
                Type = nameof(PaymentResult),
                PayloadJson = JsonSerializer.Serialize(result),
                OccurredAtUtc = result.OccurredAtUtc
            });

            await db.SaveChangesAsync();
            await tx.CommitAsync();

            _rmqChannel!.BasicAck(deliveryTag, false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Handle failed, requeue");
            _rmqChannel!.BasicNack(deliveryTag, false, requeue: true);
        }
    }

    public override void Dispose()
    {
        try { _rmqChannel?.Close(); } catch { }
        try { _connection?.Close(); } catch { }
        base.Dispose();
    }
}
