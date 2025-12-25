using Microsoft.EntityFrameworkCore;
using OrdersService.Data;

namespace OrdersService.Messaging;

public sealed class OutboxWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly RabbitMqPublisher _publisher;
    private readonly ILogger<OutboxWorker> _logger;

    public OutboxWorker(IServiceScopeFactory scopeFactory, RabbitMqPublisher publisher, ILogger<OutboxWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _publisher = publisher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // небольшой стартовый лаг, чтобы RabbitMQ успел подняться
        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var scope = _scopeFactory.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();

                // берём пачку неотправленных сообщений
                var batch = await db.OutboxMessages
                    .Where(x => x.SentAtUtc == null)
                    .OrderBy(x => x.OccurredAtUtc)
                    .Take(20)
                    .ToListAsync(stoppingToken);

                if (batch.Count == 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    continue;
                }

                foreach (var msg in batch)
                {
                    try
                    {
                        _publisher.PublishToPaymentRequest(msg.PayloadJson, msg.Id);
                        msg.SentAtUtc = DateTime.UtcNow;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Publish failed, will retry later. MessageId={MessageId}", msg.Id);
                        // НЕ ставим SentAtUtc — сообщение останется и уйдёт позже
                    }

                }

                await db.SaveChangesAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "OutboxWorker error");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }
}
