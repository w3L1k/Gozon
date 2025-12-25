using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OrdersService.Data;
using OrdersService.Data.Entities;
using SharedContracts.Enums;
using SharedContracts.Events;
using System.Text.Json;

namespace OrdersService.Controllers;

[ApiController]
[Route("orders")]
public sealed class OrdersController : ControllerBase
{
    private readonly OrdersDbContext _db;

    public OrdersController(OrdersDbContext db) => _db = db;

    public sealed record CreateOrderRequest(string UserId, decimal Amount, string? Description);

    [HttpPost]
    public async Task<ActionResult<object>> Create([FromBody] CreateOrderRequest request, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(request.UserId))
            return BadRequest("userId is required");
        if (request.Amount <= 0)
            return BadRequest("amount must be > 0");

        var order = new Order
        {
            Id = Guid.NewGuid(),
            UserId = request.UserId,
            Amount = request.Amount,
            Description = request.Description,
            Status = OrderStatus.New,
            CreatedAtUtc = DateTime.UtcNow
        };

        var evt = new OrderCreated(
            MessageId: Guid.NewGuid(),
            OrderId: order.Id,
            UserId: order.UserId,
            Amount: order.Amount,
            OccurredAtUtc: DateTime.UtcNow
        );

        var outbox = new OutboxMessage
        {
            Id = evt.MessageId,
            Type = nameof(OrderCreated),
            PayloadJson = JsonSerializer.Serialize(evt),
            OccurredAtUtc = evt.OccurredAtUtc
        };

        // Transactional Outbox: Order + Outbox в одной транзакции
        await using var tx = await _db.Database.BeginTransactionAsync(ct);
        _db.Orders.Add(order);
        _db.OutboxMessages.Add(outbox);
        await _db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        return Ok(new { orderId = order.Id, status = order.Status.ToString() });
    }

    [HttpGet]
    public async Task<ActionResult<List<Order>>> List([FromQuery] string? userId, CancellationToken ct)
    {
        var q = _db.Orders.AsNoTracking().OrderByDescending(x => x.CreatedAtUtc);
        if (!string.IsNullOrWhiteSpace(userId))
            q = q.Where(x => x.UserId == userId).OrderByDescending(x => x.CreatedAtUtc);

        return await q.ToListAsync(ct);
    }

    [HttpGet("{id:guid}")]
    public async Task<ActionResult<Order>> Get([FromRoute] Guid id, CancellationToken ct)
    {
        var order = await _db.Orders.AsNoTracking().FirstOrDefaultAsync(x => x.Id == id, ct);
        return order is null ? NotFound() : Ok(order);
    }
}
