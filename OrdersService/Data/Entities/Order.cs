using SharedContracts.Enums;

namespace OrdersService.Data.Entities;

public sealed class Order
{
    public Guid Id { get; set; }
    public string UserId { get; set; } = null!;
    public decimal Amount { get; set; }
    public string? Description { get; set; }
    public OrderStatus Status { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
