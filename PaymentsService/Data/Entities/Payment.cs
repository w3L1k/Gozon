using SharedContracts.Enums;

namespace PaymentsService.Data.Entities;

public sealed class Payment
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public string UserId { get; set; } = null!;
    public decimal Amount { get; set; }
    public PaymentStatus Status { get; set; }
    public string? Reason { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
