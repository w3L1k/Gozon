namespace OrdersService.Data.Entities;

public sealed class InboxMessage
{
    public Guid Id { get; set; }              // MessageId входящего PaymentResult
    public DateTime ReceivedAtUtc { get; set; }
}
