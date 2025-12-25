namespace PaymentsService.Data.Entities;

public sealed class InboxMessage
{
    public Guid Id { get; set; }              // MessageId входящего события
    public DateTime ReceivedAtUtc { get; set; }
}
