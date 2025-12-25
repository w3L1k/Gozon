namespace PaymentsService.Data.Entities;

public sealed class OutboxMessage
{
    public Guid Id { get; set; }              // MessageId исходящего события
    public string Type { get; set; } = null!;
    public string PayloadJson { get; set; } = null!;
    public DateTime OccurredAtUtc { get; set; }
    public DateTime? SentAtUtc { get; set; }
}
