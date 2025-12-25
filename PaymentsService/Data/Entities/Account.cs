namespace PaymentsService.Data.Entities;

public sealed class Account
{
    public string UserId { get; set; } = null!;
    public decimal Balance { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}
