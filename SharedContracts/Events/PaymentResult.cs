using SharedContracts.Enums;

namespace SharedContracts.Events;

public sealed record PaymentResult(
    Guid MessageId,
    Guid OrderId,
    PaymentStatus Status,
    string? Reason,
    DateTime OccurredAtUtc
);
