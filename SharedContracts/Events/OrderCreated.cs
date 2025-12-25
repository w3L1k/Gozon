namespace SharedContracts.Events;

public sealed record OrderCreated(
    Guid MessageId,
    Guid OrderId,
    string UserId,
    decimal Amount,
    DateTime OccurredAtUtc
);
