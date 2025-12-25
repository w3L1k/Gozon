using Microsoft.EntityFrameworkCore;
using PaymentsService.Data.Entities;

namespace PaymentsService.Data;

public sealed class PaymentsDbContext : DbContext
{
    public PaymentsDbContext(DbContextOptions<PaymentsDbContext> options) : base(options) { }

    public DbSet<Account> Accounts => Set<Account>();
    public DbSet<Payment> Payments => Set<Payment>();
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Account>().HasKey(x => x.UserId);
        modelBuilder.Entity<Account>().Property(x => x.Balance).HasPrecision(18, 2);

        modelBuilder.Entity<Payment>().HasKey(x => x.Id);
        modelBuilder.Entity<Payment>().Property(x => x.Amount).HasPrecision(18, 2);

        modelBuilder.Entity<InboxMessage>().HasKey(x => x.Id);

        modelBuilder.Entity<OutboxMessage>().HasKey(x => x.Id);
        modelBuilder.Entity<OutboxMessage>().Property(x => x.Type).IsRequired();
        modelBuilder.Entity<OutboxMessage>().Property(x => x.PayloadJson).IsRequired();
    }
}
