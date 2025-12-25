using Microsoft.EntityFrameworkCore;
using OrdersService.Data.Entities;

namespace OrdersService.Data;

public sealed class OrdersDbContext : DbContext
{
    public OrdersDbContext(DbContextOptions<OrdersDbContext> options) : base(options) { }

    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>()
            .HasKey(x => x.Id);

        modelBuilder.Entity<Order>()
            .Property(x => x.UserId)
            .IsRequired();

        modelBuilder.Entity<Order>()
            .Property(x => x.Amount)
            .HasPrecision(18, 2);

        modelBuilder.Entity<OutboxMessage>()
            .HasKey(x => x.Id);

        modelBuilder.Entity<OutboxMessage>()
            .Property(x => x.Type)
            .IsRequired();

        modelBuilder.Entity<OutboxMessage>()
            .Property(x => x.PayloadJson)
            .IsRequired();
    }
}
