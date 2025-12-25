using Microsoft.EntityFrameworkCore;
using OrdersService.Data;
using OrdersService.Messaging;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddDbContext<OrdersDbContext>(opt =>
{
    var cs = builder.Configuration.GetConnectionString("Db");
    opt.UseSqlite(cs);
});

builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddHostedService<OutboxWorker>();
builder.Services.AddHostedService<PaymentResultConsumer>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    db.Database.EnsureCreated();
}

app.UseSwagger();
app.UseSwaggerUI();

app.MapControllers();
app.Run();
