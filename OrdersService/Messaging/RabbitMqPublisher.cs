using RabbitMQ.Client;
using System.Text;

namespace OrdersService.Messaging;

public sealed class RabbitMqPublisher : IDisposable
{
    private readonly IConfiguration _cfg;
    private IConnection? _connection;
    private IModel? _channel;

    public RabbitMqPublisher(IConfiguration cfg) => _cfg = cfg;

    private void EnsureConnected()
    {
        if (_connection is { IsOpen: true } && _channel is { IsOpen: true })
            return;

        Dispose();

        var host = _cfg["RabbitMq:Host"] ?? "rabbitmq";
        var port = int.TryParse(_cfg["RabbitMq:Port"], out var p) ? p : 5672;
        var user = _cfg["RabbitMq:User"] ?? "guest";
        var pass = _cfg["RabbitMq:Pass"] ?? "guest";

        var factory = new ConnectionFactory
        {
            HostName = host,
            Port = port,
            UserName = user,
            Password = pass,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };

        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();

        _channel.QueueDeclare(
            queue: "orders.payment.request",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );
    }

    public void PublishToPaymentRequest(string json, Guid messageId)
    {
        EnsureConnected();

        var body = Encoding.UTF8.GetBytes(json);
        var props = _channel!.CreateBasicProperties();
        props.Persistent = true;
        props.MessageId = messageId.ToString();

        _channel.BasicPublish(
            exchange: "",
            routingKey: "orders.payment.request",
            basicProperties: props,
            body: body
        );
    }

    public void Dispose()
    {
        try { _channel?.Close(); } catch { }
        try { _connection?.Close(); } catch { }
        _channel = null;
        _connection = null;
    }
}
