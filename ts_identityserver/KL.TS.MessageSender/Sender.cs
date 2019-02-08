using System;
using System.Collections.Generic;
using System.Text;
using KL.TS.MQSettings;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace KL.TS.MessageSender
{

    public interface ICommand
    {
    }

    public class Sender : ISender
    {
        private readonly ILogger<Sender> _logger;

        public Sender(ILogger<Sender> logger)
        {
            _logger = logger;
        }

        public void Send<T>(T message) where T : ICommand
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = RabbitMqSettings.HostName
                };
                _logger.LogDebug($"Sending command [{typeof(T).Name}] to RabbitMQ...");
                using (var connection = factory.CreateConnection())
                {
                    #region Logging connection status
                    _logger.LogDebug($"Connection to {connection.Endpoint} established successfully!");
                    #endregion

                    using (var channel = connection.CreateModel())
                    {
                        channel.ExchangeDeclare(
                            exchange: RabbitMqSettings.ExchangeName,
                            type: ExchangeType.Fanout); ;

                        // Sending command type information in headers to deserialize when receive
                        var basicProperties = channel.CreateBasicProperties();
                        basicProperties.Headers = new Dictionary<string, object>
                        {
                            { RabbitMqSettings.MessageTypeHeaderKey, typeof(T).AssemblyQualifiedName }
                        };

                        var serializedMessage = JsonConvert.SerializeObject(message);
                        var body = Encoding.UTF8.GetBytes(serializedMessage);

                        _logger.LogDebug($"Sending message [{typeof(T).Name}]...");
                        channel.BasicPublish(
                            exchange: RabbitMqSettings.ExchangeName,
                            routingKey: RabbitMqSettings.RoutingKey,
                            basicProperties: basicProperties,
                            body: body);
                        _logger.LogInformation($"Message [{typeof(T).Name}] was sent successfully!");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Could not send message to RabbitMQ. Reason: {ex.ToString()}");
                throw;
            }
        }
    }
}