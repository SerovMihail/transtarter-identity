using Microsoft.Extensions.Configuration;

namespace KL.TS.MQSettings
{
    /// <summary>
    /// Settings for interaction with RabbitMQ messages/queues
    /// </summary>
    public static class RabbitMqSettings
    {
        private static readonly IConfigurationRoot Configuration;

        static RabbitMqSettings()
        {
            Configuration = new ConfigurationBuilder()
                .AddJsonFile("rabbit-mq-settings.json", optional: true, reloadOnChange: true)
                .Build();
        }

        /// <summary>
        /// Name of the host we are connecting to
        /// </summary>
        public static string HostName => Configuration.GetSection("RabbitConnectionSettings")["HostName"];

        /// <summary>
        /// Exchange name where all commands are sending to and receiving from
        /// </summary>
        public static string ExchangeName => Configuration.GetSection("RabbitConnectionSettings")["ExchangeName"];

        /// <summary>
        /// The routing key is a message attribute. The exchange might look at this key when deciding how to route the message to queues (depending on exchange type).
        /// </summary>
        public static string RoutingKey => Configuration.GetSection("RabbitConnectionSettings")["RoutingKey"];

        public const string MessageTypeHeaderKey = "AssemblyQualifiedName";
    }
}
