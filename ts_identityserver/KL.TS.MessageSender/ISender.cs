namespace KL.TS.MessageSender
{
    public interface ISender
    {
        /// <summary>
        /// Sending messages into RabbitMQ that implement ICommand interface
        /// </summary>
        void Send<T>(T message) where T : ICommand;
    }
}
