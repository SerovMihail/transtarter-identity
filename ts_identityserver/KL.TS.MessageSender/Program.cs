using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KL.TS.MessageSender
{
    class Program
    {
        private static async Task Main(string[] args)
        {

            var host = new HostBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddScoped<ISender, Sender>();
                })
                .UseConsoleLifetime()
                .Build();

            using (host)
            {
                #region TestSend

                //var sender = host.Services.GetService<ISender>();
                //sender.Send(new object
                //{
                //    Id = Guid.NewGuid(),
                //    Name = "Иван",
                //    SecondName = "Иванович",
                //    LastName = "Иванов",
                //    Opened = "y",
                //    AssignedById = Guid.NewGuid().ToString(),
                //    Opportunity = "0",
                //    CompanyTitle = "OOO Company",
                //    StatusId = "NEW",
                //    CurrencyId = "RUB",
                //    Title = "Mr",
                //    Email = "alex@test.com",
                //    PhoneNumber = "+79991112233"
                //});

                #endregion

                // Start the host
                await host.StartAsync();

                // Wait for the host to shutdown
                await host.WaitForShutdownAsync();
            }
        }
    }
}
