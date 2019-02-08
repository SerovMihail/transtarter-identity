using Skoruba.IdentityServer4.Admin.EntityFramework.Entities.Identity;
using System;
using System.Collections.Generic;
using System.Text;

namespace KL.TS.MessageSender
{
    public class CreateUserCommand : ICommand
    {
        public CreateUserCommand(UserIdentity dtoObject)
        {
            DtoObject = dtoObject;
        }

        public UserIdentity DtoObject { get; set; }
    }
    
}
