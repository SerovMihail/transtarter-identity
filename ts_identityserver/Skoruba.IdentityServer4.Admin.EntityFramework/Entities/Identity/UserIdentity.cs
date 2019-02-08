using System;
using Microsoft.AspNetCore.Identity;

namespace Skoruba.IdentityServer4.Admin.EntityFramework.Entities.Identity
{
	public class UserIdentity : IdentityUser<int>
	{        
        public string OrganizationVariant { get; set; }
        public string OrganizationType { get; set; }
        public string OrganizationName { get; set; }

    }
}