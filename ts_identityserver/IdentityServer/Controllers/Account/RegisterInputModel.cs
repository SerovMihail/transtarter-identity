namespace IdentityServer.Controllers.Account
{
    public class RegisterInputModel
    {        
        public string Login { get; set; }
        public string Phone { get; set; }
        public string Email { get; set; }
        public string Password { get; set; }        
        public string OrganizationVariant { get; set; }
        public string OrganizationType { get; set; }
        public string OrganizationName { get; set; }
    }
}