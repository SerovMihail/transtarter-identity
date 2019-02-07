namespace Skoruba.IdentityServer4.Constants
{
    public class IdentityServerConsts
    {
        public const string AdministrationRole = "SkorubaIdentityAdminAdministrator";
        
#if DEBUG
        public const string IdentityAdminBaseUrl = "http://localhost:5000";
#else
        public const string IdentityAdminBaseUrl = "http://192.168.39.103:9000";     
#endif
            
        public const string OidcClientId = "skoruba_identity_admin";
        public const string OidcClientName = "Skoruba Identity Admin";
        public const string AdminUserName = "admin";
        public const string AdminPassword = "Pa$$word123";
    }
}