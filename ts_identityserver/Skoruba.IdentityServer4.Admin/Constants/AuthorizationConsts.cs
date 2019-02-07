using System.Collections.Generic;

namespace Skoruba.IdentityServer4.Admin.Constants
{
    public class AuthorizationConsts
    {


       
        public const string AdministrationPolicy = "RequireAdministratorRole";
        public const string AdministrationRole = "SkorubaIdentityAdminAdministrator";

        public const string IdentityAdminCookieName = "IdentityServerAdmin";

#if DEBUG
        public const string IdentityAdminRedirectUri = "http://localhost:9000/signin-oidc";
#else
        public const string IdentityAdminRedirectUri = "http://192.168.39.103:9000/signin-oidc";
#endif

#if DEBUG
        public const string IdentityServerBaseUrl = "http://localhost:5000/";
#else
        public const string IdentityServerBaseUrl = "http://192.168.39.103:5000";
#endif

#if DEBUG
        public const string IdentityAdminBaseUrl = "http://localhost:9000/";
#else
        public const string IdentityAdminBaseUrl = "http://192.168.39.103:9000";
#endif

        public const string UserNameClaimType = "name";
        public const string SignInScheme = "Cookies";
        public const string OidcClientId = "skoruba_identity_admin";
        public const string OidcAuthenticationScheme = "oidc";
        public const string OidcResponseType = "id_token";
        public static List<string> Scopes = new List<string> { "openid", "profile", "email", "roles" };

        public const string ScopeOpenId = "openid";
        public const string ScopeProfile = "profile";
        public const string ScopeEmail = "email";
        public const string ScopeRoles = "roles";

        public const string AccountLoginPage = "Account/Login";
        public const string AccountAccessDeniedPage = "/Account/AccessDenied/";
    }
}