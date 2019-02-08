using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using IdentityServer.Controllers.Account;
using KL.TS.MessageSender;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Skoruba.IdentityServer4.Admin.EntityFramework.Entities.Identity;

namespace IdentityServer.Controllers.api
{
    [Route("api/[controller]")]
    [ApiController]
    public class AccountController : ControllerBase
    {
        private readonly UserManager<UserIdentity> _userManager;
        private readonly SignInManager<UserIdentity> _signInManager;
        private readonly Sender _sender;

        public AccountController(
            UserManager<UserIdentity> userManager,
            SignInManager<UserIdentity> signInManager,
            Sender sender
            )
        {
            _userManager = userManager;
            _signInManager = signInManager;
            _sender = sender;
        }

        [HttpPost("register")]
        [AllowAnonymous]
        //[ValidateAntiForgeryToken]
        public async Task<IActionResult> Register([FromBody] RegisterInputModel model)
        {

            var user = new UserIdentity
            {
                UserName = model.Login,
                PhoneNumber = model.Phone,
                OrganizationName = model.OrganizationName,
                OrganizationType = model.OrganizationType,
                OrganizationVariant = model.OrganizationVariant
            };

            var result = await _userManager.CreateAsync(user, model.Password);
            if (result.Succeeded)
            {
                _sender.Send(new CreateUserCommand(user));
                // For more information on how to enable account confirmation and password reset please visit http://go.microsoft.com/fwlink/?LinkID=532713
                // Send an email with this link
                //var code = await _userManager.GenerateEmailConfirmationTokenAsync(user);
                //var callbackUrl = Url.Action("ConfirmEmail", "Account", new { userId = user.Id, code = code }, protocol: HttpContext.Request.Scheme);
                //await _emailSender.SendEmailAsync(model.Email, "Confirm your account",
                //    $"Please confirm your account by clicking this link: <a href='{callbackUrl}'>link</a>");
                await _signInManager.SignInAsync(user, isPersistent: false);
                //_logger.LogInformation(3, "User created a new account with password.");
                //return RedirectToLocal(returnUrl);

                return Ok();
            }
            //AddErrors(result);


            // If we got this far, something failed, redisplay form
            return BadRequest(result.Errors.First());
        }
    }
}