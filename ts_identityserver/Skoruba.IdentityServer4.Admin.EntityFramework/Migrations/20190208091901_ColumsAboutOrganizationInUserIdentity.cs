using Microsoft.EntityFrameworkCore.Migrations;

namespace Skoruba.IdentityServer4.Admin.EntityFramework.Migrations
{
    public partial class ColumsAboutOrganizationInUserIdentity : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "OrganizationName",
                table: "Users",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "OrganizationType",
                table: "Users",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "OrganizationVariant",
                table: "Users",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "OrganizationName",
                table: "Users");

            migrationBuilder.DropColumn(
                name: "OrganizationType",
                table: "Users");

            migrationBuilder.DropColumn(
                name: "OrganizationVariant",
                table: "Users");
        }
    }
}
