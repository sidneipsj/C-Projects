using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(Recrutamento.Startup))]
namespace Recrutamento
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
