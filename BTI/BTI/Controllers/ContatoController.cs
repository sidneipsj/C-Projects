using BTI.DAO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace BTI.Controllers
{
    public class ContatoController : Controller
    {
        // GET: Contato
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult Visualiza()
        {
            return View();
        }

        public ActionResult Cadastra(CONTATO2 c)
        {
            ContadosDAO dao = new ContadosDAO();
            dao.Inserir(c);
            return View();
        }

        public ActionResult Form(CONTATO2 c)
        {
            return View();
        }
    }
}