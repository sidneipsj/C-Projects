using LojaRazor.DAO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace LojaRazor.Controllers
{
    public class MenuController : Controller
    {
        //
        // GET: /Menu/

        public ActionResult Index()
        {
            DepartamentosDAO dao = new DepartamentosDAO();
            ViewBag.Departamentos = dao.Lista();
            return View();
        }

    }
}
