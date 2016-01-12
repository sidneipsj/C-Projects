using BTI.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BTI.DAO
{
    public class ContadosDAO
    {
        public ContadosDAO()
        {
            var dbContext = new AgendaDBDataContext();
        }

        public void Inserir(CONTATO2 contato)
        {

            AgendaDBDataContext ctx = new AgendaDBDataContext();
            CONTATO2 c = new CONTATO2();
            c = contato;
            ctx.GetTable<CONTATO2>().InsertOnSubmit(c);
            ctx.SubmitChanges();
        }
    }
}