using System;
using System.Collections.Generic;
using System.Data.Linq.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    public class acessoLinqTabelas
    {
        public int GetConvId()
        {
            DBDataContext dc = 
                new DBDataContext();
            var result = from c in dc.CONVENIADOs
                         where c.CONV_ID == 174244
                         select c.CONV_ID;


            int conv = 0;
            foreach (var c in result)
            {
                conv = c;
            }

            return conv;
        }

        public CONVENIADO getConveniado()
        {
            DBDataContext db = 
                new DBDataContext();
            var sql = from c in db.CONVENIADOs
                        where SqlMethods.Like(c.TITULAR,"%Sid%")
                        select c;

            return (CONVENIADO)sql;
        }

        public List<CONVENIADO> getConveniados (int conv_id)
        {
            DBDataContext db =
                new DBDataContext();
            int id;
            var lconv = db.CONVENIADOs.Where(c => c.CONV_ID == conv_id);

            List<CONVENIADO> conv = new List<CONVENIADO>();

            foreach (var item in lconv)
            {
                conv.Add(item);
            }

            return conv;
        }
    }
}
