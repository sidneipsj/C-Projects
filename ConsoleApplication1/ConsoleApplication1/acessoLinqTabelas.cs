using System;
using System.Collections.Generic;
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
    }
}
