using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using SQLHelperv2;

namespace DAL
{
    public class DALFormasEmpLib
    {
        public static DataSet FormasPgLiberadas(int emp_id)
        {
            string sql = "SELECT A.FORMA_ID, B.DESCRICAO FROM FORMAS_EMP_LIB AS A, FORMASPAGTO AS B WHERE "
            + "A.EMP_ID = " + emp_id + " AND A.FORMA_ID = B.FORMA_ID AND B.LIBERADO = 'S' ORDER BY A.FORMA_ID";

            BD BD = new BD();
            return BD.GetDataSet(sql, null);
        }
    }
}
