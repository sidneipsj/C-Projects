using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using System.Data;

namespace DAL
{
    public class DALCredComandaStatus
    {
        public static bool StatusComanda(int cred_id, int comanda)
        {
            BD BD = new BD();
            bool retorno = false;
            SafeDataReader dr = BD.GetDataReader("SELECT STATUS FROM CRED_COMANDA_STATUS WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda, null);
            try
            {
                if (dr.Read())
                    retorno = dr.GetString(0) == "F";
            }
            finally
            {
                dr.Close();
            }
            return retorno;
        }

        public static bool UpdateStatus(int cred_id, int comanda, string status)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE CRED_COMANDA_STATUS SET STATUS = '" + status + "' WHERE CRED_ID = " + cred_id  + " AND COMANDA = " + comanda, null) == 1)
                return true;
            return false;
        }

        public static bool UpdateComID(int cred_id, int comanda, int com_id)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE CRED_COMANDA_STATUS SET COM_ID = " + com_id + " WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda, null) == 1)
                return true;
            return false;
        }

        public static string ConsultaComId(int cred_id, int comanda)
        {
            string sql = "SELECT COM_ID FROM CRED_COMANDA_STATUS WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda;
            BD BD = new BD();
            DataTable tabela = BD.GetDataTable(sql, null);

            if (tabela.Rows.Count == 0)
            {
                return String.Empty;
            }
            else
                return tabela.Rows[0][0].ToString();
        }

        public static bool ComandaValida(int comanda)
        {
            BD BD = new BD();
            bool retorno = false;
            SafeDataReader dr = BD.GetDataReader("SELECT COMANDA FROM CRED_COMANDA_STATUS WHERE COMANDA = " + comanda, null);
            try
            {
            if (dr.Read())
                retorno = dr.GetString(0) != null;
            }
            finally
            {
                dr.Close();
            }
            return retorno;
        }
    }
}
