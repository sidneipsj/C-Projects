using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using System.Data;

namespace DAL
{
    public class DALRecargaLogon
    {
        //Todos os Metodos Alterado para SqlServer
        public static DataTable SolicitarToken(string data)
        {
            string sql = "SELECT * FROM RECARGA_LOGON_SITE WHERE DATA = '" + data + "' AND TOKEN_VALIDO = 'S'";

            BD BD = new BD();
            return BD.GetDataTable(sql,null);
        }

        public static DataTable SolicitarTokenCred(string data, int cred_id, string tipo)
        {
            string sql = "SELECT * FROM RECARGA_LOGON_CRED WHERE DATA = '" + data + "' AND TOKEN_VALIDO = 'S' AND CRED_ID = " + cred_id + " AND TIPO_TOKEN = '" + tipo + "'";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static void InserirToken(string numToken, string data, string validade)
        {
            try
            {
                BD BD = new BD();

                MontadorSql mont = new MontadorSql("recarga_logon_site", MontadorType.Insert);
                mont.AddField("data", data);
                mont.AddField("token", numToken);
                mont.AddField("atu_operadora", 'S');
                mont.AddField("token_valido", 'S');
                mont.AddField("validade", validade);
                BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao inserir número do token: " + ex.Message);
            }
        }

        public static void InserirTokenCred(int cred_id, string numToken, string data, string validade, string tipo)
        {
            try
            {
                BD BD = new BD();

                MontadorSql mont = new MontadorSql("recarga_logon_cred", MontadorType.Insert);
                mont.AddField("cred_id", cred_id);
                mont.AddField("data", data);
                mont.AddField("token", numToken);
                mont.AddField("atu_operadora", 'S');
                mont.AddField("token_valido", 'S');
                mont.AddField("validade", validade);
                mont.AddField("tipo_token", tipo);
                BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao inserir número do token: " + ex.Message);
            }
        }

        public static bool UpdateTokenCred(string dtValidade, int cred_id)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE RECARGA_LOGON_CRED SET TOKEN_VALIDO = 'N' WHERE DATA = '" + dtValidade + "' AND CRED_ID = " + cred_id, null) == 1)
                return true;
            return false;
        }

        public static bool UpdateOperadorasCred(string dtValidade, int cred_id)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE RECARGA_LOGON_CRED SET ATU_OPERADORA = 'S' WHERE DATA = '" + dtValidade + "' AND TOKEN_VALIDO = 'S' AND CRED_ID = " + cred_id, null) == 1)
                return true;
            return false;
        }

        public static bool UpdateToken(string dtValidade)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE RECARGA_LOGON_SITE SET TOKEN_VALIDO = 'N' WHERE DATA = '" + dtValidade + "'", null) == 1)
                return true;
            return false;
        }
 
        public static bool UpdateOperadoras(string dtValidade)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE RECARGA_LOGON_SITE SET ATU_OPERADORA = 'S' WHERE DATA = '" + dtValidade + "' AND TOKEN_VALIDO = 'S'", null) == 1)
                return true;
            return false;
        }

        public static bool UtilizaRecarga(int empres_id)
        {
            BD BD = new BD();
            bool retorno = false;
            SafeDataReader dr = BD.GetDataReader("SELECT UTILIZA_RECARGA FROM EMPRESAS WHERE EMPRES_ID = " + empres_id, null);

            try
            {
                if (dr.Read())
                {
                    retorno = dr.GetString(0) == "S";
                }
            }
            finally
            {
                dr.Close();
            }

            return retorno;
        }

        
    }
}
