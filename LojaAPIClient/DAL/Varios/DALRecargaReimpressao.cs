using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using SQLHelperv2;

namespace DAL
{
    public class DALRecargaReimpressao
    {
        //Todos os Metodos Alterados para SqlServer
        public static DataTable VerificaReimpressao(int pos_serial_number)
        {
            string sql = "SELECT * FROM RECARGA_REIMPRESSAO WHERE POS_SERIAL_NUMBER = " + pos_serial_number;

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static void InserirReimpressao(int cred_id, int pos_serial_number, string comprovante)
        {
            try
            {
                BD BD = new BD();

                MontadorSql mont = new MontadorSql("recarga_reimpressao", MontadorType.Insert);
                mont.AddField("cred_id", cred_id);
                mont.AddField("pos_serial_number", pos_serial_number);
                mont.AddField("comprovante", comprovante);
                BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao inserir reimpressao: " + ex.Message);
            }

        }

        public static bool UpReimpressao(string comprovante, int cred_id, int pos_serial_number)
        {
            try
            {
                BD BD = new BD();
                if (BD.ExecuteNoQuery("UPDATE RECARGA_REIMPRESSAO SET COMPROVANTE = '" + comprovante + "'"
                    + "WHERE CRED_ID = " + cred_id + " AND POS_SERIAL_NUMBER = " + pos_serial_number, null) == 1)
                    return true;
                return false;
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao atualizar reimpressao: " + ex.Message);
            }
        }

        public static DataTable Comprovante(int pos_serial_number)
        {
            BD BD = new BD();
            return BD.GetDataTable("SELECT COMPROVANTE FROM RECARGA_REIMPRESSAO WHERE POS_SERIAL_NUMBER = " + pos_serial_number, null);
        }
    }
}
